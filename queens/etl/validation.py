import pandas as pd
import logging
from typing import Union, Tuple
from pathlib import Path
from .. import settings as s
from ..core import web_scraping as ws
from ..core import utils as u
from ..core import read_write as rw


def generate_config(
        data_collection: str,
        table_name: str,
        chapter_key: str,
        templates: dict,
        urls: dict,
        etl_config: dict
)-> dict:
    """
    Resolves table-specific processing parameters and packages them as a dictionary.

    Args:
        data_collection: the collection the table belongs to
        table_name: table number
        chapter_key: chapter of the table in the form "chapter_x"
        templates: dictionary of templates by data_collection. Should be set in config/.
        urls: dictionary of URLs for individual chapter by data_collections. Should be set in config/.
        etl_config: detailed runtime parameters for the ETL. Should be set in config/.

    Returns:

    """
    # get static config dict
    logging.debug(f"Fetch config for data collection; {data_collection}")
    config = etl_config[data_collection][chapter_key][table_name]

    # determine table url
    logging.debug(f"Fetch urls for chapter: {chapter_key}")
    chapter_page_url = urls[data_collection][chapter_key]

    logging.debug("Scrape table url from web")
    table_urls = ws.scrape_urls(data_collection=data_collection,
                                url=chapter_page_url)
    if table_name not in table_urls:
        raise KeyError(f"Cannot find table URL for {data_collection} {table_name} in {chapter_page_url}")

    url = table_urls[table_name]["url"]
    descr = table_urls[table_name]["description"]

    # determine the template file path
    logging.debug("Fetch template path")
    template_file_name = templates[data_collection][chapter_key]
    template_file_path = s.TEMPLATES_DIR / template_file_name

    # add url, template_path and data_collection to f_args
    config["f_args"].update({
        "url": url,
        "template_file_path": template_file_path,
        "data_collection": data_collection
    })

    config["table_description"] = descr

    return config


def validate_schema(
        data_collection: str,
        table_name: str,
        df: pd.DataFrame,
        schema_dict: dict
)-> pd.DataFrame:
    """
    Enforces schema constraints to a given table.
    Args:
        data_collection: the parent data collection
        table_name: ID of the table to validate
        df: the pandas dataframe of the table
        schema_dict: dictionary storing schema information

    Returns:
        the validated dataframe
    """

    # check for duplicates
    logging.debug("Starting schema validation")

    # reset the index to inspect all columns
    index_cols = list(df.index.names)
    df.reset_index(drop=False, inplace=True)

    # remove working columns that are not meant to provide a unique index
    for col in ["row", "label"]:
        if col not in index_cols:
            raise ValueError(f"Required column missing in table {table_name}: {col}")
        index_cols.remove(col)

    # re-set index with meaningful columns
    df.set_index(index_cols, inplace=True)
    if df.index.duplicated().sum() > 0:
        raise ValueError(f"There are duplicates in table {table_name} of data collection {data_collection}. Check mapping table.")

    df.reset_index(drop=False, inplace=True)
    schema = schema_dict[data_collection]

    # Add constant index columns                               data_collection=data_collection)
    df["table_name"] = table_name

    logging.debug("Check data types for each columns")
    for col_name in df:

        logging.debug(f"Validating column {col_name}.")
        if col_name not in schema:
            logging.error(f"Unexpected column not in schema for table {table_name}: {col_name}")
            raise ValueError(f"Unexpected column not in schema for table {data_collection} {table_name}: {col_name}")

        exp_dtype = schema[col_name]["type"]
        exp_null = schema[col_name]["nullable"]

        if s.DTYPES[exp_dtype] is float:
            df[col_name] = pd.to_numeric(df[col_name],
                                         errors="coerce")

            # check that the conversion has gone well. Some nulls are expected
            # due to suppression symbols being present in the data
            # but there should be non-null values
            non_null_count = df[col_name].notnull().sum()
            if non_null_count == 0:
                logging.error(f"Conversion to float failed: too many NULLs in column {col_name}")
                raise ValueError(f"Values cannot be parse to numeric data. Check transformator for table {data_collection} {table_name}.")

        elif s.DTYPES[exp_dtype] is int:
            df[col_name] = pd.to_numeric(df[col_name],
                                             errors="coerce",
                                             downcast="integer")
        elif s.DTYPES[exp_dtype] is str:
            # preserve nulls so that they can be raised in the next check
            df[col_name] = df[col_name].astype("string")

        # can implement further data types in the future

        # check nulls
        n_rows = len(df)
        n_non_nulls = df[col_name].notnull().sum()
        if (n_rows > n_non_nulls) and (not exp_null):
            logging.error(f"Nullability constraint violation: column {col_name} in {table_name} is not nullable but has nulls.")
            raise ValueError(f"Column {col_name} of table {table_name} is not nullable but NULLs were found.")

    logging.debug("Validation terminated with success.")
    return df



def normalize_filters(filters: dict)-> Tuple[dict, list]:
    """
    Split into a base AND dict (nested operators) and a list of OR-groups.
    - Base part: dict of fields (each field is nested op dict)
    - OR part: list of dicts (each dict same structure as base)
    """
    filters = filters or {}
    or_groups = []

    # extract and normalise $or
    if "$or" in filters:
        raw_or = filters.pop("$or")

        if isinstance(raw_or, dict):
            # tolerate dict by converting to list of single-field dicts
            or_groups = [{k: v} for k, v in raw_or.items()]

        elif isinstance(raw_or, list):
            or_groups = raw_or

        else:
            raise ValueError("`$or` must be a list of filter objects or a dict.")

    base = u.to_nested(filters)
    or_groups = [u.to_nested(g) for g in or_groups]

    return base, or_groups



def validate_query_filters(
        data_collection: str,
        table_name: str,
        group: dict,
        conn_path: Union[str, Path],
        schema_dict: dict
)-> dict:
    """
     - ensures columns exist in schema_dict[data_collection]
    - ensures columns are queryable for this table_name (metadata)
    - validates ops per type
    - casts values to the column dtype
    Returns same shape with casted values.

    Args:
        data_collection: name of parent data collection
        table_name: number of table within data collection
        group: dictionary of filters. grouped by logical operator and in nested format
        conn_path: the path of the DB file
        schema_dict: schema dictionary of the database

    Returns:
        a dictionary of typed filters

    """
    # check that filters exist as columns in the data_collection prod table
    invalid_cols = {c for c in group if c not in schema_dict[data_collection]}
    if invalid_cols:
        logging.error(f"Column(s) {invalid_cols} do not exist in {data_collection}_prod.")
        raise KeyError(f"No such column(s) in {data_collection}_prod table: {[invalid_cols]}")

    # get columns metadata
    sql_types, cast_map = rw.load_column_info(conn_path, data_collection, table_name)

    invalid_cols = [c for c in group if c not in sql_types]
    if invalid_cols:
        raise NameError(f"Column(s) {invalid_cols} cannot be queried in {table_name}.")

        # validate the operators for each condition
    for col, ops in group.items():
        sql_t = sql_types[col]
        allowed = s.VALID_OPS.get(sql_t)
        # adding this to facilitate debug
        if not allowed:
            logging.error(f"No operator policy for SQL type {sql_t} (column {col})")
            raise ValueError(f"No operator policy for SQL type '{sql_t}' (column '{col}').")

        caster = cast_map[col]

        for op, val in list(ops.items()):
            if op not in allowed:
                logging.error(f"Operator {op} not allowed on column {col} of type {sql_types[col]}")
                raise ValueError(f"Operator '{op}' not allowed for {sql_types[col]} column '{col}'.")

            try:
                if op in {"eq", "neq", "lt", "lte", "gt", "gte"}:
                    # numeric or string eq/neq; cast numerics
                    ops[op] = caster(val)
                elif op == "like":
                    if not isinstance(val, str):
                        raise TypeError("LIKE expects a string pattern")
                    ops[op] = val
            except (TypeError, ValueError) as e:
                raise TypeError(f"Cannot cast value for '{col}' ({op}): {e}")

    return group
