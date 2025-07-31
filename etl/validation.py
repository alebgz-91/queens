import pandas as pd
from config.settings import DTYPES
import src.web_scraping as ws


def generate_config(data_collection: str,
                    table_name: str,
                    chapter_key: str,
                    templates: dict,
                    urls: dict,
                    etl_config: dict):
    """
    Generates a dictionary with all necessary information for the ETL to run properly on a table.
    This requires environment variables to be set correctly in the config/ directory.
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
    config = etl_config[data_collection][chapter_key][table_name]

    # determine table url
    chapter_page_url = urls[data_collection][chapter_key]
    url = ws.get_dukes_urls(url=chapter_page_url)[table_name]["url"]

    # determine the template file path
    template_file_path = templates[data_collection][chapter_key]

    # add url, template_path and data_collection to f_args
    config["f_args"].update({
        "url": url,
        "template_file_path": template_file_path,
        "data_collection": data_collection
    })

    return config


def validate_schema(
        data_collection: str,
        table_name: str,
        df: pd.DataFrame,
        schema_dict: dict
):

    # check for duplicates
    if df.index.duplicated().sum() > 0:
        raise ValueError(f"There are duplicates in table {table_name} of data collection {data_collection}. Check mapping table.")

    schema = schema_dict[data_collection]

    # Add constant index columns
    df.reset_index(drop=False, inplace=True)


    # add id cols as a column                               data_collection=data_collection)
    df["table_name"] = table_name

    # check data types and cast
    for col_name in df:

        if col_name not in schema:
            raise ValueError(f"Unexpected column not in schema for table {data_collection} {table_name}: {col_name}")

        exp_dtype = schema[col_name]["type"]
        exp_null = schema[col_name]["nullable"]

        if DTYPES[exp_dtype] is float:
            df[col_name] = pd.to_numeric(df[col_name],
                                         errors="coerce")

            # check that the conversion has gone well. Some nulls are expected
            # due to suppression symbols being present in the data
            # but there should be non-null values
            non_null_count = df[col_name].notnull().sum()
            if non_null_count == 0:
                raise ValueError(f"Values cannot be parse to numeric data. Check transformator for table {data_collection} {table_name}.")

        elif DTYPES[exp_dtype] is int:
            df[col_name] = pd.to_numeric(df[col_name],
                                             errors="coerce",
                                             downcast="integer")
        elif DTYPES[exp_dtype] is str:
            df[col_name] = df[col_name].astype(str)

        else:
            # no action for now, will likely need to handle further types
            pass

        # check nulls
        n_rows = len(df)
        n_non_nulls = df[col_name].notnull().sum()
        if (n_rows > n_non_nulls) and (not exp_null):
            raise ValueError(f"Column {col_name} is not nullable but NULLs were found.")

    return df
