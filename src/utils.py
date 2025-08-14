import json
import inspect
import os.path
import re

def parse_json(path: str):
    """
    Opens a .json file and loads into a dictionary

    Args:
        path: file path

    Returns:
        a dictionary of the parsed content

    """
    try:
        with open(path, "r") as f:
            return json.load(f)

    except FileNotFoundError:
        raise FileNotFoundError(f"JSON file not found: {path}")

    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON format in {path}: {e}")


def table_to_chapter(table_number, data_collection):
    """
    Utility that returns a chapter key for a given table number. Can handle either raw table names
    (i.e. "1.2.3") or table keys (i.e. "dukes_1_2_3").
    Args:
        table_number: the full table number as a string
        data_collection: name of release (i.e. "dukes")

    Returns: chapter key as a string of the form 'chapter_{chapter_no}'

    """

    first_char = table_number[0]

    if first_char.isnumeric():
        return f"chapter_{first_char}"
    else:
        if first_char in ["I", "J"]:
            return "chapter_1"
        else:
            # further logic to come
            raise NotImplemented("Work in process.")


def check_path(file_path: str):
    """
    checks if a file path exists and throws an exception if not.
    Args:
        file_path: the path to check

    Returns:
        True if the path exists

    """
    abs_path = os.path.abspath(file_path)

    if not os.path.exists(abs_path):
        raise FileNotFoundError(f"The specified path does not exist: {file_path}")


def check_inputs (data_collection: str,
                  etl_config: dict,
                  table_name: str = None):
    """
    Function that checks if a table is found in the ETL_CONFIG file
    Args:
        data_collection: Name of the data_collection
        table_name: Name of the table. If None, function checks existence of data_collection only
        etl_config: dictionary of ETL configuration

    Returns:
        True if data_collection and table_name are found
    Raises:
        NameError if either data_collection or table_key are not found

    """
    if data_collection not in etl_config:
        raise NameError(f"{data_collection} data not found")
    elif table_name is not None:
        found = 0
        for chapter_key in etl_config[data_collection]:
            if table_name in etl_config[data_collection][chapter_key]:
                found += 1
        if found == 0:
            raise NameError(f"Table {table_name} value not found in {data_collection}")

    return True


def call_func(
        func: callable,
        args_dict: dict):
    """
    Call a function on a set of parameters, excluding unnecessary ones.

    Args:
        func: function callable object
        args_dict: dictionary of arguments

    Returns:
        the result of func on the subset of the arguments passed

    """

    # get the signature
    sig = inspect.signature(func)
    accepted_args = sig.parameters.keys()

    # filter the dict to only include valid arguments
    filtered_args = {k: v for k, v in args_dict.items() if k in accepted_args}

    return func(**filtered_args)


def remove_note_tags(text):
    """
    Remove notes indications of the type [note x] or [Note x]
    Args:
        text: text to parse

    Returns:

    """

    # Notes are always surrounded by square brackets
    if not isinstance(text, str):
        return text

    pattern = r"\[\s*note\s+\d+\s*\]"  # matches [note x] with optional whitespace
    return re.sub(pattern, "", text, flags=re.IGNORECASE).strip()


def generate_create_table_sql(
        table_prefix: str,
        table_env: str,
        schema_dict: dict) -> str:
    """
    Function that generates a SQL query string for creating a table
    with prescribed schema.

    Args:
        table_prefix: the table identifier, normally the data_collection
        table_env: either raw or prod
        schema_dict: a dictionary for table schema of data collections. The first column is assumed to be the index column.

    Returns:
        the generated query as a string

    """
    schema_dict = schema_dict[table_prefix]

    destination_table = f"{table_prefix}_{table_env}"

    columns = []

    for col, props in schema_dict.items():
        sql_type = props["type"]
        nullable = "" if props.get("nullable", True) else "NOT NULL"
        columns.append(f"[{col}] {sql_type} {nullable}".strip())

    cols_sql = ",\n    ".join(columns)

    create_table = f"""
        CREATE TABLE IF NOT EXISTS [{destination_table}] (\n    
        {cols_sql}\n);
        """

    return create_table


def generate_create_log_sql():
    sql = """
        CREATE TABLE IF NOT EXISTS [_ingest_log] (\n
            ingest_id INTEGER PRIMARY KEY AUTOINCREMENT,
            ingest_ts DATETIME NOT NULL,
            data_collection TEXT NOT NULL,
            table_name TEXT NOT NULL,
            url TEXT,
            success INTEGER
            );
    """
    return sql


def generate_create_metadata_sql():
    query = """
        CREATE TABLE IF NOT EXISTS [_metadata] (\n
            [data_collection]     TEXT    NOT NULL,
            [table_name]          TEXT    NOT NULL,
            [column_name]         TEXT    NOT NULL,
            [n_non_nulls]         INTEGER NOT NULL,
        [n_unique]                INTEGER NOT NULL,
            [dtype]               TEXT    NOT NULL,
            PRIMARY KEY (data_collection, table_name, column_name)
        );            
    """
    return query


def generate_select_sql(
        from_table: str,
        cols: list = None,
        where: str = None,
        distinct: bool = False
):
    """
    Generate a basic SELECT statement with custom WHERE clause. Options available
    to select distinct values and specify columns to include in the result set.
    Args:
        from_table: the source table
        cols: list of columns to read. Default is "*" (all columns)
        where: explicit WHERE clause. Supports logical operators in SQL style.
        distinct: whether to return distinct values only. Default is False.

    Returns:
        the SQL query as a string

    """
    select_block = ", ".join(cols) if cols is not None else "*\n"
    where_clause = f"WHERE \n\t{where}" if where is not None else ""
    distinct_clause = "DISTINCT" if distinct else ""

    query = f"""
        SELECT {distinct_clause} 
            {select_block}
        FROM
            {from_table}
        {where_clause};
    """

    return query

def to_nested(d: dict):
    """
    Converts a dictionary in flat format {k : v,...} to a nested disct of the form {k : {"eq" : v}}
    Also maintains existing nesting (i.e. {k : {op: v}} is left untouched.

    Args:
        d: input dictionary

    Returns:
        the transformed dictionary

    """

    nested = {}
    for k, v in d.items():
        nested[k] = v if isinstance(v, dict) else {"eq": v}

    return nested


def build_sql_for_group(
        group: dict,
        operator_map: dict,
        schema_dict: dict
):
    """
    group: {col: {op: value, ...}, ...}
    Combine operatos for a field with AND; combine fields with AND.
    """

    clauses = []
    params = []

    for col, ops in group.items():
        for op, val in ops.items():
            clause = f"{col} {operator_map[op]}"
            if schema_dict[col]["type"] == "TEXT":
                clause = clause.replace("?", "? COLLATE NOCASE")
            clauses.append(clause)
            params.append(val)

    return " AND ".join(clauses) if clauses else "1=1", params


def build_where_clause(
        base_group: dict,
        or_groups: list[dict],
        operator_map: dict,
        schema_dict: dict
):
    """
    (base AND) AND ( OR-group )  ; OR-group is OR of group SQLs
    """
    base_sql, base_params = build_sql_for_group(base_group,
                                                operator_map,
                                                schema_dict)

    # early return if no ORs are provided
    if not or_groups:
        return base_sql, base_params

    or_sqls = []
    or_params = []
    for g in or_groups:
        s, p = build_sql_for_group(g,
                                   operator_map,
                                   schema_dict)
        or_sqls.append(f"({s})")
        or_params.extend(p)

    where_sql = f"({base_sql}) AND (" + " OR ".join(or_sqls) + ")"
    params = base_params + or_params
    return where_sql, tuple(params)
