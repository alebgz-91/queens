import datetime
import logging
import sqlite3
import pandas as pd
from astropy.io.votable.converters import table_column_to_votable_datatype


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


def execute_sql(
        conn_path: str,
        sql: str
):
    """
    Executes a SQL statement with optional parameters.

    Args:
        conn_path: For SQLite this is simply the path of the .db file
        sql: query to execute as a string. Must be prepared with placeholders (?) is sql_parameters is not None

    Returns:
        None

    """
    # get cursor
    with sqlite3.connect(conn_path) as conn:
        cursor = conn.cursor()

        cursor.executescript(sql)

    return None


def ingest_frame(
        df: pd.DataFrame,
        to_table: str,
        table_name: str,
        data_collection: str,
        url: str,
        conn_path: str,
        ingest_ts: str
):
    """
    Ingests a pandas dataframe and saves an ingest log entry.

    Args:
        df: pandas dataframe to insert
        to_table: name of the destination data table
        table_name: logical table name (e.g., "dukes_1_1")
        data_collection: name of data collection
        url: source URL of the data
        conn_path: path to SQLite DB
        ingest_ts: timestamp string to save into the ingest log table

    Returns:
        ingest_id: ID of the ingest log row
    """
    # validate to_table and data_collection
    if data_collection not in to_table:
        logging.warning(f"Writing to table {to_table} but data collection is {data_collection}")

    with sqlite3.connect(conn_path) as conn:
        cursor = conn.cursor()

        # Insert a log entry first
        cursor.execute(
            """
            INSERT INTO _ingest_log 
                (ingest_ts
                , data_collection
                , table_name
                , url
                , success)
            VALUES (?, ?, ?, ?, 0)
            """,
            (ingest_ts, data_collection, table_name, url)
        )

        # get lastrowid
        ingest_id = cursor.lastrowid

        # tag dataframe with ingest_id
        df["ingest_id"] = ingest_id

        try:
            df.to_sql(to_table, conn, if_exists="append", index=False)

            # Update success flag in log
            cursor.execute(
                """
                UPDATE _ingest_log
                SET success = 1
                WHERE ingest_id = ?
                """,
                (ingest_id,)
            )

        except Exception as e:
            raise e

    return ingest_id


def raw_to_prod(
        conn_path: str,
        table_prefix: str,
        cutoff: str
):
    staging_query = f"""

        CREATE TABLE {table_prefix}_prod AS
        WITH current_ts AS
        (
            SELECT 
                table_name
                ,MAX(ingest_ts) as ingest_ts
            FROM 
                _ingest_log
            WHERE
                ingest_ts <= ?
                AND data_collection = ?
                AND success = 1
            GROUP BY
                table_name
        )

        SELECT
            log.ingest_ts
            ,data.*
        FROM 
            {table_prefix}_raw AS data
        JOIN
            current_ts as ts
        ON
            log.ingest_ts = ts.ingest_ts
            AND log.table_name = ts.table_name
        JOIN 
            _ingest_log as log
        ON 
            data.ingest_id = log.ingest_id;

    """

    with sqlite3.connect(conn_path) as conn:

        cursor = conn.cursor()

        # remove previously live data
        cursor.execute(f"DROP TABLE IF EXISTS {table_prefix}_prod;")

        # write staging table
        cursor.execute(staging_query,
                       (cutoff,table_prefix))

        return None


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


def read_sql_as_frame(
        conn_path: str,
        query: str,
        query_params: tuple = None
):
    """
    A wapper of pd.read_sql_query(), reading custom SQL queries from
    a database located in conn_str. Supports parametrised queries with positional
    placeholders (?).

    Args:
        conn_path: connection string (path of db file
        query: the SQL query as a string
        query_params: tuple of query parameters.

    Returns:
        a pandas dataframe

    """
    with sqlite3.connect(conn_path) as conn:

        df = pd.read_sql_query(query, conn,
                               params=query_params)

    return df
