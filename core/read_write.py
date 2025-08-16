import sqlite3
import pandas as pd
from pyodbc import IntegrityError

import config.settings as s
import os
import logging
import datetime
import core.utils as u


def read_and_wrangle_wb(
        file_path: str,
        has_multi_headers: bool = False,
        sheet_name: str = None,
        skip_sheets: list = None
):

    """
    Read Excel workbooks removing unnecessary header rows.
    By default, the function parses the whole workbook, excluding sheets with a single non-empty column.
    Thr behaviour can be modified to read a specific sheet, in which case the function returns a dataframe
    instead of a dictionary.

    Args:
        file_path: `io` argument in read_excel
        has_multi_headers: whether the table has a two-level column headings that starts on column B. If column B has a single header, it will be ignored automatically.
        sheet_name: name of sheet to read
        skip_sheets: list of sheets to ignore when parsing the whole workbook.

    Returns:
        a dictionary of `pd.Dataframe is sheet_name = None or a pd.DataFrame otherwise`
    """

    # read the workbook
    wb = pd.ExcelFile(file_path)

    # get the list of sheets
    sheets = wb.sheet_names

    if sheet_name is not None:
        sheets = [sheet_name]
    elif skip_sheets:
        sheets = set(sheets) - set(skip_sheets)

    # parse each worksheet removing headers
    wb_as_dict = {}

    for sheet in sheets:

        # first row will always include title
        h = 0
        df = wb.parse(sheet, header=h)

        # skip sheet if believed to be non-data
        # i.e. if 1 column only
        if len(df.columns) == 1:
            continue

        # increase header until the actual table heading is reached
        while "Unnamed" in df.columns[1]:
            h += 1
            df = wb.parse(sheet, header=h)

        # remove another row if table has multiindex columns
        if has_multi_headers:
            df = wb.parse(sheet, header=h+1)

        # add to dictionary
        wb_as_dict.update({sheet: df})

    # close the Excel workbook
    wb.close()

    # return df if specific sheet is required
    if sheet_name is not None:
        return wb_as_dict[sheet_name]
    else:
        return wb_as_dict


#TODO: abstract the function below so that it works with other data collections
# This also requires changing the signature to include a data_collection argument


def export_table(
        data_collection: str,
        file_type: str,
        table_name: str,
        output_path: str,
        output_ts: str = None
):
    """
    Utility that can export a specific table_name within a data_collection to
    flat files. Supports csv, parquet and Excel (xlsx)
    Args:
        data_collection: the data collection name
        output_path:
        output_ts: destination folder of the files. Default is data/outputs/exported/
        file_type: either 'csv', 'parquet' or 'xlsx'
        table_name: the name of the table to export (i.e. 1.2)

    Returns:
        None

    """
    try:

        if output_ts is None:
            output_ts = str(datetime.date.today())

        # get absolute path and normalise path format
        output_path = os.path.abspath(output_path)

        # read data from sql
        query = f"""
            SELECT *
            FROM 
                {data_collection}_prod
            WHERE 
                table_name = ?
        """
        df = read_sql_as_frame(conn_path=s.DB_PATH,
                               query=query,
                               query_params=(table_name,))

        file_name = (data_collection
                     + "_"
                     + table_name.replace(".","_")
                     + "_"
                     + output_ts
                     + f".{file_type}")
        output_path = os.path.join(output_path, file_name)

        logging.info(f"Saving {data_collection} {table_name} to {file_type}")
        if file_type == "csv":
            df.to_csv(output_path, index=False)
        elif file_type == "parquet":
            df.to_parquet(output_path, index=False)
        elif file_type == "xlsx":
            df.to_excel(output_path, sheet_name=table_name, index=False)
        else:
            raise TypeError(f"Exporting unsupported to file type {file_type}.")

    except Exception as e:
        logging.error(f"Export failed for {data_collection} {table_name}: \n{e}")

    logging.info(f"Successfully created {output_path + file_name}")


def export_all(
        data_collection: str,
        file_type: str,
        output_path: str,
        bulk_export: bool
):
    """
    Export all table sin a given data_collection to flat files. Supports csv, parquet and Excel file types.
    Tables can either be saved as individual files (bulk = False, the default) or
    as a single file (bulk = True). For bulk export to Excel, the individual tables are
    written to separate sheets of the same workbook.
    Args:
        data_collection: Name of the data collection
        file_type: Either 'csv', 'parquet' of 'xlsx'
        output_path: where to export the outputs
        bulk_export: if True, exports all tables into a single file. Default is False

    Returns:

    """

    # get absolute path and current timestamp
    output_path = os.path.abspath(output_path)
    output_ts = str(datetime.date.today())

    try:
        if not bulk_export:
            chapter_list = s.ETL_CONFIG[data_collection].keys()

            for chapter in chapter_list:
                logging.info(f"Saving tables from {data_collection}, {chapter}")
                table_list = s.ETL_CONFIG[data_collection][chapter].keys()

                for table_key in table_list:
                    export_table(
                        data_collection=data_collection,
                        output_path=output_path,
                        output_ts=output_ts,
                        file_type=file_type,
                             table_name=table_key)

                logging.info(f"Finished exporting [chapter]")

            logging.info(f"All tables from {data_collection} exported with success to {file_type}")

        else:
            # export all tables in the data collection to a single file
            logging.info(f"Reading the {data_collection} production table.")
            df = read_sql_as_frame(conn_path=s.DB_PATH,
                                   query=f"SELECT * FROM {data_collection}_prod")
            file_name = f"{data_collection}_{output_ts}.{file_type}"
            output_path = os.path.join(output_path, file_name)

            logging.info(f"Saving {data_collection} to {file_type}.")

            if file_type == "csv":
                df.to_csv(output_path, index=False)
            elif file_type == "parquet":
                df.to_parquet(output_path, index=False)
            elif file_type == "xlsx":
                # save one table per sheet in a single workbook
                logging.info(f"Creating workbook {output_path + file_name}.")
                with pd.ExcelWriter(output_path) as wr:

                    for table_name in df["table_name"].unique():
                        df[df.table_name == table_name].to_excel(wr,
                                                                 sheet_name=table_name,
                                                                 index=False)


            else:
                raise TypeError(f"Exporting unsupported to file type {file_type}.")

            logging.info(f"Successfully saved {output_path + file_name}")

    except Exception as e:
        logging.error(f"Export failed for {table_name}: \n{e}")


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


def read_sql_as_frame(
        conn_path: str,
        query: str,
        query_params: tuple = None
):
    """
    A wrapper of pd.read_sql_query(), reading custom SQL queries from
    a database located in conn_str. Supports parametrised queries with positional
    placeholders (?).

    Args:
        conn_path: connection string (path of db file)
        query: the SQL query as a string
        query_params: tuple of query parameters.

    Returns:
        a pandas dataframe

    """
    with sqlite3.connect(conn_path) as conn:

        df = pd.read_sql_query(query, conn,
                               params=query_params)

    return df

def table_exists(table_name: str, conn_path: str) -> bool:
    """
    Check if a table exists in the SQLite database.

    Args:
        table_name (str): Name of the table to check.
        conn_path (str): Path to the SQLite database.

    Returns:
        bool: True if table exists, False otherwise.
    """
    try:
        with sqlite3.connect(conn_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT name 
                FROM sqlite_master 
                WHERE type = 'table' 
                    AND name = ?;
            """, (table_name,))

            result = cursor.fetchone()
            return result is not None
    except Exception as e:
        logging.error(f"Error checking table existence: {e}")
        return False


def insert_metadata(
        data_collection: str,
        table_name: str,
        conn_path: str,
        schema_dict: dict
):
    """
    Reads slice of _prod table and generates metadata for the slice
    Args:
        data_collection: name of data collection (shadows the table)
        table_name: slice to select
        conn_path: DB file path
        schema_dict: dictionary that defines the schema of the parent table

    Returns:
        a pandas dataframe with metadata for table_name

    """
    from_table = data_collection + "_prod"
    where = "table_name = ?"

    query = u.generate_select_sql(from_table=from_table, where=where)
    df = read_sql_as_frame(
        conn_path=conn_path,
        query=query,
        query_params=(table_name,)
    )

    # early return for empty dataframe
    if df.empty:
        raise IntegrityError(f"No data found for {data_collection}, {table_name}. \nAn error has occurred when staging the data.")

    df = df.dropna(axis=1, how="all")
    df.drop(columns=["ingest_id", "ingest_ts"], inplace=True, errors="ignore")

    df["data_collection"] = data_collection

    metadata_df = pd.melt(
        df.head(1),
        id_vars=["data_collection", "table_name"],
        var_name="column_name",
        value_name="temp"
    ).drop(columns="temp")

    # column statistics
    metadata_df["n_non_nulls"] = (metadata_df["column_name"]
                                  .apply(lambda c: df[c].notna().sum()))
    metadata_df["n_unique"] = (metadata_df["column_name"]
                               .apply(lambda c: df[c].nunique()))
    metadata_df["dtype"] = (metadata_df["column_name"]
                            .apply(lambda c: schema_dict[data_collection][c]["type"]))

    # Write to DB
    with sqlite3.connect(conn_path) as conn:
        cursor = conn.cursor()
        cursor.execute("""
            DELETE FROM _metadata
            WHERE data_collection = ? AND table_name = ?;
        """, (data_collection, table_name))

        metadata_df.to_sql("_metadata", conn, if_exists="append", index=False)

    return metadata_df


def load_column_info(
        conn_path: str,
        data_collection: str,
        table_name: str):

    # expects metadata table with columns: column_name, dtype
    query = u.generate_select_sql(
        from_table="_metadata",
        cols=["column_name", "dtype"],
        where="data_collection = ? AND table_name = ?"
    )
    meta = read_sql_as_frame(
        conn_path,
        query,
        query_params=(data_collection, table_name)
    )

    if meta.empty:
        raise ValueError(f"No metadata for {data_collection} {table_name}")

    # build maps
    sql_types = {r["column_name"]: r["dtype"] for _, r in meta.iterrows()}

    cast = {}
    for col, sql_t in sql_types.items():
        py = s.DTYPES[sql_t]
        cast[col] = py

    return sql_types, cast