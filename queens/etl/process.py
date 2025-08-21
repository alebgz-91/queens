from queens.etl import validation as vld
from queens.core import read_write as rw
from queens.core import utils as u
from queens.etl import transformations as tr
from queens import settings as s
import logging
import datetime
import pandas as pd
from numpy import isnan


def ingest_tables(
        data_collection: str,
        table_list: list,
        ingest_ts: str = None
):
    """
    Reads, processes and writes a selection of tables, fetching new data from source URLs.

    Args:
        data_collection: name of the parent data collection for the tables (e.g. 'dukes')
        table_list: a list or iterable of tables to be parsed and ingested (e.g. ['1.1', 'J.1']
        ingest_ts: Optional ISO timestamp string. Automatically set to now if None is passed.

    Returns:
        None

    """

    if ingest_ts is None:
        ingest_ts = datetime.datetime.now().isoformat()

    try:        # this is run only after initialization so all tables exist
        # and we can process data safely
        for table in table_list:

            logging.info(f"Processing {data_collection} table {table}.")
            u.check_inputs(data_collection=data_collection,
                           table_name=table,
                           etl_config=s.ETL_CONFIG)

            chapter_key = u.table_to_chapter(table_number=table,
                                           data_collection=data_collection)

            # generate config dictionary
            logging.debug(f"Getting config for table: {table}")
            config = vld.generate_config(
                data_collection=data_collection,
                table_name=table,
                chapter_key=chapter_key,
                templates=s.TEMPLATES,
                urls=s.URLS,
                etl_config=s.ETL_CONFIG
            )

            # retrieve function callable and args
            f_name = config["f"]
            f_args = config["f_args"]
            f_call = getattr(tr, f_name)

            # execute
            logging.debug(f"Calling function {f_name}")
            res = u.call_func(func=f_call, args_dict=f_args)

            # placeholder for the time being: return results
            for table_sheet in res:
                logging.info(f"Ingesting subtable {table_sheet}.")
                df = vld.validate_schema(
                    data_collection=data_collection,
                    table_name=table_sheet,
                    df=res[table_sheet],
                    schema_dict=s.SCHEMA)

                # write into raw table
                logging.debug(f"Ingesting table {table_sheet}")
                to_table = data_collection + "_raw"
                ingest_id = rw.ingest_frame(
                    df=df,
                    table_name=table_sheet,
                    to_table=to_table,
                    data_collection=data_collection,
                    url=f_args["url"],
                    table_descr=config["table_description"],
                    conn_path=s.DB_PATH,
                    ingest_ts=ingest_ts
                )
                logging.debug(f"Sheet {table_sheet} ingested successfully with id {ingest_id}")
            logging.info(f"ETL successful for table {table}")

    except Exception as e:
        logging.error(f"ETL failed: {e}")
        raise e

    logging.info("ETL run completed successfully.")
    return None


def ingest_all_tables(data_collection: str):
    """
    Read, process and write all the available tables in a given data_collection.
    Args:
        data_collection: Name of the data collection to ingest (e.g. 'dukes')

    Returns:
        None

    """

    try:
        # verify that the data collection exists
        u.check_inputs(data_collection,
                       etl_config=s.ETL_CONFIG)
        # time snapshot
        ingest_ts = datetime.datetime.now().isoformat()

        # to get the list of tables look at static config files
        logging.info(f"Processing all tables for {data_collection}.")
        config = s.ETL_CONFIG[data_collection]

        # go through each chapter and table
        for chapter_key in config.keys():
            logging.info(f"Processing {chapter_key.replace('_', ' ')}.")

            # execute
            table_list = config[chapter_key].keys()

            ingest_tables(data_collection=data_collection,
                          table_list=table_list,
                          ingest_ts=ingest_ts)

    except Exception as e:
        logging.error(f"ERROR: {e}")
        raise e

    logging.info(f"Process ended with success: all tables ingested for {data_collection}.")
    return None


def stage_data(
        data_collection: str,
        as_of_date: str = None
):
    """
    Select the most recent version of the data and move to production table.
    Optionally, the user can select older versions of the data.
    Args:
        data_collection: the data collection to stage into production
        as_of_date: optional cutoff for data versioning. Default is today's date. Required format is '%Y-%m-%d'

    Returns:
        None

    """

    if as_of_date is not None:
        cutoff_date = datetime.datetime.strptime(as_of_date, "%Y-%m-%d")
    else:
        cutoff_date = datetime.datetime.now().isoformat()

    try:
        # check if the data collection exists
        u.check_inputs(data_collection=data_collection,
                       etl_config=s.ETL_CONFIG)

        logging.debug(f"Staging {data_collection} data.")
        rw.raw_to_prod(
            conn_path=s.DB_PATH,
            table_prefix=data_collection,
            cutoff=cutoff_date
        )

        # get the list of tables staged to prod. Note that the global table number
        # may have been split into sheets (i.e. 1.3 -> 1.3.A, 1.3.B etc.
        query = u.generate_select_sql(
            from_table=f"{data_collection}_prod",
            cols=["table_name"],
            distinct=True
        )

        table_list = rw.read_sql_as_frame(conn_path=s.DB_PATH,
                                          query=query)["table_name"]

        logging.debug("Updating metadata.")
        for table_name in table_list:
            rw.insert_metadata(
                data_collection=data_collection,
                table_name=table_name,
                conn_path=s.DB_PATH,
                schema_dict=s.SCHEMA
            )
    except Exception as e:
        logging.error(f"ERROR: staging failed for {data_collection}: \n {e}")
        raise e

    date_str = "today" if as_of_date is None else as_of_date
    logging.info(f"Data for {data_collection} successfully staged in prod. \nThis is a snapshot as of {date_str}")
    return None


def get_metadata(
        data_collection: str,
        table_name: str = None,
) -> pd.DataFrame:
    """
    Fetch queryable columns for a given table_name or for all tables in the whole of data_collection

    Args:
        data_collection: name of the data collection
        table_name: optional table name for table-specific results

    Returns:
        a pandas dataframe

    """
    # check that data collection and table_name exist
    u.check_inputs(data_collection=data_collection,
                   etl_config=s.ETL_CONFIG)

    if table_name:
        where_clause = "data_collection = ? AND table_name = ?"
        select_block = ["column_name","dtype"]
        query_params = (data_collection, table_name)
    else:
        where_clause = "data_collection = ?"
        select_block = ["table_name", "column_name", "dtype"]
        query_params = (data_collection,)

    # get metadata
    query = u.generate_select_sql(
        from_table="_metadata",
        cols=select_block,
        where=where_clause
    )

    df = rw.read_sql_as_frame(
        conn_path=s.DB_PATH,
        query=query,
        query_params=query_params
    )

    # early return for empty dataframe
    if df.empty:
        return pd.DataFrame()

    # two different outputs: simple list for table-specific results
    # and full structured table for whole data collection
    if table_name:
        df.rename(columns={
            "column_name": "Column name",
            "dtype": "Data type"
        }, inplace=True)
        return df

    else:
        # the output table will display a sign for columns that can be queried for each table
        df["n"] = 1
        df.rename(columns={
            "column_name": "Column name",
            "dtype": "Data type"
        }, inplace=True)

        # cross-tabulate
        p = pd.pivot_table(
            data=df,
            index="Column name",
            columns="table_name",
            values="n",
            aggfunc= (lambda x: "" if isnan(x.sum()) else "X"),
            fill_value=""
        )

        # append data type column
        p.reset_index(drop=False, inplace=True)

        p["Data type"] = p["Column name"].apply(
            lambda x: s.SCHEMA[data_collection][x]["type"]
        )

        return p



def get_data_info(
        data_collection: str,
        table_name: str = None
) -> pd.DataFrame:
    """
    Display basic metadata about the data currently staged in the production table
    for a given data collection and (optionally) a specific table.

    Args:
        data_collection (str): The name of the data collection (e.g., "dukes").
        table_name (str, optional): A specific table to inspect.

    Returns:
        pd.DataFrame: A summary dataframe of ingested data, or an empty dataframe if none found.
    """
    if table_name:
        where_clause = f"table_name = ?"
        query_params = (table_name,)
    else:
        where_clause = None
        query_params = None

    query = u.generate_select_sql(
        from_table=f"{data_collection}_prod",
        where=where_clause
    )

    df = rw.read_sql_as_frame(
        conn_path=s.DB_PATH,
        query=query,
        query_params=query_params
    )

    # early return for empty result set
    if df.empty:
        return pd.DataFrame()

    df["ingest_ts"] = pd.to_datetime(df["ingest_ts"])
    df["Ingest time"] = df["ingest_ts"].dt.time
    df["Ingest date"] = df["ingest_ts"].dt.date

    df = df.rename(columns={
        "table_name": "Table number",
        "table_description": "Description"
    })
    df = df.groupby(["Table number", "Description", "Ingest date", "Ingest time"])["year"].agg([
        ("Min. year", "min"),
        ("Max. year", "max"),
        ("Row count", "count")
    ]).reset_index().set_index("Table number")

    return df


def get_data_versions(
        data_collection: str,
        table_name: str = None
):
    """
    Show all successful ingestion timestamps for a given data collection.

    Args:
        data_collection: The name of the data collection (e.g., "dukes").
        table_name: Optional name of table to inspect. Default shows data_collection level versions only

    Returns:
        pd.DataFrame: A dataframe listing all ingested versions with timestamps.
    """

    if table_name:
        where_clause = "data_collection = ? AND table_name = ? AND success = 1"
        query_params = (data_collection, table_name)

    else:
        where_clause = "data_collection = ? AND success = 1"
        query_params = (data_collection,)

    select_block = ["table_name", "ingest_ts"]

    query = u.generate_select_sql(
        from_table="_ingest_log",
        cols=select_block,
        where=where_clause,
        distinct=True
    )

    df = rw.read_sql_as_frame(conn_path=s.DB_PATH,
                              query=query,
                              query_params=query_params)

    if df.empty:
        return pd.DataFrame()

    # reshape dataframe into human-readable form
    df = df.rename(
        columns={
            "table_name": "Table number",
            "data_collection": "Data collection"
    }).sort_values(
        by=["Table number", "ingest_ts"],
        ascending=[True, False]
    ).set_index(
        "Table number"
    )
    df["ingest_ts"] = pd.to_datetime(df["ingest_ts"])
    df["Ingest date"] = df["ingest_ts"].dt.date
    df["Ingest time"] = df["ingest_ts"].dt.time
    df.drop(columns=["ingest_ts"], inplace=True)

    return df