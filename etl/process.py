from sympy.simplify.fu import as_f_sign_1

import etl.validation as ts
import src.read_write as rw
import src.utils as u
import etl.transformations as tr
import config.settings as s
import logging
import datetime
import pandas as pd
from tabulate import tabulate


def update_tables(
        data_collection: str,
        table_list: list,
        ingest_ts: str = None
):
    """
    Update a selection of tables, fetching new data from source URLs.

    Args:
        data_collection: name of the release the tables belong to. Myst be lowercase
        table_list: a list or iterable of tables to be parsed and updated
        ingest_ts: timestamp string for ingest. If not passed by batch process it will set to today

    Returns:
        None

    """

    if ingest_ts is None:
        ingest_ts = datetime.datetime.now().isoformat()

    try:
        # this is run only after initialization so all tables exist
        # and we can process data safely
        for table in table_list:

            u.check_inputs(data_collection=data_collection,
                           table_name=table,
                           etl_config=s.ETL_CONFIG)

            chapter_key = u.table_to_chapter(table_number=table,
                                           data_collection=data_collection)

            # generate config dictionary
            logging.info(f"Getting config for table: {table}")
            config = ts.generate_config(
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
            logging.info(f"Calling function {f_name}")
            res = u.call_func(func=f_call, args_dict=f_args)

            # placeholder for the time being: return results
            for table_sheet in res:
                logging.info(f"Validating schema for {table_sheet}")
                df = ts.validate_schema(
                    data_collection=data_collection,
                    table_name=table_sheet,
                    df=res[table_sheet],
                    schema_dict=s.SCHEMA)

                # write into raw table
                logging.info(f"Ingesting table {table_sheet}")
                to_table = data_collection + "_raw"
                ingest_id = rw.ingest_frame(
                    df=df,
                    table_name=table_sheet,
                    to_table=to_table,
                    data_collection=data_collection,
                    url=f_args["url"],
                    conn_path=s.DB_PATH,
                    ingest_ts=ingest_ts
                )
                logging.info(f"Table {table_sheet} ingest successful with id {ingest_id}")

    except Exception as e:
        logging.error(f"ETL tailed for {data_collection}: \n{e}")
        raise e

    logging.info(f"Finished ETL update for selected tables in {data_collection}")
    return None


def update_all_tables(data_collection: str):

    try:
        # verify that the data collection exists
        u.check_inputs(data_collection,
                       etl_config=s.ETL_CONFIG)
        # time snapshot
        ingest_ts = datetime.datetime.now().isoformat()

        # to get the list of tables look at static config files
        logging.info(f"Updating all tables for {data_collection}")
        config = s.ETL_CONFIG[data_collection]

        # go through each chapter and table
        for chapter_key in config.keys():
            logging.info(f"Updating {chapter_key.replace('_', ' ')}")

            # execute
            table_list = config[chapter_key].keys()

            update_tables(data_collection=data_collection,
                          table_list=table_list,
                          ingest_ts=ingest_ts)

    except Exception as e:
        logging.error(f"Batch update failed for {data_collection}.")
        raise e

    logging.info(f"All chapters processed for {data_collection}")
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
        as_of_date: optional cutoff for data versioning. Default is today's date.

    Returns:

    """

    if as_of_date is not None:
        as_of_date = datetime.datetime.strptime(as_of_date, "%Y-%m-%d")
    else:
        as_of_date = datetime.datetime.now().isoformat()

    try:
        # check if the data collection exists
        u.check_inputs(data_collection=data_collection,
                       etl_config=s.ETL_CONFIG)

        logging.info(f"Staging {data_collection} data.")
        rw.raw_to_prod(
            conn_path=s.DB_PATH,
            table_prefix=data_collection,
            cutoff=as_of_date
        )

        logging.info("Updating metadata.")
        for table_name in s.ETL_CONFIG[data_collection]:
            m = rw.insert_metadata(
                data_collection=data_collection,
                table_name=table_name,
                conn_path=s.DB_PATH
            )
    except Exception as e:
        logging.error(f"Staging failed for {data_collection}: \n {e}")
        raise e

    date_str = "today" if as_of_date is None else as_of_date
    logging.info(f"Data for {data_collection} successfully staged in prod. \nThis is a snapshot as of {date_str}")
    return None


def get_metadata(
        data_collection: str,
        table_name: str = None
):
    """
    Display valid queryable columns for a given table_name or for all tables in the whole of data_collection

    Args:
        data_collection: name of the data collection
        table_name: optional table name for table-specific results

    Returns:
        a pandas dataframe

    """
    if table_name:
        where_clause = "data_collection = ? AND table_name = ?"
        select_block = ["column_name"]
        query_params = (data_collection, table_name)
    else:
        where_clause = "data_collection = ?"
        select_block = ["table_name", "column_name"]
        query_params = (data_collection,)

    # get metadata
    query = u.generate_select_sql(
        from_table="metadata",
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
        print(f"No results for selected {data_collection}")
        return None

    # two different outputs: simple list for table-specific results
    # and full structured table for whole data collection
    if table_name:
        print(f"Queryable columns for {table_name}:")
        for x in df["table_name"]:
            print(x)
        else:
            # the output table will display a sign for columns that can be queried for each table
            df["n"] = 1
            df = df.rename(columns={
                "column_name": "Column name"
            })

            # cross-tabulate
            p = pd.pivot_table(
                index="Column name",
                columns="table_name",
                values="n",
                aggfunc= (lambda x: "X" if x.sum() is not None else "")
            )

            print(f"Results for {data_collection}:")
            print(tabulate(df, headers="keys"))


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
    if table_name is not None:
        where_clause = f"table_name = ?"
        query_params = (table_name,)
    else:
        where_clause = None
        query_params = None

    query = u.generate_select_sql(
        from_table=f"{data_collection}_prod",
        where=where_clause,
        distinct=True
    )

    df = rw.read_sql_as_frame(
        conn_path=s.DB_PATH,
        query=query,
        query_params=query_params
    )

    if df.empty:
        print(f"No data staged for '{data_collection}'"
              f"{f', table {table_name}' if table_name else ''}.")
        return pd.DataFrame()

    df["ingest_ts"] = pd.to_datetime(df["ingest_ts"])
    df["Ingest time"] = df["ingest_ts"].dt.time
    df["Ingest date"] = df["ingest_ts"].dt.date

    df = df.rename(columns={"table_name": "Table number"})
    df = df.groupby(["Table number", "Ingest date", "Ingest time"])["year"].agg([
        ("Min. year", "min"),
        ("Max. year", "max"),
        ("Row count", "count")
    ]).reset_index().set_index("Table number")

    print(f"Found {len(df)} record(s) for '{data_collection}'"
          f"{f', table {table_name}' if table_name else ''}.\n")
    print(tabulate(df, headers="keys"))

    return df


def get_data_versions(
        data_collection: str,
        table_name: str = None
):
    """
    Show all successful ingestion timestamps for a given data collection.

    Args:
        data_collection (str): The name of the data collection (e.g., "dukes").
        table_name (str): Optional name of table to inspect. Default shows data_collection level versions only

    Returns:
        pd.DataFrame: A dataframe listing all ingested versions with timestamps.
    """

    if table_name is None:
        where_clause = "data_collection = ? AND success = 1"
        query_params = (data_collection, )
        print_str = data_collection
    else:
        where_clause = "data_collection = ? AND table_name = ? AND success = 1"
        query_params = (data_collection, table_name)
        print_str = f"{data_collection} table {table_name}"

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
        print(f"No ingested versions found for {print_str}.")
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

    print(f"Found {len(df)} ingested version(s) for {print_str}:\n")
    print(tabulate(df, headers="keys"))

    return df