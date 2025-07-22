import utils as u
import etl.transformations as tr
import etl.input_output as io
import config.settings as stgs
import sql.sql_utils as sql
import logging
import datetime

# enable logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("data/logs/etl.log"),
        logging.StreamHandler()
    ]
)


def update_tables(
        data_collection: str,
        table_list: list,
        raw_table_names=True):
    """
    Update a selection of tables, fetching new data from source URLs.

    Args:
        data_collection: name of the release the tables belong to. Myst be lowercase
        table_list: a list or iterable of tables to be parsed and updated
        raw_table_names: if False, table_list contains table_keys rather than table numbers, for example "dukes_1_2_3
 instead of "1.2.3"
    Returns:
        True if the processing is successful

    """

    try:
        # create the raw table if it does not exist
        logging.info("Creating table sql tables if not exist")
        sql_create_main_tab = sql.generate_create_table_sql(
            table_name=data_collection,
            table_env="raw",
            schema_dict=stgs.SCHEMA
        )

        sql_create_log = sql.generate_create_log_sql()

        sql.execute_sql(
            conn_path=stgs.DB_PATH,
            sql=sql_create_log + "\n" + sql_create_main_tab
        )

        for table in table_list:

            if raw_table_names:
                # generate keys to fetch config
                table_key = data_collection + "_" + table.replace(".", "_")
            else:
                table_key = table

            u.check_inputs(data_collection=data_collection,
                         table_key=table_key,
                         etl_config=stgs.ETL_CONFIG)

            chapter_key = u.table_to_chapter(table_number=table,
                                           data_collection=data_collection)

            # generate config dictionary
            logging.info(f"Getting config for table: {table}")
            config = io.generate_config(data_collection=data_collection,
                                     table_key=table_key,
                                     chapter_key=chapter_key,
                                     templates=stgs.TEMPLATES,
                                     urls=stgs.URLS,
                                     etl_config=stgs.ETL_CONFIG)

            # retrieve function callable and args
            f_name = config["f"]
            f_args = config["f_args"]
            f_call = getattr(tr, f_name)

            # execute
            logging.info(f"Calling function {f_name}")
            res = u.call_func(func=f_call, args_dict=f_args)

            # placeholder for the time being: return results
            for table_key in res:
                logging.info(f"Validating schema for {table_key}")
                df = tr.validate_schema(
                    data_collection=data_collection,
                    table_key=table_key,
                    df=res[table_key],
                    schema_dict=stgs.SCHEMA)

                # write into raw table
                logging.info(f"Ingesting table {table_key}")
                to_table = data_collection + "_raw"
                ingest_id = sql.ingest_frame(
                    df=df,
                    table_name=table_key,
                    to_table=to_table,
                    url=f_args["url"],
                    conn_path=stgs.DB_PATH
                )
                logging.info(f"Table {table_key} ingest successful with id {ingest_id}")

    except Exception as e:
        logging.error(f"ETL tailed for {data_collection}: \n{e}")
        return None

    logging.info(f"Finished ETL update for selected tables in {data_collection}")
    return None


def update_all_tables(data_collection: str):
    # to get the list of tables look at static config files
    logging.info(f"Updating all tables for {data_collection}")
    config = stgs.ETL_CONFIG[data_collection]

    # go through each chapter and table
    for chapter_key in config.keys():
        logging.info(f"Updating {chapter_key.replace('_', ' ')}")

        # execute
        table_list = config[chapter_key].keys()

        update_tables(data_collection=data_collection,
                      table_list=table_list,
                      raw_table_names=False)
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
    # check if the data collection exists
    if data_collection not in stgs.ETL_CONFIG:
        raise NameError("No such data collection,")

    if as_of_date is not None:
        as_of_date = datetime.datetime.strptime(as_of_date, "%Y-%m-%d")
    else:
        as_of_date = datetime.datetime.now().isoformat()

    try:
        sql.raw_to_prod(
            conn_path=stgs.DB_PATH,
            table_base_name=data_collection,
            cutoff=as_of_date
        )
    except Exception as e:
        logging.error(f"Staging failed for {data_collection}: \n {e}")
        return None

    logging.info(f"Data for {data_collection} successfully staged in prod as of {as_of_date}")
    return None
