import src.read_write as rw
import src.utils as u
import config.settings as s
import logging

def initialize(
    db_path: str,
    schema: dict
):

    logging.info("Creating raw tables.")
    for data_collection in schema:
        create_raw_table = u.generate_create_table_sql(data_collection,
                                                       table_env="raw",
                                                       schema_dict=schema)

        logging.info(f"Creating table {data_collection}_raw")
        rw.execute_sql(conn_path=db_path,
                       sql=create_raw_table)
        logging.info(f"Successfully created table {data_collection}_raw")

    # create log
    create_log = u.generate_create_log_sql()
    logging.info("Creating ingest log table.")
    rw.execute_sql(conn_path=db_path,
                   sql=create_log)

    logging.info("Creating metadata table.")
    create_metadata = u.generate_create_metadata_sql()
    rw.execute_sql(conn_path=db_path,
                   sql=create_metadata)
    return None