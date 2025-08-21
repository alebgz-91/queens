from pathlib import Path
from typing import Union
import logging
import queens.core.read_write as rw
from ..core import utils as u


def initialize(
        db_path: Union[str, Path],
        schema: dict
) -> bool:
    """
    Idempotent DB bootstrap. Returns True if any table was created.
    """
    created_any = False
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    # ingest log (sentinel)
    if not rw.table_exists("_ingest_log", db_path):
        logging.info("Creating _ingest_log.")
        rw.execute_sql(conn_path=db_path, sql=u.generate_create_log_sql())
        created_any = True

    # metadata
    if not rw.table_exists("_metadata", db_path):
        logging.info("Creating _metadata.")
        rw.execute_sql(conn_path=db_path, sql=u.generate_create_metadata_sql())
        created_any = True

    # raw tables per collection
    for data_collection in schema:
        raw = f"{data_collection}_raw"
        if not rw.table_exists(raw, db_path):
            logging.info(f"Creating {raw}.")
            sql = u.generate_create_table_sql(
                table_prefix=data_collection, table_env="raw", schema_dict=schema
            )
            rw.execute_sql(conn_path=db_path, sql=sql)
            created_any = True

    if not created_any:
        logging.debug("All tables already exist. Skipping initialization.")

    return created_any


def is_staged(
        db_path: Union[str, Path],
        data_collection: str
) -> bool:
    """
    Returns True if prod exists .
    """
    prod = f"{data_collection}_prod"
    return rw.table_exists(prod, db_path)