import os.path
from typing import Optional, List, Dict, Any
import pandas as pd
from pathlib import Path

from queens import settings as s
from queens.etl.bootstrap import initialize, is_staged
from queens.etl.process import (
    ingest_tables as _ingest_tables,
    ingest_all_tables as _ingest_all_tables,
    stage_data as _stage_data,
    get_data_info as _get_data_info,
    get_data_versions as _get_data_versions,
)
from queens.etl import validation as vld
from queens.core import read_write as rw
from queens.core import utils as u

# ---------- public API (import these in notebooks/code) ----------

def ingest(
        data_collection: str,
        tables: Optional[List[str]] = None
) -> None:
    """
    Ingest one or more tables for a data collection into RAW tables.
    If tables is None, ingests all tables for the collection.
    """
    # initialise DB
    initialize(s.DB_PATH, s.SCHEMA)

    if tables:
        _ingest_tables(data_collection=data_collection,
                       table_list=tables)
    else:
        _ingest_all_tables(data_collection=data_collection)


def stage(
        data_collection: str,
        as_of_date: Optional[str] = None
) -> None:
    """
    Move most recent (or cutoff) data from RAW to PROD and refresh metadata.
    """
    # initialise DB
    initialize(s.DB_PATH, s.SCHEMA)

    # stage
    _stage_data(data_collection=data_collection,
                as_of_date=as_of_date)


def info(
        data_collection: str,
        table_name: Optional[str] = None
) -> pd.DataFrame:
    """
    Return human-readable info about staged data (min/max year, row counts).
    """
    # initialise DB
    initialize(s.DB_PATH, s.SCHEMA)
    return _get_data_info(data_collection=data_collection, table_name=table_name)


def versions(data_collection: str, table_name: Optional[str] = None) -> pd.DataFrame:
    """
    Return list of ingested versions (timestamps), optionally filtered by table.
    """
    # initialise DB
    initialize(s.DB_PATH, s.SCHEMA)

    return _get_data_versions(data_collection=data_collection, table_name=table_name)


def metadata(data_collection: str, table_name: str) -> pd.DataFrame:
    """
    Return queryable columns and inferred dtypes for the staged table.
    """
    # initialise DB
    initialize(s.DB_PATH, s.SCHEMA)

    if is_staged(s.DB_PATH, data_collection=data_collection):
        q = u.generate_select_sql(
            from_table="_metadata",
            where="data_collection = ? AND table_name = ?"
        )
        return rw.read_sql_as_frame(
            conn_path=s.DB_PATH,
            query=q,
            query_params=(data_collection, table_name)
        )
    else:
        raise RuntimeError(
            f"Data collection '{data_collection}' is not staged. "
            f"Run queens.stage('{data_collection}') first."
        )


def query(
    data_collection: str,
    table_name: str,
    filters: Optional[Dict[str, Any]] = None
) -> pd.DataFrame:
    """
    Return a DataFrame directly from the PROD table,
    using the same validation rules as the API (flat or nested filters are fine).

    """
    # validate collection + table existence
    u.check_inputs(data_collection=data_collection, table_name=table_name, etl_config=s.ETL_CONFIG)

    # check if data is staged
    if not is_staged(s.DB_PATH, data_collection):
        raise RuntimeError(
            f"Data collection '{data_collection}' is not staged. "
            f"Run queens.stage('{data_collection}') first."
        )

    filters = filters or {}
    base_raw, or_raw = vld.normalize_filters(filters)
    base = vld.validate_query_filters(data_collection, table_name, base_raw, s.DB_PATH, s.SCHEMA)
    ors = [vld.validate_query_filters(data_collection, table_name, g, s.DB_PATH, s.SCHEMA) for g in or_raw]

    # ensure mandatory table_name filter
    base["table_name"] = {"eq": table_name}

    # build where
    where_sql, params = u.build_where_clause(base, ors, s.OP_SQL, s.SCHEMA[data_collection])

    # select
    q = u.generate_select_sql(
        from_table=f"{data_collection}_prod",
        where=where_sql,
        order_by=None,            # caller can reorder after
        limit=False               # use OFFSET/LIMIT directly below
    )

    df = rw.read_sql_as_frame(
        conn_path=s.DB_PATH,
        query=q,
        query_params=tuple(params)
    )

    if df is None or df.empty:
        return pd.DataFrame()

    # drop service columns
    df.drop(columns=["ingest_id", "ingest_ts"], inplace=True, errors="ignore")
    df.dropna(axis=1, how="all", inplace=True)

    return df


def export(
    data_collection: str,
    table_name: Optional[str] = None,
    file_type: str = "csv",
    output_path: Optional[str, Path] = None,
    bulk_export: bool = False,
) -> None:
    """
    Export staged data to disk using your existing core.read_write helpers.
    - If table_name is provided, export that table.
    - Else export all tables in the collection (optionally as a single bulk file).

    Args:
        data_collection: name of the parent data collection
        table_name: ID of the table to export (e.g. '1.1')
        file_type: format of exported file. Supported formats are csv, parquet, xlsx
        output_path: optional custom destination path. Default directory is stored in config file
        bulk_export: if True, exports the whole data_collection as a single file.
            If file_type='xlsx' is passed, each table is saved to a separate sheet in the same workbook

    """
    # require staging (same rule as CLI)
    if not is_staged(db_path=s.DB_PATH, data_collection=data_collection):
        raise RuntimeError(
            f"Data collection '{data_collection}' is not staged. "
            f"Run queens.stage('{data_collection}') first."
        )

    out_dir = Path(os.path.abspath(output_path)) if output_path else s.EXPORT_DIR
    out_dir.mkdir(parents=True, exist_ok=True)

    if table_name:
        rw.export_table(
            data_collection=data_collection,
            file_type=file_type,
            output_path=out_dir,
            table_name=table_name
        )
    else:
        rw.export_all(
            data_collection=data_collection,
            file_type=file_type,
            output_path=out_dir,
            bulk_export=bulk_export
        )
