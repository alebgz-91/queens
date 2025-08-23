# Library (facade)

Public functions in `queens/facade.py`. All are thin wrappers around the internal modules with DB init when needed.

```python
import queens as q

# configure once (optional)
q.set_config(db_path="~/data/queens/queens.db", export_path="~/data/queens/exports")
q.setup_logging(level="info")  # logs to console + rotating file in user dir
```

## `ingest(data_collection: str, tables: Union[List[str], str] = None) -> None`
- Ingest one or more tables into RAW (and log to `_ingest_log`). If `tables` is `None`, ingests **all** tables for the collection.
- Initialises DB tables on demand.

## `stage(data_collection: str, as_of_date: Optional[str] = None) -> None`
- Rebuild `{collection}_prod` snapshot as of the given cutoff (or latest), and refresh `_metadata`.

## `info(data_collection: str, table_name: Optional[str] = None) -> pd.DataFrame`
- Human-readable summary from PROD: min/max year, row counts per table, with timestamp breakdown.

## `versions(data_collection: str, table_name: Optional[str] = None) -> pd.DataFrame`
- List of successful ingested timestamps (optionally filtered by table).

## `metadata(data_collection: str, table_name: str) -> pd.DataFrame`
- Queryable columns for the staged table (as recorded in `_metadata`).

## `query(data_collection: str, table_name: str, filters: Optional[dict] = None) -> pd.DataFrame`
- Same filter semantics as API. Validates and casts filters; selects from `{collection}_prod`.
- Drops service columns (`ingest_id`, `ingest_ts`) and empty columns.

## `export(data_collection: str, table_name: Optional[str] = None, file_type: str = "csv", output_path: Union[str, Path] = None, bulk_export: bool = False) -> None`
- Export a single table or all tables. For Parquet, `pyarrow` is included in the installed dependencies.

Saving to Excel with `bulk=True` will produce a single Excel workbook with each `table_name` written into a separate sheet.
