# CLI

CLI entrypoint: `queens/cli.py` (Typer). On any run, logging is configured and, for certain commands, tables are auto-initialised.

## Auto-startup behaviour
For commands in `{ingest, stage, info, export, serve}`:
- Logging is configured once (file + console).
- Database is initialised idempotently (`_ingest_log`, `_metadata`, and `{collection}_raw` tables per `schema.json`).

## Commands

### `queens config`
Options:
- `--db-path PATH` (optional)
- `--export-path PATH` (optional)
- `--show-current` (flag) — prints user dir, DB path, export dir, templates dir

### `queens ingest COLLECTION [--table TABLE ...]`
- With `--table` (repeatable), ingests the given tables only.
- Without it, ingests **all** tables in the collection.

### `queens stage COLLECTION [--as_of_date YYYY-MM-DD]`
Moves latest (or cutoff) RAW versions into PROD and refreshes `_metadata`.

### `queens info COLLECTION [--table TABLE] [--vers] [--meta]`
- Default: report staged table stats (min/max year, row count) for the selection.
- `--vers`: list ingested versions (timestamps).
- `--meta`: list queryable columns and data types (prints a pivoted view across tables if `--table` omitted).

### `queens export COLLECTION [--file-type {csv,parquet,xlsx}] [--table TABLE] [--path PATH] [--bulk]`
- If `--table` is provided, export that table only.
- Else export all tables; with `--bulk`, write a single file (for `xlsx`, multiple sheets).

### `queens serve [--host HOST] [--port PORT] [--reload] [--log-level LEVEL]`
Starts the FastAPI app via Uvicorn and warns if some collections are not staged.
