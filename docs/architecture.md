# Architecture

This is a general overview of the package features and codebase.

## High level
- **RAW**: `{collection}_raw` (per collection). Rows are appended with an `ingest_id` that ties back to `_ingest_log`.
- **PROD**: `{collection}_prod` — a **snapshot** materialised from RAW using the most recent successful `ingest_ts` per `table_name` (up to a cutoff). The API **only** reads from PROD.
- **_ingest_log**: provenance of each ingest (ts, collection, table_name, url, description, success flag).
- **_metadata**: for each `(data_collection, table_name)`, stores queryable column names and their SQL dtype and simple stats (n_non_nulls, n_unique).

## Key modules

- `queens/settings.py`: user-scoped config & paths (via `platformdirs`), logging setup, JSON configs loader.
- `queens/core/read_write.py`:
  - `read_and_wrangle_wb(...)`: reads Excel, auto-detects headers (incl. `has_multi_headers`, `fixed_header`).
  - `ingest_frame(...)`: append to `{collection}_raw` and log to `_ingest_log`.
  - `raw_to_prod(...)`: rebuild `{collection}_prod` snapshot (cutoff).
  - `export_table(...)` / `export_all(...)`: write CSV/Parquet/XLSX.
  - `read_sql_as_frame(...)`, `table_exists(...)`, `insert_metadata(...)`, `load_column_info(...)`.
- `queens/core/utils.py`:
  - path and input checks, JSON parsing, note-tag removal,
  - SQL helpers: `generate_select_sql`, DDL creation helpers for RAW/log/metadata,
  - filter helpers: `to_nested`, `build_sql_for_group`, `build_where_clause`.
- `queens/core/web_scraping.py`: scrapes GOV.UK chapter pages for DUKES Excel links (`urls.json` directs to chapter pages).
- `queens/etl/validation.py`:
  - `generate_config(...)`: resolves runtime `f_args` (adds `url`, `template_file_path`, `data_collection`) and gets table `description`.
  - `validate_schema(...)`: strict schema checks, duplicate detection, dtype enforcement, nullability checks.
  - `normalize_filters(...)` / `validate_query_filters(...)`: used by API and facade query.
- `etl/transformations.py` (module name as provided): generic transformers:
  - `process_sheet_to_frame(...)` and `process_multi_sheets_to_frame(...)` (template-driven or manual mapping),
  - post-processing hooks for certain tables (e.g. `1.1.5`, `J.1`, `F.2`, `5.2`, name normalization for `4.4`/`4.5`),
  - custom flows for `5.6`, `5.10`.
- `queens/etl/bootstrap.py`: idempotent DB bootstrap (`_ingest_log`, `_metadata`, `{collection}_raw`), and `is_staged(...)` check.
- `queens/etl/process.py` (referenced by imports; your pasted file path was `quens/etl/process.py`):
  - `ingest_tables(...)`, `ingest_all_tables(...)`, `stage_data(...)`,
  - `get_metadata(...)`, `get_data_info(...)`, `get_data_versions(...)`.
- `queens/api/app.py` (pasted as `app..py`): FastAPI app; endpoints documented in the API doc.
- `queens/cli.py`: Typer CLI (commands documented in the CLI doc).
- `queens/facade.py`: programmatic, user-facing helpers (documented in Library doc).

