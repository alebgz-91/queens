# Configuration & Paths

All runtime state lives under a **user data dir** resolved by `platformdirs` (varies by OS), exposed as `queens.settings.USER_DIR`.

On first import, `settings.ensure_user_configs()` copies packaged defaults into the user dir:
- `etl_config.json`
- `schema.json`
- `templates.json`
- `urls.json`
- `templates/` (Excel mapping templates directory)

These are then always read from the user dir so users can edit them without touching the package.

## Paths (from `queens/settings.py`)
- `USER_DIR`: base directory for all user data
- `LOG_DIR = USER_DIR / "logs"`
- `EXPORT_DEFAULT_DIR = USER_DIR / "exports"`
- `CONFIG_INI = USER_DIR / "config.ini"`
- JSON configs loaded from `USER_DIR`: `ETL_CONFIG`, `SCHEMA`, `TEMPLATES`, `URLS`
- `TEMPLATES_DIR = USER_DIR / "templates"`

## DB / Export directories
- `DB_PATH`: from `config.ini` (`[paths] db_path`) or defaults to `USER_DIR/queens.db`
- `EXPORT_DIR`: from `config.ini` (`[paths] export_path`) or defaults to `USER_DIR/exports`

You can change these via:
- CLI: `queens config --db-path ... --export-path ...`
- Library: `queens.set_config(db_path=..., export_path=...)`
Both update `config.ini` and call `reload_settings()` to apply immediately.

## JSON configs
- **etl_config.json**: maps `data_collection -> chapter -> table_name` to a transformer function (`f`) and its arguments (`f_args`). Example entries include `process_sheet_to_frame`, `process_multi_sheets_to_frame`, and custom wrappers like `process_dukes_5_6` / `process_dukes_5_10`.
- **schema.json**: SQL dtypes (`TEXT`, `INTEGER`, `REAL`, `DATETIME`) and nullability for each logical column. Used by `validate_schema()` and to drive casting and filter policies.
- **templates.json**: chapter → Excel filename (used to resolve `TEMPLATES_DIR / filename` in `generate_config()`).
- **urls.json**: collection → chapter page on GOV.UK (used by the scraper to discover the actual Excel file URLs).

