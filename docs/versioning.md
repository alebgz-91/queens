# ETL & Versioning Model

## Ingestion (RAW + log)
For each requested table:
1) Resolve runtime config via `validation.generate_config()` — this:
   - scrapes the chapter page to find the **Excel** URL for the specific table,
   - resolves the **template file path** from `templates.json` and `TEMPLATES_DIR`,
   - attaches `data_collection` and a human-readable `table_description`.
2) Run the transformer function named `f` with `f_args` (from `etl_config.json`), e.g. `process_sheet_to_frame(...)`.
3) **Validate** the resulting DataFrame via `validate_schema(...)`:
   - assert presence of index components (`row`, `label`),
   - rebuild a meaningful unique index (excl. working cols),
   - check duplicates, enforce dtypes from `schema.json`, and apply nullability constraints,
   - add constant `table_name` column.
4) **ingest_frame(...)** appends rows to `{collection}_raw` and writes a provenance row to `_ingest_log`
   (`ingest_ts`, `data_collection`, `table_name`, `url`, `table_description`, `success` flag set to 1 if the write succeeds).

## Staging (PROD snapshot)
`raw_to_prod(...)` materializes `{collection}_prod` as **the latest successful version** for each table (<= cutoff date).
This is a **full-table snapshot** per `table_name` — the API reads only from PROD, never from RAW.

After staging, `insert_metadata(...)` refreshes `_metadata` for each staged `table_name`:
- rows: one per column in the staged table,
- fields: `data_collection`, `table_name`, `column_name`, `n_non_nulls`, `n_unique`, `dtype`.

## Transformations
Generic flows:
- `process_sheet_to_frame(...)`: one or more specified sheet names; template-driven or manual mapping (`ignore_mapping=True` with `id_var_position`, `id_var_name`, `unit`).
- `process_multi_sheets_to_frame(...)`: traverse a workbook, matching sheet names by pattern or numerics; melt columns into a variable; attach sheet name as another variable (e.g. `year`).

Table-specific post-processing hooks (applied by `_postprocess(...)` when `table_name` matches):
- `1.1.5`: `_postprocess_1_1_5`
- `J.1`: `_postprocess_J_1`
- `F.2`: `_postprocess_dukes_F_2`
- `5.2`: `_postprocess_dukes_5_2`
- `4.4` / `4.5`: `_postprocess_normalize_names` (normalize sheet suffixes like `4.4a` → `4.4.A`).

Custom handlers:
- `process_dukes_5_6(...)`: orchestrates the three 5.6 sheets (including `_process_dukes_5_6_summaries(...)` for the Annual summaries layout).
- `process_dukes_5_10(...)`: merges outputs for 5.10 subtables.

