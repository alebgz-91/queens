# Troubleshooting

This section lists issues that may arise at runtime, based on explicit error handling in the code.

## API returns 404 for collection/table
- The pair is checked against `ETL_CONFIG`. Ensure you ingested the correct collection and table names appear under the right chapter key.

## API returns 400: malformed filters
- `filters` must be valid JSON (string). Example: `{"year": {"gte": 2010}}`

## API returns 422
- Columns not present in schema or `_metadata` for the table.
- Operator not allowed for the column type (`settings.VALID_OPS`).
- Value could not be cast to the column dtype.

## API returns 500
- SQLite/database errors (e.g., missing tables). Ensure you have staged data: `queens stage <collection>`

## Export to Parquet fails
- Install an engine: `pip install pyarrow` (or `fastparquet`).

## No data returned by API / library `query()`
- Ensure you have staged data and your `table_name` filter is correct.
- Check `_metadata` via CLI: `queens info <collection> --meta`

