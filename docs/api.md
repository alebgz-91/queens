# API

FastAPI app (see `queens/api/app.py`). Startup configures logging to a rotating file and logs an "API started" message.

## Endpoints

### `GET /data/{collection}` (alias: `GET /{collection}`)
Query parameters:
- `table_name` (required): logical table (e.g. `"1.1"`).
- `filters` (optional, stringified JSON): flat or nested; supports `$or`. Examples:
  - `{"year": 2022, "fuel": "Petroleum products"}`
  - `{"year": {"gte": 2010}, "fuel": {"like": "%gas%"}}`
  - `{"$or": [{"fuel": "Gas"},{"fuel": "Coal"}], "year": {"gt": 2020}}`
- `limit` (int, default 1000, max 5000)
- `cursor` (optional int): pagination cursor — returns rows with `rowid > cursor`

Behaviour:
1) Validate `collection`/`table_name` against `ETL_CONFIG`.
2) Parse `filters` JSON; normalise and validate against schema and `_metadata`.
3) Build WHERE clause, **force** `table_name = ...`, and apply optional `rowid > cursor`.
4) Read from `{collection}_prod` ordered by `rowid`, limited to `limit`.
5) Return:
```json
{{
  "data": [ ...records... ],
  "table_description": "...from ingest log...",
  "next_cursor": 123456 or null
}}
```
Service columns (`rowid`, `ingest_id`, `ingest_ts`, `table_description`) are removed from `data`.

Errors:
- 404: unknown collection or table
- 400: malformed `filters` JSON
- 422: invalid columns/operators/casts or WHERE build error
- 500: database/unexpected errors

### `GET /metadata/{collection}`
Query parameters:
- `table_name` (required)

Returns the `_metadata` rows for the `(data_collection, table_name)` pair as a JSON list. Same 404/500 error handling as above. The response format is as follows:
```
{"data": List[dict]
```

