# Filtering Rules (API & Library)

The API and the `queens.query()` function share the same validation layer from `queens.etl.validation`.

## Shapes
- **Flat**: `{"year": 2022, "fuel": "Gas"}` becomes `{"year": {"eq": 2022}, "fuel": {"eq": "Gas"}}`
- **Nested**: `{"year": {"gte": 2010}, "fuel": {"like": "%gas%"}}`
- **OR groups** via `$or`: 
  - `{"$or": [{"fuel": "Gas"}, {"fuel": "Coal"}], "year": {"gt": 2020}}`

`normalize_filters()` normalises these into:
- a **base** dict (AND of fields),
- an optional list of **OR** groups (each element is itself an AND group).

## Validation & Casting
`validate_query_filters(collection, table_name, group, conn_path, schema_dict)` enforces:
1) Columns exist in `schema_dict[collection]` (**schema.json**).
2) Columns are **queryable for this table** — verified via `_metadata` (using `read_write.load_column_info()`).
3) Operators are allowed per SQL type (from `settings.VALID_OPS`).
4) Values are cast using column dtypes derived from metadata (`settings.DTYPES`).

## SQL generation
`utils.build_where_clause(base, or_groups, operator_map=settings.OP_SQL, schema)` builds a SQL WHERE string and parameters.
- TEXT comparisons add `COLLATE NOCASE`.
- Operator map:
  - `eq` → `= ?`
  - `neq` → `<> ?`
  - `lt`/`lte`/`gt`/`gte` → standard comparisons
  - `like` → `LIKE ?`

