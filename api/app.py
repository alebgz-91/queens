import sqlite3

import fastapi as f
from typing import Optional
import src.utils as u
import json
import etl.validation as vld

from src.read_write import read_sql_as_frame
import config.settings as s

app = f.FastAPI(title="UK Energy Data API")


@app.get("/data/{collection}")
@app.get("/{collection}")
def get_data(
    collection: str = f.Path(..., description="Data collection key, e.g. 'dukes'"),
    table_name: str = f.Query(..., description="Table identifier within the collection, e.g. '1.1'"),
    filters: Optional[str] = f.Query(
        None,
        description=(
            "JSON string of filters. Supports flat and nested forms. "
            'Examples: {"year": 2022, "fuel": "Petroleum products"} '
            'or {"year": {"gte": 2010}, "fuel": {"like": "%gas%"}} '
            'or {"$or": [{"fuel": "Gas"},{"fuel": "Coal"}], "year": {"gt": 2020}}'
        ),
        examples={
            "flat": '{"year": 2022, "fuel": "Petroleum products"}',
            "nested": '{"year":{"gte":2010,"lt":2021},"fuel":{"like":"%gas%"}}',
            "with_or": '{"$or":[{"fuel":"Gas"},{"fuel":"Coal"}], "year":{"gt":2020}}',
        },
    ),
):
    """
    Return rows from `{collection}_prod` filtered by `table_name` (required)
    plus optional advanced filters. Columns `ingest_id`,`ingest_ts` are removed.
    """

    try:
        # verify existence of input
        u.check_inputs(
            data_collection=collection,
            table_name=table_name,
            etl_config=s.ETL_CONFIG)

    except NameError as e:
        # unknown data collection
        raise f.HTTPException(status_code=404, detail=str(e))

    # parse filters (string to dict)
    try:
        filters_dict = json.loads(filters) if filters else {}
    except json.JSONDecodeError as e:
        # malformed filter string
        raise f.HTTPException(status_code=400, detail=str(e))

    # normalise filters
    try:
        base_raw, or_raw = vld.normalize_filters(filters_dict)

        # validate+cast each group
        base = vld.validate_query_filters(collection, table_name, base_raw, s.DB_PATH, s.SCHEMA)
        ors = [vld.validate_query_filters(collection, table_name, g,s.DB_PATH, s.SCHEMA) for g in or_raw]
    except (KeyError, ValueError, TypeError, NameError) as e:
        # invalid columns, operators or value passed
        raise f.HTTPException(status_code=422, detail=str(e))

    # build WHERE
    base["table_name"] = {"eq": table_name}  # ensure mandatory filter
    try:
        schema_dict = s.SCHEMA[collection]
        where_sql, query_params = u.build_where_clause(
            base,
            ors,
            s.OP_SQL,
            schema_dict
        )
    except Exception as e:
        raise f.HTTPException(status_code=422, detail=str(e))

    # final query
    try:
        query = u.generate_select_sql(
            from_table=f"{collection}_prod",
            where=where_sql
        )

        df = read_sql_as_frame(
            conn_path=s.DB_PATH,
            query=query,
            query_params=query_params
        )
    except (sqlite3.OperationalError, sqlite3.DatabaseError) as e:
        raise f.HTTPException(status_code=500, detail=f"Database error: {e}")
    except Exception as e:
        raise f.HTTPException(status_code=500, detail=f"Unexpected error: {e}")

    # clean output
    if df is not None and not df.empty:
        df.drop(columns=["ingest_id", "ingest_ts"], inplace=True, errors="ignore")
        df.dropna(axis=1, how="all", inplace=True)
        return df.to_dict(orient="records")
    else:
        return []


@app.get("/metadata/{collection}")
def get_metadata(
        collection: str,
        table_name: str
):

    try:
        # verify existence of input
        u.check_inputs(
            data_collection=collection,
            table_name=table_name,
            etl_config=s.ETL_CONFIG)

    except NameError as e:
        # unknown data collection
        raise f.HTTPException(status_code=404, detail=str(e))

    try:
        query = u.generate_select_sql(
            from_table="_metadata",
            where="data_collection = ? AND table_name =?"
        )
        df = read_sql_as_frame(
            conn_path=s.DB_PATH,
            query=query,
            query_params=(collection, table_name)
        )
    except (sqlite3.OperationalError, sqlite3.DatabaseError) as e:
        raise f.HTTPException(status_code=500, detail=f"Database error: {e}")
    except Exception as e:
        raise f.HTTPException(status_code=500, detail=f"Unexpected error: {e}")

    return df.to_dict(orient="records")
