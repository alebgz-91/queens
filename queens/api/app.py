import os
# silence numexpr message when importing pandas
os.environ.setdefault("NUMEXPR_NUM_THREADS", "8")

import sqlite3
import fastapi as f
from typing import Optional
import logging
import queens.core.utils as u
import json
import queens.etl.validation as vld

from queens.core.read_write import read_sql_as_frame
from queens import settings as s

DEFAULT_LIMIT = 1000
MAX_LIMIT = 5000


app = f.FastAPI(title="QUEENS API")

# -----------------
# startup
# -----------------

@app.on_event("startup")
def _startup_logging():
    # Log API events to a separate file; no console to avoid double-logs with uvicorn
    s.setup_logging(to_console=False, to_file=True, file_name="queens_api.log")
    logging.getLogger(__name__).info("QUEENS API started.")

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
    ),
    limit: int = f.Query(DEFAULT_LIMIT, ge=1, description=f"Max rows per page (<= {MAX_LIMIT})"),
    cursor: Optional[int] = f.Query(None, description="Pagination cursor (internal rowid); return rows with rowid > cursor"),

):
    """
    Return rows from `{collection}_prod` filtered by `table_name` + optional filters.
    Cursor pagination: results are ordered by internal `rowid`. Pass back `next_cursor` from the
    previous response to get the next page. Columns `ingest_id`,`ingest_ts` are removed.

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
    base["table_name"] = {"eq": table_name}
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

    limit = min(int(limit), MAX_LIMIT)

    # final query
    try:
        # extend where for cursor
        where_curs = where_sql
        if cursor is not None:
            where_curs = f"({where_sql}) AND (rowid > ?)"
            query_params.append(int(cursor))

        # order records by implicit rowid
        query = u.generate_select_sql(
            cols=["rowid", "*"],
            from_table=f"{collection}_prod",
            where=where_curs,
            order_by=["rowid"],
            limit=True
        )

        # add limit to parameters
        query_params.append(limit)
        print(query)
        print(query_params)

        df = read_sql_as_frame(
            conn_path=s.DB_PATH,
            query=query,
            query_params=tuple(query_params)
        )
    except (sqlite3.OperationalError, sqlite3.DatabaseError) as e:
        raise f.HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        raise f.HTTPException(status_code=500, detail=f"Unexpected error: {str(e)}")

    if df is None or df.empty:
        return {"data": [], "next_cursor": None}

    # optimistic last-page check
    if len(df) < limit:
        next_cursor = None
    else:
        next_cursor = int(df["rowid"].iloc[-1])

    # get table description
    table_description = df["table_description"].values[0]

    # drop service/internal columns
    df.drop(columns=["rowid",
                     "ingest_id",
                     "ingest_ts",
                     "table_description"],
            inplace=True,
            errors="ignore")
    df.dropna(axis=1, how="all", inplace=True)

    # compound response
    return {"data": df.to_dict(orient="records"),
            "table_description": table_description,
            "next_cursor": next_cursor}



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
