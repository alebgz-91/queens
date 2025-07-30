import fastapi as f
from typing import Optional, Dict, Any
import pandas as pd
import src.utils as u

from src.read_write import read_sql_as_frame
import config.settings as s


app = f.FastAPI(title="UK Energy Data API")


@app.get("/data/{collection}")
def get_data(
    collection: str = f.Path(...),
    table_name: str = f.Query(...,
                              description="Name of the table to query (for example, '1.1'"),
    filters: Optional[str] = f.Query(None,
                                     description="JSON-like string of filters, e.g. {'year': 2020, 'fuel': 'Petroleum products'}.")
):
    """
    Fetch data from specific data collection's production table,
    filtered by table_name (required) and other optional filters.

    Args:
        collection: name of source data collection
        table_name: name of the dataset to retrieve in the prod table
        filters: optional filters

    Returns:

    """

    try:
        # verify existence of input
        u.check_inputs(data_collection=collection,
                       etl_config=s.ETL_CONFIG)

        from_table = f"{collection}_prod"

        where_clause = "table_name = ?"
        query_params = (table_name,)

        if filters:
            for k, val in filters.items():
                where_clause += f" AND {k} = ?"
                query_params = query_params + (val,)

        query = u.generate_select_sql(
            from_table=from_table,
            where=where_clause
        )

        df = read_sql_as_frame(
            conn_path=s.DB_PATH,
            query=query,
            query_params=query_params
        )

        # remove unused columns
        df.dropna(axis=1, how="all", inplace=True)
    except Exception as e:
        raise f.HTTPException(status_code=500, detail=str(e))

    return df.to_dict(orient="records")
