import fastapi as f
from typing import Optional, Dict, Any
import pandas as pd
from conda.gateways.connection.adapters.ftp import data_callback_factory
from conda_package_handling.utils import checksums
from debugpy.launcher.debuggee import describe
from gensim.similarities.fastss import FastSS
from panel.examples.gallery.demos.VTKInteractive import description
import src.utils as u

from src.read_write import read_sql_as_frame
import config.settings as s


app = f.FastAPI(title="UK Energy Data API")


@app.get("/data/{collection}")
def get_data(
    collection: str,
    table_name: str = Query(..., description="Name of the table to query (for example, '1.1'"),
    filters: Optional[Dict[str, Any]] = f.Query(None)
):
    """
    Fetch data from specific data collection's production table,
    filtereb by table_name (required) and other optional filters.

    Args:
        collection: name of source data collection
        table_name: name of the dataset to retrieve in the prod table
        filters: optional filters

    Returns:

    """

    try:
        # verify existence of input
        u.check_inputs(data_collection=collection)

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
