import os
# silence numexpr message before anything imports pandas
os.environ.setdefault("NUMEXPR_NUM_THREADS", "8")

import typer
from typing import Optional, List
from tabulate import tabulate
import pandas as pd
import uvicorn
from pathlib import Path

from . import settings as s
from .etl.bootstrap import initialize, is_staged
from .etl.process import (ingest_tables, ingest_all_tables,
                                get_data_info, get_metadata, stage_data,
                                get_data_versions)
from .core  import read_write as rw

app = typer.Typer()

@app.callback()
def auto_startup(ctx: typer.Context)-> None:
    """
    Initialise DB tables only for commands that write or expect tables to exist.
    Also configures logging once (file + console).
    """
    # configure logging once for any CLI invocation
    s.setup_logging(file_name="queens_cli.log",
                    level="info")

    # only certain commands require auto-startup
    commands_requiring_init = {"ingest", "stage", "info", "export", "serve"}
    if ctx.invoked_subcommand in commands_requiring_init:
        created = initialize(db_path=s.DB_PATH, schema=s.SCHEMA)
        if created:
            typer.echo("Initialized QUEENS DB/tables.")
        else:
            typer.echo("DB/tables already initialized.")


@app.command()
def config(
    db_path: Optional[str] = typer.Option(None, "--db-path"),
    export_path: Optional[str] = typer.Option(None, "--export-path"),
    show_current: bool = typer.Option(False, "--show-current")
):
    if show_current:
        typer.echo(f"User dir:    {s.USER_DIR}")
        typer.echo(f"DB path:     {s.DB_PATH}")
        typer.echo(f"Export dir:  {s.EXPORT_DIR}")
        typer.echo(f"Templates:   {s.TEMPLATES_DIR}")
        raise typer.Exit(code=0)

    if (db_path is None) and (export_path is None):
        typer.echo("Nothing to change. Use --db-path and/or --export-path or --show-current.")
        raise typer.Exit(code=0)

    try:
        s.set_config(db_path=db_path, export_path=export_path)
        typer.echo("Configuration updated.")
    except Exception as e:
        if e:
            typer.echo(f"ERROR: execution terminated. \n {e}")


@app.command()
def ingest(
    collection: str,
    tables: Optional[List[str]] = typer.Option(None, "--table", "-t", help="Table(s) to update")
)-> None:
    """
    Update specific tables or all tables in a collection.
    """
    try:
        if tables:
            typer.echo(f"Updating {tables} in {collection}...")
            ingest_tables(
                data_collection=collection,
                table_list=tables)
        else:
            typer.echo(f"Updating all tables in {collection}...")
            ingest_all_tables(data_collection=collection)
        typer.echo("Ingestion process completed with success.")

    except Exception as e:
        typer.echo(f"ERROR - execution terminated: {e}")
        raise typer.Exit()


@app.command()
def stage(
    collection: str,
    as_of_date: Optional[str] = typer.Option(None, "--as_of_date", "--d", help="The cutoff point for data versioning.")
)-> None:
    """
    Stage the most recent data version for a collection.
    """
    try:
        typer.echo(f"Staging {collection} data...")
        stage_data(data_collection=collection,
                   as_of_date=as_of_date)
        typer.echo(f"Data for {collection} staged successfully.")

    except Exception as e:
        typer.echo(f"ERROR: execution terminated: \n{e}")
        raise typer.Exit()


@app.command()
def info(
    collection: str,
    table: Optional[str] = typer.Option(None, "--table", "-t", help="Optional table name to inspect"),
    vers: Optional[bool] = typer.Option(False, "--vers", "-v", help="Displays the ingested versions for the specified selection."),
    meta: Optional[bool] = typer.Option(False, "--meta", "-m", help="Displays queryable column names and data types for each table in the selection.")
)-> None:
    """
    Displays information on data according to the selected parameters, The defauls behaviour is to
    display table statistics for staged data. It can also display a list of ingested versions and schema information.
    Args:
        collection: data collection name
        table: table name
        vers: whether to display the data version log
        meta: whether to display column metadata

    Returns:
        None

    """
    try:
        if vers:
            df = get_data_versions(data_collection=collection, table_name=table)


        else:
            if not is_staged(s.DB_PATH, collection):
                typer.echo(f"No data have been staged yet. Run 'queens stage {collection}' first.")
                raise typer.Exit(code=0)
            if meta:
                df = get_metadata(data_collection=collection, table_name=table)
            else:
                df = get_data_info(data_collection=collection, table_name=table)

        conditional_str = f", table {table}" if table else ""
        if df.empty:
            typer.echo(f"No results found for {collection}{conditional_str}.")
            raise typer.Exit(code=0)
        else:
            typer.echo(f"Found {len(df)} result(s) for {collection}{conditional_str}:")
            # df.set_index(df.columns[0], inplace=True)

            # for this table, break the result in chunks for readability
            if meta and (table is None):
                dtype_col = df["Data type"]
                df.drop(columns=["Data type"], inplace=True)

                for i in range(0, len(df.columns), 8):
                    chunk = df.iloc[:, i:(i + 8)]
                    chunk = pd.concat([chunk, dtype_col], axis=1)

                    typer.echo(tabulate(chunk, headers="keys"))
                    typer.echo("\n")
            else:
                typer.echo(tabulate(df, headers="keys"))

    except Exception as e:
        typer.echo(f"Execution terminated. \n{e}")
        raise typer.Exit()


@app.command()
def export(
        collection: str,
        file_type: Optional[str] = typer.Option("csv", "--file-type", "-f", help="Format to use for export. Options are csv, parquet or xlsx, default is xsc"),
        table: Optional[str] = typer.Option(None, "--table", "-t", help="Optional table name to download"),
        path: Optional[str] = typer.Option(s.EXPORT_DIR, "--path", "-p", help="Optional destination path"),
        bulk: Optional[bool] = typer.Option(False, "--bulk", "-b", help="Whether to save all the data in a single file or not")
)-> None:
    # interrupt if data colleciton is not staged
    if not is_staged(db_path=s.DB_PATH, data_collection=collection):
        missing = f"{collection} staging table is missing"
        hint = f"Run: `queens stage {collection}`"

        typer.echo(f"Cannot export: {missing}. {hint}.")
        raise typer.Exit(code=1)

    # create path if not exists.
    path = Path(os.path.abspath(path))
    path.mkdir(parents=True, exist_ok=True)

    try:
        if table:
            typer.echo(f"Exporting {collection} table {table}...")
            rw.export_table(
                data_collection=collection,
                file_type=file_type,
                output_path=path,
                table_name=table
            )
        else:
            bulk_str = " as a single file" if bulk else ""
            typer.echo(f"Exporting all tables in {collection}{bulk_str}---")
            rw.export_all(
                data_collection=collection,
                file_type=file_type,
                output_path=path,
                bulk_export=bulk
            )
    except Exception as e:
        typer.echo(f"ERROR: execution terminated. \n{e}")
        raise typer.Exit(code=1)


@app.command()
def serve(
    host: str = typer.Option("127.0.0.1", "--port"),
    port: int =  typer.Option(8000, "--port"),
    reload: bool = typer.Option(False, "--reload", help="Dev reload (spawns reloader process)"),
    log_level: str = typer.Option("info", "--log-level")
)-> None:
    """
    Starts the QUEENS API using Uvicorn. The app is not started unless at least one data collection has been staged.
    """

    collections = list(s.SCHEMA.keys())  # or s.ETL_CONFIG.keys()
    missing = []
    for coll in collections:
        if not is_staged(s.DB_PATH, coll):
            missing.append(coll)

    if missing:
        if len(missing) == len(s.SCHEMA.keys()):
            typer.echo("No data collection staged. Run `queens stage <collection>` before proceeding")
            raise typer.Exit(code=1)

        # simple warning if at least one data collection has been staged
        typer.echo(
            f"WARNING: the following data collections have not been staged: {missing}" +
            "Data from these collections will not be served through the API."
        )

    # start API (uvicorn)
    try:
        server_config = uvicorn.Config(
            app="queens.api.app:app",  # <-- package-qualified
            host=host,
            port=port,
            reload=reload,
            log_level=log_level,
        )
        server = uvicorn.Server(server_config)
        typer.echo(f"Starting API at http://{host}:{port} (reload={reload})")
        server.run()
        if not server.started and not server.should_exit:
            typer.echo("Uvicorn did not start (unknown state).")
            raise typer.Exit(code=1)

        # normal exit
        raise typer.Exit(code=0)

    except KeyboardInterrupt:
        typer.echo("\nShutting down...")
        raise typer.Exit(code=0)

    except OSError as e:
        # address already in use (Linux errno 98; Windows 10013/10048)
        if getattr(e, "errno", None) in (98, 10013, 10048):
            typer.echo(f"Port {port} is already in use on {host}. "
                       f"Try --port {port + 1} or stop the other process.", err=True)
            raise typer.Exit(code=2)

        typer.echo(f"OSError while starting Uvicorn: {e}", err=True)
        raise typer.Exit(code=1)

    except Exception as e:
        typer.echo(f"Failed to start server: {e}")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()