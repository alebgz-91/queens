import typer
from typing import Optional, List
import logging

# enable logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("data/logs/etl.log"),
        logging.StreamHandler()
    ]
)

from etl.bootstrap import initialize
from etl.process import *
import src.read_write as rw
import config.settings as s

app = typer.Typer()

@app.callback()
def auto_startup(c: typer.Context):
    """
    Initialises the ETL pipeline with default config.
    """
    commands_requiring_init = {"update"}

    # list of necessary tables in DB
    if c.invoked_subcommand in commands_requiring_init:

        table_list = [k + "_raw" for k in s.SCHEMA] + ["_ingest_log", "metadata"]
        if all([rw.table_exists(t, s.DB_PATH) for t in table_list]):
            typer.echo("Tables already initialised.")
        else:
            initialize(
                db_path=s.DB_PATH,
                schema=s.SCHEMA
            )



@app.command()
def config(
    db_path: str = typer.Option(None, help="Set path to SQLite DB"),
    export_path: str = typer.Option(None, help="Set export directory"),
    show_current: bool = typer.Option(False, "--show-current", help="Show current settings")

):

    config_file = "config/config.ini"
    if show_current:

        typer.echo("Current configuration:")
        typer.echo(f"Database path: {s.DB_PATH}")
        typer.echo(f"Export path: {s.EXPORT_PATH}")
        raise typer.Exit()

    # update the config file if any parameters are passed and are valid
    try:
        updated = False
        if db_path:
            s.config_ini["DATABSE"]["db_path"] = db_path
            updated = True
        if export_path:
            u.check_path(export_path)
            s.config_ini["EXPORT"]["export_path"] = export_path
            updated = True

        # save new configs
        if updated:
            with open(config_file, "w") as f:
                s.config_ini.write(f)
    except Exception as e:
        typer.echo(f"Error: {e}")




@app.command()
def update(
    collection: str,
    tables: Optional[List[str]] = typer.Option(None, "--table", "-t", help="Table(s) to update")
):
    """
    Update specific tables or all tables in a collection.
    """
    try:
        if tables:
            typer.echo(f"Updating {tables} in {collection}...")
            update_tables(
                data_collection=collection,
                table_list=tables)
        else:
            typer.echo(f"Updating all tables in {collection}...")
            update_all_tables(data_collection=collection)
    except Exception as e:
        typer.echo(f"ERROR - execution terminated: {e}")


@app.command()
def stage(
    collection: str,
    as_of_date: Optional[str] = typer.Option(None, "--as_of_date", "--d", help="The cutoff point for data versioning.")
):
    """
    Stage the most recent data version for a collection.
    """
    typer.echo(f"Staging {collection} data...")
    stage_data(data_collection=collection, as_of_date=as_of_date)


@app.command()
def info(
    collection: str,
    table: Optional[str] = typer.Option(None, "--table", "-t", help="Optional table name to inspect")
):
    get_data_info(data_collection=collection,
                  table_name=str(table))

@app.command()
def versions(
        collection: str,
        table: Optional[str] = typer.Option(None, "--table", "-t", help="Optional table name to inspect")
):
    get_data_versions(data_collection=collection, table_name=table)


@app.command()
def export(
        collection: str,
        file_type: Optional[str] = typer.Option("csv", "--file-type", "-f", help="Format to use for export. Options are csv, parquet or xlsx, default is xsc"),
        table: Optional[str] = typer.Option(None, "--table", "-t", help="Optional table name to download"),
        path: Optional[str] = typer.Option(s.EXPORT_PATH, "--path", "-p", help="Optional destination path"),
        bulk: Optional[bool] = typer.Option(False, "--bulk", "-b", help="Whether to save all the data in a single file or not")
):
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




if __name__ == "__main__":
    app()