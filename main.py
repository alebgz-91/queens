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

from etl.process import *
import src.read_write as rw
import config.settings as s

app = typer.Typer()

@app.command()
def update(
    collection: str,
    tables: Optional[List[str]] = typer.Option(None, "--table", "-t", help="Table(s) to update")
):
    """
    Update specific tables or all tables in a collection.
    """
    if tables:
        typer.echo(f"Updating {tables} in {collection}...")
        update_tables(
            data_collection=collection,
            table_list=tables)
    else:
        typer.echo(f"Updating all tables in {collection}...")
        update_all_tables(data_collection=collection)


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