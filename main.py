import typer
from typing import Optional, List
from etl.process import *

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
    table: Optional[List[str]] = typer.Option(None, "--table", "-t", help="Table(s) to update"),
):
    get_data_info(data_collection=collection, table_name=table)

@app.command()
def versions(collection: str):
    get_data_versions(data_collection=collection)


if __name__ == "__main__":
    app()