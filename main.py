import typer
from typing import Optional, List

from param import output

from etl.process import *
from etl.input_output import export_all, export_table
from config.settings import EXPORT_PATH

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
        format: Optional[str] = typer.Option(None, "--table", "-t", help="Format to use for export. Options are csv, parquet or xlsx, default is xsc"),
        table: Optional[str] = typer.Option(None, "--table", "-t", help="Optional table name to download"),
        path: Optional[str] = typer.Option(EXPORT_PATH, "--path", "-p", help="Optional destination path"),
        bulk: Optional[bool] = typer.Option(False, "--bulk", "-b", help="Whether to save all the data in a single file or not")
):
    if table:
        typer.echo(f"Exporting {collection} table {table}...")
        export_table(
            data_collection=collection,
            file_type=format,
            output_path=path,
            table_name=table
        )
    else:
        bulk_str = " as a single file" if bulk else ""
        typer.echo(f"Exporting all tables in {collection}{bulk_str}---")
        export_all(
            data_collection=collection,
            file_type=format,
            output_path=path,
            bulk_export=bulk
        )




if __name__ == "__main__":
    app()