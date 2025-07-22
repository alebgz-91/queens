import typer
from etl.process import update_tables, update_all_tables, stage_data

app = typer.Typer()

@app.command()
def update(
    collection: str = typer.Argument(...,
                                     help="Data collection name (e.g. 'dukes')"),
    tables: list[str] = typer.Option(None,
                                     "--table",
                                     "-t",
                                     help="Table(s) to update")
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
    collection: typer.Argument(...,
                               help="Data collection name (e.g. 'dukes')"),
    as_of_data: typer.Option(None,
                             "--as_of_date",
                             "--d",
                             help="The cutoff point for data versioning.")
):
    typer.echo(f"Staging {collection} data...")
    stage_date(data_collection=collection, as_of_date=as_of_date)

if __name__ == "__main__":
    app()