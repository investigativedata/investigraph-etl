import typer

from investigraph.pipeline import run as run_etl

cli = typer.Typer()


@cli.command()
def run(dataset: str):
    """
    Execute a dataset pipeline
    """
    run_etl(dataset)
