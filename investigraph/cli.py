from typing import Optional

import typer
from typing_extensions import Annotated

from investigraph.model import FlowOptions
from investigraph.pipeline import run

cli = typer.Typer()


@cli.command("run")
def cli_run(
    dataset: str,
    fragments_uri: Annotated[Optional[str], typer.Option(...)] = None,
    entities_uri: Annotated[Optional[str], typer.Option(...)] = None,
    aggregate: Annotated[Optional[bool], typer.Option(True)] = True,
):
    """
    Execute a dataset pipeline
    """
    options = FlowOptions(
        dataset=dataset,
        fragments_uri=fragments_uri,
        entities_uri=entities_uri,
        aggregate=aggregate,
    )
    run(options)
