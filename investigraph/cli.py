import shutil
from typing import Optional

import typer
from prefect.settings import PREFECT_HOME
from rich import print
from typing_extensions import Annotated

from investigraph.block import get_block
from investigraph.model import FlowOptions
from investigraph.pipeline import run
from investigraph.settings import DATASETS_BLOCK, DATASETS_REPO

cli = typer.Typer()


@cli.command("run")
def cli_run(
    dataset: str,
    block: Annotated[Optional[str], typer.Option("-b")] = None,
    config: Annotated[Optional[str], typer.Option("-c")] = None,
    fragments_uri: Annotated[Optional[str], typer.Option(...)] = None,
    entities_uri: Annotated[Optional[str], typer.Option(...)] = None,
    aggregate: Annotated[Optional[bool], typer.Option(...)] = True,
):
    """
    Execute a dataset pipeline
    """
    options = FlowOptions(
        dataset=dataset,
        block=block,
        config=config,
        fragments_uri=fragments_uri,
        entities_uri=entities_uri,
        aggregate=aggregate,
    )
    run(options)


@cli.command("add-block")
def cli_add_block(
    block: Annotated[
        str,
        typer.Option(
            "-b",
            prompt=f"Datasets configuration block, for example: {DATASETS_BLOCK}",
        ),
    ],
    uri: Annotated[
        str, typer.Option("-u", prompt=f"Block source uri, example: {DATASETS_REPO}")
    ],
):
    """
    Configure a datasets block (currently only github and local filesystem supported.)
    """
    block = get_block(block)
    try:
        block.register(uri)
        print(f"[bold green]OK[/bold green] block `{block}` created.")
    except ValueError as e:
        if "already in use" in str(e):
            print(f"[bold red]Error[/bold red] block `{block}` already existing.")
        else:
            raise e


@cli.command("reset")
def cli_reset(yes: Annotated[str, typer.Option(prompt="Are you sure? [yes/no]")]):
    """
    Reset all prefect data in `PREFECT_HOME`
    """
    if yes == "yes":
        path = PREFECT_HOME.value()
        shutil.rmtree(str(path), ignore_errors=True)
        print(f"[bold green]OK[/bold green] deleted everything in `{path}`.")
