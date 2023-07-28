import shutil
import sys
from pathlib import Path
from typing import Optional

import orjson
import typer
from prefect.settings import PREFECT_HOME
from rich import print
from smart_open import open
from typing_extensions import Annotated

from investigraph.inspect import inspect_config, inspect_extract, inspect_transform
from investigraph.model.block import get_block
from investigraph.model.dataset import Catalog
from investigraph.model.flow import FlowOptions
from investigraph.pipeline import run
from investigraph.settings import DATASETS_BLOCK, DATASETS_REPO

cli = typer.Typer()


@cli.command("run")
def cli_run(
    dataset: Annotated[Optional[str], typer.Option("-d")] = None,
    block: Annotated[Optional[str], typer.Option("-b")] = None,
    config: Annotated[Optional[str], typer.Option("-c")] = None,
    index_uri: Annotated[Optional[str], typer.Option(...)] = None,
    fragments_uri: Annotated[Optional[str], typer.Option(...)] = None,
    entities_uri: Annotated[Optional[str], typer.Option(...)] = None,
    aggregate: Annotated[Optional[bool], typer.Option(...)] = True,
    chunk_size: Annotated[Optional[int], typer.Option(...)] = None,
):
    """
    Execute a dataset pipeline
    """
    options = FlowOptions(
        dataset=dataset,
        block=block,
        config=config,
        index_uri=index_uri,
        fragments_uri=fragments_uri,
        entities_uri=entities_uri,
        aggregate=aggregate,
        chunk_size=chunk_size,
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


@cli.command("inspect")
def cli_inspect(
    config_path: Annotated[Path, typer.Argument()],
    extract: Annotated[Optional[bool], typer.Option()] = False,
    transform: Annotated[Optional[bool], typer.Option()] = False,
    to_json: Annotated[Optional[bool], typer.Option()] = False,
):
    config = inspect_config(config_path)
    print(f"[bold green]OK[/bold green] `{config_path}`")
    print(f"[bold]dataset:[/bold] {config.dataset.name}")
    print(f"[bold]title:[/bold] {config.dataset.title}")

    if extract:
        for name, df in inspect_extract(config):
            print(f"[bold green]OK[/bold green] {name}")
            if to_json:
                for _, row in df.iterrows():
                    print(
                        orjson.dumps(
                            row.to_dict(), option=orjson.OPT_APPEND_NEWLINE
                        ).decode()
                    )
            else:
                print(df.to_markdown(index=False))

    if transform:
        for name, proxies in inspect_transform(config):
            print(f"[bold green]OK[/bold green] {name}")
            for proxy in proxies:
                data = orjson.dumps(
                    proxy.to_dict(), option=orjson.OPT_APPEND_NEWLINE
                ).decode()
                print(data)


@cli.command("build-catalog")
def cli_catalog(
    path: Annotated[Path, typer.Argument()],
    uri: Annotated[str, typer.Option("-o")] = "-",
    flatten: Annotated[Optional[bool], typer.Option(...)] = False,
):
    """
    Build a catalog from datasets metadata and write it to anywhere from stdout
    (default) to any uri `smart_open` can handle, e.g.:

        investigraph build-catalog catalog.yml -u s3://mybucket/catalog.json
    """
    catalog = Catalog.from_path(path)
    if uri != "-":
        catalog.uri = uri
    if flatten:
        datasets = [d.dict() for d in catalog.get_datasets()]
        data = {
            "datasets": datasets,
            "catalog": catalog.metadata(),
        }
    else:
        data = catalog.dict()
    data = orjson.dumps(data, option=orjson.OPT_APPEND_NEWLINE)
    if uri == "-":
        sys.stdout.write(data.decode())
    else:
        with open(uri, "wb") as fh:
            fh.write(data)


@cli.command("reset")
def cli_reset(yes: Annotated[str, typer.Option(prompt="Are you sure? [yes/no]")]):
    """
    Reset all prefect data in `PREFECT_HOME`
    """
    if yes == "yes":
        path = PREFECT_HOME.value()
        shutil.rmtree(str(path), ignore_errors=True)
        print(f"[bold green]OK[/bold green] deleted everything in `{path}`.")
