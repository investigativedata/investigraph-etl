import shutil
import sys
from pathlib import Path
from typing import Annotated, Optional

import orjson
import typer
from anystore.io import smart_write
from ftmq.model import Catalog
from prefect.settings import PREFECT_HOME
from rich import print
from rich.console import Console
from rich.table import Table

from investigraph.inspect import inspect_config, inspect_extract, inspect_transform
from investigraph.model.flow import FlowOptions
from investigraph.pipeline import run
from investigraph.settings import VERSION

cli = typer.Typer(no_args_is_help=True)
console = Console()


@cli.callback(invoke_without_command=True)
def cli_version(
    version: Annotated[Optional[bool], typer.Option(..., help="Show version")] = False
):
    if version:
        print(VERSION)
        raise typer.Exit()


@cli.command("run")
def cli_run(
    config: Annotated[
        str,
        typer.Option("-c", help="Any local or remote json or yaml uri"),
    ],
    index_uri: Annotated[Optional[str], typer.Option(...)] = None,
    fragments_uri: Annotated[Optional[str], typer.Option(...)] = None,
    entities_uri: Annotated[Optional[str], typer.Option(...)] = None,
    aggregate: Annotated[Optional[bool], typer.Option(...)] = True,
    chunk_size: Annotated[Optional[int], typer.Option(...)] = 1_000,
):
    """
    Execute a dataset pipeline
    """
    options = FlowOptions(
        config=config,
        index_uri=index_uri,
        fragments_uri=fragments_uri,
        entities_uri=entities_uri,
        aggregate=aggregate,
        chunk_size=chunk_size,
    )
    run(options)


@cli.command("inspect")
def cli_inspect(
    config_path: Annotated[Path, typer.Argument()],
    extract: Annotated[Optional[bool], typer.Option("-e", "--extract")] = False,
    transform: Annotated[Optional[bool], typer.Option("-t", "--transform")] = False,
    limit: Annotated[Optional[int], typer.Option("-l", "--limit")] = 5,
    to_csv: Annotated[Optional[bool], typer.Option()] = False,
    to_json: Annotated[Optional[bool], typer.Option()] = False,
    usecols: Annotated[
        Optional[str],
        typer.Option(
            "-c",
            "--usecols",
            help="Comma separated list of column names or ix to display",
        ),
    ] = None,
):
    config = inspect_config(config_path)
    if not to_json and not to_csv:
        print(f"[bold green]OK[/bold green] `{config_path}`")
        print(f"[bold]dataset:[/bold] {config.dataset.name}")
        print(f"[bold]title:[/bold] {config.dataset.title}")

    if extract:
        for name, df in inspect_extract(config, limit):
            if usecols:
                df = df[[c for c in usecols.split(",") if c in df.columns]]
            if not to_json and not to_csv:
                print(f"[bold green]OK[/bold green] {name}")
            if to_json:
                for _, row in df.iterrows():
                    typer.echo(
                        orjson.dumps(row.to_dict(), option=orjson.OPT_APPEND_NEWLINE)
                    )
            elif to_csv:
                df.to_csv(sys.stdout, index=False)
            else:
                table = Table(*df.columns.map(str))
                df = df.fillna("").map(str)
                for _, row in df.iterrows():
                    table.add_row(*row.values)
                console.print(table)

    if transform:
        for name, proxies in inspect_transform(config, limit):
            if not to_json:
                print(f"[bold green]OK[/bold green] {name}")
            for proxy in proxies:
                data = proxy.to_dict()
                if not to_json:
                    print(data)
                else:
                    typer.echo(orjson.dumps(data))


@cli.command("build-catalog")
def cli_catalog(
    path: Annotated[Path, typer.Argument()],
    uri: Annotated[str, typer.Option("-o")] = "-",
    flatten: Annotated[Optional[bool], typer.Option(...)] = False,
):
    """
    Build a catalog from datasets metadata and write it to anywhere from stdout
    (default) to any uri `fsspec` can handle, e.g.:

        investigraph build-catalog catalog.yml -u s3://mybucket/catalog.json
    """
    catalog = Catalog.from_path(path)
    if flatten:
        datasets = [d.model_dump() for d in catalog.get_datasets()]
        data = {
            "datasets": datasets,
            "catalog": catalog.metadata(),
        }
    else:
        data = catalog.model_dump()
    data = orjson.dumps(data, option=orjson.OPT_APPEND_NEWLINE)
    if uri == "-":
        sys.stdout.write(data.decode())
    else:
        smart_write(uri, data)


@cli.command("reset")
def cli_reset(yes: Annotated[str, typer.Option(prompt="Are you sure? [yes/no]")]):
    """
    Reset all prefect data in `PREFECT_HOME`
    """
    if yes == "yes":
        path = PREFECT_HOME.value()
        shutil.rmtree(str(path), ignore_errors=True)
        print(f"[bold green]OK[/bold green] deleted everything in `{path}`.")
