from pathlib import Path

import orjson
from ftmq.util import make_proxy
from typer.testing import CliRunner

from investigraph.cli import cli

runner = CliRunner()


def test_cli_base():
    result = runner.invoke(cli, "--help")
    assert result.exit_code == 0


def test_cli_run(fixtures_path: Path):
    config = str(fixtures_path / "gdho" / "config.local.yml")
    result = runner.invoke(cli, ["run", "-c", config])
    assert result.exit_code == 0

    # no arguments
    result = runner.invoke(cli, ["run"])
    assert result.exit_code > 0


def test_cli_inspect(fixtures_path: Path):
    config = str(fixtures_path / "gdho" / "config.local.yml")
    result = runner.invoke(cli, ["inspect", config])
    assert result.exit_code == 0
    result = runner.invoke(cli, ["inspect", config, "-e"])
    assert result.exit_code == 0
    result = runner.invoke(cli, ["inspect", config, "--extract"])
    assert result.exit_code == 0
    result = runner.invoke(cli, ["inspect", config, "-t"])
    assert result.exit_code == 0
    result = runner.invoke(cli, ["inspect", config, "--transform"])
    assert result.exit_code == 0

    result = runner.invoke(cli, ["inspect", config, "--transform", "--to-json"])
    assert result.exit_code == 0
    tested = False
    for line in result.stdout.split("\n"):
        data = orjson.loads(line)
        proxy = make_proxy(data)
        assert "gdho" in proxy.datasets
        tested = True
        break
    assert tested

    result = runner.invoke(
        cli, ["inspect", config, "--transform", "--to-json", "-l", "1"]
    )
    assert result.exit_code == 0
    assert len(result.stdout.split("\n")) == 1
