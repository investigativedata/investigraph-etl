from pathlib import Path

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
    assert result.exit_code == 1


def test_cli_inspect(fixtures_path: Path):
    config = str(fixtures_path / "gdho" / "config.local.yml")
    result = runner.invoke(cli, ["inspect", config])
    assert result.exit_code == 0
    result = runner.invoke(cli, ["inspect", config, "--extract"])
    assert result.exit_code == 0
    result = runner.invoke(cli, ["inspect", config, "--transform"])
    assert result.exit_code == 0
