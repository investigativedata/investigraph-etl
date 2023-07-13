from pathlib import Path

from typer.testing import CliRunner

from investigraph.cli import cli
from investigraph.settings import DATASETS_REPO

runner = CliRunner()


def test_cli_base():
    result = runner.invoke(cli, "--help")
    assert result.exit_code == 0


def test_cli_run(fixtures_path: Path):
    config = str(fixtures_path / "gdho" / "config.local.yml")
    result = runner.invoke(cli, ["run", "gdho", "-c", config])
    assert result.exit_code == 0


def test_cli_add_block():
    result = runner.invoke(
        cli, ["add-block", "-b", "local-file-system/testdata", "-u", "/"]
    )
    assert result.exit_code == 0
    result = runner.invoke(
        cli, ["add-block", "-b", "github/testdata", "-u", DATASETS_REPO]
    )
    assert result.exit_code == 0


def test_cli_inspect(fixtures_path: Path):
    config = str(fixtures_path / "gdho" / "config.local.yml")
    result = runner.invoke(cli, ["inspect", config])
    assert result.exit_code == 0
    result = runner.invoke(cli, ["inspect", config, "--extract"])
    assert result.exit_code == 0
    result = runner.invoke(cli, ["inspect", config, "--transform"])
    assert result.exit_code == 0
