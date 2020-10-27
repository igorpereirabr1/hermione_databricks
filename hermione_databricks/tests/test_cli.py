from click.testing import CliRunner
from ..cli import cli, logo
from pathlib import Path


def test_installation_ok():
    runner = CliRunner()
    res = runner.invoke(cli)
    assert res.exit_code == 0


def test_info():
    runner = CliRunner()
    res = runner.invoke(cli, ["info"])
    assert logo in res.output


def test_implementation_script_folders():
    exist = Path().cwd().joinpath("hermione_databricks/databricks_file_text").exists()
    assert exist
