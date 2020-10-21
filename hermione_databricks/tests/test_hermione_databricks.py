from click.testing import CliRunner
from hermione_databricks.cli import cli, logo
import os

def test_installation_ok():
    runner = CliRunner()
    res = runner.invoke(cli)
    assert res.exit_code == 0

def test_info():
    runner = CliRunner()
    res = runner.invoke(cli, ['info'])
    assert logo in res.output

def test_implementation_script_folders():
    assert os.path.exists(os.path.join(os.getcwd(), 'hermione_databricks', 'databricks_file_text'))
