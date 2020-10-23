from click.testing import CliRunner
from hermione_databricks.cli import cli, logo
from pathlib import Path
from hermione_databricks.api.sync import Sync
from hermione_databricks.api.resources import Config
from hermione_databricks.api.project import Project
import shutil


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

def test_create_new_project():
    #Delete old test project
    try:
        shutil.rmtree(Path.cwd().joinpath("project_test"))
    except OSError as e:
        pass

    project = Project(
        project_name="project_test",
        project_description="project_description",
        local_path=Path.cwd(),
        workspace_path="/Users/xxxx@xxxxxxxx.com/",
        fs_path="dbfs:/Users/xxxx@xxxxxxxx.com/",
    )

    project.create_local_project()
    
    test_ok = Path.cwd().joinpath(project._project_name).exists()

    try:
        shutil.rmtree(Path.cwd().joinpath("project_test"))
    except OSError as e:
        pass

    assert test_ok


def test_project_config():

    #Delete old test project
    try:
        shutil.rmtree(Path.cwd().joinpath("project_test"))
    except OSError as e:
        pass
    

    project = Project(
        project_name="project_test",
        project_description="project_description",
        local_path=Path.cwd(),
        workspace_path="/Users/xxxx@xxxxxxxx.com/",
        fs_path="dbfs:/Users/xxxx@xxxxxxxx.com/",
    )

    project.create_local_project()

    project_config = Config(
        project_name=project._project_name,
        project_path=project._local_path,
        workspace_path=project._workspace_path,
        fs_path=project._fs_path,
    )
    project_config.create_config()

    test_ok = Path(project_config._config_file_path).exists()

    try:
        shutil.rmtree(Path.cwd().joinpath("project_test"))
    except OSError as e:
        pass

    assert test_ok


