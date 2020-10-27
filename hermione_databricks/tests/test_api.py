from ..api.project import Project
from ..api.resources import Resources, Templates, Config
from ..api.sync import Sync
from pathlib import Path
import shutil


def new_project():
    # Delete old test project
    try:
        shutil.rmtree(Path.cwd().joinpath("project_test"))
    except OSError:
        pass

    project = Project(
        project_name="project_test",
        project_description="project_description",
        local_path=Path.cwd(),
        workspace_path="/Users/xxxx@xxxxxxxx.com/",
        fs_path="dbfs:/Users/xxxx@xxxxxxxx.com/",
    )

    project.create_local_project()

    test_passed = Path.cwd().joinpath(project._project_name).exists()

    assert test_passed


def project_config():

    project_config = Config(
        project_name="project_test",
        project_path=Path.cwd(),
        workspace_path="/Users/xxxx@xxxxxxxx.com/",
        fs_path="dbfs:/Users/xxxx@xxxxxxxx.com/",
    )
    project_config.create_config()

    test_passed = Path(project_config._config_file_path).exists()

    try:
        shutil.rmtree(Path.cwd().joinpath("project_test"))
    except OSError:
        pass

    assert test_passed