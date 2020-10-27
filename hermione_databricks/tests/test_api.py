from ..api.project import Project
from ..api.resources import Resources, Templates, Config
from ..api.sync import Sync
from pathlib import Path
import shutil


def test_new_project():

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

    project_config = Config(
        project_name=project._project_name,
        project_path=project._local_path,
        workspace_path=project._workspace_path,
        fs_path=project._fs_path,
    )
    project_config.create_config()

    test_passed = Path(project_config._config_file_path).exists()

    try:
        shutil.rmtree(Path.cwd().joinpath("project_test"))
    except OSError:
        pass

    assert test_passed