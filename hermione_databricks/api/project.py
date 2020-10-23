from databricks_cli.workspace.api import WorkspaceApi
from databricks_cli.configure.config import get_config
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.stack.api import StackApi
import hermione_databricks as herm
from pathlib import Path
import shutil
import codecs
import os
import json


def write_local_files(souce_path: str = None, dst_path: str = None, **kwargs: dict):
    """Function to write the local projet files, responsible also to customize them according with the
       project parameters provided by the user.

    Attributes:
        souce_path: Path to the original file.
        dst_path: Path where the custom file will be saved.
        kwargs: dict where the keys and values will be used in the replace function:
               for k, v in kwargs.items():
                   file = file.replace(k, v).
    """

    # Create local dir if not exist
    os.makedirs(os.path.dirname(dst_path), exist_ok=True)

    # Open the source txt file
    with codecs.open(souce_path, "r", "utf-8") as infile:
        file = infile.read()
    # Iterate over the words that should be replaced in the text file
    for k, v in kwargs.items():
        file = file.replace(k, v)
    # Save the final file
    with codecs.open(dst_path, "w+", "utf-8") as outfile:
        outfile.writelines(file)


class Project(object):
    """Class to manage Projects using hermione-databricks.

    Attributes:
        project_name: Project name.
        project_description: A Nice project description.
        local_path: Local path where the project will be created and the git will be configured.
        workspace_path: Databricks workspace path where the workspace files will be stored.
        fs_path:  dbfs path where the project artifacts, like mlflow experiments and models will be saved.
        project_template_path: You can choice this option if you want use a personal project templace structure.
    """

    def __init__(
        self,
        project_name: str = None,
        project_description: str = None,
        local_path: str = None,
        workspace_path: str = None,
        fs_path: str = None,
        project_template_path: str = None,
    ):

        self.project_name = project_name
        self.project_description = project_description
        self.local_path = local_path
        self.workspace_path = workspace_path
        self.fs_path = fs_path
        self.project_template_path = project_template_path

        return None

    @property
    def project_name(self) -> str:
        """project_name: Project name."""
        return self._project_name

    @project_name.setter
    def project_name(self, value: str):
        input_value = value or self.project_name
        if input_value is None:
            raise ValueError("Config 'project_name' cannot be empty.")
        self._project_name = input_value

    @property
    def project_description(self) -> str:
        """project_description: project_description"""
        return self._project_description

    @project_description.setter
    def project_description(self, value: str):
        input_value = value or self.project_description
        if input_value is None:
            raise ValueError("Config 'project_description' cannot be empty.")
        self._project_description = input_value

    @property
    def local_path(self) -> str:
        """Local path where the project will be created and the git will be configured."""
        return self._local_path

    @local_path.setter
    def local_path(self, value: str):
        if value is None:
            input_value = Path.cwd().joinpath(self.project_name)
        else:
            input_value = Path(value).joinpath(self.project_name)
        self._local_path = input_value

    @property
    def workspace_path(self) -> str:
        """Databricks workspace path where the workspace files will be stored."""
        return self._workspace_path

    @workspace_path.setter
    def workspace_path(self, value: str):
        input_value = value or self.workspace_path
        if input_value is None:
            raise ValueError("Config 'workspace_path' cannot be empty.")
        self._workspace_path = Path(input_value).joinpath(self.project_name).as_posix()

    @property
    def fs_path(self) -> str:
        """FileSystem path where is the project data, artifacts, models, mlflow experiments should be saved."""
        return self._fs_path

    @fs_path.setter
    def fs_path(self, value: str):
        input_value = value or self.fs_path
        if input_value is None:
            raise ValueError("Config 'fs_path' cannot be empty.")
        self._fs_path = Path(input_value).joinpath(self.project_name).as_posix()

    @property
    def project_template_path(self) -> str:
        """Path with a custom project template"""
        return self._project_template_path

    @project_template_path.setter
    def project_template_path(self, value: str):
        input_value = value
        if input_value is None:
            input_value = Path(herm.__path__[0]).joinpath("databricks_file_text")
        self._project_template_path = input_value

    def create_local_project(self):

        """Function to create the local files:
        .Current Dir
        ├── project_name
        |   ├── README.ipynb
        |   ├── config.json
        |   ├── notebooks
        |   |   └── exploratory_analysis.ipynb
        |   ├── preprocessing
        |   |   └── preprocessing.ipynb
        |   └── model
        |       └── model.ipynb
        |
        ├── FileSystem
        |   ├── artifacts
        |       └── config.json
        |   └── data
        |       └── raw
        |       └── feature
        |       └── ml_input
        |       └── ml_output
        """
        # Check if the project aready exist
        if self._local_path.exists():
            raise FileExistsError(
                "The project path:{} aready exist, please delete or create a new one".format(
                    self._local_path
                )
            )
        else:
            # Create a new project folder based on the project template
            shutil.copytree(self._project_template_path, self._local_path)
            # Iterate over the existent project files to customize them
            for subdir, dirs, files in os.walk(self._local_path):
                for file in files:
                    temp_path = Path(subdir).joinpath(file).as_posix()
                    kwargs = {
                        "project_name": self._project_name,
                        "project_description": self._project_description,
                        "project_local_path": self._local_path.as_posix(),
                        "project_workspace_path": self._workspace_path,
                        "project_fs_path": self._fs_path,
                        "model_input_path": Path(self._fs_path)
                        .joinpath("FileSystem/data/ml_input")
                        .as_posix(),
                        "model_output_path": Path(self._fs_path)
                        .joinpath("FileSystem/data/ml_output")
                        .as_posix(),
                        "model_artifacts_path": Path(self._fs_path)
                        .joinpath("FileSystem/artifacts/")
                        .as_posix(),
                    }

                    with codecs.open(temp_path, "r", "utf-8") as infile:
                        file = infile.read()
                    for k, v in kwargs.items():
                        file = file.replace(k, v)
                    with codecs.open(temp_path, "w+", "utf-8") as outfile:
                        outfile.writelines(file)

        return None