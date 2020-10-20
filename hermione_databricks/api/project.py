from databricks_cli.workspace.api import WorkspaceApi
from databricks_cli.configure.config import get_config
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.stack.api import StackApi
from hermione_databricks.project.writer import write_local_files
import hermione_databricks as herm
from pathlib import Path
import shutil
import codecs
import os
import json

class Sync():
    """sync_type: push/pull the entire project"""
    def __init__(self,config_json:str = None,sync_type: str = None):
        self._config_json = config_json
        self.sync_type = sync_type
        return None

    @property
    def config_json(self) -> json:
        """config_json: must be a json documment or path"""
        return self._config_json

    @config_json.setter
    def config_json(self, value: str):
        input_value = value or json.loads(open(self.config_json, "r").read())
        if input_value is None:
            raise ValueError("config_json must be a path or a json documment but cannot be empty.")
        self._sconfig_json = input_value

    @property
    def sync_type(self) -> str:
        """sync_type: push/pull the entire project"""
        return self._sync_type

    @sync_type.setter
    def sync_type(self, value: str):
        input_value = value or self.sync_type
        if input_value is None:
            raise ValueError("sync_type cannot be empty.")
        if input_value not in ["push","pull"]:
            raise ValueError("sync_type should be push or pull")
        self._sync_type = input_value

    def sync_project(self):

        # Define the databricks configuration
        config = get_config()
        client = ApiClient(host=config.host, token=config.token)
        stack_api = StackApi(client)
        # project config file

        if self._sync_type=="push":
            stack_api.deploy(
            stack_config=self._config_json,
            stack_status=None,
            headers=None)
        elif self._sync_type=="pull":
            stack_api.download(
            stack_config=self._config_json,
            headers=None)

        return None



class Project(object):
    """Configuration for a new Project using hermione-databricks.

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

    def __notebook_resource_template(
        self, resource_id: str = None, source_path: str = None, target_path: str = None
    ):

        """Create a Notebook resource template"""

        template = {
            "id": "workspace-{}-NOTEBOOK".format(resource_id).lower(),
            "service": "workspace",
            "properties": {
                "source_path": source_path,
                "path": target_path,
                "object_type": "NOTEBOOK",
            },
        }
        return template

    def __file_resource_template(
        self, resource_id: str = None, source_path: str = None, target_path: str = None
    ):
        """Create a Notebook resource template"""

        template = {
            "id": "workspace-{}-FileSystem".format(resource_id).lower(),
            "service": "dbfs",
            "properties": {
                "source_path": source_path,
                "path": target_path,
                "is_dir": True,
            },
        }
        return template

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

    def _create_config_file(self):
        workspace_types = [
            ".dbc",
            ".scala",
            ".py",
            ".sql",
            ".r",
            ".ipynb",
            ".Rmd",
            ".html",
        ]

        resources = []

        for root, subdirectories, files in os.walk(self._local_path):
            for file in files:
                file_path = Path(root).joinpath(file)
                # Check if the file shold be send to the workspace
                if file_path.suffix in workspace_types and file_path.is_file():
                    resource_id = file_path.stem
                    source_path = file_path.as_posix()
                    dest_path = source_path.replace(
                        self._local_path.as_posix(), self._workspace_path
                    )
                    # Create a new reource based in the workspace template
                    resource = self.__notebook_resource_template(
                        resource_id, source_path, dest_path
                    )
                    resources.append(resource)

        for root, subdirectories, files in os.walk(
            self._local_path.joinpath("FileSystem")
        ):
            for subdirectory in subdirectories:
                subpath = Path(root).joinpath(subdirectory)
                resource_id = subpath.stem
                source_path = subpath.as_posix()
                dest_path = source_path.replace(
                    self._local_path.as_posix(), self._fs_path
                )
                # Create a new reource based in the workspace template
                resource = self.__file_resource_template(
                    resource_id, source_path, dest_path
                )
                resources.append(resource)

        self._json_config = {"name": self._project_name, "resources": resources}
        config_file_path = self._local_path.joinpath(
            "FileSystem/artifacts/config.json"
        ).as_posix()
        # Create the json config file
        with open(config_file_path, "w", encoding="utf-8") as f:
            json.dump(self._json_config, f, ensure_ascii=False, indent=4)

        return None

    def _sync_new_project(self):
        """Function to push a new local project to Databricks:
        .workspace path
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
        ├── FileSystem(DBFS)
        |   ├── artifacts
        |       └── config.json
        |   └── data
        |       └── raw
        |       └── feature
        |       └── ml_input
        |       └── ml_output
        """

        # Define the databricks configuration
        config = get_config()
        client = ApiClient(host=config.host, token=config.token)
        stack_api = StackApi(client)
        # project config file
        stack_api.deploy(
            stack_config=self._json_config,
            stack_status=None,
            headers=None,
        )

        return None