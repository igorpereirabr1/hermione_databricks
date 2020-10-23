from pathlib import Path
import json
import os


class Templates:
    """Class with the templates for the project Resources(files,folders) using hermione-databricks.

    Attributes:
        config_path: path to project Resources (config.json) file
    """

    def __init__(
        self, resource_id: str = None, source_path: str = None, target_path: str = None
    ):

        self.resource_id = resource_id
        self.source_path = source_path
        self.target_path = target_path

        return None

    @property
    def resource_id(self) -> str:
        """resource_id: must be a str"""
        return self._resource_id

    @resource_id.setter
    def resource_id(self, value: str = None):
        input_value = value or self.resource_id
        if input_value is None:
            raise ValueError("resource_id: must be a str and cannot be empty.")
        elif len(input_value) > 30:
            raise ValueError(
                "resource_id: Invalid str length,current:{} maximum allowed:30 ".format(
                    len(input_value)
                )
            )
        self._resource_id = input_value.lower()

    @property
    def source_path(self) -> str:
        """source_path: must be a str"""
        return self._source_path

    @source_path.setter
    def source_path(self, value: str = None):
        input_value = value or self.source_path
        if input_value is None:
            raise ValueError("source_path: must be a path str and cannot be empty.")
        self._source_path = input_value.lower()

    @property
    def target_path(self) -> str:
        """target_path: must be a str"""
        return self._target_path

    @target_path.setter
    def target_path(self, value: str = None):
        input_value = value or self.target_path
        if input_value is None:
            raise ValueError("target_path: must be a path str and cannot be empty.")
        self._target_path = input_value.lower()

    def _notebook_resource_template(self):

        """Create a Notebook resource"""

        template = {
            "id": "workspace-{}-NOTEBOOK".format(self._resource_id),
            "service": "workspace",
            "properties": {
                "source_path": self._source_path,
                "path": self._target_path,
                "object_type": "NOTEBOOK",
            },
        }

        self._template = template
        return self._template

    def _fs_resource_template(self):
        """Create a DBFS resource"""

        template = {
            "id": "workspace-{}-FileSystem".format(self._resource_id),
            "service": "dbfs",
            "properties": {
                "source_path": self._source_path,
                "path": self._target_path,
                "is_dir": True,
            },
        }
        self._template = template
        return self._template


class Config:
    """Class to create a new config.json file based on an existent list of resources

    Attributes:
        project_path: Path to an existent project
        workspace_path: Databricks workspace path where the workspace files will be stored.
        fs_path:  dbfs path where the project artifacts, like mlflow experiments and models will be saved.
    """

    def __init__(
        self,
        project_name: str = None,
        project_path: str = None,
        workspace_path: str = None,
        fs_path: str = None,
    ):
        self.project_name = project_name
        self.project_path = project_path
        self.workspace_path = workspace_path
        self.fs_path = fs_path
        return None

    @property
    def project_name(self):
        return self._project_name

    @project_name.setter
    def project_name(self, value: str = None):
        input_value = value or self.project_name
        if input_value is None:
            raise ValueError("The project_name cannot be empty.")
        self._project_name = input_value

    @property
    def project_path(self):
        return self._project_path

    @project_path.setter
    def project_path(self, value: str = None):
        input_value = value or self.project_path
        if input_value is None:
            raise ValueError("The project_path cannot be empty.")
        elif input_value.exists() == False:
            raise FileExistsError(
                "The project path does not exist:{}".format(input_value)
            )
        self._project_path = input_value

    @property
    def workspace_path(self) -> str:
        """Databricks workspace path where the workspace files will be stored."""
        return self._workspace_path

    @workspace_path.setter
    def workspace_path(self, value: str):
        input_value = value or self.workspace_path
        if input_value is None:
            raise ValueError("Config 'workspace_path' cannot be empty.")
        self._workspace_path = Path(input_value).joinpath(self._project_name).as_posix()

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

    def create_config(
        self,
    ):

        """Function to create a new (config.json) file

        Attributes:
            config_path: path to project Resources (config.json) file
        """
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

        for root, subdirectories, files in os.walk(self._project_path):
            for file in files:
                file_path = Path(root).joinpath(file)
                # Check if the file shold be send to the workspace
                if file_path.suffix in workspace_types and file_path.is_file():
                    resource_id = file_path.stem
                    source_path = file_path.as_posix()
                    dest_path = source_path.replace(
                        self._project_path.as_posix(), self._workspace_path
                    )
                    # Create a new reource based in the workspace template
                    template = Templates(resource_id, source_path, dest_path)
                    resource = template._notebook_resource_template()
                    resources.append(resource)

        for root, subdirectories, files in os.walk(
            self._project_path.joinpath("FileSystem")
        ):
            for subdirectory in subdirectories:
                subpath = Path(root).joinpath(subdirectory)
                resource_id = subpath.stem
                source_path = subpath.as_posix()
                dest_path = source_path.replace(
                    self._project_path.as_posix(), self._fs_path
                )
                # Create a new reource based in the workspace template
                template = Templates(resource_id, source_path, dest_path)
                resource = template._fs_resource_template()
                resources.append(resource)
        # Define the config.json desttination path
        self._config_file_path = self._project_path.joinpath(
            "FileSystem/artifacts/config.json"
        ).as_posix()

        # Create the config.json based on the resources
        self._json_config = {"name": self.project_name, "resources": resources}

        # Write the json config file
        with open(self._config_file_path, "w", encoding="utf-8") as f:
            json.dump(self._json_config, f, ensure_ascii=False, indent=4)

        return None

    def update_config(self):

        """Function to update an existent project Resources (config.json) file

        Attributes:
            config_path: path to project Resources (config.json) file
        """

        return None

    def delete_config(self):

        """Function to delete an existent project Resources (config.json) file

        Attributes:
            config_path: path to project Resources (config.json) file
        """

        return None


class Resources:
    """Class to create,update/delete new project Resources(files,folders) using hermione-databricks.

    Attributes:
        config_path: path to project Resources (config.json) file
    """

    def __init__(self, config_path: str = None):
        self.config_path = config_path
        return None

    @property
    def config_path(self):
        """config_path: must be a valid path to a config.json file"""
        return self._config_path

    @config_path.setter
    def config_path(self, value: str = None):
        input_value = (
            value
            or self.config_path
            or Path.cwd().joinpath("config.json")
            or Path.cwd().joinpath("FileSystem/artifacts/config.json")
        )
        if input_value is None:
            raise ValueError("config_path cannot be empty.")
        elif input_value.exists() == False:
            raise FileExistsError(
                "There is no config.json file in this path, or project forlder:{}".format(
                    input_value
                )
            )
        self._config_path = input_value

    @property
    def config_file(self) -> json.loads:
        """config_file: must be a valid json file"""
        return self._config_json

    @config_file.setter
    def config_file(self, value: str = None) -> json.loads:
        f = open(self._config_path, "r").read()
        try:
            input_value = json.loads(value) or json.loads(f)
        except json.JSONDecodeError as error:
            raise ValueError("Invalid json file") from error

        self._config_json = input_value
