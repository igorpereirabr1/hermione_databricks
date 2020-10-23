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

    def _config_json_template(self, template_name: str = None, resources: list = []):
        """Function to create a new (empty) config.json template

        Attributes:
            template_name: Name of the remplate
            resources: List of resources from this template
        """
        template = {"name": template_name, "resources": resources}

        return template


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

    def create_config(self, project_path: str = None):

        """Function to create a new project Resources (config.json) file

        Attributes:
            project_path: path to project
        """
        _project_path = Path(project_path)
        if _project_path.exists() == False:
            raise FileExistsError("Invalid project parth:{}".format(_project_path))

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
