from databricks_cli.workspace.api import WorkspaceApi
from databricks_cli.configure.config import get_config
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.stack.api import StackApi
import json


class Sync:
    """Class to manage Sync project/files using hermione-databricks.

    Attributes:
        config_json: path to a config.json project file
        sync_type: push/pull the entire project
    """

    def __init__(self, config_json: str = None, sync_type: str = None):
        self.config_json = config_json
        self.sync_type = sync_type
        return None

    @property
    def config_json(self) -> json.loads:
        """config_json: must be a json documment or path"""
        return self._config_json

    @config_json.setter
    def config_json(self, value: str):
        if value is None:
            input_value = json.loads(open(self.config_json, "r").read())
        else:
            input_value = json.loads(open(value, "r").read())

        if input_value is None:
            raise ValueError(
                "config_json must be a path or a json documment but cannot be empty."
            )
        self._config_json = input_value

    @property
    def sync_type(self) -> str:
        """sync_type: push/pull the entire project"""
        return self._sync_type

    @sync_type.setter
    def sync_type(self, value: str):
        input_value = value or self.sync_type
        if input_value is None:
            raise ValueError("sync_type cannot be empty.")
        if input_value not in ["push", "pull"]:
            raise ValueError("sync_type should be push or pull")
        self._sync_type = input_value

    def sync_project(self):

        # Define the databricks configuration
        config = get_config()
        client = ApiClient(host=config.host, token=config.token)
        stack_api = StackApi(client)
        # project config file

        if self._sync_type == "push":
            stack_api.deploy(
                stack_config=self._config_json, stack_status=None, headers=None
            )
        elif self._sync_type == "pull":
            stack_api.download(stack_config=self._config_json, headers=None)

        return None