from pathlib import Path
import json
import os


class Resources:
    def __init__(self):

        return None

    def _notebook_resource_template(
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

    def _fs_resource_template(
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