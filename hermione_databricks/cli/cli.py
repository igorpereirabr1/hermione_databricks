# -*- coding: utf-8 -*-

from hermione_databricks.api.sync import Sync
from hermione_databricks.api.resources import Config
from hermione_databricks.api.project import Project

import click
from click import ParamType
from pathlib import Path
import os
from databricks_cli.configure.provider import (
    DatabricksConfig,
    update_and_persist_config,
    ProfileConfigProvider,
)


from databricks_cli.utils import CONTEXT_SETTINGS
from databricks_cli.configure.config import (
    profile_option,
    get_profile_from_context,
    debug_option,
)

from hermione_databricks.__init__ import __version__ as version

logo = r"""
  _    _                     _                             
 | |  | |                   (_)                            
 | |__| | ___ _ __ _ __ ___  _  ___  _ __   ___            
 |  __  |/ _ \ '__| '_ ` _ \| |/ _ \| '_ \ / _ \           
 | |  | |  __/ |  | | | | | | | (_) | | | |  __/           
 |_|  |_|\___|_|  |_| |_| |_|_|\___/|_| |_|\___|         
          _____        _        _          _      _ 
         |  __ \      | |      | |        (_)    | |       
         | |  | | __ _| |_ __ _| |__  _ __ _  ___| | _____ 
         | |  | |/ _` | __/ _` | '_ \| '__| |/ __| |/ / __|
         | |__| | (_| | || (_| | |_) | |  | | (__|   <\__ \
 v{} |_____/ \__,_|\__\__,_|_.__/|_|  |_|\___|_|\_\___/
          
                                                       """.format(
    version
)


class NaturalOrderGroup(click.Group):
    def list_commands(self, ctx):
        return self.commands.keys()


class _DbfsHost(ParamType):
    """
    Used to validate the configured host
    """

    def convert(self, value, param, ctx):
        if value.startswith("https://"):
            return value
        else:
            self.fail("The host does not start with https://")


def _configure_cli_token(profile, insecure):
    PROMPT_HOST = "Databricks Host (should begin with https://)"
    PROMPT_TOKEN = "Token"  #  NOQA
    config = ProfileConfigProvider(profile).get_config() or DatabricksConfig.empty()
    host = click.prompt(PROMPT_HOST, default=config.host, type=_DbfsHost())
    token = click.prompt(PROMPT_TOKEN, default=config.token, hide_input=True)
    new_config = DatabricksConfig.from_token(host, token, insecure)
    update_and_persist_config(profile, new_config)


@click.group(cls=NaturalOrderGroup)
def cli():
    """
    Just Creeate the CLI and define commands order
    """
    pass


@cli.command()
def info():
    """
    Checks that hermione-databricks is correctly installed.
    """
    click.secho(logo, fg="yellow")


@cli.command(
    context_settings=CONTEXT_SETTINGS,
    short_help="Configures host and authentication info for Databricks CLI.",
)
@click.option("--token", show_default=True, is_flag=True, default=True)
@click.option("--insecure", show_default=True, is_flag=True, default=None)
@debug_option
@profile_option
def setup(token, insecure):
    """
    Configures host and authentication info for the Databricks CLI.
    """
    profile = get_profile_from_context()
    insecure_str = str(insecure) if insecure is not None else None
    if token:
        _configure_cli_token(profile, insecure_str)

    return None


@cli.command(short_help="Create a new the Databricks ML Project. ")
def new():
    """
    Create a new the Databricks ML Project, based on workspace and dbfs parameters
    """
    project_name = click.prompt("Project Name", default="My first Project")
    project_description = click.prompt("Project Description", default=project_name)
    project_workspace_path = click.prompt(
        "Databricks Host Workspace path (sample:/Users/xxxx@xxxxxxxx.com/)",
        default=None,
    )

    project_dbfs_path = click.prompt(
        "Databricks Host DBFS path (sample:dbfs:/Users/xxxx@xxxxxxxx.com/)",
        default=None,
    )

    # Now create local files
    click.echo(f"Creating project {project_name}")
    # Folders from Databricks Workspace
    project = Project(
        project_name=project_name,
        project_description=project_description,
        local_path=Path.cwd(),
        workspace_path=project_workspace_path,
        fs_path=project_dbfs_path,
    )

    project.create_local_project()
    project_config = Config(
        project_name=project._project_name,
        project_path=project._local_path,
        workspace_path=project._workspace_path,
        fs_path=project._fs_path,
    )
    project_config.create_config()

    sync = Sync(config_json=project_config._config_file_path, sync_type="push")

    sync.sync_project()

    # Create git repo
    os.chdir(Path.cwd().joinpath(project_name))
    os.system("git init")
    print(
        "A git repository was created. You should add your files and make your first commit.\n"
    )

    return None


@cli.command(short_help="Sync the remote changes to the local repository(pull).")
def sync_local():
    """
    Pull the remote changes to the local repository
    """
    if os.path.exists("config.json"):
        command = "databricks stack download config.json -o"
        os.system(command)
    else:
        click.echo("There is no config.json file available in the current path")

    return None


@cli.command(short_help="Sync the local changes to the remote repository(push).")
def sync_remote():
    """
    Push the local changes to the remote repository
    """
    if os.path.exists("config.json"):
        command = "databricks stack deploy config.json -o"
        os.system(command)
    else:
        click.echo("There is no config.json file available in the current path")

    return None