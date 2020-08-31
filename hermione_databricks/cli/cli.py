import click
from click import ParamType
import os
import sys
from databricks_cli.configure.provider import DatabricksConfig, update_and_persist_config, ProfileConfigProvider
from databricks_cli.utils import CONTEXT_SETTINGS
from databricks_cli.configure.config import profile_option, get_profile_from_context, debug_option
from databricks_cli.workspace.api import WorkspaceApi
from hermione_databricks.project.writer import *
from hermione_databricks.__init__ import __version__ as version
import hermione_databricks


LOCAL_PATH = os.getcwd()
lib_path = hermione_databricks.__path__[0]
databricks_files_path = os.path.join(lib_path,"databricks_file_text")

logo = r"""
 _                         _                  
| |__   ___ _ __ _ __ ___ (_) ___  _ __   ___ 
| '_ \ / _ \ '__| '_ ` _ \| |/ _ \| '_ \ / _ \
| | | |  __/ |  | | | | | | | (_) | | | |  __/
|_| |_|\___|_|  |_| |_| |_|_|\___/|_| |_|\___|
v{}
""".format(version)



def _configure_cli_token(profile, insecure):
    PROMPT_HOST = 'Databricks Host (should begin with https://)'
    PROMPT_TOKEN = 'Token' #  NOQA
    config = ProfileConfigProvider(profile).get_config() or DatabricksConfig.empty()
    host = click.prompt(PROMPT_HOST, default=config.host, type=_DbfsHost())
    token = click.prompt(PROMPT_TOKEN, default=config.token, hide_input=True)
    new_config = DatabricksConfig.from_token(host, token, insecure)
    update_and_persist_config(profile, new_config)


@click.group()
def cli():
    pass


@cli.command()
def info():
    """
    Checks that hermione is correctly installed
    """
    click.echo(logo)

@cli.command(context_settings=CONTEXT_SETTINGS,
               short_help='Configures the Databricks CLI.')
@click.option('--token', show_default=True, is_flag=True, default=True)
@click.option('--insecure', show_default=True, is_flag=True, default=None)
@debug_option
@profile_option
def databricks_config(token, insecure):
    """
    Configures host and authentication info for the Databricks CLI.
    """
    profile = get_profile_from_context()
    insecure_str = str(insecure) if insecure is not None else None
    if token:
        _configure_cli_token(profile, insecure_str)
    else:
        pass

@cli.command(short_help='Create a new the Databricks ML Project. ')
def new_databricks_project():
    """
    Create a new the Databricks ML Project, based on workspace and dbfs parameters
    """
    project_name = click.prompt("Project Name", default="My first Project")
    project_description = click.prompt("Project Description", default=project_name)
    project_workspace_path = click.prompt("Databricks Host Workspace path (sample:/Users/xxxx@xxxxxxxx.com/MyFirstProject)", default=None)
    project_dbfs_path = click.prompt("Databricks Host DBFS path (sample:dbfs:/Users/xxxx@xxxxxxxx.com/MyFirstProject)", default=project_workspace_path)
    project_local_path = os.path.join(LOCAL_PATH, project_name)
    # Now create local files
    click.echo(f"Creating project {project_name}")
    # Folders from Databricks Workspace
    os.makedirs(os.path.join(project_local_path,'model'))#Databricks Workspace
    os.makedirs(os.path.join(project_local_path,'notebooks'))#Databricks Workspace

    # Start to create the project files
    
    # Readme file
    souce_path = os.path.join(databricks_files_path,"README.txt")
    dst_path = os.path.join(project_local_path,'README.ipynb')
    kwargs = {"project_name":project_name
            ,"project_description":project_description
            ,"project_workspace_path":project_workspace_path
            ,"project_dbfs_path":project_dbfs_path}

    write_local_files(souce_path,dst_path,**kwargs)

    # preprocessing Notebook
    souce_path = os.path.join(databricks_files_path,"preprocessing.txt")
    dst_path = os.path.join(project_local_path,'preprocessing/preprocessing.ipynb')
    kwargs = {"project_name":project_name
            ,"model_input_path":os.path.join(project_dbfs_path,'model/input/')
            ,"model_output_path":os.path.join(project_dbfs_path,'model/output/')
            ,"model_artifacts_path":os.path.join(project_dbfs_path,'model/artifacts/')}

    write_local_files(souce_path,dst_path,**kwargs)

    # project config file
    souce_path = os.path.join(databricks_files_path,"stack_configuration.json")
    dst_path = os.path.join(project_local_path,'stack_configuration.json')
    kwargs = {"project_name":project_name,
          "project_local_path":project_local_path,
          "project_workspace_path":project_workspace_path,
          "project_dbfs_path":project_dbfs_path
         }

    write_local_files(souce_path,dst_path,**kwargs)

    #Upload project to Databricks

    workspace_command = "databricks workspace import_dir "+project_local_path+" "+os.path.join(project_workspace_path,project_name) 
    dbfs_command = "databricks workspace import_dir "+project_local_path+" "+os.path.join(project_workspace_path,project_name) 
    os.system(workspace_command)
    for path in ["artifacts","input","output"]:
        dbfs_path = os.path.join(project_dbfs_path,project_name,"model",path)
        dbfs_command = "databricks fs mkdirs "+dbfs_path
        os.system(dbfs_command)

    # Local Folders from Databricks File System(DBFS)
    os.makedirs(os.path.join(project_local_path,'model/input/'))#Databricks DBFS
    os.makedirs(os.path.join(project_local_path,'model/output/'))#Databricks DBFS
    os.makedirs(os.path.join(project_local_path,'model/artifacts/'))#Databricks DBFS

    return None

        
class _DbfsHost(ParamType):
    """
    Used to validate the configured host
    """
    def convert(self, value, param, ctx):
        if value.startswith('https://'):
            return value
        else:
            self.fail('The host does not start with https://')