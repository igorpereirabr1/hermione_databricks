# -*- coding: utf-8 -*-

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
          
                                                       """.format(version)


class NaturalOrderGroup(click.Group):
    # def __init__(self, name=None, commands=None, **attrs):
    #     if commands is None:
    #         commands = OrderedDict()
    #     elif not isinstance(commands, OrderedDict):
    #         commands = OrderedDict(commands)
    #     click.Group.__init__(self, name=name,
    #                          commands=commands,
    #                          **attrs)

    def list_commands(self, ctx):
        return self.commands.keys()

class _DbfsHost(ParamType):
    """
    Used to validate the configured host
    """
    def convert(self, value, param, ctx):
        if value.startswith('https://'):
            return value
        else:
            self.fail('The host does not start with https://')

def fix_path(path):
    return str(path).replace("\\","/")

def _configure_cli_token(profile, insecure):
    PROMPT_HOST = 'Databricks Host (should begin with https://)'
    PROMPT_TOKEN = 'Token' #  NOQA
    config = ProfileConfigProvider(profile).get_config() or DatabricksConfig.empty()
    host = click.prompt(PROMPT_HOST, default=config.host, type=_DbfsHost())
    token = click.prompt(PROMPT_TOKEN, default=config.token, hide_input=True)
    new_config = DatabricksConfig.from_token(host, token, insecure)
    update_and_persist_config(profile, new_config)

@click.group(cls=NaturalOrderGroup)
def cli():
    pass

@cli.command()
def info():
    """
    Checks that hermione-databricks is correctly installed.
    """
    click.secho(logo, fg="yellow")

@cli.command(context_settings=CONTEXT_SETTINGS,
               short_help='Configures host and authentication info for Databricks CLI.')
@click.option('--token', show_default=True, is_flag=True, default=True)
@click.option('--insecure', show_default=True, is_flag=True, default=None)
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
    else:
        pass


@cli.command(short_help='Create a new the Databricks ML Project. ')
def new():
    """
    Create a new the Databricks ML Project, based on workspace and dbfs parameters
    """
    project_name = click.prompt("Project Name", default="My first Project")
    project_description = click.prompt("Project Description", default=project_name)
    project_workspace_path = click.prompt("Databricks Host Workspace path (sample:/Users/xxxx@xxxxxxxx.com/)", default=None)
    project_workspace_path = fix_path(os.path.join(project_workspace_path,project_name))
    project_dbfs_path = click.prompt("Databricks Host DBFS path (sample:dbfs:/Users/xxxx@xxxxxxxx.com/)", default=None)
    project_dbfs_path =  fix_path(os.path.join(project_dbfs_path,project_name))
    project_local_path = fix_path(os.path.join(LOCAL_PATH, project_name))
    # Now create local files
    click.echo(f"Creating project {project_name}")
    # Folders from Databricks Workspace
    os.makedirs(os.path.join(project_local_path,'model/workspace'))#Databricks Workspace
    os.makedirs(os.path.join(project_local_path,'notebooks'))#Databricks Workspace

    # Local Folders from Databricks File System(DBFS)
    os.makedirs(os.path.join(project_local_path,'model/dbfs/input/'))#Databricks DBFS
    os.makedirs(os.path.join(project_local_path,'model/dbfs/output/'))#Databricks DBFS
    os.makedirs(os.path.join(project_local_path,'model/dbfs/artifacts/'))#Databricks DBFS

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
            ,"model_input_path":fix_path(os.path.join(project_dbfs_path,'model/input/'))
            ,"model_output_path":fix_path(os.path.join(project_dbfs_path,'model/output/'))
            ,"model_artifacts_path":fix_path(os.path.join(project_dbfs_path,'model/artifacts/'))}

    write_local_files(souce_path,dst_path,**kwargs)

    # exploratory_analysis Notebook
    souce_path = os.path.join(databricks_files_path,"exploratory_analysis.txt")
    dst_path = os.path.join(project_local_path,'notebooks/exploratory_analysis.ipynb')
    kwargs = {"project_name":project_name
            ,"model_input_path":fix_path(os.path.join(project_dbfs_path,'model/input/'))
            ,"model_output_path":fix_path(os.path.join(project_dbfs_path,'model/output/'))
            ,"model_artifacts_path":fix_path(os.path.join(project_dbfs_path,'model/artifacts/'))}

    write_local_files(souce_path,dst_path,**kwargs)

    # model Notebook
    souce_path = os.path.join(databricks_files_path,"model.txt")
    dst_path = os.path.join(project_local_path,'model/workspace/model.ipynb')
    kwargs = {"project_name":project_name
            ,"model_input_path":fix_path(os.path.join(project_dbfs_path,'model/input/'))
            ,"model_output_path":fix_path(os.path.join(project_dbfs_path,'model/output/'))
            ,"model_artifacts_path":fix_path(os.path.join(project_dbfs_path,'model/artifacts/'))}

    write_local_files(souce_path,dst_path,**kwargs)

    # project config file
    souce_path = os.path.join(databricks_files_path,"stack_configuration.json")
    dst_path = os.path.join(project_local_path,'config.json')
    kwargs = {"project_name":project_name,
          "project_local_path":project_local_path,
          "project_workspace_path":project_workspace_path,
          "project_dbfs_path":project_dbfs_path
         }
    # Need some fixes to use it
    write_local_files(souce_path,dst_path,**kwargs)

    #Upload project to Databricks

    command = "databricks stack deploy "+ os.path.join(project_local_path,"config.json")+" -o"
    os.system(command)


    # Create git repo
    os.chdir(project_local_path)
    os.system('git init')
    print("A git repository was created. You should add your files and make your first commit.\n")

    return None

@cli.command(short_help='Sync the remote changes to the local repository(pull).')
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

@cli.command(short_help='Sync the local changes to the remote repository(push).')
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

#@cli.command(short_help='Delete current project(local/remotely).')
#def delete():
#    """
#    Create a new the Databricks ML Project, based on workspace and dbfs parameters
#    """