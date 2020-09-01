# coding: utf8
import os
from datetime import datetime
import codecs
import hermione_databricks
from pathlib import Path

lib_path = hermione_databricks.__path__[0]
databricks_files_path = "databricks_file_text"


def write_local_files(souce_path,dst_path,**kwargs):
    
    #Create local dir if not exist
    os.makedirs(os.path.dirname(dst_path), exist_ok=True)
    
    # Open the source txt file
    with codecs.open(souce_path, 'r', "utf-8") as infile:
        file = infile.read()
    # Iterate over the words that should be replaced in the text file    
    for k,v in kwargs.items():
        file = file.replace(k,v)
    # Save the final file
    with codecs.open(dst_path, 'w+', "utf-8") as outfile:
        outfile.writelines(file)
        
def write_config_json(**kwargs):

    return None