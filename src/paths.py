import os
script_directory  = os.path.dirname(os.path.abspath(__file__))
project_directory = os.path.dirname(script_directory)
path_to_config    = os.path.join(project_directory,"config")
path_to_logs      = os.path.join(project_directory,"log")
path_to_data      = os.path.join(project_directory,"data")
path_to_docs      = os.path.join(project_directory,"docs")
path_to_sql       = os.path.join(project_directory,"sql")
