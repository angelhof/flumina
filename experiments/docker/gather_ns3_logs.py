import sys
import os
from datetime import datetime
import shutil

def copy_logs_from_to(from_dirs, to_dir_path):
    os.makedirs(to_dir_path)

    ## Gather the log file names

    log_file_names_deep = [(path, os.listdir(path)) for path in from_dirs]
    log_file_names = [os.path.join(path, name) for (path, names) in log_file_names_deep for name in names]
    # print(log_file_names)

    for file_name in log_file_names:
        shutil.copy(file_name, to_dir_path)

    print("Copied logs in:", to_dir_path)

## 1st argument is the name of the folder to gather the logs
dir_prefix = sys.argv[1]

## The second argument contains the names of the nodes
## that were executed in the scenario
nodes = sys.argv[2].split()

## The third argument is the suffix of the folder
dir_suffix = str(len(nodes)) + "_" + sys.argv[3]
# print(dir_prefix, nodes)

## Make the directory to save the current logs
timestamp = datetime.now().replace(microsecond=0).isoformat()
dir_name = dir_prefix + '_' + timestamp + '_' + dir_suffix
dir_path = os.path.join('archive', dir_name)

log_folders = [os.path.join('var', 'log', node) for node in nodes]

copy_logs_from_to(log_folders, dir_path)
