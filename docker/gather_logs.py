import sys
import os
from datetime import datetime
import shutil

## 1st argument is the name of the folder to gather the logs
dir_prefix = sys.argv[1]

## The second argument contains the names of the nodes
## that were executed in the scenario
nodes = sys.argv[2].split()
# print(dir_prefix, nodes)

## Make the directory to save the current logs
timestamp = datetime.now().replace(microsecond=0).isoformat()
dir_name = dir_prefix + '_' + timestamp
dir_path = os.path.join('docker_logs', dir_name)
# print("Saving logs at:", dir_path)
os.mkdir(dir_path)

## Gather the log file names
log_folders = [os.path.join('read-only', node, 'logs') for node in nodes]
log_file_names_deep = [(path, os.listdir(path)) for path in log_folders]
log_file_names = [os.path.join(path, name) for (path, names) in log_file_names_deep for name in names]
# print(log_file_names)

for file_name in log_file_names:
    shutil.copy(file_name, dir_path)

print("Copied logs in:", dir_path)
