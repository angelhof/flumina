import sys
import os
from datetime import datetime
## TODO: Find a better way to import other python files
sys.path.append(os.path.relpath("../scripts"))
from lib import copy_logs_from_to
import shutil


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
dir_path = os.path.join('docker_logs', dir_name)

log_folders = [os.path.join('read-only', node, 'logs') for node in nodes]

copy_logs_from_to(log_folders, dir_path)
