import subprocess
import sys
import os
from datetime import datetime
import shutil
## TODO: Find a better way to import other python files
sys.path.append(os.path.relpath("./scripts"))
from lib import copy_logs_from_to


## The purpose of this script is to generate different exec strings for the
## main erlang node to execute. They all call abexample:distributed_experiment
## which is the configurable ab example.

## We would like to vary the following values in this script
## 1) Rate multiplier (Higher rate means higher load in the system)
## 2) Ratio of A to B messages. This can be useful to show at which point
##    it no more worth paralellizing on As.
## 3) Heartbeat rate. This is only useful for b heartbeats, and the higher
##    the heartbeat rate, we expect the latency to become lower because the
##    system buffers will not grow as much
## 4) Number of A nodes. Increases this, increases the rate multiplier,
##    and the ratios of as to bs. This should usually be fixed to a high enough
##    value so that parallelization of the system can make sense.
##    Varying this is also important to show how well the system scales with
##    36 cores that our evaluation server has. What is the scale curve?
## 5) Sequential or greedy optimization. This is minor, but we should also
##    get this measurement.
## 6) Message delay over all node links
## 7) Link bandwidth. What is the maximum size of messages that can be sent
##    between two nodes. This is important to vary, as the higher it is,
##    the more our system's throughput (and latency) will shine. If the
##    bandwidth is smaller than the total amount of messages, then it is
##    impossible for the messages to all be sent and processed in a single node
##    without lagging behind.

## The fixed thing in this experiment is the amount of a messages for each
## a implementation tag, and that is 1000000 messages. The rate multiplier
## 1 corresponds to 1 message per millisecond.

## TODO: Find how to call this from the other python file
# def copy_logs_from_to(from_dirs, to_dir_path):
#     ## Delete the directory if it exists
#     if os.path.isdir(to_dir_path):
#         shutil.rmtree(to_dir_path)
#     os.mkdir(to_dir_path)

#     ## Gather the log file names

#     log_file_names_deep = [(path, os.listdir(path)) for path in from_dirs]
#     log_file_names = [os.path.join(path, name) for (path, names) in log_file_names_deep for name in names]
#     # print(log_file_names)

#     for file_name in log_file_names:
#         shutil.copy(file_name, to_dir_path)

#     print "Copied logs in:", to_dir_path

def format_node(node_name):
    return "\\'%s@%s.local\\'" % (node_name, node_name)

def run_configuration(rate_multiplier, ratio_ab, heartbeat_rate, a_node_numbers, optimizer):
    a_nodes_names = ['a%dnode' % (node) for node in range(1,a_node_numbers+1)]
    a_nodes_string = " ".join(a_nodes_names)
    exec_a_nodes = [format_node(node_name) for node_name in a_nodes_names]
    exec_b_node = format_node("main")
    exec_nodes_string = ",".join([exec_b_node] + exec_a_nodes)
    exec_prefix = '-noshell -run util exec abexample. distributed_experiment. '
    args = '[[%s],%d,%d,%d,%s].' % (exec_nodes_string, rate_multiplier, ratio_ab, heartbeat_rate, optimizer)
    exec_suffix = ' -s erlang halt'
    exec_string = exec_prefix + args + exec_suffix
    print exec_string
    subprocess.check_call(["scenarios/no_ns3_docker_scenario.sh", a_nodes_string, exec_string])

    ## 1st argument is the name of the folder to gather the logs
    dir_prefix = 'multi_run_ab_experiment'
    nodes = ['main'] + a_nodes_names
    conf_string = '%s_%s_%s_%s_%s' % (rate_multiplier, ratio_ab, heartbeat_rate, a_node_numbers, optimizer)
    
    ## Make the directory to save the current logs
    dir_name = dir_prefix + '_' + conf_string
    to_dir_path = os.path.join('docker', 'docker_logs', dir_name)

    log_folders = [os.path.join('docker', 'read-only', node, 'logs') for node in nodes]

    copy_logs_from_to(log_folders, to_dir_path)
    ## TODO: Make this runnable for the ns3 script
    

def run_configurations(rate_multipliers, ratios_ab, heartbeat_rates, a_nodes_numbers, optimizers):
    ## Find a better way to do this than
    ## indented for loops :'(
    for rate_m in rate_multipliers:
        for ratio_ab in ratios_ab:
            for heartbeat_rate in heartbeat_rates:
                for a_node in a_nodes_numbers:
                    for optimizer in optimizers:
                        run_configuration(rate_m, ratio_ab, heartbeat_rate, a_node, optimizer)


## Experiment 1
## ===============
## In this experiment we vary the rate of messages. We want to measure
## latency and throughput as the rate of messages increases to showcase
## how does the system handle varying loads.
##
## NOTE: The number of a nodes should be reasonably high. Maybe 4 - 8 nodes?
rate_multipliers = [5, 10, 20, 50, 100]
ratios_ab = [1000]
heartbeat_rates = [10]
a_nodes_numbers = [4]
optimizers = ["optimizer_greedy"]

run_configurations(rate_multipliers, ratios_ab, heartbeat_rates, a_nodes_numbers, optimizers)

## Notes:
## On my machine it chokes above 50 rate and never gives any response.
## For some reason, on the server (after 40 nodes it chokes), which is actually pretty reasonable
## because there are only 36 cores.
##
## I noticed that on my machine, some of the mailboxes wait to release bs and some
## wait to release as.
##
## An issue is that producers take up scheduling time, and so for 5 nodes, we need
## 5 "threads" for 'a' producers + 1 for b producer
## 5 "threads" for 'a' mailboxes + 1 for b mailbox
## 5 "threads" for 'a' nodes + 1 for b node  

## Experiment 2
## ===============
## In this experiment we vary the number of a nodes. We want to measure
## latency and throughput as the number of nodes increases to showcase
## how well our system scales with the number of processors/cores
##
## NOTE: The rate should be reasonably high. Maybe 20 - 50?
rate_multipliers = [10]
ratios_ab = [1000]
heartbeat_rates = [10]
a_nodes_numbers = [1, 2, 5, 10, 20, 25, 50]
optimizers = ["optimizer_greedy"]

run_configurations(rate_multipliers, ratios_ab, heartbeat_rates, a_nodes_numbers, optimizers)

## Claim showcased by the experiments:
## A claim that we can make for the first two experiments is that the system scales well
## both when increasing the rate, as well as when increasing the 

rate_multipliers = [10]
ratios_ab = [1000]
heartbeat_rates = [10]
a_nodes_numbers = [10]
optimizers = ["optimizer_greedy"]

# run_configurations(rate_multipliers, ratios_ab, heartbeat_rates, a_nodes_numbers, optimizers)
