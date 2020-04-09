import subprocess
import sys
import os
from datetime import datetime
import shutil
## TODO: Find a better way to import other python files
sys.path.append(os.path.relpath("./scripts"))
sys.path.append(os.path.relpath("./analysis"))
from lib import copy_logs_from_to, move_ns3_logs
from plot_scaling_latency_throughput import plot_scaleup_rate, plot_scaleup_node_rate


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

class NS3Conf:
    def __init__(self, total_time="600", data_rate="1Gbps", delay="5000ns", tracing=False):
        self.total_time = total_time
        self.data_rate = data_rate
        self.delay = delay
        self.tracing = tracing

    def generate_args(self, file_prefix):
        args = ("--TotalTime={} "
                "--ns3::CsmaChannel::DataRate={} "
                "--ns3::CsmaChannel::Delay={} "
                "--Tracing={} "
                "--FilenamePrefix={} ")
        return args.format(self.total_time,
                           self.data_rate,
                           self.delay,
                           'true' if self.tracing else 'false',
                           file_prefix)


def format_node(node_name):
    return "'%s@%s.local'" % (node_name, node_name)

def remove_prefix(text, prefix):
    if text.startswith(prefix):
        return text[len(prefix):]
    return text

def run_configuration(experiment, rate_multiplier, ratio_ab, heartbeat_rate, a_node_numbers, optimizer, run_ns3=False, ns3_conf=NS3Conf()):
    a_nodes_names = ['a%dnode' % (node) for node in range(1,a_node_numbers+1)]
    exec_a_nodes = [format_node(node_name) for node_name in a_nodes_names]
    exec_b_node = format_node("main")
    exec_nodes_string = ",".join([exec_b_node] + exec_a_nodes)
    exec_prefix = '-noshell -run util exec abexample. distributed_experiment. '
    args = '[[%s],%d,%d,%d,%s].' % (exec_nodes_string, rate_multiplier, ratio_ab, heartbeat_rate, optimizer)
    exec_suffix = ' -s erlang halt'
    exec_string = exec_prefix + args + exec_suffix
    print(exec_string)
    simulator_args = ["ns3/simulate.sh", "-m", "main", "-e", exec_string]
    if run_ns3:
        file_prefix = "ns3_log"
        ns3_args = ns3_conf.generate_args(file_prefix)
        simulator_args.extend(["-n", ns3_args])
    simulator_args.extend(a_nodes_names)
    subprocess.check_call(simulator_args)

    ## 1st argument is the name of the folder to gather the logs
    dir_prefix = 'ab_exp'
    nodes = ['main'] + a_nodes_names
    conf_string = '%d_%s_%s_%s_%s_%s' % (experiment, rate_multiplier, ratio_ab, heartbeat_rate, a_node_numbers, optimizer)

    ## Make the directory to save the current logs
    ## TODO: Also add a timestamp?
    dir_name = dir_prefix + conf_string
    to_dir_path = os.path.join('archive', dir_name)

    log_folders = [os.path.join('var', 'log', node) for node in nodes]

    copy_logs_from_to(log_folders, to_dir_path)
    ## TODO: Make this runnable for the ns3 script
    if run_ns3:
        move_ns3_logs(file_prefix, to_dir_path)


## SAD COPY PASTE FROM THE FUNCTION ABOVE
def run_debs_configuration(rate_multiplier, num_houses, optimizer, run_ns3=False, ns3_conf=NS3Conf()):
    house_nodes_names = ['house%d' % (node) for node in range(0,num_houses)]
    exec_house_nodes = [format_node(node_name) for node_name in house_nodes_names]
    exec_main_node = format_node("main")
    exec_nodes_string = ",".join([exec_main_node] + exec_house_nodes)
    exec_prefix = '-noshell -run util exec debs_2014_query1. setup_experiment. '
    end_timeslice_period = 3600
    end_timeslice_heartbeat_period = 72
    args = '[%s,[%s],%d,%d,%d,%s].' % (optimizer, exec_nodes_string, end_timeslice_period,
                                       end_timeslice_heartbeat_period, rate_multiplier,
                                       'log_latency_throughput')
    exec_suffix = ' -s erlang halt'
    exec_string = exec_prefix + args + exec_suffix
    print(exec_string)
    simulator_args = ["ns3/simulate.sh", "-m", "main", "-e", exec_string]
    ## Change this to run THE REALISTIC EXAMPLE
    if run_ns3:
        file_prefix = "ns3_log"
        ns3_args = ns3_conf.generate_args(file_prefix)
        simulator_args.extend(["-n", ns3_args])
    simulator_args.extend(house_nodes_names)
    subprocess.check_call(simulator_args)

    ## 1st argument is the name of the folder to gather the logs
    dir_prefix = 'debs_house_query1'
    nodes = ['main'] + house_nodes_names
    conf_string = '%s_%s_%s' % (rate_multiplier, num_houses, optimizer)

    ## Make the directory to save the current logs
    ## TODO: Also add a timestamp?
    dir_name = dir_prefix + '_' + conf_string
    to_dir_path = os.path.join('archive', dir_name)

    log_folders = [os.path.join('var', 'log', node) for node in nodes]

    copy_logs_from_to(log_folders, to_dir_path)
    ## TODO: Make this runnable for the ns3 script
    if run_ns3:
        move_ns3_logs(file_prefix, to_dir_path)


## ANOTHER SAD COPY PASTE...
def run_debs_configurations(rate_multiplier, num_houses, optimizers, run_ns3=False, ns3_conf=NS3Conf()):
    ## Find a better way to do this than
    ## indented for loops :'(
    for rate_m in rate_multipliers:
        for house_node in num_houses:
            for optimizer in optimizers:
                run_debs_configuration(rate_m, house_node, optimizer, run_ns3, ns3_conf)


def run_outlier_detection_configuration(rate_multiplier, num_houses, optimizer,
                                        run_ns3=False, ns3_conf=NS3Conf()):
    house_nodes_names = ['str%d' % (node) for node in range(0,num_houses)]
    exec_house_nodes = [format_node(node_name) for node_name in house_nodes_names]
    exec_main_node = format_node("main")
    exec_nodes_string = ",".join([exec_main_node] + exec_house_nodes)
    exec_prefix = '-noshell -run util exec outlier_detection. setup_experiment. '
    check_outlier_period_ms = 1000 * 1000
    check_outlier_heartbeat_period_ms = 10 * 1000
    total_rate_multiplier = rate_multiplier
    args = '[%s,[%s],%d,%d,%d,%s].' % (optimizer, exec_nodes_string, check_outlier_period_ms,
                                       check_outlier_heartbeat_period_ms, total_rate_multiplier,
                                       'log_latency_throughput')
    exec_suffix = ' -s erlang halt'
    exec_string = exec_prefix + args + exec_suffix
    print(exec_string)
    simulator_args = ["ns3/simulate.sh", "-m", "main", "-e", exec_string]
    ## Change this to run THE REALISTIC EXAMPLE
    if run_ns3:
        file_prefix = "ns3_log"
        ns3_args = ns3_conf.generate_args(file_prefix)
        simulator_args.extend(["-n", ns3_args])
    simulator_args.extend(house_nodes_names)
    subprocess.check_call(simulator_args)

    ## 1st argument is the name of the folder to gather the logs
    dir_prefix = 'outlier_detection'
    nodes = ['main'] + house_nodes_names
    conf_string = '%s_%s_%s_%s' % (rate_multiplier, num_houses, optimizer, run_ns3)

    ## Make the directory to save the current logs
    ## TODO: Also add a timestamp?
    dir_name = dir_prefix + '_' + conf_string
    to_dir_path = os.path.join('archive', dir_name)

    log_folders = [os.path.join('var', 'log', node) for node in nodes]

    copy_logs_from_to(log_folders, to_dir_path)
    ## TODO: Make this runnable for the ns3 script
    if run_ns3:
        move_ns3_logs(file_prefix, to_dir_path)


## ANOTHER SAD COPY PASTE...
def run_outlier_detection_configurations(rate_multiplier, num_houses,
                                         optimizers, run_ns3=False, ns3_conf=NS3Conf()):
    ## Find a better way to do this than
    ## indented for loops :'(
    for rate_m in rate_multipliers:
        for house_node in num_houses:
            for optimizer in optimizers:
                run_outlier_detection_configuration(rate_m, house_node, optimizer, run_ns3, ns3_conf)



def run_configurations(experiment, rate_multipliers, ratios_ab, heartbeat_rates, a_nodes_numbers, optimizers, run_ns3=False, ns3_conf=NS3Conf()):
    ## Find a better way to do this than
    ## indented for loops :'(
    for rate_m in rate_multipliers:
        for ratio_ab in ratios_ab:
            for heartbeat_rate in heartbeat_rates:
                for a_node in a_nodes_numbers:
                    for optimizer in optimizers:
                        run_configuration(experiment, rate_m, ratio_ab, heartbeat_rate, a_node, optimizer, run_ns3, ns3_conf)


## Experiment 1
## ===============
## In this experiment we vary the rate of messages. We want to measure
## latency and throughput as the rate of messages increases to showcase
## how does the system handle varying loads.
##
## NOTE: The number of a nodes should be reasonably high. Maybe 4 - 8 nodes?
## NOTE: I have to fine tune these numbers to fit the server
rate_multipliers = range(10, 35, 2)
ratios_ab = [1000]
heartbeat_rates = [10]
a_nodes_numbers = [18]
optimizers = ["optimizer_greedy"]

#run_configurations(1, rate_multipliers, ratios_ab, heartbeat_rates, a_nodes_numbers, optimizers, run_ns3=True)

#dirname = os.path.join('archive')
# plot_scaleup_rate(dirname, 'multi_run_ab_experiment',
#                   rate_multipliers, ratios_ab[0], heartbeat_rates[0], a_nodes_numbers[0], optimizers[0])

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
rate_multipliers = [15]
ratios_ab = [1000]
heartbeat_rates = [10]
## Note:
## Ideally we want to plot up to 38 (for rate multiplier 15), but I cannot get the server to behave and give us
## steady results for those, so I will give results up to 32
a_nodes_numbers = range(2, 33, 2)
optimizers = ["optimizer_greedy"]

#run_configurations(2, rate_multipliers, ratios_ab, heartbeat_rates, a_nodes_numbers, optimizers, run_ns3=True)

    #dirname = os.path.join('archive')
#plot_scaleup_node_rate(dirname, 'multi_run_ab_experiment',
#                       rate_multipliers[0], ratios_ab[0], heartbeat_rates[0], a_nodes_numbers, optimizers[0])

## An issue with the above experiment is that when setting up 50 nodes, and trying to run
## them all, the nodes don't connect. It might be because they all try to make ? or it is
## something that has to do with the docker network?

## Claim showcased by the experiments:
## A claim that we can make for the first two experiments is that the system scales well
## both when increasing the rate, as well as when increasing the

## Experiment 3
## ============
## In this experiment we vary the a-to-b ratio.
## Should we enable passing pairs of (ratio_ab, heartbeat_rate)?
rate_multipliers = [15]
ratios_ab = [10, 20, 50, 100, 200, 500, 1000]
heartbeat_rates = [10]
a_nodes_numbers = [5]
optimizers = ["optimizer_greedy"]

#run_configurations(
#    3, rate_multipliers, ratios_ab, heartbeat_rates,
#    a_nodes_numbers, optimizers, run_ns3=True
#)

## Experiment 4
## ============
## In this experiment we compare the greedy vs. sequential optimizer.
## Perhaps it makes sense to make the network slightly worse this time.
#
## Note: This experiment was also run with the "greedy hybrid" optimizer,
## where the optimizer is greedy, but the configuration tree is mapped
## to the main node. This requires a change in the source code
## (src/optimizer_greedy.erl). Konstantinos knows what needs to be
## changed.
rate_multipliers = [15]
ratios_ab = [1000]
heartbeat_rates = [10]
a_nodes_numbers = [4]
optimizers = ["optimizer_greedy", "optimizer_sequential"]

#run_configurations(
#    4, rate_multipliers, ratios_ab, heartbeat_rates,
#    a_nodes_numbers, optimizers, run_ns3=True
##    run_ns3=True, ns3_conf=NS3Conf(data_rate="100Mbps")
#)

## Experiment 5
## ============
## Varying the heartbeat ratio.
rate_multipliers = [15]
ratios_ab = [10000]
heartbeat_rates = [1, 2, 5, 10, 100, 1000, 10000]
a_nodes_numbers = [5]
optimizers = ["optimizer_greedy"]

#run_configurations(
#    5, rate_multipliers, ratios_ab, heartbeat_rates,
#    a_nodes_numbers, optimizers, run_ns3=True
#)


## Realistic Experiment
## ===============


rate_multipliers = [360]
num_houses = [20]
optimizers = ["optimizer_greedy"]

#run_debs_configurations(rate_multipliers, num_houses, optimizers, run_ns3=True, ns3_conf=NS3Conf(total_time="7500"))


## Outlier Detection
## =================

rate_multipliers = [32]
num_streams = [4, 8]
optimizers = ["optimizer_greedy"]

# run_outlier_detection_configurations(rate_multipliers, num_streams, optimizers, run_ns3=True, ns3_conf=NS3Conf(total_time="7500"))

# run_outlier_detection_configurations(rate_multipliers, num_streams, optimizers, run_ns3=False)
