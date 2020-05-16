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

def gather_logs(dir_prefix, nodes, conf_string, run_ns3=False, run_ec2=False, log_name="/dev/null"):
    assert(not (run_ns3 and run_ec2))

    stime = datetime.now()
    print("|-- Gathering logs...", end=" ", flush=True)
    ## Make the directory to save the current logs
    dir_name = dir_prefix + '_' + conf_string
    to_dir_path = os.path.join('archive', dir_name)

    if(not run_ec2):
        all_log_folders = [os.path.join('var', 'log', node) for node in nodes]
    else:
        shutil.rmtree('temp_logs')
        os.makedirs('temp_logs')
        log_folders = [os.path.join('temp_logs', node.split('@')[0]) for node in nodes]
        hostnames = get_ec2_hostnames()
        for i in range(len(nodes)):
            hostname = hostnames[i]
            gather_logs_args = ['scripts/gather_logs_from_ec2_node.sh', hostname, log_folders[i]]
            with open(log_name, "a") as f:
                subprocess.check_call(gather_logs_args, stdout=f)

        main_log_folder = os.path.join('temp_logs', 'main')
        shutil.copytree('logs', main_log_folder)
        all_log_folders = [main_log_folder] + log_folders

    copy_logs_from_to(all_log_folders, to_dir_path)
    etime = datetime.now()
    print("took:", etime - stime)
    print("Copied logs in:", to_dir_path)

def clean_logs():
    stime = datetime.now()
    print("|-- Deleting the log directory...", end=" ", flush=True)
    shutil.rmtree('logs', ignore_errors=True)
    os.makedirs('logs')
    etime = datetime.now()
    print("took:", etime - stime)


def get_ec2_hostnames():
    filename = os.path.join('ec2', 'hostnames')
    with open(filename) as f:
        hostnames = [line.rstrip() for line in f.readlines()]
    return hostnames

def get_ec2_main_hostname():
    filename = os.path.join('ec2', 'main_hostname')
    with open(filename) as f:
        hostnames = [line.rstrip() for line in f.readlines()]
    assert(len(hostnames) == 1)
    return hostnames[0]

MAIN_SNAME = 'main'

def start_ec2_erlang_nodes(snames, hostnames):
    stime = datetime.now()
    print("|-- Starting Erlang nodes...", end=" ", flush=True)
    snames_hostnames = zip(snames, hostnames)
    for sname, hostname in snames_hostnames:
        start_node_args = ['scripts/start_erlang_node.sh', sname, hostname]
        # print(start_node_args)
        subprocess.check_call(start_node_args)

    etime = datetime.now()
    print("took:", etime - stime)

def stop_ec2_erlang_nodes(node_names, main_stdout_log):
    stime = datetime.now()
    print("|-- Stopping erlang nodes...", end=" ", flush=True)
    for node_name in node_names:
        stop_node_args = ['scripts/stop_erlang_node.sh', node_name]
        with open(main_stdout_log, "a") as f:
            subprocess.check_call(stop_node_args, stdout=f)
        # print(stop_node_args)
    etime = datetime.now()
    print("took:", etime - stime)

def execute_ec2_configuration(experiment, rate_multiplier, ratio_ab, heartbeat_rate, a_node_numbers, optimizer):

    print("Experiment:", experiment, rate_multiplier, ratio_ab, heartbeat_rate, a_node_numbers, optimizer)
    main_stdout_log = "/tmp/flumina_main_stdout"
    print("|-- The stdout is logged in:", main_stdout_log)

    ## Read the hostnames from the ec2 internal hostnames file
    my_sname = 'main'
    my_hostname_prefix = 'ip-172-31-35-213'
    my_node_name = '{}@{}'.format(my_sname, my_hostname_prefix)
    hostnames = get_ec2_hostnames()

    ## Clean the log directory
    clean_logs()

    ## For each of the a_nodes, create a name and start the erlang
    ## node on a hostname (assuming the ec2 instance is up).
    node_names = []
    snames = []
    used_hostnames = []
    for i in range(a_node_numbers):
        sname = 'flumina{}'.format(i+1)
        snames.append(sname)
        hostname = hostnames[i]
        used_hostnames.append(hostname)
        ## Also compute the node name
        hostname_prefix = hostname.split('.')[0]
        node_name = '{}@{}'.format(sname, hostname_prefix)
        node_names.append(node_name)

    ## Start the Erlang nodes
    start_ec2_erlang_nodes(snames, used_hostnames)

    ## Run the main experiment
    args_prefix = ['make', 'exec']
    name_opt = ['NAME_OPT=-sname main -setcookie flumina']

    exp_module = 'abexample.'
    exp_function = 'distributed_experiment.'
    atom_node_names = ["\\'{}\\'".format(node_name)
                       for node_name in [my_node_name] + node_names]
    node_names_exp_arg = ','.join(atom_node_names)
    exp_arguments = "[[{}],{},{},{},{}].".format(node_names_exp_arg, rate_multiplier,
                                                 ratio_ab, heartbeat_rate, optimizer)
    exec_args_string = '{} {} {}'.format(exp_module, exp_function, exp_arguments)
    exec_args = ['args={}'.format(exec_args_string)]
    all_args = args_prefix + name_opt + exec_args
    # print(all_args)
    stime = datetime.now()
    print("|-- Running experiment...", end=" ", flush=True)
    with open(main_stdout_log, "w") as f:
        p = subprocess.run(all_args, stdout=f)
    etime = datetime.now()
    print("took:", etime - stime)

    ## Stop the nodes in the ips
    stop_ec2_erlang_nodes(node_names, main_stdout_log)

    ## Gather logs
    dir_prefix = 'ab_exp'
    conf_string = '{}_{}_{}_{}_{}_{}'.format(experiment, rate_multiplier,
                                             ratio_ab, heartbeat_rate, a_node_numbers, optimizer)
    gather_logs(dir_prefix, node_names, conf_string, run_ec2=True, log_name=main_stdout_log)

    ## Check the return code after we have stopped the nodes
    p.check_returncode()




def run_configuration(experiment, rate_multiplier, ratio_ab, heartbeat_rate, a_node_numbers, optimizer, run_ns3=False, ns3_conf=NS3Conf(), run_ec2=False):
    assert(not (run_ec2 and run_ns3))
    if(run_ec2):
        execute_ec2_configuration(experiment, rate_multiplier, ratio_ab,
                                  heartbeat_rate, a_node_numbers, optimizer)
    else:
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


## Given the arguments of the stream-table join experiment this
## constructs the argument string and the necessary node names that
## have to be spawned as docker containers.
def collect_stream_table_join_experiment_nodes(num_ids, num_page_view_parallel, rate_multiplier,
                                               uua_producer_in_main, run_ec2=False):

    ## For EC2 we have to get the host names from a hostnames file
    hostnames = get_ec2_hostnames()

    ## Given the number of unique identifiers and the number of
    ## parallel page view streams, we can create a list that contains
    ## for each uid, and for each tag, the arriving node(s).
    uid_tag_node_list = []
    used_hostnames = []
    counter = 1
    for uid in range(num_ids):

        ## The page view hosts
        page_view_nodes = []
        for i in range(num_page_view_parallel):
            # print("Uid:", uid, "i:", i)
            node_sname = "flumina{}".format(counter)
            if(not(run_ec2)):
                node_name = format_node(node_sname)
            else:
                hostname = hostnames[counter-1]
                used_hostnames.append(hostname)
                hostname_prefix = hostname.split('.')[0]
                node_name = "\\'{}@{}\\'".format(node_sname, hostname_prefix)
            counter += 1
            page_view_nodes.append(node_name)

        get_user_address_node = page_view_nodes[0]

        ## Sometimes we might want the update_user_address producer to
        ## be located in main so that we can get good latency
        ## measurements (producers and syncs should be colocated).
        if(uua_producer_in_main):
            if(not(run_ec2)):
               update_user_address_node = format_node(my_sname)
            else:
               my_sname = MAIN_SNAME
               my_ec2_hostname = get_ec2_main_hostname()
               my_hostname_prefix = my_ec2_hostname.split('.')[0]
               my_node_name = '{}@{}'.format(my_sname, my_hostname_prefix)
               update_user_address_node = page_view_nodes[0]
        else:
            update_user_address_node = page_view_nodes[0]


        ## Format the string
        uid_tag_node_string = "{" + str(uid) + ",[{'page_view',[" + ",".join(page_view_nodes)
        uid_tag_node_string += "]},{'get_user_address',[" + get_user_address_node
        uid_tag_node_string += "]},{'update_user_address',[" + update_user_address_node + "]}]}"
        uid_tag_node_list.append(uid_tag_node_string)
    # print(uid_tag_node_list)

    ## If we are simulating with ns3/simulate.sh we need a list of all
    ## the docker containers to start
    snames = ["flumina{}".format(i+1) for i in range(counter-1)]
    # print(all_node_names)

    uid_tag_node_arg_string = "[{}],{}".format(",".join(uid_tag_node_list),rate_multiplier)
    final_args_string = "[{" + uid_tag_node_arg_string + "}]."
    return (final_args_string, snames, used_hostnames)


def run_stream_table_join_configuration(num_ids, num_page_view_parallel, rate_multiplier, run_ns3=False, run_ec2=False):
    assert(not (run_ns3 and run_ec2))

    print("Stream-Table Join Experiment:", num_ids, num_page_view_parallel, rate_multiplier)

    ## Prepate the node names and the experiment argument string
    update_user_address_producers_in_main = True
    uid_tag_node_arg_string, snames, used_hostnames = collect_stream_table_join_experiment_nodes(num_ids, num_page_view_parallel, rate_multiplier, update_user_address_producers_in_main, run_ec2=run_ec2)

    if(not run_ec2):
        ## This should never be executed with NS3
        assert(not run_ns3)
        ## Useful for EC2
        main_stdout_log = "/dev/null"
        ## Setting up the arguments
        exec_prefix = '-noshell -run util exec stream_table_join_example. experiment. '
        args = uid_tag_node_arg_string
        exec_suffix = ' -s init stop'
        exec_string = exec_prefix + args + exec_suffix

        print(exec_string)
        simulator_args = ["ns3/simulate.sh", "-m", "main", "-e", exec_string]
        simulator_args.extend(snames)
        print(simulator_args)
        subprocess.check_call(simulator_args)
    else:
        main_stdout_log = "/tmp/flumina_main_stdout"
        print("|-- The stdout is logged in:", main_stdout_log)

        ## Read the hostnames from the ec2 internal hostnames file
        my_sname = MAIN_SNAME
        my_ec2_hostname = get_ec2_main_hostname()
        my_hostname_prefix = my_ec2_hostname.split('.')[0]
        my_node_name = '{}@{}'.format(my_sname, my_hostname_prefix)

        node_names = ['{}@{}'.format(sname, hostname.split('.')[0])
                      for sname, hostname in zip(snames, used_hostnames)]

        ## Start the erlang nodes in the other EC2 instances
        start_ec2_erlang_nodes(snames, used_hostnames)

        ## Run the main experiment
        args_prefix = ['make', 'exec']
        name_opt = ['NAME_OPT=-sname main -setcookie flumina']

        exp_module = 'stream_table_join_example.'
        exp_function = 'experiment.'
        exp_arguments = uid_tag_node_arg_string

        exec_args_string = '{} {} {}'.format(exp_module, exp_function, exp_arguments)
        exec_args = ['args={}'.format(exec_args_string)]
        all_args = args_prefix + name_opt + exec_args
        # print(all_args)
        stime = datetime.now()
        print("|-- Running experiment...", end=" ", flush=True)
        with open(main_stdout_log, "w") as f:
            p = subprocess.run(all_args, stdout=f)
        etime = datetime.now()
        print("took:", etime - stime)

        ## Stop the nodes in the ips
        stop_ec2_erlang_nodes(node_names, main_stdout_log)

    ## Prepare log gathering
    dir_prefix = 'stream_table_join'
    nodes = ['main'] + snames
    conf_string = '{}_{}_{}_{}'.format(num_ids, num_page_view_parallel, rate_multiplier, run_ec2)

    gather_logs(dir_prefix, snames, conf_string, run_ns3=run_ns3, run_ec2=run_ec2, log_name=main_stdout_log)

## ANOTHER SAD COPY PASTE...
def run_outlier_detection_configurations(rate_multiplier, num_houses,
                                         optimizers, run_ns3=False, ns3_conf=NS3Conf()):
    ## Find a better way to do this than
    ## indented for loops :'(
    for rate_m in rate_multipliers:
        for house_node in num_houses:
            for optimizer in optimizers:
                run_outlier_detection_configuration(rate_m, house_node, optimizer, run_ns3, ns3_conf)

## ANOTHER SAD COPY PASTE...
def run_stream_table_join_configurations(num_ids, num_page_view_parallel, rate_multipliers, run_ns3=False, run_ec2=False):
    for num_id in num_ids:
        for num_page_view_p in num_page_view_parallel:
            for rate_multiplier in rate_multipliers:
                run_stream_table_join_configuration(num_id, num_page_view_p,
                                                    rate_multiplier, run_ns3=run_ns3, run_ec2=run_ec2)


def run_configurations(experiment, rate_multipliers, ratios_ab, heartbeat_rates, a_nodes_numbers, optimizers, run_ns3=False, ns3_conf=NS3Conf(), run_ec2=False):
    assert(not (run_ns3 and run_ec2))
    ## Find a better way to do this than
    ## indented for loops :'(
    for rate_m in rate_multipliers:
        for ratio_ab in ratios_ab:
            for heartbeat_rate in heartbeat_rates:
                for a_node in a_nodes_numbers:
                    for optimizer in optimizers:
                        run_configuration(experiment, rate_m, ratio_ab, heartbeat_rate, a_node, optimizer, run_ns3, ns3_conf, run_ec2=run_ec2)


## Experiment 1
## ===============
## In this experiment we vary the rate of messages. We want to measure
## latency and throughput as the rate of messages increases to showcase
## how does the system handle varying loads.
##
## NOTE: The number of a nodes should be reasonably high. Maybe 4 - 8 nodes?
## NOTE: I have to fine tune these numbers to fit the server
# rate_multipliers = range(30, 32, 2)
rate_multipliers = range(20, 50, 2)
ratios_ab = [1000]
heartbeat_rates = [10]
# a_nodes_numbers = [10]
a_nodes_numbers = [10]
optimizers = ["optimizer_greedy"]

# run_configurations(1, rate_multipliers, ratios_ab, heartbeat_rates, a_nodes_numbers, optimizers, run_ns3=True)
## t2.micro instances reach peak for rate 38
# run_configurations(1, rate_multipliers, ratios_ab, heartbeat_rates, a_nodes_numbers, optimizers, run_ec2=True)

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

## Stream-Table Join Experiment
## ===============

num_ids = [2]
num_page_view_parallel = [5]
# rate_multipliers = [20]
rate_multipliers = range(1,15,2)
# It works fine with rates between 2-10

# run_stream_table_join_configurations(num_ids, num_page_view_parallel, rate_multipliers, run_ns3=False)
run_stream_table_join_configurations(num_ids, num_page_view_parallel, rate_multipliers, run_ec2=True)


## realistic Experiment
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
