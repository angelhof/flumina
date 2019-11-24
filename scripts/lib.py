import sys
import os
from datetime import datetime
import shutil
from operator import itemgetter
import itertools
import matplotlib.pyplot as plt
import numpy as np

def copy_logs_from_to(from_dirs, to_dir_path):
    ## Delete the directory if it exists
    if os.path.isdir(to_dir_path):
        shutil.rmtree(to_dir_path)
    os.makedirs(to_dir_path)

    ## Gather the log file names

    log_file_names_deep = [(path, os.listdir(path)) for path in from_dirs]
    log_file_names = [os.path.join(path, name) for (path, names) in log_file_names_deep for name in names]
    # print(log_file_names)

    for file_name in log_file_names:
        shutil.copy(file_name, to_dir_path)

    print("Copied logs in:", to_dir_path)

# Moves ns3 log files to the directory to_dir. Does not
# remove to_dir if it exists so that it can be combined
# with the previous function which does remove to_dir.
def move_ns3_logs(file_prefix, to_dir):
    ns3_dir = os.environ.get('NS3_HOME')
    if ns3_dir is not None:
        files = [
            os.path.join(ns3_dir, f)
            for f in os.listdir(ns3_dir)
            if f.startswith(file_prefix)
        ]
        for f in files:
            shutil.move(f, to_dir)


##
## Latency parsing and preprocessing
##

def parse_producer_line(line):
    timestamp = line.split("}")[-2].split(',')[-1]
    message = line[2:].split("}")[0] + '}'
    return (message, int(timestamp))

def parse_sink_line(line):
    timestamp = line.split("}")[-2].split(',')[-1]
    message = line[2:].split("}")[0].split('{')[-1]
    return ('{' + message + '}', int(timestamp))

def read_preprocess_latency_data(log_dir_name):

    log_file_names = os.listdir(log_dir_name)
    producer_file_names = [os.path.join(log_dir_name, filename)
                           for filename in log_file_names
                           if filename.startswith('producer')]
    sink_file_names = [os.path.join(log_dir_name, filename)
                       for filename in log_file_names
                       if filename.startswith('sink')]

    producer_data = []
    for producer_file_name in producer_file_names:
        file = open(producer_file_name)
        lines = file.readlines()
        file_data = list(map(parse_producer_line, lines))
        producer_data += file_data

    sink_data = []
    for sink_file_name in sink_file_names:
        file = open(sink_file_name)
        lines = file.readlines()
        file_data = list(map(parse_sink_line, lines))
        sink_data += file_data
        
    # print(producer_data[:10])
    # print(sink_data[:10])
    
    # print producer_data[-10:]
    # print sink_data[-10:]
    
    producer_dic = {msg: ts for msg, ts in producer_data}
    sink_dic = {msg: ts for msg, ts in sink_data}
        
    latency_dic = {}
    for msg in sink_dic.keys():
        producer_ts = producer_dic[msg]
        sink_ts = sink_dic[msg]
        ## Keep the sink timestamp to sort by time
        latency_dic[msg] = (sink_ts - producer_ts, sink_ts)

    # print latency_dic

    latency_pairs = sorted(latency_dic.values(), key=itemgetter(1))

    ## Latencies and timestamps are per millisecond
    raw_timestamps = [ts for lat, ts in latency_pairs]
    if not raw_timestamps:
        print("{}: No raw timestamps!".format(log_dir_name))
    first_ts = raw_timestamps[0]
    timestamps = [(ts - first_ts) / 1000000.0 for ts in raw_timestamps]
    latencies = [lat / 1000000.0 for lat, ts in latency_pairs]

    # print latencies[:10]
    # print latencies[-10:]

    # timestamps = [ts for ts, n in final_timestamp_message_pairs]
    # first_ts = timestamps[0]
    # ## Those are shifted to 0 and are in seconds
    # shifted_timestamps = [(ts - first_ts) / 1000000.0 for ts in timestamps]
    
    # ts_diffs = [ ts1 - ts0 for ts1, ts0 in zip(shifted_timestamps[1:], shifted_timestamps[:-1])]
    # print(ts_diffs[:10])
    # ns = [n for ts, n in final_timestamp_message_pairs]
    # ## This is messages per 1 millisecond
    # throughput = [n / ts_diff for n, ts_diff in zip(ns[1:], ts_diffs)]
    
    # yp = None
    # ## Interpolate to get the the steady throughput
    # xi = np.linspace(shifted_timestamps[0], shifted_timestamps[-1], 3 * len(timestamps))
    # yi = np.interp(xi, shifted_timestamps[1:], throughput, yp)

    # ## The plot shows messages per millisecond
    return (timestamps, latencies)

##
## Throughput parsing and preprocessing
##

def parse_throughput_line(line):
    timestamp_messages = line.split("}")[-2]
    timestamp, message = timestamp_messages.split(",")[-2:]
    return (int(timestamp), int(message))
        
def read_preprocess_throughput_data(log_dir_name):

    log_file_names = os.listdir(log_dir_name)
    throughput_file_names = [os.path.join(log_dir_name, filename)
                           for filename in log_file_names
                           if filename.startswith('throughput')]
    ## There should only be one
    filename = throughput_file_names[0]
    
    file = open(filename)
    lines = file.readlines()
    timestamp_message_pairs = list(map(parse_throughput_line, lines))
    timestamp_message_dict = {}
    for timestamp, n in timestamp_message_pairs:
        if timestamp in timestamp_message_dict:
            timestamp_message_dict[timestamp] += n
        else:
            timestamp_message_dict[timestamp] = n


    final_timestamp_message_pairs = sorted(timestamp_message_dict.items(), key=itemgetter(0))

    ## Remove trailing zeros 
    while final_timestamp_message_pairs and final_timestamp_message_pairs[-1][1] == 0:
        final_timestamp_message_pairs.pop()

    ## Remove leading zeros 
    while final_timestamp_message_pairs and final_timestamp_message_pairs[0][1] == 0:
        final_timestamp_message_pairs.pop(0)
    
    
    timestamps = [ts for ts, n in final_timestamp_message_pairs]
    first_ts = timestamps[0]
    ## Those are shifted to 0 and are in seconds
    shifted_timestamps = [(ts - first_ts) / 1000000.0 for ts in timestamps]

    ts_diffs = [ ts1 - ts0 for ts1, ts0 in zip(shifted_timestamps[1:], shifted_timestamps[:-1])]
    # print(ts_diffs[:10])
    ns = [n for ts, n in final_timestamp_message_pairs]
    ## This is messages per 1 millisecond
    throughput = [n / ts_diff for n, ts_diff in zip(ns[1:], ts_diffs)]

    yp = None
    ## Interpolate to get the the steady throughput
    xi = np.linspace(shifted_timestamps[0], shifted_timestamps[-1], 3 * len(timestamps))
    yi = np.interp(xi, shifted_timestamps[1:], throughput, yp)
    return (xi, yi)


##
## Network load parsing and preprocessing
##

def get_network_stats_file(log_dir):
    files = [f for f in os.listdir(log_dir) if f.endswith('stats.txt')]
    if len(files) == 0:
        sys.exit('Network statistics file not found!')
    elif len(files) >= 2:
        sys.exit('Multiple network statistics files found!')
    return os.path.join(log_dir, files[0])


def get_network_data(log_dir):
    stats_file = get_network_stats_file(log_dir)
    with open(stats_file) as f:
        first_line = f.readline()

        # The first line says for example:
        #
        # Total IPv4 data: 459688780
        #
        # So the number starts at position 17
        return int(first_line[17:])
