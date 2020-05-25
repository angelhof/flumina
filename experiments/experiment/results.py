import os
import re
import sys
from operator import itemgetter
from os import path

import numpy as np


def get_flink_latencies(result_path):
    out_file = path.join(result_path, 'out.txt')
    with open(out_file, 'r') as f:
        pattern = re.compile(r'\[latency: (\d+) ms\]')
        latencies = [int(re.search(pattern, line).group(1)) for line in f.readlines()]
        p10 = np.percentile(latencies, 10)
        p50 = np.percentile(latencies, 50)
        p90 = np.percentile(latencies, 90)
        return p10, p50, p90


def get_flink_throughput(result_path):
    stats_file = path.join(result_path, 'stats.txt')
    throughput_pattern = re.compile(r'Mean throughput \(events/ms\): ([0-9.]+)')
    with open(stats_file, 'r') as f:
        for line in f:
            match = re.match(throughput_pattern, line)
            if match:
                return float(match.group(1))


# Old code for processing Erlang latencies from lib.py
# TODO: Make it a bit cleaner

def parse_vb_producer_line(line):
    timestamp = line.split("}")[-2].split(',')[-1]
    message = line[2:].split("}")[0] + '}'
    return message, int(timestamp)


def parse_vb_sink_line(line):
    timestamp = line.split("}")[-2].split(',')[-1]
    message = line[2:].split("}")[0].split('{')[-1]
    return '{' + message + '}', int(timestamp)

def parse_stream_table_join_producer_line(line):
    timestamp = line.split(',')[-1].rstrip('\n}')
    message = line.split('}}')[0].lstrip('{')
    return '{{' + message + '}}', int(timestamp)

def parse_stream_table_join_sink_line(line):
    timestamp = line.split(',')[-1].rstrip('\n}')
    message = line.split('}}')[0].lstrip('{')
    return '{{' + message + '}}', int(timestamp)


def parse_full_vb_producer_line(line):
    timestamp = line.split("}")[-2].split(',')[-1]
    if(line[4] == 'a'):
        message = "}".join(line[2:].split("}")[0:2]) + "}"
    else:
        message = line[2:].split("}")[0] + '}'
    # print(line)
    # print(message, int(timestamp))
    # exit(1)
    return message, int(timestamp)


def parse_full_vb_sink_line(line):
    timestamp = line.split("}")[-2].split(',')[-1]
    if(line[4] == 'a'):
        message = "}".join(line[2:].split("}")[0:2]) + "}"
    else:
        message = '{' + line[2:].split("}")[0].split('{')[-1] + '}'
    # print(line)
    # print(message, int(timestamp))
    # exit(1)
    return message, int(timestamp)


## Here we need to register all different producer-sink line-parsing
## functions for different experiments
def parse_producer_line(line, experiment):
    if(experiment == "value-barrier"):
        return parse_vb_producer_line(line)
    elif(experiment == "stream-table-join"):
        return parse_stream_table_join_producer_line(line)
    elif(experiment == "full-value-barrier"):
        return parse_full_vb_producer_line(line)
    else:
        print("Error: Don't know how to parse producer lines for {} experiment!".format(experiment))
        exit(1)

def parse_sink_line(line, experiment):
    if(experiment == "value-barrier"):
        return parse_vb_sink_line(line)
    elif(experiment == "stream-table-join"):
        return parse_stream_table_join_sink_line(line)
    elif(experiment == "full-value-barrier"):
        return parse_full_vb_sink_line(line)
    else:
        print("Error: Don't know how to parse sink lines for {} experiment!".format(experiment))
        exit(1)


def read_preprocess_latency_data(log_dir_name, experiment="value-barrier"):
    log_file_names = os.listdir(log_dir_name)
    producer_file_names = [path.join(log_dir_name, filename)
                           for filename in log_file_names
                           if filename.startswith('producer_<')]
    sink_file_names = [path.join(log_dir_name, filename)
                       for filename in log_file_names
                       if filename.startswith('sink_<')]

    producer_dic = {}
    for producer_file_name in producer_file_names:
        with open(producer_file_name) as file:
            producer_dic.update(parse_producer_line(line, experiment) for line in file.readlines())

    # print(producer_dic)
    sink_dic = {}
    for sink_file_name in sink_file_names:
        with open(sink_file_name) as file:
            sink_dic.update(parse_sink_line(line, experiment) for line in file.readlines())
    # print(sink_dic)

    if(experiment == "full-value-barrier"):
        unsorted_latency_pairs = [(sink_dic[msg] - producer_dic[msg], sink_dic[msg])
                                  for msg in sink_dic.keys() if msg in producer_dic]
        not_found_keys = [msg for msg in sink_dic.keys() if not msg in producer_dic]
        if(len(not_found_keys) > 0):
            print(" !! {} keys not found:".format(len(not_found_keys)))
    else:
        unsorted_latency_pairs = [(sink_dic[msg] - producer_dic[msg], sink_dic[msg])
                                  for msg in sink_dic.keys()]
    latency_pairs = sorted(unsorted_latency_pairs, key=itemgetter(1))

    ## Latencies and timestamps are per millisecond
    raw_timestamps = [ts for lat, ts in latency_pairs]
    if not raw_timestamps:
        print(f'{log_dir_name}: No raw timestamps!')
    first_ts = raw_timestamps[0]
    timestamps = [(ts - first_ts) / 1_000_000.0 for ts in raw_timestamps]
    latencies = [lat / 1_000_000.0 for lat, ts in latency_pairs]

    return timestamps, latencies


def get_erlang_latencies(result_path):
    ts, latencies = read_preprocess_latency_data(result_path)
    p10 = np.percentile(latencies, 10)
    p50 = np.percentile(latencies, 50)
    p90 = np.percentile(latencies, 90)
    return p10, p50, p90


# Processing Erlang throughputs

def get_flumina_net_runtime(log_dir):
    producer_filename = path.join(log_dir, 'producers_time.log')
    with open(producer_filename) as file:
        lines = file.readlines()
        start_time_ns = int(lines[0].split(':')[-1])

    sink_filename = path.join(log_dir, 'sink_stats.log')
    with open(sink_filename) as file:
        lines = file.readlines()
        end_time_ns = int(lines[1].split(':')[-1])

    net_runtime_ms = (end_time_ns - start_time_ns) / 1000000
    return net_runtime_ms

def get_events_processed(log_dir):
    filename = path.join(log_dir, 'experiment_stats.log')
    with open(filename) as file:
        lines = file.readlines()
        number_events = int(lines[0].split(':')[-1])
        return number_events

def get_erlang_throughput(log_dir):
    runtime = get_flumina_net_runtime(log_dir)
    events = get_events_processed(log_dir)
    new_throughput = events / runtime
    # print("New:", new_throughput)
    return new_throughput

# NS3 log parsing

def get_network_stats_file(log_dir):
    files = [f for f in os.listdir(log_dir) if f.startswith('ns3') and f.endswith('stats.txt')]
    if len(files) == 0:
        sys.exit('Network statistics file not found!')
    elif len(files) >= 2:
        sys.exit('Multiple network statistics files found!')
    return path.join(log_dir, files[0])


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
