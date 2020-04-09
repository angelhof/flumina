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
    time_pattern = re.compile(r'Total time \(ms\): (\d+)')
    events_pattern = re.compile(r'Events processed: (\d+)')
    with open(stats_file, 'r') as f:
        time = float(re.match(time_pattern, f.readline()).group(1))
        events = float(re.match(events_pattern, f.readline()).group(1))
    return events / time


# Old code for processing Erlang latencies from lib.py
# TODO: Make it a bit cleaner

def parse_producer_line(line):
    timestamp = line.split("}")[-2].split(',')[-1]
    message = line[2:].split("}")[0] + '}'
    return message, int(timestamp)


def parse_sink_line(line):
    timestamp = line.split("}")[-2].split(',')[-1]
    message = line[2:].split("}")[0].split('{')[-1]
    return '{' + message + '}', int(timestamp)


def read_preprocess_latency_data(log_dir_name):
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
            producer_dic.update(parse_producer_line(line) for line in file.readlines())

    sink_dic = {}
    for sink_file_name in sink_file_names:
        with open(sink_file_name) as file:
            sink_dic.update(parse_sink_line(line) for line in file.readlines())

    unsorted_latency_pairs = [(sink_dic[msg] - producer_dic[msg], sink_dic[msg]) for msg in sink_dic.keys()]
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
