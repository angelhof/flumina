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
                           if filename.startswith('producer')]
    sink_file_names = [path.join(log_dir_name, filename)
                       for filename in log_file_names
                       if filename.startswith('sink')]

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


# Old code for processing Erlang throughputs from lib.py
# TODO: Make it cleaner

def parse_throughput_line(line):
    timestamp_messages = line.split("}")[-2]
    timestamp, message = timestamp_messages.split(",")[-2:]
    return int(timestamp), int(message)


def read_preprocess_throughput_data(log_dir_name):
    log_file_names = os.listdir(log_dir_name)
    throughput_file_names = [path.join(log_dir_name, filename)
                             for filename in log_file_names
                             if filename.startswith('throughput')]
    # There should only be one
    filename = throughput_file_names[0]

    timestamp_message_dict = {}
    with open(filename) as file:
        timestamp_message_pairs = [parse_throughput_line(line) for line in file.readlines()]
        for timestamp, n in timestamp_message_pairs:
            if timestamp in timestamp_message_dict:
                timestamp_message_dict[timestamp] += n
            else:
                timestamp_message_dict[timestamp] = n
    final_timestamp_message_pairs = sorted(timestamp_message_dict.items(), key=itemgetter(0))

    # Remove trailing zeros
    while final_timestamp_message_pairs and final_timestamp_message_pairs[-1][1] == 0:
        final_timestamp_message_pairs.pop()

    # Remove leading zeros
    while final_timestamp_message_pairs and final_timestamp_message_pairs[0][1] == 0:
        final_timestamp_message_pairs.pop(0)

    timestamps = [ts for ts, n in final_timestamp_message_pairs]
    first_ts = timestamps[0]
    # Those are shifted to 0 and are in seconds
    shifted_timestamps = [(ts - first_ts) / 1_000_000.0 for ts in timestamps]

    ts_diffs = [ts1 - ts0 for ts1, ts0 in zip(shifted_timestamps[1:], shifted_timestamps[:-1])]
    ns = [n for ts, n in final_timestamp_message_pairs]
    # This is messages per 1 millisecond
    throughput = [n / ts_diff for n, ts_diff in zip(ns[1:], ts_diffs)]

    yp = None
    ## Interpolate to get the the steady throughput
    ts = np.linspace(shifted_timestamps[0], shifted_timestamps[-1], 3 * len(timestamps))
    ths = np.interp(ts, shifted_timestamps[1:], throughput, yp)

    return ts, ths


def get_erlang_throughput(result_path):
    # TODO: There has to be a better way. Mean throughput is total events / total time.
    ts, ths = read_preprocess_throughput_data(result_path)
    return np.mean(ths)


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
