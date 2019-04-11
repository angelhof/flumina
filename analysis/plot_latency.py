import sys
import os
from operator import itemgetter
import itertools
import matplotlib.pyplot as plt
import numpy as np

## 1st argument should be the log directory name
log_dir_name = sys.argv[1]

def parse_producer_line(line):
    timestamp = line.split("}")[-2].split(',')[-1]
    message = line[1:].split("}")[0] + '}'
    return (message, int(timestamp))

def parse_sink_line(line):
    timestamp = line.split("}")[-2].split(',')[-1]
    message = line[1:].split("}")[0].split('{')[-1]
    return ('{' + message + '}', int(timestamp))

def read_preprocess_latency_data(log_directory_name):

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
        
    # print producer_data[:10]    
    # print sink_data[:10]
    
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
    first_ts = raw_timestamps[0]
    timestamps = [(ts - first_ts) / 1000000.0 for ts in raw_timestamps]
    latencies = [lat / 1000000.0 for lat, ts in latency_pairs]

    print latencies[:10]
    print latencies[-10:]

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
    


timestamps, latencies = read_preprocess_latency_data(log_dir_name)
fig, ax = plt.subplots()
ax.plot(timestamps, latencies)
plt.show()


