import sys
import os

sys.path.append(os.path.relpath("./scripts"))
from lib import *
import numpy as np

# logs_dir = os.path.join('archive', 'debs_house_query1_360_20_optimizer_greedy')
logs_dir = os.path.join('logs')
_, latencies = read_preprocess_latency_data(logs_dir)
_, throughputs = read_preprocess_throughput_data(logs_dir)

median_latency = np.percentile(latencies, 50)
ten_latency = np.percentile(latencies, 10)
ninety_latency = np.percentile(latencies, 90)

avg_throughput = np.mean(throughputs)

print("med_lat: {} ten_lat: {} ninety_lat: {} avg_throughput: {}".format(median_latency, ten_latency, ninety_latency, avg_throughput))
