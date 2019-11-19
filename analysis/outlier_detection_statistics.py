import sys
import os

sys.path.append(os.path.relpath("./scripts"))
from lib import *
import numpy as np
import matplotlib.pyplot as plt


num_nodes = [1, 2, 4, 8, 16]
confs = [(15, i) for i in num_nodes]

# confs = list(zip([15, 15, 15, 12, 10], num_nodes))
y_throughputs = []

for rate, nodes in confs:
    logs_dir = os.path.join('archive', 'outlier_detection_{}_{}_optimizer_greedy'.format(rate, nodes))
    # logs_dir = os.path.join('archive', 'outlier_detection_{}_{}_optimizer_greedy_ns3'.format(rate, nodes))
    # logs_dir = os.path.join('logs')
    _, latencies = read_preprocess_latency_data(logs_dir)
    _, throughputs = read_preprocess_throughput_data(logs_dir)

    median_latency = np.percentile(latencies, 50)
    ten_latency = np.percentile(latencies, 10)
    ninety_latency = np.percentile(latencies, 90)

    avg_throughput = np.mean(throughputs)

    print("Number of nodes:", nodes)
    print("med_lat: {} ten_lat: {} ninety_lat: {} avg_throughput: {}".format(median_latency, ten_latency, ninety_latency, avg_throughput))
    y_throughputs.append(avg_throughput * 1000000)


fig, ax = plt.subplots()

## Plot Throughput
# ax.set_ylabel('Throughput (msgs/ms)')
# ax.set_xlabel('Processing Nodes')
# ax.plot(num_nodes, y_throughputs, '-o', linewidth=0.5)

# ## x = y lines
# diagonal = [y_throughputs[0] * i for i in num_nodes]
# ax.plot(num_nodes, diagonal, linewidth=0.3)

## Plot speedup
ax.set_ylabel('Speedup')
ax.set_xlabel('Processing Nodes')
speedups = [t / y_throughputs[0] for t in y_throughputs]
ax.plot(num_nodes, speedups, '-o', linewidth=0.5)
## x = y lines
ax.plot(num_nodes, num_nodes, linewidth=0.3)


plt.tight_layout()
plt.savefig(os.path.join('plots', "outlier_detection_throughput_scaleup.pdf"))
