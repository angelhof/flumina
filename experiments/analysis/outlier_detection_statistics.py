import sys
import os

sys.path.append(os.path.relpath("./scripts"))
from lib import *
import numpy as np
import matplotlib.pyplot as plt

SMALL_SIZE = 22
MEDIUM_SIZE = 24
BIGGER_SIZE = 26

plt.rc('font', size=MEDIUM_SIZE)          # controls default text sizes
plt.rc('axes', titlesize=BIGGER_SIZE)     # fontsize of the axes title
plt.rc('axes', labelsize=MEDIUM_SIZE)    # fontsize of the x and y labels
plt.rc('xtick', labelsize=MEDIUM_SIZE)    # fontsize of the tick labels
plt.rc('ytick', labelsize=MEDIUM_SIZE)    # fontsize of the tick labels
plt.rc('legend', fontsize=SMALL_SIZE)    # legend fontsize
plt.rc('figure', titlesize=BIGGER_SIZE)  # fontsize of the figure title

plt.rcParams['mathtext.fontset'] = 'stix'
plt.rcParams['font.family'] = 'STIXGeneral'


def get_throughputs(confs, format_str):
    y_throughputs = []

    for rate, nodes in confs:
        logs_dir = os.path.join('archive', format_str.format(rate, nodes))
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
    return y_throughputs

def get_ns3_stats_file(log_dir):
    files = [f for f in os.listdir(log_dir) if f.endswith('stats.txt')]
    if len(files) == 0:
        sys.exit('Time series file not found!')
    elif len(files) >= 2:
        sys.exit('Multiple time series files found!')
    return os.path.join(log_dir, files[0])


def get_network_load(confs, format_str):
    network_loads = []

    for rate, nodes in confs:
        logs_dir = os.path.join('archive', format_str.format(rate, nodes))
        ns3_stats_filename = get_ns3_stats_file(logs_dir)

        with open(ns3_stats_filename, "r") as f:
            first_line = f.readlines()[0]

        network_load = int(first_line.split(":")[1]) / 1000000
        print("Number of nodes:", nodes)
        print("Network load: {} MB".format(network_load))
        network_loads.append(network_load)
    return network_loads


## Lattice levels = 2
num_nodes = [1, 2, 4, 8, 16]
confs = [(15, i) for i in num_nodes]

## Lattice levels = 1
num_nodes = [1, 2, 4, 8, 16]
confs = [(60, i) for i in num_nodes]

## ============ Throughputs =================

# confs = list(zip([15, 15, 15, 12, 10], num_nodes))
greedy_throughputs = get_throughputs(confs, 'outlier_detection_{}_{}_optimizer_greedy')
centralized_greedy_throughputs = get_throughputs(confs, 'outlier_detection_{}_{}_optimizer_centralized_greedy')

fig, ax = plt.subplots()

## Plot Throughput
# ax.set_ylabel('Throughput (msgs/ms)')
# ax.set_xlabel('Processing Nodes')
# ax.plot(num_nodes, y_throughputs, '-o', linewidth=0.5)

# ## x = y lines
# diagonal = [y_throughputs[0] * i for i in num_nodes]
# ax.plot(num_nodes, diagonal, linewidth=0.3)

## Plot speedup
ax.set_ylabel('Relative Throughput')
ax.set_xlabel('Processing Nodes')
greedy_speedups = [t / greedy_throughputs[0] for t in greedy_throughputs]
centralized_greedy_speedups = [t / centralized_greedy_throughputs[0] for t in centralized_greedy_throughputs]
ax.plot(num_nodes, greedy_speedups, '-o', linewidth=0.5, label='Distributed')
# ax.plot(num_nodes, centralized_greedy_speedups, '-+', linewidth=0.5)
## x = y lines
ax.plot(num_nodes, num_nodes, '-', color='tab:gray', linewidth=0.5, label='Ideal')
plt.xticks(num_nodes)
plt.legend()

print(greedy_speedups)

plt.tight_layout()
plt.savefig(os.path.join('plots', "outlier_detection_throughput_scaleup.pdf"))

## ============== Network Load ==============

print("Greedy")
greedy_network_loads = get_network_load(confs, 'outlier_detection_{}_{}_optimizer_greedy')

print("Centralized Greedy")
centralized_greedy_network_loads = get_network_load(confs, 'outlier_detection_{}_{}_optimizer_centralized_greedy')

fig, ax = plt.subplots()

## Plot speedup
ax.set_ylabel('Total Network Load (MB)')
ax.set_xlabel('Processing Nodes')
greedy_relative_loads = [t / greedy_network_loads[0] for t in greedy_network_loads]
centralized_greedy_relative_loads = [t / centralized_greedy_network_loads[0] for t in centralized_greedy_network_loads]

ax.plot(num_nodes, greedy_network_loads, '-o', linewidth=0.5, label='Distributed')
ax.plot(num_nodes, centralized_greedy_network_loads, '-+', linewidth=0.5, label='Centralized')
plt.xticks(num_nodes)
plt.legend()

plt.tight_layout()
plt.savefig(os.path.join('plots', "outlier_detection_network_scaleup.pdf"))
