import sys
import os
from datetime import datetime
import shutil
from operator import itemgetter
import itertools
sys.path.append(os.path.relpath("./scripts"))
from lib import *
import matplotlib.pyplot as plt
import numpy as np

def plot_scaleup_rate(dirname, prefix, rate_multipliers, ratio_ab, heartbeat_rate, a_nodes_number, optimizer):

    dirnames = ['%s_%d_%d_%d_%d_%s' % (prefix, rate_mult, ratio_ab, heartbeat_rate, a_nodes_number, optimizer)
                for rate_mult in rate_multipliers]

    ## We assume that all directories are there
    path_dirnames = [os.path.join(dirname, name) for name in dirnames]
    latencies = [read_preprocess_latency_data(path_dirname) for path_dirname in path_dirnames]
    throughputs = [read_preprocess_throughput_data(path_dirname) for path_dirname in path_dirnames]

    ## Get the average, 10th, and 90th percentile for both latencies and throughputs
    avg_latencies = [np.mean(lats) for ts, lats in latencies]
    ten_latencies = [np.percentile(lats, 10) for ts, lats in latencies]
    ninety_latencies = [np.percentile(lats, 90) for ts, lats in latencies]
    ten_latencies_diff = [ l - ml for l, ml in zip(avg_latencies, ten_latencies)]
    ninety_latencies_diff = [ ml - l for l, ml in zip(avg_latencies, ninety_latencies)]
    
    avg_throughputs = [np.mean(ths) for ts, ths in throughputs]
    ten_throughputs = [np.percentile(ths, 10) for ts, ths in throughputs]
    ninety_throughputs = [np.percentile(ths, 90) for ts, ths in throughputs]
    ten_throughputs_diff = [ l - ml for l, ml in zip(avg_throughputs, ten_throughputs)]
    ninety_throughputs_diff = [ ml - l for l, ml in zip(avg_throughputs, ninety_throughputs)]
    
    # print ten_latencies_diff

    inds = range(len(rate_multipliers))

    ## Plot latencies
    fig, ax1 = plt.subplots()
    color = 'tab:red'
    ax1.set_xlabel('rate multiplier')
    ax1.set_ylabel('latency (ms)', color=color)
    ## Errorbar alternative
    ax1.errorbar(inds, avg_latencies, [ten_latencies_diff, ninety_latencies_diff],
                 linestyle='-', marker='o', label='mean latency', color=color)
    ## All plots alternative
    # ax1.plot(inds, avg_latencies, '-o', label='mean latency', color=color)
    # ax1.plot(inds, ten_latencies, '-o', label='10th percentile latency', color=color)
    # ax1.plot(inds, ninety_latencies, '-o', label='90th percentile latency', color=color)
    ax1.tick_params(axis='y', labelcolor=color)

    
    ## Plot all throughputs
    ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis
    color = 'tab:blue'
    ax2.set_ylabel('throughput (#msgs/ms', color=color)  # we already handled the x-label with ax1
    ## Errorbar alternative
    ax2.errorbar(inds, avg_throughputs, [ten_throughputs_diff, ninety_throughputs_diff],
                 linestyle='-', marker='o', label='mean throughput', color=color)
    ## All plots alternative
    # ax2.plot(inds, avg_throughputs, '-o', label='mean throughput', color=color)
    # ax2.plot(inds, ten_throughputs, '-o', label='10th percentile throughput', color=color)
    # ax2.plot(inds, ninety_throughputs, '-o', label='90th percentile throughput', color=color)
    ax2.tick_params(axis='y', labelcolor=color)
    
    fig.tight_layout()  # otherwise the right y-label is slightly clipped
    # plt.legend(loc='best')
    plt.xticks(inds, rate_multipliers)
    plt.show()
    
    # plt.xlabel('rate multiplier')
    
    # plt.show()
    






