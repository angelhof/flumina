import sys
import os
from datetime import datetime
import shutil
from operator import itemgetter
import itertools

from matplotlib.ticker import FormatStrFormatter

sys.path.append(os.path.relpath("./scripts"))
from lib import *
import matplotlib.pyplot as plt
import numpy as np

def plot_scaleup_node_rate(dirname, prefix, rate_multiplier, ratio_ab, heartbeat_rate, a_nodes_numbers, optimizer):

    dirnames = ['%s_%d_%d_%d_%d_%s' % (prefix, rate_multiplier, ratio_ab, heartbeat_rate, a_node, optimizer)
                for a_node in a_nodes_numbers]
    common_plot_scaleup(dirname, dirnames, a_nodes_numbers, 'Number of nodes', 'ab_varied_nodes')

def plot_scaleup_rate(dirname, prefix, rate_multipliers, ratio_ab, heartbeat_rate, a_nodes_number, optimizer):

    dirnames = ['%s_%d_%d_%d_%d_%s' % (prefix, rate_mult, ratio_ab, heartbeat_rate, a_nodes_number, optimizer)
                for rate_mult in rate_multipliers]
    common_plot_scaleup(dirname, dirnames, rate_multipliers, 'Rate Multiplier', 'ab_varied_rates')


def plot_scaleup_ratioab(dirname, prefix, rate_multiplier, ratios_ab, heartbeat_rate, a_nodes_number, optimizer):
    dirnames = ['%s_%d_%d_%d_%d_%s' % (prefix, rate_multiplier, ratio_ab, heartbeat_rate, a_nodes_number, optimizer)
                for ratio_ab in ratios_ab]
    common_plot_scaleup(dirname, dirnames, ratios_ab, 'Rate_a / Rate_b', 'ab_varied_abratio', 'linear')


def plot_scaleup_heartbeats(dirname, prefix, rate_multiplier, ratio_ab, heartbeat_rates, a_nodes_number, optimizer):
    dirnames = ['%s_%d_%d_%d_%d_%s' % (prefix, rate_multiplier, ratio_ab, heartbeat_rate, a_nodes_number, optimizer)
                for heartbeat_rate in heartbeat_rates]
    output_name = '%s_rate-%d_heart-%d_as-%d_%s' % (prefix, rate_multiplier, heartbeat_rate, a_nodes_number, optimizer)

    path_dirnames = [os.path.join(dirname, name) for name in dirnames]
    latencies = [read_preprocess_latency_data(path_dirname) for path_dirname in path_dirnames]
    network_data = [get_network_data(path_dirname) / (1024.0 * 1024.0)  for path_dirname in path_dirnames]

    ## Get the average, 10th, and 90th percentile latencies
    median_latencies = [np.percentile(lats, 50) for ts, lats in latencies]
    ten_latencies = [np.percentile(lats, 10) for ts, lats in latencies]
    ninety_latencies = [np.percentile(lats, 90) for ts, lats in latencies]

    fig, ax = plt.subplots()
    ax.set_xlabel('Network data (MB)')
    ax.set_ylabel('Latency (ms)')
    plt.xscale('log')
    plt.yscale('log')
    plt.xticks(network_data, ["" if item < 0.7 else "{0:.1f}".format(item) for item in network_data], rotation=-60)
    ax.plot(network_data, ninety_latencies, '-^', label = '90th percentile', color = 'tab:red', linewidth=0.5)
    ax.plot(network_data, median_latencies, '-o', label = 'median', linewidth=0.5)
    ax.plot(network_data, ten_latencies, '-s', label = '10th percentile', color = 'tab:green', linewidth=0.5)
    ax.legend()
    plt.tight_layout()
    plt.savefig(os.path.join('plots', 'ab_varied_hbratio.pdf'))


def common_plot_scaleup(dirname, dirnames, xticks, xlabel, output_name, yscale='log'):
    ## We assume that all directories are there
    path_dirnames = [os.path.join(dirname, name) for name in dirnames]
    latencies = [read_preprocess_latency_data(path_dirname) for path_dirname in path_dirnames]
    throughputs = [read_preprocess_throughput_data(path_dirname) for path_dirname in path_dirnames]

    ## Get the average, 10th, and 90th percentile for both latencies and throughputs
    median_latencies = [np.percentile(lats, 50) for ts, lats in latencies]
    ten_latencies = [np.percentile(lats, 10) for ts, lats in latencies]
    ninety_latencies = [np.percentile(lats, 90) for ts, lats in latencies]
    mean_throughputs = [np.mean(ths) for ts, ths in throughputs]

    fig, ax = plt.subplots()
    ax.set_xlabel('Throughput (msgs/ms)')
    ax.set_ylabel('Latency (ms)')
    plt.yscale(yscale)
    ax.plot(mean_throughputs, ninety_latencies, '-^', label='90th percentile', color='tab:red', linewidth=0.5)
    ax.plot(mean_throughputs, median_latencies, '-o', label='median', linewidth=0.5)
    ax.plot(mean_throughputs, ten_latencies, '-s', label='10th percentile', color='tab:green', linewidth=0.5)
    ax.legend()

    plt.tight_layout()
    plt.savefig(os.path.join('plots', output_name + ".pdf"))


def common_plot_scaleup_old(dirname, dirnames, xticks, xlabel, output_name):
    ## We assume that all directories are there
    path_dirnames = [os.path.join(dirname, name) for name in dirnames]
    latencies = [read_preprocess_latency_data(path_dirname) for path_dirname in path_dirnames]
    throughputs = [read_preprocess_throughput_data(path_dirname) for path_dirname in path_dirnames]

    ## Get the average, 10th, and 90th percentile for both latencies and throughputs
    # avg_latencies = [np.mean(lats) for ts, lats in latencies]
    avg_latencies = [np.percentile(lats, 50) for ts, lats in latencies]
    ten_latencies = [np.percentile(lats, 10) for ts, lats in latencies]
    ninety_latencies = [np.percentile(lats, 90) for ts, lats in latencies]
    ten_latencies_diff = [l - ml for l, ml in zip(avg_latencies, ten_latencies)]
    ninety_latencies_diff = [ml - l for l, ml in zip(avg_latencies, ninety_latencies)]

    avg_throughputs = [np.mean(ths) for ts, ths in throughputs]
    # avg_throughputs = [np.percentile(ths, 50) for ts, ths in throughputs]
    ten_throughputs = [np.percentile(ths, 10) for ts, ths in throughputs]
    ninety_throughputs = [np.percentile(ths, 90) for ts, ths in throughputs]
    ten_throughputs_diff = [l - ml for l, ml in zip(avg_throughputs, ten_throughputs)]
    ninety_throughputs_diff = [ml - l for l, ml in zip(avg_throughputs, ninety_throughputs)]

    # print ten_latencies_diff

    inds = range(len(xticks))

    ## Plot latencies
    fig, ax1 = plt.subplots()
    color = 'tab:red'
    ax1.set_xlabel(xlabel)
    ax1.set_ylabel('latency (ms)', color = color)
    ## Errorbar alternative
    ax1.errorbar(inds, avg_latencies, [ten_latencies_diff, ninety_latencies_diff],
                 linestyle = '-', marker = 'o', label = 'mean latency', color = color,
                 capthick = 1, capsize = 4)
    ## All plots alternative
    # ax1.plot(inds, avg_latencies, '-o', label='mean latency', color=color)
    # ax1.plot(inds, ten_latencies, '-o', label='10th percentile latency', color=color)
    # ax1.plot(inds, ninety_latencies, '-o', label='90th percentile latency', color=color)
    ax1.tick_params(axis = 'y', labelcolor = color)
    # ax1.set_ylim(top=max(ninety_latencies) * 1.1)
    # ax1.set_ylim(top=100)
    # ax1.set_yscale("log")
    ax1.set_ylim(bottom = 0)

    ## Plot all throughputs
    ax2 = ax1.twinx()  # instantiate a second axes that shares the same x-axis
    color = 'tab:blue'
    ax2.set_ylabel('throughput (#messages/ms)', color = color)  # we already handled the x-label with ax1
    ## Errorbar alternative
    # ax2.errorbar(inds, avg_throughputs, [ten_throughputs_diff, ninety_throughputs_diff],
    #              linestyle='-', marker='o', label='mean throughput', color=color)
    ## All plots alternative
    ax2.plot(inds, avg_throughputs, '-o', label = 'mean throughput', color = color)
    # ax2.plot(inds, ten_throughputs, '-o', label='10th percentile throughput', color=color)
    # ax2.plot(inds, ninety_throughputs, '-o', label='90th percentile throughput', color=color)
    ax2.tick_params(axis = 'y', labelcolor = color)
    ax2.set_ylim(bottom = 0)

    fig.tight_layout()  # otherwise the right y-label is slightly clipped
    # plt.legend(loc='best')
    plt.xticks(inds, xticks)
    plt.title('Mean Throughput and Median Latency (10th - 90th percentile) ')
    plt.tight_layout()
    plt.savefig(os.path.join('plots', output_name + ".png"))
    plt.show()

    # plt.xlabel('rate multiplier')

    # plt.show()


if __name__ == '__main__':
    plt.rcParams.update({'font.size': 14})

    # The full range of rates is range(10, 35, 2)
    plot_scaleup_rate('archive', 'ab_exp1', range(10, 27, 2), 1000, 10, 18, 'optimizer_greedy')

    # The full range of nodes is range(2, 33, 2)
    plot_scaleup_node_rate('archive', 'ab_exp2', 15, 1000, 10, range(2, 29, 2), 'optimizer_greedy')

    #The full range of a-to-b ratios is [10, 20, 25, 30, 35, 40, 50, 100, 200, 500, 1000]
    plot_scaleup_ratioab('archive', 'ab_exp3', 15, [40, 50, 100, 200, 500, 1000], 10, 5, 'optimizer_greedy')

    plot_scaleup_heartbeats('archive', 'ab_exp5', 15, 10000, [1, 2, 5, 10, 100, 1000, 10000], 5, 'optimizer_greedy')
