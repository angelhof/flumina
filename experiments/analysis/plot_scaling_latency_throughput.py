import sys
import os
from datetime import datetime
import shutil
from operator import itemgetter
import itertools

from matplotlib.ticker import FormatStrFormatter

sys.path.append(os.path.relpath("./scripts"))
sys.path.append(os.path.relpath("./experiment"))
from lib import *
import results
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

def plot_scaleup_full_value_barrier_rate(dirname, prefix, rate_multipliers, ratio_ab, heartbeat_rate, a_nodes_number, optimizer):

    dirnames = ['%s_%d_%d_%d_%d_%s' % (prefix, rate_mult, ratio_ab, heartbeat_rate, a_nodes_number, optimizer)
                for rate_mult in rate_multipliers]
    common_plot_scaleup(dirname, dirnames, rate_multipliers, 'Rate Multiplier', 'full_ab_varied_rates', experiment="full-value-barrier")

def plot_scaleup_ratioab(dirname, prefix, rate_multiplier, ratios_ab, heartbeat_rate, a_nodes_number, optimizer):
    dirnames = ['%s_%d_%d_%d_%d_%s' % (prefix, rate_multiplier, ratio_ab, heartbeat_rate, a_nodes_number, optimizer)
                for ratio_ab in ratios_ab]
    common_plot_scaleup(dirname, dirnames, ratios_ab, 'Rate_a / Rate_b', 'ab_varied_abratio', 'linear')

def plot_stream_table_join_scaleup_rate(dirname, prefix, uids, page_view_nodes, thin_uids, rate_multipliers):
    dirnames = ['%s_%d_%d_%d_%d' % (prefix, uids, page_view_nodes, thin_uids, rate_multiplier)
                for rate_multiplier in rate_multipliers]
    common_plot_scaleup(dirname, dirnames, rate_multipliers, 'Rate Multiplier', 'stream_join_rate_scaleup', experiment="stream-table-join")

def plot_stream_table_join_scaleup_nodes(dirname, prefix, uids, page_view_nodes, thin_uids, rate_multiplier):
    dirnames = ['%s_%d_%d_%d_%d' % (prefix, uids, page_view_node, thin_uids, rate_multiplier)
                for page_view_node in page_view_nodes]
    common_plot_scaleup(dirname, dirnames, page_view_nodes, 'Rate Multiplier', 'stream_join_node_scaleup', yscale='linear', experiment="stream-table-join")


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


def common_plot_scaleup(dirname, dirnames, xticks, xlabel, output_name, yscale='log', experiment="value-barrier"):
    print("Plotting:", output_name, experiment)
    ## We assume that all directories are there
    path_dirnames = [os.path.join(dirname, name) for name in dirnames]
    latencies = [results.read_preprocess_latency_data(path_dirname, experiment) for path_dirname in path_dirnames]
    throughputs = [results.get_erlang_throughput(path_dirname) for path_dirname in path_dirnames]

    ## Get the average, 10th, and 90th percentile for both latencies and throughputs
    median_latencies = [np.percentile(lats, 50) for ts, lats in latencies]
    ten_latencies = [np.percentile(lats, 10) for ts, lats in latencies]
    ninety_latencies = [np.percentile(lats, 90) for ts, lats in latencies]
    # mean_throughputs = [np.mean(ths) for ts, ths in throughputs]

    fig, ax = plt.subplots()
    ax.set_xlabel('Throughput (msgs/ms)')
    ax.set_ylabel('Latency (ms)')
    plt.yscale(yscale)
    ax.plot(throughputs, ninety_latencies, '-^', label='90th percentile', color='tab:red', linewidth=0.5)
    ax.plot(throughputs, median_latencies, '-o', label='median', linewidth=0.5)
    ax.plot(throughputs, ten_latencies, '-s', label='10th percentile', color='tab:green', linewidth=0.5)
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


def collect_latency_line(dirname, dirnames):
    return []

def plot_latency_over_nodes_and_ratios(dirname, prefix):
    experiment = "value-barrier"
    # ratios = [10, 100, 1000, 10000]
    ratios = [100, 1000, 10000]
    all_nodes = range(2, 42, 2)
    # nodes = range(2, 24, 2)
    multiplier = 10


    ## Collect and plot results
    fig, ax = plt.subplots()
    ax.set_xlabel('#Workers')
    ax.set_ylabel('Latency (ms)')
    # plt.yscale("log")
    plt.ylim(top=40)
    for ratio in ratios:
        if(ratio == 100):
            nodes = range(2,23,2)
        else:
            nodes = all_nodes
        heartbeat_rate = ratio // 100
        dirnames = ['{}_{}_{}_{}_{}_optimizer_greedy'.format(prefix,
                                                             multiplier,
                                                             ratio, heartbeat_rate, a_node)
                    for a_node in nodes]
        path_dirnames = [os.path.join(dirname, name) for name in dirnames]
        latencies = [results.read_preprocess_latency_data(path_dirname, experiment)
                     for path_dirname in path_dirnames]
        median_latencies = [np.percentile(lats, 50) for ts, lats in latencies]
        ten_latencies = [np.percentile(lats, 10) for ts, lats in latencies]
        ninety_latencies = [np.percentile(lats, 90) for ts, lats in latencies]
        ten_latencies_diff = [l - ml for l, ml in zip(median_latencies, ten_latencies)]
        ninety_latencies_diff = [ml - l for l, ml in zip(median_latencies, ninety_latencies)]

        ## Plot
        ax.errorbar(nodes, median_latencies, [ten_latencies_diff, ninety_latencies_diff],
                    linestyle = '-', marker = 'o', label = 'vb-ratio: {}'.format(ratio),
                    # color = color,
                    capthick = 1, capsize = 4)
    ax.legend()
    fig.set_size_inches(5, 6)

    plt.tight_layout()
    plt.savefig(os.path.join('plots', "synchronization_cost_ratios_nodes.pdf"),bbox_inches='tight')


def plot_latency_over_heartbeats_ratios(dirname, prefix):
    experiment = "value-barrier"
    ratios = [1000, 10000]
    nodes = 5
    multiplier = 10
    heartbeat_rates = [10, 20, 50, 100, 200, 500, 1000]

    ## Collect results
    all_latencies = []
    for ratio in ratios:
        dirnames = ['{}_{}_{}_{}_{}_optimizer_greedy'.format(prefix,
                                                             multiplier,
                                                             ratio, heartbeat_rate, nodes)
                    for heartbeat_rate in heartbeat_rates]
        path_dirnames = [os.path.join(dirname, name) for name in dirnames]
        latencies = [results.read_preprocess_latency_data(path_dirname, experiment)
                     for path_dirname in path_dirnames]
        median_latencies = [np.percentile(lats, 50) for ts, lats in latencies]
        ten_latencies = [np.percentile(lats, 10) for ts, lats in latencies]
        ninety_latencies = [np.percentile(lats, 90) for ts, lats in latencies]
        ten_latencies_diff = [l - ml for l, ml in zip(median_latencies, ten_latencies)]
        ninety_latencies_diff = [ml - l for l, ml in zip(median_latencies, ninety_latencies)]
        all_latencies.append((median_latencies, ten_latencies_diff, ninety_latencies_diff))

    ## Plot results
    fig, ax = plt.subplots()
    ax.set_xlabel('Heartbeat Rate (per Barrier event)')
    ax.set_ylabel('Latency (ms)')
    # plt.yscale("log")
    for i, lats in enumerate(all_latencies):
        median_lats, ten_lats, ninety_lats = lats
        ax.errorbar(heartbeat_rates, median_lats, [ten_lats, ninety_lats],
                    linestyle = '-', marker = 'o', label = 'vb-ratio: {}'.format(ratios[i]),
                    # color = color,
                    capthick = 1, capsize = 4)
    ax.legend()
    # fig.set_size_inches(6, 4)
    fig.set_size_inches(5, 6)
    plt.tight_layout()
    plt.savefig(os.path.join('plots', "synchronization_cost_heartbeats_ratios.pdf"),bbox_inches='tight')


def plot_checkpointing_latencies(dirname, prefix):
    experiment = "value-barrier"
    ratio = 1000
    ratio = 100
    nodes = 5
    multiplier = 10
    heartbeat_rate = 10

    ## Collect results
    no_check_dirname = '{}_{}_{}_{}_{}_optimizer_greedy'.format(prefix,
                                                                multiplier,
                                                                ratio, heartbeat_rate, nodes)
    no_check_full_dirname = os.path.join(dirname, no_check_dirname)
    _ts, no_check_latencies = results.read_preprocess_latency_data(no_check_full_dirname, experiment)

    check_dirname = '{}_{}_{}_{}_{}_optimizer_greedy_with_checkpoint'.format(prefix,
                                                                             multiplier,
                                                                             ratio, heartbeat_rate, nodes)
    check_full_dirname = os.path.join(dirname, check_dirname)
    _cts, check_latencies = results.read_preprocess_latency_data(check_full_dirname, experiment)

    # print(check_latencies[:10])
    # exit(1)
    no_check_latencies.sort()
    check_latencies.sort()
    fractions = [x / len(no_check_latencies) for x in range(len(no_check_latencies))]

    ## Plot results
    fig, ax = plt.subplots()
    ax.set_xlabel('Latency')
    ax.set_ylabel('Fraction')
    plt.xscale("log")
    plt.ylim(top=1)
    plt.ylim(bottom=0)
    ax.plot(no_check_latencies, fractions, label = 'No Checkpointing')
    ax.plot(check_latencies, fractions, label = 'Checkpointing')
    ax.legend()
    fig.set_size_inches(10, 3)
    fig.set_size_inches(5, 6)
    plt.tight_layout()
    plt.savefig(os.path.join('plots', "checkpointing_costs.pdf"),bbox_inches='tight')


def plot_relative_max_throughputs_ab_example(dirname):
    prefix = "ab_exp_1"
    experiment = "value-barrier"
    ratio_ab = 10000
    heartbeat_rate = 100
    optimizer = "optimizer_greedy"
    ## After 16 it starts getting worse
    a_nodes_numbers =  [1] + list(range(2,17,2))
    rate_multipliers = list(range(100))

    maximums = []
    max_rates = []
    for a_nodes_number in a_nodes_numbers:
        dirnames = ['%s_%d_%d_%d_%d_%s' % (prefix, rate_mult, ratio_ab, heartbeat_rate, a_nodes_number, optimizer)
                    for rate_mult in rate_multipliers]
        path_dirnames = [os.path.join(dirname, name) for name in dirnames]
        throughputs = [results.get_erlang_throughput(path_dirname) for path_dirname in path_dirnames]
        max_throughput = max(throughputs)
        index = throughputs.index(max_throughput)
        max_rates.append(rate_multipliers[index])
        maximums.append(max_throughput)

    # print(maximums)
    print(max_rates)
    flink_maximums = [280.0, 465, 748, 1261, 1493, 2036, 1615, 1880, 1587]
    plot_relative_max_throughputs_common(experiment, a_nodes_numbers, maximums, flink_maximums)


def plot_relative_max_throughputs_stream_table_join(dirname):
    prefix = "stream_table_join"
    experiment = "stream-table-join"
    uids = 2
    page_view_nodes = [1] + list(range(2,21,2))
    thin_uids = 0
    rate_multipliers = list(range(100))

    maximums = []
    max_rates = []

    ## Sequential time
    dirnames = ['%s_1_1_1_%d' % (prefix, rate_multiplier)
                for rate_multiplier in rate_multipliers]
    path_dirnames = [os.path.join(dirname, name) for name in dirnames]
    throughputs = [results.get_erlang_throughput(path_dirname) for path_dirname in path_dirnames]
    max_throughput = max(throughputs)
    index = throughputs.index(max_throughput)
    max_rates.append(rate_multipliers[index])
    maximums.append(max_throughput)

    ## Parallel times
    for pvn in page_view_nodes:
        dirnames = ['%s_%d_%d_%d_%d' % (prefix, uids, pvn, thin_uids, rate_multiplier)
                    for rate_multiplier in rate_multipliers]
        path_dirnames = [os.path.join(dirname, name) for name in dirnames]
        throughputs = [results.get_erlang_throughput(path_dirname) for path_dirname in path_dirnames]
        max_throughput = max(throughputs)
        index = throughputs.index(max_throughput)
        max_rates.append(rate_multipliers[index])
        maximums.append(max_throughput)

    # print(maximums)
    print("Max rates:", max_rates)
    ticks = [1] + [pvn * 2 for pvn in page_view_nodes]
    flink_maximums = [447.0, 778, 996, 884, 1096, 1032, 1051, 979, 995]
    plot_relative_max_throughputs_common(experiment, ticks[:len(flink_maximums)], maximums[:len(flink_maximums)], flink_maximums)


def plot_relative_max_throughputs_full_value_barrier_example(dirname):
    prefix = "ab_exp_full_1"
    experiment = "full-value-barrier"
    ratio_ab = 10000
    heartbeat_rate = 100
    optimizer = "optimizer_greedy"
    ## After 16 it starts getting worse
    a_nodes_numbers =  [1] + list(range(2,17,2))
    rate_multipliers = list(range(100))

    maximums = []
    max_rates = []
    for a_nodes_number in a_nodes_numbers:
        dirnames = ['%s_%d_%d_%d_%d_%s' % (prefix, rate_mult, ratio_ab, heartbeat_rate, a_nodes_number, optimizer)
                    for rate_mult in rate_multipliers]
        path_dirnames = [os.path.join(dirname, name) for name in dirnames]
        throughputs = [results.get_erlang_throughput(path_dirname) for path_dirname in path_dirnames]
        max_throughput = max(throughputs)
        index = throughputs.index(max_throughput)
        max_rates.append(rate_multipliers[index])
        maximums.append(max_throughput)

    # print(maximums)
    print(max_rates)
    flink_maximums = [337.0, 551, 466, 508, 497, 513, 485, 463, 441]
    plot_relative_max_throughputs_common(experiment, a_nodes_numbers, maximums, flink_maximums)


def plot_relative_max_throughputs_common(experiment, ticks, maximums, flink_maximums):
    print("Maximums:", maximums)
    speedups = [maxim / maximums[0] for maxim in maximums]
    flink_speedups = [fm / flink_maximums[0] for fm in flink_maximums]
    print("Speedups:", speedups)
    print("Flink Speedups:", flink_speedups)
    fig, ax = plt.subplots()
    ax.set_xlabel('Parallelism')
    ax.set_ylabel('Throughput Increase')
    # plt.xscale("log")
    ax.plot(ticks, speedups, '-o', label='Flumina')
    if (len(flink_speedups) > 0):
        ax.plot(ticks, flink_speedups, '-^', label='Flink')
    plt.xticks(ticks)
    ax.legend()
    # fig.set_size_inches(10, 3)
    plt.tight_layout()
    plt.savefig(os.path.join('plots', "{}_max_throughput_scaleup.pdf".format(experiment)),bbox_inches='tight')

if __name__ == '__main__':
    plt.rcParams.update({'font.size': 14})

    SMALL_SIZE = 22
    MEDIUM_SIZE = 24
    BIGGER_SIZE = 30

    plt.rc('font', size=SMALL_SIZE)          # controls default text sizes
    plt.rc('axes', titlesize=MEDIUM_SIZE)     # fontsize of the axes title
    plt.rc('axes', labelsize=MEDIUM_SIZE)    # fontsize of the x and y labels
    plt.rc('xtick', labelsize=SMALL_SIZE)    # fontsize of the tick labels
    plt.rc('ytick', labelsize=SMALL_SIZE)    # fontsize of the tick labels
    plt.rc('legend', fontsize=SMALL_SIZE)    # legend fontsize
    plt.rc('figure', titlesize=BIGGER_SIZE)  # fontsize of the figure title

    plt.rcParams['mathtext.fontset'] = 'stix'
    plt.rcParams['font.family'] = 'STIXGeneral'


    ## Plot Max throughput scaleup for ab-example
    plot_relative_max_throughputs_ab_example('archive/ab-example-max-throughput-scaleup/archive')

    ## Plot Max throughput scaleup for stream-table-join
    plot_relative_max_throughputs_stream_table_join('archive/stream-table-max-throughput-scaleup/archive')

    ## Plot max throughput scaleup for full value barrier
    plot_relative_max_throughputs_full_value_barrier_example('archive/full-ab-example-max-throughput-scaleup/archive')

    # ## Plot sequential throughput vs latency for ab-example
    # ## Old ranges
    # plot_scaleup_rate('archive/sequential-ab-example/archive', 'ab_exp_1', range(40, 92, 2), 1000, 10, 1, 'optimizer_greedy')
    plot_scaleup_rate('archive/sequential-ab-example-new/archive', 'ab_exp_1', range(100, 201, 10), 1000, 10, 1, 'optimizer_greedy')

    # plot_stream_table_join_scaleup_rate('archive/sequential-stream-table-join-example/archive', 'stream_table_join', 1, 1, 1, range(20,71,2))

    # ## Plot sequential throughput vs latency for full-value-barrier-example
    # plot_scaleup_full_value_barrier_rate('archive/sequential-full-value-barrier-example/archive', 'ab_exp_full_1', range(22, 61, 2), 10000, 100, 1, 'optimizer_greedy')





    # plot_scaleup_rate('archive/ab_example_rate_scaleup_20-64/archive', 'ab_exp_1', range(30, 62, 2), 1000, 10, 5, 'optimizer_greedy')

    # plot_scaleup_node_rate('archive/ab_example_node_scaleup_2-20/archive', 'ab_exp_2', 40, 1000, 10, range(2, 13, 2), 'optimizer_greedy')

    # # # plot_stream_table_join_scaleup_rate('archive/rate_scaleup_10-100/archive', 'stream_table_join', 2, 5, range(10,105,5))
    # plot_stream_table_join_scaleup_rate('archive/rate_scaleup_4-64_fat_main_prods_in_workers/archive', 'stream_table_join', 2, 1, 0, range(10,57,2))


    ## Rate 15 -- 10k ratio
    # plot_stream_table_join_scaleup_nodes('archive/node_scaleup_2-20_fat_main_prods_in_workers/archive', 'stream_table_join', 2, range(2,17,2), 0, 15)

    ## Rate 20 -- 10k ratio
    # plot_stream_table_join_scaleup_nodes('archive/stream_table_join_node_scaleup_2-20_rate_20/archive', 'stream_table_join', 2, range(2,13,2), 0, 20)

    # Rate 20 -- 50k ratio
    # plot_stream_table_join_scaleup_nodes('archive/stream_table_join_node_scaleup_2-20_rate_20_ratio_50k/archive', 'stream_table_join', 2, range(2,11,2), 0, 20)

    ## Rate 20 -- 100k ratio
    # plot_stream_table_join_scaleup_nodes('archive/stream_table_join_node_scaleup_2-20_rate_20_ratio_100k/archive', 'stream_table_join', 2, range(2,21,2), 0, 20)

    

    # ## Full value barrier
    # plot_scaleup_full_value_barrier_rate('archive/full_ab_rate/archive', 'ab_exp_full_1', range(20, 39, 2), 10000, 100, 5, 'optimizer_greedy')


    ## Synchronization costs
    plot_latency_over_nodes_and_ratios('archive/synchronization_costs/ratios_nodes/archive', 'ab_exp_2')

    plot_latency_over_heartbeats_ratios('archive/synchronization_costs/ratios_nodes/archive', 'ab_exp_2')

    ## Checkpointing_cost
    plot_checkpointing_latencies('archive/checkpointing_cost/archive', 'ab_exp_2')


    # plot_stream_table_join_scaleup_nodes('archive/archive', 'stream_table_join', 2, range(1,11), 5)

    # # The full range of nodes is range(2, 33, 2)
    # plot_scaleup_node_rate('archive', 'ab_exp2', 15, 1000, 10, range(2, 29, 2), 'optimizer_greedy')

    # #The full range of a-to-b ratios is [10, 20, 25, 30, 35, 40, 50, 100, 200, 500, 1000]
    # plot_scaleup_ratioab('archive', 'ab_exp3', 15, [40, 50, 100, 200, 500, 1000], 10, 5, 'optimizer_greedy')

    # plot_scaleup_heartbeats('archive', 'ab_exp5', 15, 10000, [1, 2, 5, 10, 100, 1000, 10000], 5, 'optimizer_greedy')
