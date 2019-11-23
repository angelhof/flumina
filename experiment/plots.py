from os import path

import matplotlib.pyplot as plt

import results


def plot_node_scaleup(erlang_dir, flink_dir, output_file):
    erlang_subdirs = [path.join(erlang_dir, f'ab_exp2_15_1000_10_{n}_optimizer_greedy')
                      for n in range(2, 29, 2)]
    flink_subdirs = [path.join(flink_dir, f'n{n}_r15_q1000_h10')
                     for n in range(2, 29, 2)]
    plot_scaleup(erlang_subdirs, flink_subdirs, output_file)


def plot_rate_scaleup(erlang_dir, flink_dir, output_file):
    erlang_subdirs = [path.join(erlang_dir, f'ab_exp1_{r}_1000_10_18_optimizer_greedy')
                      for r in range(10, 27, 2)]
    flink_subdirs = [path.join(flink_dir, f'n18_r{r}_q1000_h10')
                     for r in [10, 18, 26]]
    plot_scaleup(erlang_subdirs, flink_subdirs, output_file)


def plot_vb_ratio_scaleup(erlang_dir, flink_dir, output_file):
    erlang_subdirs = [path.join(erlang_dir, f'ab_exp3_15_{vb_ratio}_10_5_optimizer_greedy')
                      for vb_ratio in [1000, 500, 200, 100, 50, 40]]
    flink_subdirs = [path.join(flink_dir, f'n5_r15_q{vb_ratio}_h10')
                     for vb_ratio in [1000, 40]]
    plot_scaleup(erlang_subdirs, flink_subdirs, output_file)


def plot_scaleup(erlang_subdirs, flink_subdirs, output_file):
    erlang_latencies = [results.get_erlang_latencies(subdir) for subdir in erlang_subdirs]
    erlang_latencies_mean = [p50 for p10, p50, p90 in erlang_latencies]
    erlang_latencies_diff_10 = [p50 - p10 for p10, p50, p90 in erlang_latencies]
    erlang_latencies_diff_90 = [p90 - p50 for p10, p50, p90 in erlang_latencies]
    erlang_throughputs = [results.get_erlang_throughput(subdir) for subdir in erlang_subdirs]

    flink_latencies = [results.get_flink_latencies(subdir) for subdir in flink_subdirs]
    flink_latencies_mean = [p50 for p10, p50, p90 in flink_latencies]
    flink_latencies_diff_10 = [p50 - p10 for p10, p50, p90 in flink_latencies]
    flink_latencies_diff_90 = [p90 - p50 for p10, p50, p90 in flink_latencies]
    flink_throughputs = [results.get_flink_throughput(subdir) for subdir in flink_subdirs]

    plt.rcParams.update({'font.size': 18})
    fig, ax = plt.subplots()
    ax.set_xlabel('Throughput (events/ms)')
    ax.set_ylabel('Latency (ms)')
    plt.yscale('log')
    ax.errorbar(erlang_throughputs,
                erlang_latencies_mean, [erlang_latencies_diff_10, erlang_latencies_diff_90],
                linestyle='-', marker='o', label='Flumina',
                linewidth=1, capthick=1, capsize=3, color='tab:blue')
    ax.errorbar(flink_throughputs,
                flink_latencies_mean, [flink_latencies_diff_10, flink_latencies_diff_90],
                linestyle='--', marker='^', label='Flink',
                linewidth=1, capthick=1, capsize=3, color='tab:red')
    ax.legend()

    plt.tight_layout()
    plt.savefig(output_file)
    # plt.show()


def plot_hb_ratio_scaleup(erlang_dir, flink_dir, output_file):
    ratios = [1, 2, 5, 10, 100, 1000, 10_000]
    erlang_subdirs = [path.join(erlang_dir, f'ab_exp5_15_10000_{hb_ratio}_5_optimizer_greedy')
                      for hb_ratio in ratios]
    flink_subdirs = [path.join(flink_dir, f'n5_r15_q10000_h{hb_ratio}')
                     for hb_ratio in ratios]

    erlang_latencies = [results.get_erlang_latencies(subdir) for subdir in erlang_subdirs]
    erlang_latencies_mean = [p50 for p10, p50, p90 in erlang_latencies]
    erlang_latencies_diff_10 = [p50 - p10 for p10, p50, p90 in erlang_latencies]
    erlang_latencies_diff_90 = [p90 - p50 for p10, p50, p90 in erlang_latencies]
    erlang_network_data = [results.get_network_data(subdir) / (1024.0 * 1024.0) for subdir in erlang_subdirs]

    flink_latencies = [results.get_flink_latencies(subdir) for subdir in flink_subdirs]
    flink_latencies_mean = [p50 for p10, p50, p90 in flink_latencies]
    flink_latencies_diff_10 = [p50 - p10 for p10, p50, p90 in flink_latencies]
    flink_latencies_diff_90 = [p90 - p50 for p10, p50, p90 in flink_latencies]
    flink_network_data = [results.get_network_data(subdir) / (1024.0 * 1024.0) for subdir in flink_subdirs]

    plt.rcParams.update({'font.size': 18})
    fig, ax = plt.subplots()
    ax.set_xlabel('Network data (MB)')
    ax.set_ylabel('Latency (ms)')
    plt.xscale('log')
    plt.yscale('log')
    plt.xticks(erlang_network_data,
               ['' if item < 0.7 else f'{item:.1f}' if item < 100.0 else f'{item:.0f}'
                for item in erlang_network_data], rotation=-60)
    ax.errorbar(erlang_network_data,
                erlang_latencies_mean, [erlang_latencies_diff_10, erlang_latencies_diff_90],
                linestyle='-', marker='o', label='Flumina',
                linewidth=1, capthick=1, capsize=3, color='tab:blue')
    ax.errorbar(flink_network_data,
                flink_latencies_mean, [flink_latencies_diff_10, flink_latencies_diff_90],
                linestyle='--', marker='^', label='Flink',
                linewidth=1, capthick=1, capsize=3, color='tab:red')
    ax.legend()

    plt.tight_layout()
    plt.savefig(output_file)
    # plt.show()
