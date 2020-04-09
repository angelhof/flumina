import csv
import sys
import os
import matplotlib.pyplot as plt


def get_timeseries_file(log_dir):
    files = [f for f in os.listdir(log_dir) if f.endswith('time-series.csv')]
    if len(files) == 0:
        sys.exit('Time series file not found!')
    elif len(files) >= 2:
        sys.exit('Multiple time series files found!')
    return os.path.join(log_dir, files[0])


def get_data(timeseries_file):
    timestamps = []
    mbytes = []
    with open(timeseries_file, 'r') as f:
        reader = csv.reader(f, delimiter = ',')
        for line in reader:
            timestamps.append(float(line[0]) / 1000.0 - 5.0)
            mbytes.append(2.0 * float(line[2]) / 1024.0 / 1024.0)
    return timestamps, mbytes


def main():
    plt.rcParams.update({'font.size': 14})

    fig, ax = plt.subplots()
    ax.set_xlabel('Time (s)')
    ax.set_ylabel('Network load (MB/s)')

    log_dirs = [
        os.path.join('archive', 'ab_exp4_15_1000_10_4_optimizer_greedy_hybrid'),
        os.path.join('archive', 'ab_exp4_15_1000_10_4_optimizer_sequential'),
        os.path.join('archive', 'ab_exp4_15_1000_10_4_optimizer_greedy_real')
    ]
    colors = ['tab:blue', 'tab:red', 'tab:green']
    markers = ['-', '--', '-.']
    labels = ['greedy hybrid', 'sequential', 'greedy']

    for i, log_dir in enumerate(log_dirs):
        timeseries_file = get_timeseries_file(log_dir)
        timestamps, mbytes = get_data(timeseries_file)
        ax.plot(timestamps, mbytes, markers[i], color=colors[i], label=labels[i], linewidth=1.0, ms=4.0, markevery=0.2)

    ax.legend()
    plt.tight_layout()
    plt.savefig(os.path.join('plots', "ab_varied_optimizers_network_load.pdf"))


if __name__ == '__main__':
    main()
