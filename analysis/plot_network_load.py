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
    kbytes = []
    with open(timeseries_file, 'r') as f:
        reader = csv.reader(f, delimiter = ',')
        for line in reader:
            timestamps.append(float(line[0]) / 1000.0 - 5.0)
            kbytes.append(2.0 * float(line[2]) / 1024.0)
    return timestamps, kbytes


def main():
    fig, ax = plt.subplots()
    ax.set_xlabel('Time (s)')
    ax.set_ylabel('Network load (KB/s)')

    colors = ['tab:red', 'tab:blue', 'tab:green']

    for i, log_dir in enumerate(sys.argv[1:]):
        timeseries_file = get_timeseries_file(log_dir)
        timestamps, kbytes = get_data(timeseries_file)
        ax.plot(timestamps, kbytes, '-', color=colors[i])

    plt.show()


if __name__ == '__main__':
    main()
