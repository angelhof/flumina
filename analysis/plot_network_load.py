import csv
import sys
import os
import matplotlib.pyplot as plt


def get_timeseries_path(log_dir):
    files = [f for f in os.listdir(log_dir) if f.endswith('time-series.csv')]
    if len(files) == 0:
        sys.exit('Time series file not found!')
    elif len(files >= 2):
        sys.exit('Multiple time series files found!')
    return os.path.join(log_dir, files[0])


def main():
    log_dir = sys.argv[1]
    timeseries_path = get_timeseries_path(log_dir)

    timestamps = []
    kbytes = []
    with open(timeseries_path, 'r') as timeseries_file:
        reader = csv.reader(timeseries_file, delimiter=',')
        for line in reader:
            timestamps.append(float(line[0]) / 1000.0)
            kbytes.append(float(line[2]) / 1024.0)

    fig, ax = plt.subplots()
    ax.plot(timestamps, kbytes)
    plt.show()


if __name__ == '__main__':
    main()
