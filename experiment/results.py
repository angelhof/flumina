import re
from os import path

import numpy as np


def process(result_path):
    if not path.isdir(result_path):
        print(f'Couldn''t find directory: {result_path}')
        exit(1)
    out_file = path.join(result_path, 'out.txt')
    if not path.isfile(out_file):
        print(f'Couldn''t find file: {out_file}')
        exit(1)
    with open(out_file, 'r') as f:
        pattern = re.compile(r'\[latency: (\d+) ms\]')
        latencies = [int(re.search(pattern, line).group(1)) for line in f.readlines()]
        p10 = np.percentile(latencies, 10)
        p50 = np.percentile(latencies, 50)
        p90 = np.percentile(latencies, 90)
        print(f'Latency percentiles (ms):\n\t10th: {p10:.0f}\n\t50th: {p50:.0f}\n\t90th: {p90:.0f}')
