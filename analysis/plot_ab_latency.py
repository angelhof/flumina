import sys
import os
from operator import itemgetter
import itertools
import matplotlib.pyplot as plt
import numpy as np
## TODO: Find a better way to import other python files
sys.path.append(os.path.relpath("./scripts"))
from lib import *

plt.rcParams.update({'font.size': 14})

fig, ax = plt.subplots()
ax.set_xlabel('Time (s)')
ax.set_ylabel('Latency (ms)')

log_dirs = [os.path.join('archive', 'ab_exp4_15_1000_10_4_optimizer_sequential'),
            os.path.join('archive', 'ab_exp4_15_1000_10_4_optimizer_greedy_hybrid'),
            os.path.join('archive', 'ab_exp4_15_1000_10_4_optimizer_greedy_real')]
colors = ['tab:blue', 'tab:red', 'tab:green']
markers = ['o', 'x', 's']
labels = ['sequential', 'greedy hybrid', 'greedy']

for i, log_dir_name in enumerate(log_dirs):
    timestamps, latencies = read_preprocess_latency_data(log_dir_name)
    timestamps = [ts / 1000.0 for ts in timestamps]
    ax.plot(timestamps, latencies, markers[i], color=colors[i], mfc='none', ms=4.0, markevery=0.04, label=labels[i])
ax.legend()
plt.tight_layout()
plt.savefig(os.path.join('plots', "ab_varied_optimizers_latency.pdf"))


