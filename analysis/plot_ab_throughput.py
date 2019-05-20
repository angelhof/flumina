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
ax.set_ylabel('Throughput (msgs/ms)')

log_dirs = [os.path.join('archive', 'ab_exp4_15_1000_10_4_optimizer_greedy_real'),
            os.path.join('archive', 'ab_exp4_15_1000_10_4_optimizer_greedy_hybrid'),
            os.path.join('archive', 'ab_exp4_15_1000_10_4_optimizer_sequential')]
colors = ['tab:green', 'tab:red', 'tab:blue']
markers = ['-s', '-x', '-o']
labels = ['greedy', 'greedy hybrid', 'sequential']

for i, log_dir_name in enumerate(log_dirs):
    ## The plot shows messages per millisecond
    xi, yi = read_preprocess_throughput_data(log_dir_name)
    xi = [ts / 1000.0 for ts in xi]
    ax.plot(xi, yi, markers[i], color=colors[i], linewidth=1.0, ms=4.0, markevery=0.1, label=labels[i])
ax.legend()
plt.tight_layout()
plt.savefig(os.path.join('plots', "ab_varied_optimizers_throughput.pdf"))

