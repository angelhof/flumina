import sys
import os
from operator import itemgetter
import itertools
import matplotlib.pyplot as plt
import numpy as np
## TODO: Find a better way to import other python files
sys.path.append(os.path.relpath("./scripts"))
from lib import *

plt.rcParams.update({'font.size': 12})

fig, ax = plt.subplots()
ax.set_xlabel('Time (s)')
ax.set_ylabel('Latency (ms)')

colors = ['tab:blue', 'tab:red', 'tab:green']
symbols = ['s', 'x', 'o']

## Every argument is a log directory name; we plot them on the same plot
for i, log_dir_name in enumerate(sys.argv[1:]):
    timestamps, latencies = read_preprocess_latency_data(log_dir_name)
    timestamps = [ts / 1000.0 for ts in timestamps]
    #ax.plot(timestamps, latencies, symbols[i], color=colors[i], mfc='none', ms=4.0, markevery=0.05)
    ax.plot(timestamps, latencies, '.', color=colors[i])
#ax.legend()
plt.tight_layout()
plt.show()


