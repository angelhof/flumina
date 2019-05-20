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
ax.set_ylabel('Throughput (msgs/ms)')

colors = ['tab:blue', 'tab:red', 'tab:green']

## Every argument is a log directory name; we plot them on the same plot
for i, log_dir_name in enumerate(sys.argv[1:]):
    ## The plot shows messages per millisecond
    xi, yi = read_preprocess_throughput_data(log_dir_name)
    xi = [ts / 1000.0 for ts in xi]
    ax.plot(xi, yi, '-', color=colors[i], linewidth=1.0)
plt.tight_layout()
plt.show()


