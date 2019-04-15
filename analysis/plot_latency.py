import sys
import os
from operator import itemgetter
import itertools
import matplotlib.pyplot as plt
import numpy as np
## TODO: Find a better way to import other python files
sys.path.append(os.path.relpath("./scripts"))
from lib import *

## 1st argument should be the log directory name
log_dir_name = sys.argv[1]

timestamps, latencies = read_preprocess_latency_data(log_dir_name)
fig, ax = plt.subplots()
ax.plot(timestamps, latencies, '.')
plt.show()


