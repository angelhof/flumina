import sys
import os
from operator import itemgetter
import itertools
import matplotlib.pyplot as plt
import numpy as np
## TODO: Find a better way to import other python files
sys.path.append(os.path.relpath("./scripts"))
from lib import *

## 1st argument should be the throughput filename
filename = sys.argv[1]
    
## The plot shows messages per millisecond
xi, yi = read_preprocess_throughput_data(filename)

fig, ax = plt.subplots()
ax.plot(xi, yi)
plt.show()


