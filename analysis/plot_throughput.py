import sys
from operator import itemgetter
import itertools
import matplotlib.pyplot as plt
import numpy as np

## 1st argument should be the throughput filename
filename = sys.argv[1]

def parse_line(line):
    timestamp_messages = line.split("}")[-2]
    timestamp, message = timestamp_messages.split(",")[-2:]
    return (int(timestamp), int(message))
        


file = open(filename)
lines = file.readlines()
timestamp_message_pairs = list(map(parse_line, lines))
timestamp_message_dict = {}
for timestamp, n in timestamp_message_pairs:
    if timestamp in timestamp_message_dict:
        timestamp_message_dict[timestamp] += n
    else:
        timestamp_message_dict[timestamp] = n


final_timestamp_message_pairs = sorted(timestamp_message_dict.items(), key=itemgetter(0))

timestamps = [ts for ts, n in final_timestamp_message_pairs]
first_ts = timestamps[0]
## Those are shifted to 0 and are in seconds
shifted_timestamps = [(ts - first_ts) / 1000000.0 for ts in timestamps]

ts_diffs = [ ts1 - ts0 for ts1, ts0 in zip(shifted_timestamps[1:], shifted_timestamps[:-1])]
print(ts_diffs[:10])
ns = [n for ts, n in final_timestamp_message_pairs]
## This is messages per 1 millisecond
throughput = [n / ts_diff for n, ts_diff in zip(ns[1:], ts_diffs)]

yp = None
## Interpolate to get the the steady throughput
xi = np.linspace(shifted_timestamps[0], shifted_timestamps[-1], 3 * len(timestamps))
yi = np.interp(xi, shifted_timestamps[1:], throughput, yp)

## The plot shows messages per millisecond

fig, ax = plt.subplots()
ax.plot(xi, yi)
plt.show()


