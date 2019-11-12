#!/bin/bash

# Downloads and extracts the dataset for outlier detection
wget http://kdd.ics.uci.edu/databases/kddcup99/kddcup.data.gz
gunzip kddcup.data.gz
python3 kddcup_add_timestamps.py kddcup.data kddcup_timestamps.data
