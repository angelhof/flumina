#!/bin/bash

# Downloads and extracts the dataset for outlier detection
wget http://kdd.ics.uci.edu/databases/kddcup99/kddcup.data.gz
gunzip kddcup.data.gz
python3 kddcup_add_timestamps.py kddcup.data kddcup_timestamps.data

## Run the itemsets on the total
python3 kddcup_column_itemsets.py kddcup_timestamps.data kddcup_itemsets.csv

## Get a smaller part of the data
head -n 20000 kddcup_timestamps.data > sample_kddcup_data_20k

## Split them in different files round robin
python3 separate_rr.py sample_kddcup_data_20k sample_kddcup_data_5k 4
