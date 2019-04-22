#!/bin/bash

# Copies data with timestamps strictly less than ${timestamp}

timestamp=1378022401

for file in /home/konstantinos/houses/debs_house_*_load
do
  echo ${file##*/}
  line=$(grep -n -m 1 ",${timestamp}," ${file} | cut -d: -f1)
  head -n $(( line - 1 )) ${file} > test_${file##*/}
done
