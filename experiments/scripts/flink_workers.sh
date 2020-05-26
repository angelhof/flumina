#!/bin/bash

# The input parameter is how many workers should be generated
COUNT=${1}

cat ec2/flink_hostnames \
  | xargs -L 1 -P 10 scripts/check_accessible.sh \
  | shuf -n ${COUNT}
