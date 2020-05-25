#!/bin/bash

cat ec2/flink_hostnames | xargs -L 1 -P 10 scripts/check_accessible.sh
