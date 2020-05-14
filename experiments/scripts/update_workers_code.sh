#!/bin/bash

cat ec2/hostnames |
    xargs -L 1 scripts/update_worker_code.sh
