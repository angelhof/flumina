#!/bin/bash

cat ec2/hostnames |
    xargs -L 1 scripts/update_worker_code.sh

echo "Updated $(wc -l ec2/hostnames | cut -f 1 -d ' ') workers."
