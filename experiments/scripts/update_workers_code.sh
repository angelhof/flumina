#!/bin/bash

rm /tmp/updated_hosts

cat ec2/hostnames |
    xargs -L 1 -P 10 scripts/check_accessible.sh |
    xargs -L 1 scripts/update_worker_code.sh

echo "Updated $(wc -l /tmp/updated_hosts | cut -f 1 -d ' ') workers."
