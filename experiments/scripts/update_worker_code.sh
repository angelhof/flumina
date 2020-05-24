#!/bin/bash

hostname=$1

flumina_dir=/home/ubuntu/flumina-devel

if nc -z -w1 $hostname 22 2>/dev/null ; then
    echo "Updating binaries in: $hostname..."
    scp -i ~/.ssh/internal_rsa -r -q ${flumina_dir}/ebin ubuntu@$hostname:${flumina_dir} &&
        scp -i ~/.ssh/internal_rsa -r -q ${flumina_dir}/experiments/ebin ubuntu@$hostname:${flumina_dir}/experiments &&
        echo $hostname >> /tmp/updated_hosts
else
    echo "Didn't update binaries in $hostname because it is not accessible..."
fi
