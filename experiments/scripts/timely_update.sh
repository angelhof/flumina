#!/bin/bash
# Copy Timely source files to remote (in src and hosts)

hostname=$1

timely_dir=/home/caleb/git/research/flumina-dev/experiments/naiad-experiment
remote_timely_dir=/home/ubuntu/flumina-devel/experiments/naiad-experiment

echo "Updating Timely files in: $hostname..."
ssh $hostname rm -rf ${remote_timely_dir}/src &&
    ssh $hostname rm -rf ${remote_timely_dir}/hosts &&
    scp -i ~/.ssh/id_rsa -r -q ${timely_dir}/src $hostname:${remote_timely_dir} &&
    scp -i ~/.ssh/id_rsa -r -q ${timely_dir}/hosts $hostname:${remote_timely_dir} &&
    echo $hostname >> /tmp/updated_hosts &&
    echo "  Success: $hostname"
