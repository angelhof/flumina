#!/bin/bash
# Copy Timely source files to remote (in src and hosts)

hostname=$1

timely_dir=/home/caleb/git/research/flumina-dev/experiments/timely-experiment
remote_timely_dir=/home/ubuntu/flumina-devel/experiments/timely-experiment

echo "Updating Timely files in: $hostname..."
for dir in src hosts examples Cargo.toml rustfmt.toml
do
    echo ">> copying file or folder: $dir" &&
        ssh $hostname rm -rf ${remote_timely_dir}/${dir} &&
        scp -i ~/.ssh/id_rsa -r -q ${timely_dir}/${dir} $hostname:${remote_timely_dir} &&
        echo "$hostname,$dir" >> /tmp/updated_hosts &&
        echo ">> success"
done
