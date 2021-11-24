#!/bin/bash
# Copy Timely EC2 results files (in results/) back to local

hostname=$1

timely_dir=/home/caleb/git/research/flumina-dev/experiments/timely-experiment
remote_timely_dir=/home/ubuntu/flumina-devel/experiments/timely-experiment

echo "Getting Timely results files in: $hostname..."
echo ">> copying folder: results/ to: ec2_results/"
    scp -i ~/.ssh/id_rsa -r -q $hostname:${remote_timely_dir}/results ${timely_dir}/ec2_results &&
    echo ">> success"
