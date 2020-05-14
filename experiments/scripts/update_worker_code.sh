#!/bin/bash

hostname=$1

flumina_dir=/home/ubuntu/flumina-devel

echo "Updating binaries in: $hostname..."

scp -i ~/.ssh/internal_rsa -r -q ${flumina_dir}/ebin ubuntu@$hostname:${flumina_dir}
scp -i ~/.ssh/internal_rsa -r -q ${flumina_dir}/experiments/ebin ubuntu@$hostname:${flumina_dir}/experiments
