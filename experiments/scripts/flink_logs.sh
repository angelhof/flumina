#!/bin/bash

# The input parameters:
#   - experiment suite
SUITE="${1}"

# Create the logs directory
mkdir -p ./archive/${SUITE}/logs

# Make sure the Flink cluster is stopped
${FLINK_HOME}/bin/stop-cluster.sh

# Collect and clean the worker logs
for host in $(cat flink_workers); do
  mkdir ./archive/${SUITE}/logs/${host}
  scp ${host}:flink/log/* ./archive/${SUITE}/logs/${host}/
  ssh ${host} "rm ./flink/log/*"
done

mkdir ./archive/${SUITE}/logs/jobmanager
mv ${FLINK_HOME}/log/* ./archive/${SUITE}/logs/jobmanager
