#!/bin/bash

# The input parameters are:
#   - the number of Flink workers
COUNT="${1}"

# Generate Flink workers
./scripts/flink_workers.sh ${COUNT} > flink_workers
cp flink_workers ${FLINK_HOME}/conf/slaves

# Start the cluster -- we want to start it from the home
# directory
DIR="${PWD}"
cd ${HOME}
${FLINK_HOME}/bin/start-cluster.sh
cd ${DIR}
