#!/bin/bash

# The input parameters are:
#   - the experiment suites to run
#     (check experiment/suites.py or run python experiment/ -l)
SUITES="${@}"

# Make sure the Python virtual environment is active
source .venv/bin/activate

for suite in ${SUITES}; do
  # Start the cluster -- we want to start it from the home
  # directory
  DIR="${PWD}"
  cd ${HOME}
  ${FLINK_HOME}/bin/start-cluster.sh

  # Start the experiment
  cd ${DIR}
  python ./experiment/ \
    -s ${suite} \
    --flink-workers flink_workers \
    --rmi-host ${HOSTNAME} \
    >./${suite}.log 2>&1

  # Create the logs directory
  mkdir ./archive/${suite}/logs

  # Make sure the Flink cluster is stopped
  ${FLINK_HOME}/bin/stop-cluster.sh

  # Collect and clean the worker logs
  for host in $(cat flink_workers); do
    mkdir ./archive/${suite}/logs/${host}
    scp ${host}:flink/log/* ./archive/${suite}/logs/${host}/
    ssh ${host} "rm ./flink/log/*"
  done

  mkdir ./archive/${suite}/logs/jobmanager
  mv ${FLINK_HOME}/log/* ./archive/${suite}/logs/jobmanager
done
