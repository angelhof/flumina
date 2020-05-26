#!/bin/bash

# The input parameters are:
#   - the experiment suite to run
#     (check experiment/suites.py or run python experiment/ -l)
#   - the number of Flink workers
SUITE="${1}"
COUNT="${2}"

# Make sure the Python virtual environment is active
source .venv/bin/activate

# Generate Flink workers
./scripts/flink_workers.sh ${COUNT} > flink_workers
cp flink_workers ${FLINK_HOME}/conf/slaves

# Start the cluster -- we want to start it from the home
# directory
DIR="${PWD}"
cd ${HOME}
${FLINK_HOME}/bin/start-cluster.sh

# Start the experiment
cd ${DIR}
python ./experiment/ \
  -s ${SUITE} \
  --flink-workers flink_workers \
  >./exp.log 2>&1 &

echo -e "\nThe experiment is now running. You can detach the process by running"
echo -e "\n  disown -ah\n"
echo -e "Once the process is detached, you can log out.\n"
