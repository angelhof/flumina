#!/bin/bash

# This script serves for synchronization. First it waits for Flink's REST API
# to be up and running; then it periodically queries the API for the number of
# task managers and waits until that number is $1.
# In each step it outputs a string to a named pipe.

function usage {
  echo "Usage: $(basename ${0}) <REST URL> <number of taskmanagers>"
}

if [ -z "${1}" ]
then
  usage
  exit 1
fi

if [ -z "${2}" ]
then
  usage
  exit 1
fi

PIPE="/conf/notify"

if [ ! -p "${PIPE}" ]
then
  echo "Named pipe ${PIPE} does not exist!"
  exit 1
fi

REST_URL="${1}"
REST_QUERY="${REST_URL}/overview"
NUM_TASKMNGRS="${2}"

function wait_taskmanagers {
  while [ "$(curl -s ${REST_QUERY} | jq .taskmanagers)" -ne "${1}" ]
  do
    sleep 0.2
  done
}

# We give the Flink job manager a bit of time before we start polling
sleep 4

wait_taskmanagers 0
echo "jobmanager up" > $PIPE

wait_taskmanagers "${NUM_TASKMNGRS}"
echo "taskmanagers up" > $PIPE
