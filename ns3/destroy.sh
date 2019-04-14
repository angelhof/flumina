#!/bin/bash

# Stop the containers

echo "Stopping docker containers..."

docker stop $(docker ps -a -q)

# Bring down the network interfaces and clean up

while (( "$#" ))
do
  node="${1}"
  shift

  echo "Cleaning up the devices and PID files for ${node}..."

  ./ns3/singleDestroy.sh ${node}
  PID=$(cat ./var/run/${node}.pid)
  rm /var/run/netns/${PID}
  rm ./var/run/${node}.pid
done

echo "DONE"

