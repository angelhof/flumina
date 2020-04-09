#!/bin/bash

nodes=()
while (( "$#" ))
do
  nodes+=("${1}")
  shift
done

# Explicitly destroy int and ext device pairs

for node in ${nodes[@]}
do
  ${NET_UTILS}/ip link delete int-${node}
done

# Stop the containers

echo "Stopping docker containers..."

docker stop $(for node in ${nodes[@]}; do echo -n "${node}.local "; done)

# Bring down the network interfaces and clean up

for node in ${nodes[@]}
do
  echo "Cleaning up the devices and PID files for ${node}..."

  ./ns3/singleDestroy.sh ${node}
  PID=$(cat ./var/run/${node}.pid)
  rm /var/run/netns/${PID}
  rm ./var/run/${node}.pid
done

echo "DONE"

