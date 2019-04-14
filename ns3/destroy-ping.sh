#!/bin/bash

nodes=(a1 a2)

# Stop the containers

docker stop ${nodes[*]}

for node in ${nodes[*]}
do
  ./docker/singleDestroy.sh ${node}
  PID=$(cat ./var/run/${node}.pid)
  rm /var/run/netns/${PID}
  rm ./var/run/${node}.pid
done

