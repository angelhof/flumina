#!/bin/bash

nodes=(a1 a2)

# Stop the containers

docker stop $(docker ps -a -q)
#docker rm $(docker ps -a -q)

for node in ${nodes[*]}
do
  ./docker/singleDestroy.sh ${node}
  PID=$(cat var/run/${node}.pid)
  rm -rf /var/run/netns/${PID}
  rm -rf var/run/${node}.pid
done

