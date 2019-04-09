#!/bin/bash

A1="a1node"
A2="a2node"
B="main"

nodes=(${A1} ${A2} ${B})

# Stop the containers

docker stop $(docker ps -a -q)
docker rm $(docker ps -a -q)

for node in ${nodes[*]}
do
  ./docker/singleDestroy.sh ${node}
  PID=$(cat var/run/${node}.pid)
  rm -rf /var/run/netns/${PID}
  rm -rf var/run/${node}.pid
done

