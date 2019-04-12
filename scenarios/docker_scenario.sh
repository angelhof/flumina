#!/bin/bash

A1="a1node"
A2="a2node"
A3="a3node"
B="main"

# Old non parametric command
# EXEC="-noshell -run util exec abexample. real_distributed. [[\'${A1}@${A1}.local\',\'${A2}@${A2}.local\',\'${B}@${B}.local\']]. -s erlang halt"

RATE_MULTI=20
RATIO_AB=1000
HEARTBEAT_RATIO=1
EXEC="-noshell -run util exec abexample. distributed_experiment. [[\'${B}@${B}.local\',\'${A1}@${A1}.local\',\'${A2}@${A2}.local\',\'${A3}@${A3}.local\'],${RATE_MULTI},${RATIO_AB},${HEARTBEAT_RATIO}]. -s erlang halt"

cd docker

## Kill any old container with the same names
docker container kill "${A1}.local"
docker container kill "${A2}.local"
docker container kill "${A3}.local"
docker container kill "${B}.local"

## Create the containers and run the script
./make-docker-container.sh "${A1}" true
./make-docker-container.sh "${A2}" true
./make-docker-container.sh "${A3}" true
./make-docker-container.sh "${B}" false "${EXEC}"

## After the end of the script gather the logs in a folder with name and date
python3 gather_logs.py "abexample_scenario" "${A1} ${A2} ${A3} ${B}" "${RATE_MULTI}_${RATIO_AB}_${HEARTBEAT_RATIO}"
