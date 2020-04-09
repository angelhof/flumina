#!/bin/bash

A1="a1node"
A2="a2node"
A3="a3node"
ANODES="${A1} ${A2} ${A3}"
B="main"

RATE_MULTI=20
RATIO_AB=1000
HEARTBEAT_RATIO=1
OPTIMIZER=optimizer_greedy
EXEC="-noshell -run util exec abexample. distributed_experiment. [[\'${B}@${B}.local\',\'${A1}@${A1}.local\',\'${A2}@${A2}.local\',\'${A3}@${A3}.local\'],${RATE_MULTI},${RATIO_AB},${HEARTBEAT_RATIO},${OPTIMIZER}]. -s erlang halt"

scenarios/no_ns3_docker_scenario.sh "${ANODES}" "${EXEC}"


cd docker
## After the end of the script gather the logs in a folder with name and date
python3 gather_logs.py "abexample_scenario" "${A1} ${A2} ${A3} ${B}" "${RATE_MULTI}_${RATIO_AB}_${HEARTBEAT_RATIO}"
