#!/bin/bash

A1="a1node"
A2="a2node"
A3="a3node"
B="main"

RATE_MULTI=40
RATIO_AB=1000
EXEC="-noshell -run util exec abexample. distributed_experiment. [['${B}@${B}.local','${A1}@${A1}.local','${A2}@${A2}.local','${A3}@${A3}.local'],${RATE_MULTI},${RATIO_AB}]. -s erlang halt"

simulTime=60 # seconds

./ns3/simulate.sh -m "${B}" \
  -e "${EXEC}" \
  -t "${simulTime}" \
  ${A1} ${A2} ${A3}

