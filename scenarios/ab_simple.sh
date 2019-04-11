#!/bin/bash

A1="a1node"
A2="a2node"
B="main"

mainExec="-noshell -run util exec abexample. real_distributed. [['${A1}@${A1}.local','${A2}@${A2}.local','${B}@${B}.local']]. -s erlang halt"

simulTime=60 # seconds

./ns3/simulate.sh -m "${B}" \
  -e "${mainExec}" \
  -t "${simulTime}" \
  ${A1} ${A2}

