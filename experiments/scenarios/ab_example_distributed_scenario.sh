#!/bin/bash

A1_NODE="a1node@127.0.0.1"
A2_NODE="a2node@127.0.0.1"
A3_NODE="a3node@127.0.0.1"
B_NODE="main@127.0.0.1"

# EXEC="abexample. real_distributed. []."

RATE_MULTI=30
RATIO_AB=1000
HEARTBEAT_RATIO=100
EXEC="abexample. distributed_experiment. [[\'${B_NODE}\',\'${A1_NODE}\',\'${A2_NODE}\',\'${A3_NODE}\'],${RATE_MULTI},${RATIO_AB},${HEARTBEAT_RATIO}]."

make open_erl_noshell NAME_OPT="-name ${A1_NODE} -setcookie abexample" 2>&1 >"logs/${A1_NODE}.log" &
make open_erl_noshell NAME_OPT="-name ${A2_NODE} -setcookie abexample" 2>&1 >"logs/${A2_NODE}.log" &
make open_erl_noshell NAME_OPT="-name ${A3_NODE} -setcookie abexample" 2>&1 >"logs/${A3_NODE}.log" &
make exec NAME_OPT="-name ${B_NODE} -setcookie abexample" args="${EXEC}"
