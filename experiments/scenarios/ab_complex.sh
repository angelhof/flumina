#!/bin/bash

A1="a1node"
A2="a2node"
A3="a3node"
A4="a4node"
A5="a5node"
B="main"

RATE_MULTI=20
RATIO_AB=1000
HEARTBEAT_RATIO=10
optimizer="optimizer_greedy"

erlArgs="-noshell -run util exec abexample. distributed_experiment. [['${B}@${B}.local','${A1}@${A1}.local','${A2}@${A2}.local','${A3}@${A3}.local','${A4}@${A4}.local','${A5}@${A5}.local'],${RATE_MULTI},${RATIO_AB},${HEARTBEAT_RATIO},${optimizer}]. -s erlang halt"

simulTime=60 # seconds
delay="6560ns"
dataRate="100Mbps"

ns3Args="--TotalTime=${simulTime} --ns3::CsmaChannel::DataRate=${dataRate} --ns3::CsmaChannel::Delay=${delay} --Tracing=true --FilenamePrefix=ab_complex"

./ns3/simulate.sh -m "${B}" \
  -e "${erlArgs}" \
  -n "${ns3Args}" \
  ${A1} ${A2} ${A3} ${A4} ${A5}

