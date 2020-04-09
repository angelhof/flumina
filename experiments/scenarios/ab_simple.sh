#!/bin/bash

A1="a1node"
A2="a2node"
B="main"

erlArgs="-noshell -run util exec abexample. real_distributed. [['${A1}@${A1}.local','${A2}@${A2}.local','${B}@${B}.local']]. -s erlang halt"

simulTime=60 # seconds
delay="6560ns"
dataRate="100Mbps"

ns3Args="--TotalTime=${simulTime} --ns3::CsmaChannel::DataRate=${dataRate} --ns3::CsmaChannel::Delay=${delay} --Tracing=true --FilenamePrefix=ab_simple"

./ns3/simulate.sh -m "${B}" \
  -e "${erlArgs}" \
  -n "${ns3Args}" \
  ${A1} ${A2}

