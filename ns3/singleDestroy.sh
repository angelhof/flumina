#!/bin/bash

# This file basically destroy the network bridge and TAP interface
# created by the singleSetup.sh script

if [ -z "$1" ]
then
  echo "No name supplied"
  exit 1
fi

NAME=$1

# We assume the variable NET_UTILS is set to a directory that
# contains tunctl, brctl, and ip with the CAP_NET_ADMIN capability.

# ifconfig br-$NAME down

${NET_UTILS}/ip link set dev br-$NAME down
${NET_UTILS}/brctl delif br-$NAME tap-$NAME
${NET_UTILS}/brctl delbr br-$NAME

# ifconfig tap-$NAME down

${NET_UTILS}/ip link set dev tap-$NAME down
${NET_UTILS}/tunctl -d tap-$NAME

