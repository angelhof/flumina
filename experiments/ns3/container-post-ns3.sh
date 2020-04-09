#!/bin/bash

# Here's a sketch of the devices and their connections:
#
#                                        Docker container
#                                    +----------------------+
#                                    |                      |
#   +-----+   +--------+   +-----+   |   +-------------+    |
#   | tap |---| bridge |---| ext |-------| int == eth0 |    |
#   +-----+   +--------+   +-----+   |   +-------------+    |
#                                    +----------------------+
# tap -- connects to NS3
# bridge -- connects tap and ext into a single network
# ext -- paired with int
# int -- exists in the container's network namespace where it's called eth0

# Here we do the opposite of the container-pre-ns3.sh:
# we bring down and delete the bridge and tap devices.
# We do this after the NS3 process has finished.

if [ -z "$1" ]
then
  echo "No name supplied"
  exit 1
fi

NAME=$1
BRIDGE=br-$NAME
TAP=tap-$NAME

# The BRIDGE device
${NET_UTILS}/ip link set dev $BRIDGE down
${NET_UTILS}/brctl delif $BRIDGE $TAP
${NET_UTILS}/brctl delbr $BRIDGE

# The TAP device
${NET_UTILS}/ip link set dev $TAP down
${NET_UTILS}/tunctl -d $TAP
