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

# In this script we create the tap device, which needs to exist before we
# start the NS3 process. We also immediately create the bridge device.

if [ -z "$1" ]
then
  echo "No name supplied"
  exit 1
fi

NAME=$1
BRIDGE=br-$NAME
TAP=tap-$NAME

# We assume the variable NET_UTILS is set to a directory that
# contains tunctl, brctl, and ip with the CAP_NET_ADMIN capability.

# The TAP device
${NET_UTILS}/tunctl -t $TAP
${NET_UTILS}/ip addr add 0.0.0.0 dev $TAP
${NET_UTILS}/ip link set dev $TAP promisc on
${NET_UTILS}/ip link set dev $TAP up

# The BRIDGE device
${NET_UTILS}/brctl addbr $BRIDGE
${NET_UTILS}/brctl addif $BRIDGE $TAP
${NET_UTILS}/ip link set dev $BRIDGE up
