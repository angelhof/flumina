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

# Here we do the opposite of the container-start-network.sh:
# we bring down and delete EXT--INT pair.
# This should be done before the container is stopped.

if [ -z "$1" ]
then
  echo "No name supplied"
  exit 1
fi

if [ -z "$2" ]
then
  echo "No PID supplied"
  exit 1
fi

NAME=$1
PID=$2

EXT=ext-$NAME
BRIDGE=br-$NAME

# We assume the variable NET_UTILS is set to a directory that
# contains tunctl, brctl, and ip with the CAP_NET_ADMIN capability.

${NET_UTILS}/ip link set $EXT down
${NET_UTILS}/brctl delif $BRIDGE $EXT
${NET_UTILS}/ip link delete $EXT

# Remove the network namespace symlink
rm /var/run/netns/${PID}
