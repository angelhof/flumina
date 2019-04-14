#!/bin/bash

# This file creates a bridge with the tool brctl (ethernet bridge administration)
# and a TAP interface with the tool tunctl (create and manage persistent TUN/TAP interfaces)
# then the tap interface is configured in promisc mode, added to the bridge
# and started.

# The whole purpose of this script is to create the end of the NS3 node.
# So the NS3 nodes will try to connect to the tap-$NAME device,
# since this is connected to the bridge, and a docker container will be connected
# to the same bridge via other mechanism ... that will make the docker container
# to be able to communicate via the NS3 simulation.

if [ -z "$1" ]
then
  echo "No name supplied"
  exit 1
fi

if [ -z "$2" ]
then
  echo "No user supplied"
  exit 1
fi

NAME=$1
TAPUSER=$2

# We assume the variable NET_UTILS is set to a directory that
# contains tunctl, brctl, and ip with the CAP_NET_ADMIN capability.

${NET_UTILS}/tunctl -u $TAPUSER -t tap-$NAME

# The next call to ifconfig is replaced by three calls to ip
# ifconfig tap-$NAME 0.0.0.0 promisc up

${NET_UTILS}/ip addr add 0.0.0.0 dev tap-$NAME
${NET_UTILS}/ip link set dev tap-$NAME promisc on
${NET_UTILS}/ip link set dev tap-$NAME up

${NET_UTILS}/brctl addbr br-$NAME
${NET_UTILS}/brctl addif br-$NAME tap-$NAME

# ifconfig br-$NAME up

${NET_UTILS}/ip link set dev br-$NAME up

