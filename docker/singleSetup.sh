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

NAME=$1

sudo brctl addbr br-$NAME
sudo tunctl -t tap-$NAME
sudo ifconfig tap-$NAME 0.0.0.0 promisc up
sudo brctl addif br-$NAME tap-$NAME
sudo ifconfig br-$NAME up
