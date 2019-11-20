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

# In this script we assume that the tap and bridge devices already exist, the
# NS3 process is running, and the Docker container is running. In fact, one of
# the input parameters is the container's PID.
#
# Here we create the EXT--INT pair of devices, connect the EXT side with the
# bridge, and move the INT side into the container's network namespace.

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

if [ -z "$3" ]
then
  echo "No IP supplied"
  exit 1
fi

NAME=$1
PID=$2
IP=$3

INT=int-$NAME
EXT=ext-$NAME
BRIDGE=br-$NAME

# Random MAC address
hexchars="0123456789ABCDEF"
end=$( for i in {1..8} ; do echo -n ${hexchars:$(( $RANDOM % 16 )):1} ; done | sed -e 's/\(..\)/:\1/g' )
MAC_ADDR="12:34"$end

# We assume the directory /var/run/netns exists and has
# the correct privileges. That is, the current user should
# have write permission.
#
# Inside this directory we create a symlink to the namespace of the
# Docker container. We will move the INT device into the namespace to make
# it visible from within the container.
ln -s /proc/$PID/ns/net /var/run/netns/$PID

# Create a pair of "peer" interfaces INT and EXT,
# bind the EXT end to the bridge, and bring it up.
# We assume the variable NET_UTILS is set to a directory that
# contains tunctl, brctl, and ip with the CAP_NET_ADMIN capability.
${NET_UTILS}/ip link add $EXT type veth peer name $INT
${NET_UTILS}/brctl addif $BRIDGE $EXT
${NET_UTILS}/ip link set $EXT up

# Place INT inside the container's network namespace,
# rename to eth0, and activate it with the provided IP address
${NET_UTILS}/ip link set $INT netns $PID

# There is no way to avoid running the following four commands
# as non-root. One way to allow running the commands is to add
# the following line to /etc/sudoers:
#
#   %netns  ALL=(ALL) NOPASSWD: ${NET_UTILS}/ip netns *
sudo ${NET_UTILS}/ip netns exec $PID ${NET_UTILS}/ip link set dev $INT name eth0
sudo ${NET_UTILS}/ip netns exec $PID ${NET_UTILS}/ip link set eth0 address $MAC_ADDR
sudo ${NET_UTILS}/ip netns exec $PID ${NET_UTILS}/ip addr add $IP/16 dev eth0
sudo ${NET_UTILS}/ip netns exec $PID ${NET_UTILS}/ip link set eth0 up

echo "Node ${NAME}'s eth0 device is up with ip ${IP}"
