#!/bin/bash

# set -x

if [ -z "$1" ]
then
  echo "No name supplied"
  exit 1
fi

if [ -z "$2" ]
then
  echo "No index supplied"
  exit 1
fi

NAME=$1
SIDE_A=int-$NAME
SIDE_B=ext-$NAME
PID=$(cat ./var/run/$NAME.pid)
BRIDGE=br-$NAME
INDEX=$2

let SEGMENT3=(INDEX/250)
let SEGMENT4=(INDEX%250)+1

# Random MAC address
hexchars="0123456789ABCDEF"
end=$( for i in {1..8} ; do echo -n ${hexchars:$(( $RANDOM % 16 )):1} ; done | sed -e 's/\(..\)/:\1/g' )
MAC_ADDR="12:34"$end

# At another shell, learn the container process ID
# and create its namespace entry in /var/run/netns/
# for the "ip netns" command we will be using below

# We assume the directory /var/run/netns exists and has
# the correct privileges. That is, the current user should
# have write permission.
ln -s /proc/$PID/ns/net /var/run/netns/$PID

# Create a pair of "peer" interfaces A and B,
# bind the A end to the bridge, and bring it up.
# We assume the environment variable NET_UTILS is set to
# a directorya that contains brctl and ip with the CAP_NET_ADMIN
# capability.
${NET_UTILS}/ip link add $SIDE_A type veth peer name $SIDE_B
${NET_UTILS}/brctl addif $BRIDGE $SIDE_A
${NET_UTILS}/ip link set $SIDE_A up

# Place B inside the container's network namespace,
# rename to eth0, and activate it with a free IP

${NET_UTILS}/ip link set $SIDE_B netns $PID

# There is no way to avoid running the following four commands
# as non-root. One way to allow running the commands is to add
# the following line to /etc/sudoers:
#
#   %netns  ALL=(ALL) NOPASSWD: ${NET_UTILS}/ip netns *

sudo ${NET_UTILS}/ip netns exec $PID ${NET_UTILS}/ip link set dev $SIDE_B name eth0
sudo ${NET_UTILS}/ip netns exec $PID ${NET_UTILS}/ip link set eth0 address $MAC_ADDR
sudo ${NET_UTILS}/ip netns exec $PID ${NET_UTILS}/ip addr add 10.12.$SEGMENT3.$SEGMENT4/16 dev eth0
sudo ${NET_UTILS}/ip netns exec $PID ${NET_UTILS}/ip link set eth0 up

echo "Node ${NAME}'s eth0 device is up with ip 10.12.${SEGMENT3}.${SEGMENT4}"

