#!/bin/bash

# This file basically destroy the network bridge and TAP interface
# created by the singleSetup.sh script

if [ -z "$1" ]
then
  echo "No name supplied"
  exit 1
fi

NAME=$1

sudo ifconfig br-$NAME down
sudo brctl delif br-$NAME tap-$NAME
sudo brctl delbr br-$NAME
sudo ifconfig tap-$NAME down
sudo tunctl -d tap-$NAME
