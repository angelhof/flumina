#!/bin/bash

HOST="${1}"

ssh ${HOST} << EOF
  set -eux
  sudo apt-get install -y chrony
  sudo sed -i "4i# Amazon Time Sync Service\nserver 169.254.169.123 prefer iburst minpoll 4 maxpoll 4\n" /etc/chrony/chrony.conf
  sudo /etc/init.d/chrony restart
EOF
