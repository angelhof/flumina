#!/bin/bash

if [ ! -d /var/run/netns ]
then
  mkdir /var/run/netns
fi

chown root:netns /var/run/netns
chmod 775 /var/run/netns
