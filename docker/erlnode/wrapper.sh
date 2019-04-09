#!/bin/bash

LOG=/proto/logs/wrapper.log

# Wait until the eth0 interface is up. We use this as a synchronization barrier;
# make sure to set up all the configuration for the node before enabling the
# interface.

ETH0=$(ip addr | grep "eth0.*state UP" | wc -l)

while [ $ETH0 -eq 0 ]
do
  echo "Waiting for eth0..." >> ${LOG}
  sleep 2
  ETH0=$(ip addr | grep "eth0.*state UP" | wc -l)
done

# Read the configuration

NODE=$(cat /conf/node)
EXEC=$(cat /conf/exec)

cat /conf/hosts >> /etc/hosts

echo -e "Starting the node\n\tNODE=${NODE}\n\tEXEC=${EXEC}" >> ${LOG}

export ERL_CRASH_DUMP="/proto/logs/erl_crash.dump"

/usr/local/bin/erl \
  -name ${NODE}@${NODE}.local \
  -setcookie docker \
  -pa ebin \
  ${EXEC} >> ${LOG} 2>&1

echo "Exiting" >> ${LOG}

