#!/bin/sh

# Read the configuration

NODE=$(cat /conf/node)
ARGS=$(cat /conf/args)
NS3=$(cat /conf/ns3)

LOG=/proto/logs/wrapper-${NODE}.log

if [ "${NS3}" -eq "1" ]
then
  cat /conf/hosts >> /etc/hosts
  
  # Wait until the eth0 interface is up. We use this as a synchronization barrier;
  # make sure to configure the network for the node before enabling the
  # interface.

  ETH0=$(ip addr | grep "eth0.*state UP" | wc -l)

  while [ "${ETH0}" -eq "0" ]
  do
    echo "Waiting for eth0..." >> ${LOG}
    sleep 2
    ETH0=$(ip addr | grep "eth0.*state UP" | wc -l)
  done
fi

echo -e "Starting the node\n\tNODE=${NODE}\n\tARGS=${ARGS}" >> ${LOG}

/usr/local/bin/erl \
  -name ${NODE}@${NODE}.local \
  -setcookie docker \
  -pa ebin \
  ${ARGS} >> ${LOG} 2>&1

echo "Exiting the wrapper" >> ${LOG}

