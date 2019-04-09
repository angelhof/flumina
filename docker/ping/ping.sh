#!/usr/bin/env sh

LOG=/log/ping.log

ETH0=$(ip addr | grep "eth0.*state UP" | wc -l)

while [ $ETH0 -eq 0 ]
do
  echo "Waiting for eth0..." >> ${LOG}
  sleep 2
  ETH0=$(ip addr | grep "eth0.*state UP" | wc -l)
done

PEER=$(cat /conf/peer)

cat /etc/hosts >> ${LOG}

cat /conf/hosts >> /etc/hosts

ping ${PEER} >> ${LOG} 2>&1
