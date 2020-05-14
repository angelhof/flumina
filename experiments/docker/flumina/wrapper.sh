#!/bin/sh

function usage {
  echo "Usage: $(basename ${0}) ( [--with-ns3] <node> <args> | help )"
}

if [ "${1}" == "help" ]
then
  usage
  exit 0
fi

if [ "${1}" == "--with-ns3" ]
then
  NS3=1
  shift
else
  NS3=0
fi

NODE="${1}"
ARGS="${2}"
LOG=/flumina/logs/wrapper-${NODE}.log
PIPE=/conf/notify

if [ "${NS3}" -eq "1" ]
then
  sudo /bin/sh -c "cat /conf/hosts >> /etc/hosts"

  echo "The contents of /etc/hosts:" >> ${LOG}
  cat /etc/hosts >> ${LOG}

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

echo "Docker: Node ${NODE} is done..." >> ${LOG}

if [ -p "${PIPE}" ]
then
  echo "${NODE}" > ${PIPE}
fi

echo "Exiting the wrapper" >> ${LOG}
