#!/bin/bash

function usage {
  echo "Usage: $(basename ${0}) ( jobmanager [--with-ns3] [--wait-taskmanagers n] | taskmanager [--with-ns3] | help )"
}

CMD="${1}"
shift

if [ "${CMD}" == "help" ]
then
  usage
  exit 0
fi

if [ "${1}" == "--with-ns3" ]
then
  shift

  sudo bash -c "cat /conf/hosts >> /etc/hosts"

  # Wait until the eth0 interface is up. We use this as a synchronization barrier;
  # make sure to configure the network for the node before enabling the
  # interface.

  ETH0=$(ip addr | grep "eth0.*state UP" | wc -l)

  while [ "${ETH0}" -eq "0" ]
  do
    echo "Waiting for eth0..."
    sleep 1
    ETH0=$(ip addr | grep "eth0.*state UP" | wc -l)
  done
fi

if [ "${CMD}" == "taskmanager" ]
then
  exec /opt/flink/bin/taskmanager.sh start-foreground "${@}"
elif [ "${CMD}" == "jobmanager" ]
then
  HOSTNAME="$(hostname -f)"

  sed -i -e "s/jobmanager\.rpc\.address:.*/jobmanager.rpc.address: ${HOSTNAME}/g" /opt/flink/conf/flink-conf.yaml

  if [ "${1}" == "--wait-taskmanagers" ]
  then
    shift

    if [ -z "${1}" ]
    then
      usage
      exit 1
    fi

    NUM_TASKMNGRS="${1}"
    shift

    /wait.sh "http://${HOSTNAME}:8081" "${NUM_TASKMNGRS}" &
  fi
  
  exec /opt/flink/bin/jobmanager.sh start-foreground "${@}"
fi

exec "${@}"
