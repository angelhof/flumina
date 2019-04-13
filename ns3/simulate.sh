#!/bin/bash

nodes=()

while (( "$#" ))
do
  case "$1" in
    -m|--main)
      main="$2"
      shift 2
      ;;
    -e|--exec)
      mainExec="$2"
      shift 2
      ;;
    -t|--time)
      simulTime="$2"
      shift 2
      ;;
    *)
      nodes+=("$1")
      shift
      ;;
  esac
done

nodes=("${main}" "${nodes[@]}")
workdir=${PWD}

echo "Setting up the simulation context..."

# Generate a hosts file for the nodes

hosts=$(mktemp /tmp/hosts-XXXXXXXX)

for i in ${!nodes[*]}
do
  # Generate an IP address based on the index. The address needs
  # to match the one generated in ../docker/container.sh.
  # TODO: figure out a better way to do this.
  seg3=$(( i / 250 ))
  seg4=$(( i % 250 + 1 ))

  echo -e "10.12.${seg3}.${seg4}\t${nodes[${i}]}.local" >> ${hosts}
done

# Create configuration, logging, taps, and bridges

for i in ${!nodes[*]}
do
  node=${nodes[${i}]}
  if [ "${i}" -eq "0" ]
  then
    exec="${mainExec}"
  else
    exec=""
  fi

  mkdir -p var/log/${node}
  mkdir -p var/conf/${node}

  echo -n ${node} > var/conf/${node}/node
  echo -n ${exec} > var/conf/${node}/exec
  cp ${hosts} var/conf/${node}/hosts

  sudo ./ns3/singleSetup.sh ${node} ${USER}
done

sudo ./ns3/singleEndSetup.sh

# Run the docker containers. Assumes existence of an image called erlnode.

echo "Starting the docker containers..."

mkdir -p var/run

for node in ${nodes[*]}
do
  # --user $(id -u):$(id -g) \
  docker run \
    -dit \
    --rm \
    --privileged \
    --net=none \
    --name ${node} \
    -v "${workdir}/var/conf/${node}":/conf \
    -v "${workdir}/var/log/${node}":/proto/logs \
    erlnode

  docker inspect --format '{{ .State.Pid }}' ${node} > var/run/${node}.pid
done

# Run the ns3 process. Assumes it is compiled and located in $NS3_HOME/scratch.

echo "Starting the ns3 process..."

cd ${NS3_HOME}
./waf --run "scratch/tap-vm --TotalTime=${simulTime} ${nodes[*]}" &
wafPid=$!
echo ${wafPid} > ${workdir}/var/run/ns3.pid
cd ${workdir}

sleep 25

# Set up the device containers -- this unblocks the nodes

echo "Unblocking the containers and starting the simulation..."

for i in ${!nodes[*]}
do
  if [ "${i}" -gt "0" ]
  then
    ./docker/container.sh ${nodes[${i}]} ${i}
  fi
done

# We set up and unblock the main node last

sleep 1
./docker/container.sh ${main} 0

# Wait for Waf to finish

echo "Waiting for the ns3 process to finish..."

wait ${wafPid}
rm ${workdir}/var/run/ns3.pid

echo "Destroying the simulation context... "

# Stop the containers

docker stop $(docker ps -a -q)

# Bring down the network interfaces and clean up

for node in ${nodes[*]}
do
  sudo ./ns3/singleDestroy.sh ${node}
  PID=$(cat var/run/${node}.pid)
  sudo rm -rf /var/run/netns/${PID}
  rm -rf var/run/${node}.pid
done

echo "DONE"

