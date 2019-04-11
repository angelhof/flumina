#!/bin/bash

A1="a1node"
A2="a2node"
B="main"

nodes=(${A1} ${A2} ${B})
execs=(
  ""
  ""
  "-noshell -run util exec abexample. real_distributed. [['${A1}@${A1}.local','${A2}@${A2}.local','${B}@${B}.local']]. -s erlang halt"
)
totalTime=60 # seconds

workdir=${PWD}

# Generate a hosts file for the nodes

hosts=$(mktemp /tmp/hosts-XXXXXXXX)

for i in ${!nodes[*]}
do
  # Generate an IP address based on the index. The address needs
  # to match the one generated in ../docker/container.sh.
  # TODO: figure out a better way to do this.
  seg3=$(( (i + 1) / 250 ))
  seg4=$(( (i + 1) % 250 ))

  echo -e "10.12.${seg3}.${seg4}\t${nodes[${i}]}.local" >> ${hosts}
done

# Create configuration, logging, taps, and bridges

for i in ${!nodes[*]}
do
  node=${nodes[${i}]}
  exec=${execs[${i}]}

  mkdir -p var/log/${node}
  mkdir -p var/conf/${node}

  echo -n ${node} > var/conf/${node}/node
  echo -n ${exec} > var/conf/${node}/exec
  cp ${hosts} var/conf/${node}/hosts

  sudo ./docker/singleSetup.sh ${node} ${USER}
done

sudo ./docker/singleEndSetup.sh

# Run the docker containers. Assumes existence of an image called erlnode.

mkdir -p var/run

for node in ${nodes[*]}
do
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

cd ${NS3_HOME}
./waf --run "scratch/tap-vm --TotalTime=${totalTime} ${nodes[*]}" &
echo $! > ${workdir}/var/run/ns3.pid
cd ${workdir}
sleep 25

# Set up the device containers -- this unblocks the nodes

for i in ${!nodes[*]}
do
  ./docker/container.sh ${nodes[${i}]} ${i}
done

