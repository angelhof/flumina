#!/bin/bash

nodes=()
ns3=0

while (( "$#" ))
do
  case "$1" in
    -m|--main)
      main="$2"
      shift 2
      ;;
    -e|--erlArgs)
      erlArgs="$2"
      shift 2
      ;;
    -n|--ns3Args)
      ns3=1
      ns3Args="$2"
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

function log {
  echo "[${SECONDS}s] ${1}"
}

log "Setting up the simulation context..."

# Generate a hosts file for the nodes

hosts=$(mktemp /tmp/hosts-XXXXXXXX)

for i in ${!nodes[@]}
do
  # Generate an IP address based on the index. The address needs
  # to match the one generated in ../docker/container.sh.
  # TODO: figure out a better way to do this.
  seg3=$(( i / 250 ))
  seg4=$(( i % 250 + 1 ))

  echo -e "10.12.${seg3}.${seg4}\t${nodes[${i}]}.local" >> ${hosts}
done

# Create configuration, logging, taps, and bridges

for node in ${nodes[@]}
do
  if [ "${node}" = "${main}" ]
  then
    args="${erlArgs}"
  else
    args=""
  fi

  ## First delete any old data in the
  ## log and conf directory
  rm -rf ${workdir}/var/log/${node}
  rm -rf ${workdir}/var/conf/${node}

  mkdir -p ${workdir}/var/log/${node}
  mkdir -p ${workdir}/var/conf/${node}

  echo -n ${node} > ${workdir}/var/conf/${node}/node
  echo -n ${args} > ${workdir}/var/conf/${node}/args
  echo -n ${ns3} > ${workdir}/var/conf/${node}/ns3
  cp ${hosts} ${workdir}/var/conf/${node}/hosts

  if [ "${ns3}" -eq "1" ]
  then
    ./ns3/singleSetup.sh ${node} ${USER}
  fi
done

# Create the notification pipe for the main node

mkfifo ${workdir}/var/conf/${main}/notify

# Run the docker containers. Assumes existence of an image called erlnode.

log "Starting the docker containers..."

mkdir -p ${workdir}/var/run

# Set docker options depending on whether we're running ns3

if [ "${ns3}" -eq "1" ]
then
  net="none"
else
  net="temp"
fi

# Run the non-main nodes

for node in ${nodes[@]}
do
  if [ "${node}" != "${main}" ]
  then
    # --user $(id -u):$(id -g) \
    docker run \
      -dit \
      --rm \
      --privileged \
      --net=${net} \
      --name "${node}.local" \
      --hostname "${node}.local" \
      -v "${workdir}/var/conf/${node}":/conf \
      -v "${workdir}/var/log/${node}":/flumina/logs \
      -v "${workdir}/data":/flumina/data \
      erlnode

    docker inspect --format '{{ .State.Pid }}' "${node}.local" > ${workdir}/var/run/${node}.pid
  fi
done

# Run the main node

sleep 1
docker run \
  -dit \
  --rm \
  --privileged \
  --net=${net} \
  --name "${main}.local" \
  --hostname "${main}.local" \
  -v "${workdir}/var/conf/${main}":/conf \
  -v "${workdir}/var/log/${main}":/flumina/logs \
  -v "${workdir}/data":/flumina/data \
  erlnode

docker inspect --format '{{ .State.Pid }}' "${main}.local" > ${workdir}/var/run/${main}.pid

if [ "${ns3}" -eq "1" ]
then
  # Run the ns3 process. Assumes it is compiled and located in $NS3_HOME/scratch.

  log "Starting the ns3 process..."

  cd ${NS3_HOME}

  ./waf --run "scratch/tap-vm ${ns3Args} --MainNode=${nodes[*]}" &
  wafPid=$!
  echo ${wafPid} > ${workdir}/var/run/ns3.pid
  cd ${workdir}

  sleep 5

  # Set up the device containers -- this unblocks the nodes

  log "Unblocking the containers and starting the simulation..."

  for i in ${!nodes[@]}
  do
    if [ "${i}" -gt "0" ]
    then
      ./docker/container.sh ${nodes[${i}]} ${i}
    fi
  done

  # We set up and unblock the main node last

  sleep 1
  ./docker/container.sh ${main} 0

  # Wait for the simulation to finish. First we wait for the
  # signal from the main node that it has finished...

  log "Waiting for the simulation to finish..."

  notification=""
  while [ "${notification}" != "${main}" ]; do
    notification=$(cat ${workdir}/var/conf/${main}/notify)
    log "Received notification: ${notification}"
  done

  # Signal the tap-vm process to end the NS3 simulation, then
  # wait for WAF to finish

  tapvm=$(pidof -s tap-vm)
  log "Signaling tap-vm (PID ${tapvm}) to stop the NS3 simulation"
  kill -SIGINT ${tapvm}
  wait ${wafPid}
  rm ${workdir}/var/run/ns3.pid
else
  # We are not running ns3; just wait for main to finish

  log "Waiting for ${main}.local to finish..."

  docker wait "${main}.local"
fi

log "Destroying the simulation context..."

# Destroy the int and ext peer devices

for node in ${nodes[@]}
do
  ${NET_UTILS}/ip link delete int-${node}
done

# Stop the containers

docker stop $(for node in ${nodes[@]}; do echo -n "${node}.local "; done)

if [ "${ns3}" -eq "1" ]
then
  # Bring down the network interfaces and clean up

  for node in ${nodes[@]}
  do
    ./ns3/singleDestroy.sh ${node}
    PID=$(cat ${workdir}/var/run/${node}.pid)
    rm /var/run/netns/${PID}
    rm ${workdir}/var/run/${node}.pid
  done
fi

log "DONE"
