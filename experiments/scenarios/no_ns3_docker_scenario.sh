#!/bin/bash

ANODES_ARR=($1)
B="main"
EXEC=$2

## This script accepts as arguments the anode names and the exec
## It kills and starts the node containers and then runs the experiment

# echo ${EXEC}

cd docker

## Kill any old container with the same names
for i in "${ANODES_ARR[@]}"
do
    echo "Killing $i container"
    docker container kill "${i}.local"
done

echo "Killing ${B} container"
docker container kill "${B}.local"

## Create the containers and run the script
for i in "${ANODES_ARR[@]}"
do
    echo "Starting $i container"
    ./make-docker-container.sh "${i}" true "+S 2:2"
done

echo "Starting $B container"
./make-docker-container.sh "${B}" false "${EXEC}"

## Kill any spawned container
for i in "${ANODES_ARR[@]}"
do
    echo "Killing $i container"
    docker container kill "${i}.local"
done
