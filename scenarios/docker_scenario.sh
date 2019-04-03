#!/bin/bash

A1="a1node"
A2="a2node"
B="main"

EXEC="-noshell -run util exec abexample. real_distributed. [[\'${A1}@${A1}.local\',\'${A2}@${A2}.local\',\'${B}@${B}.local\']]. -s erlang halt"

cd docker

## Kill any old container with the same names
docker container kill "${A1}.local"
docker container kill "${A2}.local"
docker container kill "${B}.local"

## Create the containers and run the script
./make-docker-container.sh "${A1}" true
./make-docker-container.sh "${A2}" true
./make-docker-container.sh "${B}" false "${EXEC}"

