#!/bin/bash

# Should be run from the root directory of the prototype

docker build \
  --build-arg uid=$(id -u) \
  --build-arg gid=$(id -g) \
  -f ./docker/erlnode/Dockerfile \
  -t erlnode \
  .
