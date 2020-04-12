#!/bin/bash

# Should be run from the root directory of the repo

docker build \
  --build-arg uid=$(id -u) \
  --build-arg gid=$(id -g) \
  -f ./experiments/docker/flumina/Dockerfile \
  -t flumina \
  .
