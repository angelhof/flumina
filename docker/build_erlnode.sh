#!/bin/bash

# Should be run from the root directory of the prototype

docker build -f ./docker/erlnode/Dockerfile -t erlnode .
