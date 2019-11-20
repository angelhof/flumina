#!/bin/bash

# Should be run from the root directory of the prototype

FLINK_VERSION="1.9.1"
SCALA_VERSION="2.11"
FLUMINA_EXPERIMENT_JAR="flink-experiment/target/flink-experiment-1.0-SNAPSHOT.jar"

# Download Flink if it is not already present

if [ ! -f "flink-archive/flink.tgz" ]; then
  FLINK_BASE_URL="$(curl -s https://www.apache.org/dyn/closer.cgi\?preferred\=true)flink/flink-${FLINK_VERSION}/"
  FLINK_DIST_FILE_NAME="flink-${FLINK_VERSION}-bin-scala_${SCALA_VERSION}.tgz"
  FLINK_URL="${FLINK_BASE_URL}${FLINK_DIST_FILE_NAME}"

  echo "Downloading the Flink archive from ${FLINK_URL}"

  if [ ! -d "flink-archive" ]; then
    mkdir flink-archive
  fi

  curl -# ${FLINK_URL} --output flink-archive/flink.tgz
fi

if [ ! -f "${FLUMINA_EXPERIMENT_JAR}" ]; then
  echo "Couldn't find ${FLUMINA_EXPERIMENT_JAR}"
  echo "Did you forget to run 'mvn package' in flink-experiment?"
  exit 2
fi

docker build \
  --build-arg flink_dist="flink-archive/flink.tgz" \
  --build-arg job_artifact="${FLUMINA_EXPERIMENT_JAR}" \
  --build-arg uid=$(id -u) \
  --build-arg gid=$(id -g) \
  -f docker/flinknode/Dockerfile \
  -t flinknode \
  .
