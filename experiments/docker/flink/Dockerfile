# This Dockerfile builds an image containing a job to run Flink experiments.
# Three things to have in mind:
#
# (1) The Dockerfile expects four build arguments to be passed when executing
#     the `docker build` command:
#
#       flink_dist   -- path to the Flink archive
#       job_artifact -- path to the JAR file containing the Flink job
#       uid          -- the user ID of the user who is running the experiments
#       gid          -- the group ID of the user who is running the experiments
#
#     Most likely, the user who will be running the experiments is the same
#     as the user who is building the image. In that case, the correct arguments
#     are passed using with the following options to the `docker build` command:
#
#       --build-arg uid=$(id -u) --build-arg gid=$(id -g)
#
# (2) The context for this container should be the root of the Flumina
#     directory. That is, to build the container from the current directory,
#     add the following option to the `docker build` command. (Please make sure
#     that the path is correct after any future restructuring of the repo.)
#
#       -f Dockerfile ../../..
#
#     To build the image from the root of the repo, the correct option is the
#     following. (Again, check to make sure the path is not broken!)
#
#       -f experiments/docker/flumina/Dockerfile .
#
# (3) The image should be named 'flumina-flink'. This is expected by the rest of
#     the infrastructure for running experiments. The corresponding option to
#     the `docker build` command:
#
#       -t flumina-flink

FROM openjdk:8-jre-alpine

# Prerequisites:
#   - bash -- needed because Flink's scripts are bash scripts
#   - libc6-compat -- no idea; saw it in
#       https://github.com/apache/flink/blob/master/flink-container/docker/Dockerfile
#   - sudo -- needed for editing /etc/hosts
#   - curl -- needed for accessing the Flink's REST API
#   - jq   -- needed for parsing json on command line
RUN apk add --no-cache bash libc6-compat sudo curl jq

ARG flink_dist
ARG job_artifact
ARG uid
ARG gid

ENV FLINK_HOME /opt/flink

# We create a user called flink with the uid and gid matching the outside user.
# We add the user to the wheel group and allow wheel to execute sudo without
# entering password.

RUN set -x && \
  addgroup -g $gid flink && \
  adduser -D -H -G flink -h $FLINK_HOME -u $uid flink && \
  addgroup flink wheel && \
  sed -e 's/^# \(%wheel.*NOPASSWD.*\)/\1/g' -i /etc/sudoers

ADD --chown=flink:flink $flink_dist /opt
COPY --chown=flink:flink $job_artifact /job.jar
COPY --chown=flink:flink experiments/docker/flink/docker-entrypoint.sh /
COPY --chown=flink:flink experiments/docker/flink/wait.sh /

RUN set -x && \
  ln -s /opt/flink-[0-9]* $FLINK_HOME && \
  chown -R flink:flink /opt/flink-* && \
  chown -h flink:flink $FLINK_HOME && \
  chmod +x /docker-entrypoint.sh && \
  chmod +x /wait.sh

USER flink:flink
EXPOSE 8081 6123
ENTRYPOINT [ "/docker-entrypoint.sh" ]
