# This Dockerfile builds an image containing Flumina, both the core and the
# examples used to run the experiments. Three things to have in mind:
#
# (1) The Dockerfile expects two build arguments to be passed when executing
#     the `docker build` command:
#
#       uid -- the user ID of the user who will be running the experiments
#       gid -- the group ID of the user who will be running the experiments
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
# (3) The image should be named 'flumina'. This is expected by the rest of the
#     infrastructure for running experiments. The corresponding option to the
#     `docker build` command:
#
#       -t flumina

### Stage 1: build the Flumina binaries

from erlang:22 as builder

ENV ERL_TOP /usr/local

COPY . /flumina
WORKDIR /flumina
RUN make clean && make all

ENV FLUMINA_TOP /flumina
WORKDIR /flumina/experiments
RUN make clean && make all

### Stage 2: copy the binaries to the alpine version

from erlang:22-alpine

# Prerequisites:
#   - sudo -- needed for editing /etc/hosts
RUN apk add --no-cache sudo

ARG uid
ARG gid

# We create a user called flumina with the uid and gid matching the outside user.
# We add the user to the wheel group and allow wheel to execute sudo without
# entering password.

RUN set -x && \
  addgroup -g $gid flumina && \
  adduser -D -H -G flumina -h /flumina -u $uid flumina && \
  addgroup flumina wheel && \
  sed -e 's/^# \(%wheel.*NOPASSWD.*\)/\1/g' -i /etc/sudoers && \
  mkdir /flumina && \
  chown flumina:flumina /flumina

COPY --from=builder \
     --chown=flumina:flumina \
     /flumina/ebin \
     /flumina/ebin
COPY --from=builder \
     --chown=flumina:flumina \
     /flumina/experiments/ebin \
     /flumina/ebin
COPY --chown=flumina:flumina \
     experiments/docker/flumina/wrapper.sh \
     /flumina/wrapper.sh
RUN chmod +x /flumina/wrapper.sh

WORKDIR /flumina
USER flumina:flumina

ENTRYPOINT [ "/flumina/wrapper.sh" ]
