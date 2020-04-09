# The context for this container should be the root of the Flumina examples
# directory. That is, to build the container from the current directory,
# run the following.
#
#   docker build -t erlnode -f Dockerfile ../..
#
# Or, to build it from the root of the repo:
#
#   docker build -t erlnode -f docker/erlnode/Dockerfile .

# Stage 1: build the Flumina examples

from flumina as builder

COPY . /flumina-examples
WORKDIR /flumina-examples
RUN make clean && make all

# Stage 2: copy the binaries to the alpine version

from erlang:22-alpine

# Prerequisites:
#   - sudo -- needed for editing /etc/hosts
RUN apk add --no-cache sudo

ARG uid=NOT_SET
ARG gid=NOT_SET

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

COPY --from=builder --chown=flumina:flumina /flumina/ebin /flumina/ebin
COPY --from=builder --chown=flumina:flumina /flumina-examples/ebin /flumina/ebin
COPY --chown=flumina:flumina docker/erlnode/wrapper.sh /flumina/wrapper.sh
RUN chmod +x /flumina/wrapper.sh

WORKDIR /flumina
USER flumina:flumina

ENTRYPOINT [ "/flumina/wrapper.sh" ]
