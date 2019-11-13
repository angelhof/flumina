# The context for this container should be the root of the Flumina prototype.
# That is, to build the container from the current directory, run the following.
#
#   docker build -t erlnode -f Dockerfile ../..
#
# Or, to build it from the root of the repo:
#
#   docker build -t erlnode -f docker/erlnode/Dockerfile .

# Stage 1: build the prototype

from erlang:21.0 as builder

ENV ERL_TOP /usr/local

COPY . /proto
WORKDIR /proto
RUN make clean && make all

# Stage 2: copy the binaries to the alpine version

from erlang:21.0-alpine

RUN mkdir /proto
COPY --from=builder /proto/ebin /proto/ebin
COPY docker/erlnode/wrapper.sh /proto/wrapper.sh
RUN chmod +x /proto/wrapper.sh
WORKDIR /proto

ENTRYPOINT [ "/proto/wrapper.sh" ]
