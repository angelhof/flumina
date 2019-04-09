# The context for this container should be the root of the streams prototype.
# That is, to build the container from the current directory, run the following.
#
#   docker build -t erlnode -f Dockerfile ../..
#
# Or, to build it from the root of the repo:
#
#   docker build -t erlnode -f docker/erlnode/Dockerfile .

from erlang:21.0

ENV ERL_TOP /usr/local

COPY . /proto
WORKDIR /proto
RUN make clean && make all

ENTRYPOINT [ "./wrapper.sh" ]
