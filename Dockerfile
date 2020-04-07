# The main purpose of this image is to be used as a builder stage for
# images that actually contain Flumina programs. See an example of this
# in the flumina-examples repository.
#
#   docker build -t flumina .
#

from erlang:22

ENV ERL_TOP /usr/local

COPY . /flumina
WORKDIR /flumina
RUN make clean && make all

# The FLUMINA_TOP environment variable remains defined in images that
# use this image as a base.

ENV FLUMINA_TOP /flumina
