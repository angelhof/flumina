## Note:
## At the moment all the nodes have the same read
## only directory
##
## TODO: Maybe they should pull from github?
mkdir -p ./read-only
cp -r ../erlang-dot read-only/erlang-dot
cp -r ../examples read-only/examples
cp -r ../include read-only/include
cp -r ../scenarios read-only/scenarios
cp -r ../src read-only/src
cp ../Makefile read-only/Makefile

rm -f read-only/ebin/*

set -x

## Notes:
## We need to use fully qualified names for hostname, so names with a dot, e.g. node1.local
##
## Call this script with a name for the node as a first argument
docker run -e ERL_TOP='/usr/local/'\
       -v "${PWD}/read-only/":/stream-processing-prototype -it --rm\
       --network temp\
       --name "$1.local"\
       --hostname "$1.local"\
       erlang:21.0 erl -name "$1@$1.local" -setcookie docker
