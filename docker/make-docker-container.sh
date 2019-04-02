## Note:
## At the moment all the nodes have the same read
## only directory
##
## TODO: Maybe they should pull from github?
mkdir -p "read-only/$1"
cp -r ../erlang-dot "read-only/$1"
cp -r ../examples "read-only/$1"
cp -r ../include "read-only/$1"
cp -r ../scenarios "read-only/$1"
cp -r ../src "read-only/$1"
cp ../Makefile "read-only/$1/Makefile"

rm -f "read-only/$1/ebin/*"

set -x

## Notes:
## We need to use fully qualified names for hostname, so names with a dot, e.g. node1.local
##
## Call this script with a name for the node as a first argument
docker run -e ERL_TOP='/usr/local/'\
       -v "${PWD}/read-only/$1/":/stream-processing-prototype -it --rm\
       --network temp\
       --name "$1.local"\
       --hostname "$1.local"\
       erlang:21.0 bash -c \
       "cd stream-processing-prototype && make all && erl -name $1@$1.local -setcookie docker -pa ebin"


## abexample:real_distributed(['a1node@a1node.local', 'a2node@a2node.local', 'main@main.local']).
