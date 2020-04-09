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
rm -rf "read-only/$1/logs"
mkdir -p "read-only/$1/logs"


rm -f "read-only/$1/ebin/*"


## Arguments
## The first argument of the script is the hostname and erlang node name
## The second argument is whether detached is true or false
## The third argument is what whill be executed with erlang

# set -x
## Notes:
## We need to use fully qualified names for hostname, so names with a dot, e.g. node1.local
docker run -e ERL_TOP='/usr/local'\
       -v "${PWD}/read-only/$1/":/stream-processing-prototype -it --rm\
       --network temp\
       --name "$1.local"\
       --hostname "$1.local"\
       -d=$2\
       erlang:21.0 bash -c \
       "cd stream-processing-prototype && make all && erl -name $1@$1.local -setcookie docker -pa ebin $3"

sleep 3
