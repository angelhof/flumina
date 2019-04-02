mkdir -p ./read-only
cp -r ../erlang-dot read-only/erlang-dot
cp -r ../examples read-only/examples
cp -r ../include read-only/include
cp -r ../scenarios read-only/scenarios
cp -r ../src read-only/src
cp ../Makefile read-only/Makefile

rm -f read-only/ebin/*

docker run -e ERL_TOP='/usr/local/'\
       -v "${PWD}/read-only/":/stream-processing-prototype -it --rm -h erlang.local erlang:21.0 bash
