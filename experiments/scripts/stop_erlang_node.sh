#!/bin/bash

node_name=$1

echo "rpc:call('${node_name}', erlang, halt, [])." | erl -sname cleanup -setcookie flumina || true
