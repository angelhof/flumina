#!/bin/bash

node_name=$1

echo "rpc:call('${node_name}', init, stop, [])." | erl -sname main -setcookie flumina
