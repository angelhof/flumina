#!/bin/bash

echo "Starting the nodes"
ssh -i ~/.ssh/internal_rsa ubuntu@ip-172-31-41-102.us-east-2.compute.internal erl -detached -noinput -sname flumina1 -setcookie flumina -pa flumina-devel/ebin flumina-devel/experiments/ebin

ssh -i ~/.ssh/internal_rsa ubuntu@ip-172-31-38-231.us-east-2.compute.internal erl -detached -noinput -sname flumina2 -setcookie flumina -pa flumina-devel/ebin flumina-devel/experiments/ebin

echo "Running the main abexample experiment"
make exec NAME_OPT="-sname main -setcookie flumina" args="abexample. real_distributed. [[\'flumina1@ip-172-31-41-102\',\'flumina2@ip-172-31-38-231\',\'main@ip-172-31-35-213\']]."

echo "Closing the nodes"
echo "rpc:call('flumina1@ip-172-31-41-102', init, stop, [])." | erl -sname main -setcookie flumina
echo "rpc:call('flumina2@ip-172-31-38-231', init, stop, [])." | erl -sname main -setcookie flumina

