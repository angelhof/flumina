#!/bin/bash

echo "Starting the nodes"
scripts/start_erlang_node.sh flumina1 ip-172-31-41-102.us-east-2.compute.internal
scripts/start_erlang_node.sh flumina2 ip-172-31-38-231.us-east-2.compute.internal

echo "Running the main abexample experiment"
make exec NAME_OPT="-sname main -setcookie flumina" args="abexample. real_distributed. [[\'flumina1@ip-172-31-41-102\',\'flumina2@ip-172-31-38-231\',\'main@ip-172-31-35-213\']]."

echo "Closing the nodes"
scripts/stop_erlang_node.sh "flumina1@ip-172-31-41-102"
scripts/stop_erlang_node.sh "flumina2@ip-172-31-38-231"

