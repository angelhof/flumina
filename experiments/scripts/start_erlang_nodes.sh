#!/bin/bash

ssh -i ~/.ssh/internal_rsa ubuntu@ec2-18-188-245-57.us-east-2.compute.amazonaws.com erl -detached -noinput -sname flumina1 -setcookie flumina 
