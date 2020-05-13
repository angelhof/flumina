#!/bin/bash

sname=$1
hostname=$2

ssh -i ~/.ssh/internal_rsa ubuntu@$hostname erl -detached -noinput -sname $sname -setcookie flumina -pa flumina-devel/ebin flumina-devel/experiments/ebin
