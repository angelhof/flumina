#!/bin/bash

hostname=$1
to_dir=$2

scp -i ~/.ssh/internal_rsa -r -q ubuntu@$hostname:/home/ubuntu/logs $to_dir
ssh -i ~/.ssh/internal_rsa ubuntu@$hostname rm -rf logs
