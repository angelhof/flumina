#!/bin/bash

hostname=$1

nc -z -w1 $hostname 22 2>/dev/null && echo $hostname
true
