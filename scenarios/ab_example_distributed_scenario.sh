#!/bin/sh

## TODO: Add the hostname in a parametrized way

A1_NODE="a1node@127.0.0.1"
A2_NODE="a1node@127.0.0.1"
B_NODE="main@127.0.0.1"

EXEC="abexample. real_distributed. [[\'${A1_NODE}\',\'${A2_NODE}\',\'${B_NODE}\']]."

make open_erl_noshell NAME_OPT="-name ${A1_NODE}" >/dev/null 2>&1 &
make open_erl_noshell NAME_OPT="-name ${A1_NODE}" >/dev/null 2>&1 &
make exec NAME_OPT="-name ${B_NODE}" args="${EXEC}"
