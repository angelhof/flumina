#!/bin/sh

make open_erl NNAME=a1node >/dev/null 2>&1 &
make open_erl NNAME=a2node >/dev/null 2>&1 &
make exec args="abexample. real_distributed. []."
