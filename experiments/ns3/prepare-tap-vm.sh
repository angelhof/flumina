#!/bin/bash

workdir=${PWD}

cp tap-vm.cc ${NS3_HOME}/scratch/
cd ${NS3_HOME}
./waf

cd ${workdir}

