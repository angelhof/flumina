#!/bin/bash

## Install Erlang
wget https://packages.erlang-solutions.com/erlang-solutions_1.0_all.deb
sudo dpkg -i erlang-solutions_1.0_all.deb
sudo apt-get update
sudo apt-get install esl-erlang

## Set the erlang environment variable for Flumina
echo "export ERL_TOP=/usr/lib/erlang/" >> ~/.bashrc
export ERL_TOP=/usr/lib/erlang/

## Install dependencies
sudo apt install make
sudo apt install pip3-install
pip3 install matplotlib

## Set up the Github account with the
git config --global user.name 'Konstantinos Kallas'
git config --global user.email 'konstantinos.kallas@hotmail.com'

## Set password caching
git config --global credential.helper cache

## Clone Flumina
git clone https://github.com/angelhof/flumina-devel.git
cd flumina-devel
make prepare_dialyzer
make

## Set FLUMINA_TOP that is needed by experiments
export FLUMINA_TOP=$PWD
echo "export FLUMINA_TOP=$PWD" >> ~/.bashrc

## Make the experiments
cd experiments
make
make abexample
make tests

