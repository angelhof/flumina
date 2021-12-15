# Flumina

A parallel programming model for online applications with complex synchronization requirements.
Flumina can used for *stream processing* workloads and is evaluated against [Flink](https://github.com/apache/flink) and [Timely Dataflow](https://github.com/TimelyDataflow/timely-dataflow).

For more information about Flumina, check out our PPoPP'22 paper:

- **Stream Processing with Dependency-Guided Synchronization.** Konstantinos Kallas, Filip Niksic, Caleb Stanford, and Rajeev Alur. Principles and Practice of Parallel Programming (PPoPP), February 2022.
[Extended version](https://arxiv.org/abs/2104.04512).

## Repository cleanup TODOs (November 23, 2021)

- I currently can't run Flumina, I got some errors. I think we should try to quickly get it working and provide instructions (trying to do this in <1hr of work)

- Sort and clean up `experiments` folder

  - We need an `examples/` directory and a basic example program to run.

- We need to clean up unused programs/files, I don't know all the junk that's in the repo though so it's fine if we miss some stuff.

## Installation and basic tests

1. Install Erlang: [link](https://www.erlang.org/downloads) or follow [this guide](https://medium.com/erlang-central/erlang-quick-install-9c5dcaa5b634)

**NOTE:** We have tested running the code with Erlang 23 and Erlang 24. If installation is not working, make sure you have one of these two versions installed.

2. Set your environment variable `ERL_TOP` where your Erlang OTP
installation is. If downloaded using apt, then it should be in
`/usr/lib/erlang/`. On mac, if downloaded using homebrew, then it should be in `/opt/homebrew/lib/erlang/`; otherwise also check `/opt/local/bin/to_erl/`.
E.g. (replacing the path with the correct directory for your installation):
```
export ERL_TOP=/usr/lib/erlang/
```

3. Set `FLUMINA_TOP` to the `flumina` directory.
(in the top-level directory, run `echo $PWD` to get the full path.)

4. Run `make` to build Flumina.

5. Run `cd experiments` directory: run `make` there to build the Flumina experiments (note: for baseline comparison, the Flink and Timely developments are built separately and not handled by this makefile).

6. Finally, still in `experiments`, run `make tests` to run tests. You should see output like this:
```
mkdir -p logs
  All 203 tests passed.
  All 420 tests passed.
  All 200 tests passed.
```

## "Hello, World" example

TODO

## Running the baseline experiments (Flink and Timely)

TODO

## Issues

This is a research prototype implementation in Erlang and is not currently being maintained. However, if you encounter any issues running the code, please post an issue and we will try to take a look.
