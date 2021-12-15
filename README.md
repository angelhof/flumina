# Flumina

A parallel programming model for online applications with complex synchronization requirements.
Flumina can used for *stream processing* workloads and is evaluated against [Flink](https://github.com/apache/flink) and [Timely Dataflow](https://github.com/TimelyDataflow/timely-dataflow).

For more information about Flumina, check out our PPoPP'22 paper:

- **Stream Processing with Dependency-Guided Synchronization.** Konstantinos Kallas, Filip Niksic, Caleb Stanford, and Rajeev Alur. Principles and Practice of Parallel Programming (PPoPP), February 2022.
[Extended version](https://arxiv.org/abs/2104.04512).

## Repository cleanup TODOs (November 23, 2021)

- Installation instructions: is step 3 necessary below?

- I currently can't run Flumina, I got some errors. I think we should try to quickly get it working and provide instructions (trying to do this in <1hr of work)

- Sort and clean up `experiments` folder

  - We need an `examples/` directory and a basic example program to run.

- Can we get rid of erlang dot? Samza? NS3?

- We need to clean up unused programs/files, I don't know all the junk that's in the repo though so it's fine if we miss some stuff.

## Installation

1. Install Erlang: [link](https://www.erlang.org/downloads) or follow [this guide](https://medium.com/erlang-central/erlang-quick-install-9c5dcaa5b634)

2. Set your environment variable `ERL_TOP` where your Erlang OTP
installation is. If downloaded using apt, then it should be in
`/usr/lib/erlang/`. On mac, if downloaded using homebrew, then it should be in `/opt/homebrew/lib/erlang/`; otherwise also check `/opt/local/bin/to_erl/`.

```
export ERL_TOP=/usr/lib/erlang/
```

3. Set `FLUMINA_TOP` to the `flumina` directory.

4. Run `make` to build Flumina.

5. `cd` into the `experiments` directory and then run `make` there to build experiments.

6. Run `make tests` to run tests.


KK: I think this is not necessary
3. (TODO Konstantinos: is this step necessary? I installed and built Flumina without it. What is `eep.beam`? I couldn't find anywhere.) Set the `EEP_EBIN_DIR` environment variable to point to the directory
that contains `eep.beam`.
```
export EEP_EBIN_DIR=?????
```

## Examples and experiments

**Inside experiments dir** (`cd experiments`):

You first have to set the `FLUMINA_TOP` environment variable to point
at the Flumina directory.

Then run `make` and then `make tests` to make sure that everything is working properly.

### Execute Scenario

Scenarios are just shell scripts and should be executed from the main directory, for example:

```
scenarios/docker_scenario.sh
```

## Issues

This is a research prototype implementation in Erlang and is not currently being maintained. However, if you encounter any issues running the code, please post an issue and we will try to take a look.
