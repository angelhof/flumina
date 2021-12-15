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

After having built the experiments and running the tests, you can run the "Hello, World" Flumina example.

You can inspect the source code of this example in `abexample.erl`. The function that configures it is called `big_conf/3` (https://github.com/angelhof/flumina-devel/blob/master/experiments/examples/abexample.erl#L91).

It roughly sets up the following:
- Sets three input streams and their rates, two fast streams for `a` tags, and one slow stream for `b` tags.
- It then invokes the compiler to generate a synchronization plan for these input streams.
- It then spawns three producers for these streams, and runs the synchronization plan.

You can run this example by running `make abexample` in `experiments`. The output will look something like this:
```
Mod: abexample, Fun: big_conf, Opts: [...]
Dependency Graph:
[{{b, ...},{{a,2}, ...}},
 {{{a,2}, ...},{b, ...}},
 {{b, ...},{{a,1}, ...}},
 {{{a,1}, ...},{b, ...}}]
Root tree:
{{[{b, ...}], ...},
 [{{[{{a,2}, ...}], ...},[]},
  {{[{{a,1}, ...}], ...},[]}]}
Possible setup trees: 1
Configuration:
...
Spawning 'steady_retimestamp' producer forimpl tag: ...
Spawning 'steady_retimestamp' producer forimpl tag: ...
Spawning 'steady_retimestamp' producer forimpl tag: ...
Producer for implementation tag: {{a,1}, ...} is done initializing.
Producer for implementation tag: {{a,2}, ...} is done initializing.
Producer for implementation tag: {b, ...} is done initializing.
Configuration done
{'EXIT',<0.83.0>,normal}
{sum,{{b,1000},2}}
{sum,{{b,2000},13}}
...
```

The output contains the implementation tag dependence graph produced by the dependencies of the input tags, a root tree (which is a simplified version of a synchronization plan), and a synchronization plan (which is called configuration).

It then spawns producers, and starts running the experiment.

## Notes on the experiments

The most recent experiments for Flumina and baselines were run using Amazon AWS EC2 instances,
with a large amount of custom scripting and work to set up correctly.
Some local versions can be run, but the full experimental setup would be difficult to replicate.
For example, clocks have to be synchronized across AWS instances, and this was done by running a Chrony script on all instances (`scripts/setup_chrony.sh`).
Most of the other experiment scripts for Flumina, Flink, and Timely are in `timely/scripts`.
This repository also contains some outdated and not-maintained infrastructure for running experiments using the NS3 network simulator and using Docker instances.

The Flink experiments are contained in `/experiments/flink-experiment` (with relevant scripts in `experiments/scripts` and in other directories).
Unfortunately, we do not have instructions for the Flink experiments at this time (but feel free to post a GitHub issue for your use case, see below).

## Running the Timely experiments locally

The Timely experiments are in `/experiments/timely-experiment` (also with relevant scripts in `experiments/scripts`),
and they are shipped with a usable CLI.
Inside that directory, run `cargo build` or `cargo build --release`,
then `cargo run` or `cargo run --release` for the CLI.
Be warned that Timely is MUCH faster in release mode for all queries,
so the `--release` flag is critical for performance experiments.
A short demo of the CLI:

- The experiments are organized into different examples: `fd` is the fraud-detection example, `vb` is the value-barrier example, and `pv-good` and `pv-bad` are two different implementations of the page-view example. Each command will describe its options if you try running it. For example, running `cargo run --release vb` gives the following expected parameters:
```
<val-rate-per-milli>
<vals-per-hb-per-worker>
<hbs-per-bar>
<exp-duration-secs>
<workers>
<nodes>
<this-node>
```
  OPTIONAL ADDITIONAL PARAMETER: a final parameter can be added to run multiple experiments in parallel: `s` for single process (default), `l` for multiple nodes locally, and `e` for running with multiple nodes on EC2.

  The most important parameters are the last 4: how long the experiment should run, how many workers and nodes to run (if nodes is more than one, you need to open more than one shell and run on each node), and which node this is (between 0 and number of nodes). For example, here's a plain single-threaded experiment, run for 5 seconds:
```
cargo run --release vb 1 10 10 5 1 1 0
```
  The experiment returns with the latency and throughput measurements of the experiment. It can also be run with two workers (threads):
```
cargo run --release vb 1 10 10 5 2 1 0
```
  where the throughput, as expected, doubles, and the latency may go up a bit. Finally it can be run with two processes locally, in two different shells, with two workers each:
```
# [in first shell]
cargo run --release vb 1 10 10 5 2 2 0 l
# [in second shell]
cargo run --release vb 1 10 10 5 2 2 1 l
```

- `exp1`, `exp2`, etc. are pre-set experiments (hardcoded in `main.rs`) which basically run the above for different numbers of workers and numbers of nodes and calculate a whole bunch of different throughputs.

- Finally, The `-gen` versions are experiments which run data generation only, with no data processing.
They can be useful to see what the input looks like, e.g.:
```
cargo run --release vb-gen 1 10 10 1 1 1 0
```

## Issues

This is a research prototype implementation in Erlang and is not currently being maintained. However, if you encounter any issues running the code, please post an issue on GitHub and we will try to take a look.
