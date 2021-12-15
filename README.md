# Flumina

A parallel programming model for online applications with complex synchronization requirements.
Flumina can used for *stream processing* workloads and is evaluated against [Flink](https://github.com/apache/flink) and [Timely Dataflow](https://github.com/TimelyDataflow/timely-dataflow).

For more information about Flumina, check out our PPoPP'22 paper:

- **Stream Processing with Dependency-Guided Synchronization.** Konstantinos Kallas, Filip Niksic, Caleb Stanford, and Rajeev Alur. Principles and Practice of Parallel Programming (PPoPP), February 2022.
[Extended version](https://arxiv.org/abs/2104.04512).


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

## Running the baseline experiments (Flink and Timely)

TODO

## Issues

This is a research prototype implementation in Erlang and is not currently being maintained. However, if you encounter any issues running the code, please post an issue and we will try to take a look.
