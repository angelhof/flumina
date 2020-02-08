## Overview

This repo contains a prototype implementation of Flumina, a
programming model for stateful streaming computations. It is still in
a research prototype state, and there are several issues and
extensions that need to be implemented. Most of them are written in
[notes.org](https://github.com/angelhof/flumina/blob/master/notes.org).

A repository containing examples of streaming application implement on
Flumina can be found
[here](https://github.com/angelhof/flumina-examples/).

### Build

Set your environment variable ERL_TOP where your Erlang OTP
installation is. If downloaded using apt, then it should be in
`/usr/lib/erlang/`.

Then run `make prepare_dialyzer` to set up Dialyzer, and `make` to
build Flumina.
