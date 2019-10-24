### Build

Set your environment variable ERL_TOP where your Erlang OTP installation is. If downloaded using apt, then it should be in `/usr/lib/erlang/`.

Then run `make prepare_dialyzer` to set up Dialyzer.

Finally run `make` and then `make tests` to make sure that everything is working properly.

### Execute Scenario

Scenarios are just shell scripts and should be executed from the main directory, for example:

```
scenarios/docker_scenario.sh
```
