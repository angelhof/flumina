OTP_DIR = ${ERL_TOP}
ERLC = $(OTP_DIR)/bin/erlc
ERL = $(OTP_DIR)/bin/erl

# ERL_COMPILE_FLAGS = +native '+{hipe, [o3]}'
ERL_COMPILE_FLAGS = +debug_info
EBIN_DIR   = ebin
EBIN_DIRS  = ebin ebin/*
ERL_FILES  = $(wildcard *.erl)
BEAM_FILES = $(subst .erl,.beam,$(ERL_FILES))

HALT = -s erlang halt

## Create needed folders (if not exist):
$(shell [ -d "$(EBIN_DIR)/" ] || mkdir $(EBIN_DIR)/)


.PHONY: all

all: $(BEAM_FILES) $(NIF_FILES)

%.beam: %.erl
	$(ERLC) $(ERL_COMPILE_FLAGS) -o $(EBIN_DIR) $<

dialyzer:
	dialyzer --src -r .

open_erl:
	$(ERL) -pa $(EBIN_DIRS)

abexample:
	$(ERL) -pa $(EBIN_DIRS) -noshell -run abexample main $(args) $(HALT)

taxiexample:
	$(ERL) -pa $(EBIN_DIRS) -noshell -run taxiexample main $(args) $(HALT)
