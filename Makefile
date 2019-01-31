# ERLC = ${ERL_TOP}/bin/erlc
OTP_DIR = /home/konstantinos/Desktop/University/thesis_source_code/otp/
ERLC = $(OTP_DIR)/bin/erlc
ERL = $(OTP_DIR)/bin/erl
# ERLC = ../../thesis_source_code/master_otp/otp/bin/erlc
# ERL = ../../thesis_source_code/master_otp/otp/bin/erl

# ERL_COMPILE_FLAGS = +native '+{hipe, [o3]}'
ERL_COMPILE_FLAGS = +debug_info
EBIN_DIR   = ebin
EBIN_DIRS  = ebin ebin/* /home/konstantinos/Desktop/University/thesis_source_code/erllvm-bench/ebin/*
ERL_FILES  = $(wildcard *.erl)
BEAM_FILES = $(subst .erl,.beam,$(ERL_FILES))
C_FILES    = $(wildcard *.c)
NIF_FILES  = $(subst .c,.so,$(C_FILES))

HALT = -s erlang halt

## Create needed folders (if not exist):
$(shell [ -d "$(EBIN_DIR)/" ] || mkdir $(EBIN_DIR)/)


.PHONY: all

all: $(BEAM_FILES) $(NIF_FILES)

%.beam: %.erl
	$(ERLC) $(ERL_COMPILE_FLAGS) -o $(EBIN_DIR) $<

dialyzer:
	../../thesis_source_code/otp/bin/dialyzer --src -r .

open_erl:
	$(ERL) -pa $(EBIN_DIRS)

abexample:
	$(ERL) -pa $(EBIN_DIRS) -noshell -run abexample main $(args) $(HALT)
