OTP_DIR = ${ERL_TOP}
ERLC = $(OTP_DIR)/bin/erlc
ERL = $(OTP_DIR)/bin/erl

# ERL_COMPILE_FLAGS = +native '+{hipe, [o3]}'
ERL_COMPILE_FLAGS = +debug_info
EBIN_DIR   = ebin
EBIN_DIRS  = ebin ebin/* erlang-dot/ebin
I_DIRS = erlang-dot/include/
ERL_FILES  = $(wildcard *.erl)
BEAM_FILES = $(subst .erl,.beam,$(ERL_FILES))

HALT = -s erlang halt

## Create needed folders (if not exist):
$(shell [ -d "$(EBIN_DIR)/" ] || mkdir $(EBIN_DIR)/)


.PHONY: all

dialyzer: compile_all
	@echo ""
	@echo " --- --- --- --- DIALYZER --- --- --- --- "
	dialyzer --src .

compile_all: $(BEAM_FILES) $(NIF_FILES)

%.beam: %.erl
	$(ERLC) $(ERL_COMPILE_FLAGS) -I $(I_DIRS) -o $(EBIN_DIR) $<


open_erl:
	$(ERL) -pa $(EBIN_DIRS)

abexample:
	$(ERL) -pa $(EBIN_DIRS) -noshell -run abexample main $(args) $(HALT)

taxiexample_tumble:
	$(ERL) -pa $(EBIN_DIRS) -noshell -run taxiexample main $(args) $(HALT)

taxiexample_slide:
	$(ERL) -pa $(EBIN_DIRS) -noshell -run taxiexample distributed_1 $(args) $(HALT)

exec:
	$(ERL) -pa $(EBIN_DIRS) -noshell -run util exec $(args) $(HALT)

tests:
	@$(ERL) -pa $(EBIN_DIRS) -noshell -run abexample test $(HALT)
	@$(ERL) -pa $(EBIN_DIRS) -noshell -run taxiexample test $(HALT)
	@$(ERL) -pa $(EBIN_DIRS) -noshell -run smart_home_example test $(HALT)
