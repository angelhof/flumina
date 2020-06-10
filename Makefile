OTP_DIR = ${ERL_TOP}
ERLC = $(OTP_DIR)/bin/erlc
ERL = $(OTP_DIR)/bin/erl

# ERL_COMPILE_FLAGS = +native '+{hipe, [o3]}'
ERL_COMPILE_FLAGS = +debug_info
EBIN_DIR   = ebin
EBIN_DIRS  = ebin ebin/* erlang-dot/ebin ${EEP_EBIN_DIR}
I_DIR1 = erlang-dot/include/
I_DIR2 = ./include
INCLUDES = $(wildcard $(I_DIR2)/*.hrl)
I_DIRS = -I $(I_DIR1) -I $(I_DIR2)
ERL_FILES  = $(wildcard *.erl)
BEAM_FILES = $(subst .erl,.beam,$(ERL_FILES))
NNAME ?= main
NAME_OPT ?= -sname $(NNAME)

HALT = -s erlang halt

## Create needed folders (if not exist):
$(shell [ -d "$(EBIN_DIR)/" ] || mkdir $(EBIN_DIR)/)

.PHONY: all clean

dialyzer: all
	@echo ""
	@echo " --- --- --- --- DIALYZER --- --- --- --- "
	dialyzer --src -r src

all: $(BEAM_FILES)
	@(cd src && make EBIN_DIR=../$(EBIN_DIR) ERLC=$(ERLC) ERL_COMPILE_FLAGS="$(ERL_COMPILE_FLAGS)" \
		I_DIR1="../$(I_DIR1)" I_DIR2="../$(I_DIR2)" $@)

%.beam: %.erl ${INCLUDES}
	$(ERLC) $(ERL_COMPILE_FLAGS) $(I_DIRS) -o $(EBIN_DIR) $<

open_erl:
	$(ERL) $(NAME_OPT) -pa $(EBIN_DIRS)

open_erl_noshell:
	$(ERL) $(NAME_OPT) -pa $(EBIN_DIRS) -noshell

exec:
	$(ERL) $(NAME_OPT) -pa $(EBIN_DIRS) -noshell -run util exec $(args) $(HALT)

prepare_dialyzer:
	dialyzer --build_plt --apps erts stdlib kernel

docker:
	./experiments/docker/build_flumina.sh

clean:
	rm -f $(EBIN_DIR)/*
