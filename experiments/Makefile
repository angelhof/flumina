OTP_DIR = ${ERL_TOP}
ERLC = $(OTP_DIR)/bin/erlc
ERL = $(OTP_DIR)/bin/erl

FLUMINA_DIR = ${FLUMINA_TOP}
FLUMINA_EBIN = $(FLUMINA_DIR)/ebin
FLUMINA_SRC = $(FLUMINA_DIR)/src

# ERL_COMPILE_FLAGS = +native '+{hipe, [o3]}'
ERL_COMPILE_FLAGS = +debug_info
EBIN_DIR   = ebin
EBIN_DIRS  = ebin $(FLUMINA_EBIN) ${EEP_EBIN_DIR}
# Was used by Dialyzer
# I_DIR1 = include
# I_DIRS = -I $(I_DIR1)
I_DIRS =
ERL_FILES  = $(wildcard *.erl)
BEAM_FILES = $(subst .erl,.beam,$(ERL_FILES))
NNAME ?= main
NAME_OPT ?= -sname $(NNAME)

HALT = -s init stop

## Create needed folders (if not exist):
$(shell [ -d "$(EBIN_DIR)/" ] || mkdir $(EBIN_DIR)/)


.PHONY: all clean

all: $(BEAM_FILES)
	@(cd examples && make EBIN_DIR=../$(EBIN_DIR) ERLC=$(ERLC) ERL_COMPILE_FLAGS="$(ERL_COMPILE_FLAGS)" \
		I_DIRS="$(I_DIRS)" LIBRARIES="$(FLUMINA_EBIN)" $@)

## TODO: Find a better way of doing that rather than having Flumina source in the dialyzer
dialyzer: all
	@echo ""
	@echo " --- --- --- --- DIALYZER --- --- --- --- "
	dialyzer --src -r examples $(FLUMINA_SRC) $(I_DIRS) -pa $(FLUMINA_EBIN)

%.beam: %.erl
	$(ERLC) $(ERL_COMPILE_FLAGS) $(I_DIRS) -o $(EBIN_DIR) $<

open_erl:
	$(ERL) $(NAME_OPT) -pa $(EBIN_DIRS)

open_erl_noshell:
	$(ERL) $(NAME_OPT) -pa $(EBIN_DIRS) -noshell

abexample:
	$(ERL) $(NAME_OPT) -pa $(EBIN_DIRS) -noshell -run abexample greedy_big $(args) $(HALT)

abexample_modulo:
	$(ERL) $(NAME_OPT) -pa $(EBIN_DIRS) -noshell -run abexample greedy_big_modulo $(args) $(HALT)

taxiexample_tumble:
	$(ERL) $(NAME_OPT) -pa $(EBIN_DIRS) -noshell -run taxiexample main $(args) $(HALT)

taxiexample_slide:
	$(ERL) $(NAME_OPT) -pa $(EBIN_DIRS) -noshell -run taxiexample distributed_1 $(args) $(HALT)

outlier_detection_seq:
	$(ERL) $(NAME_OPT) -pa $(EBIN_DIRS) -noshell -run outlier_detection seq $(args) $(HALT)

outlier_detection_distr:
	$(ERL) $(NAME_OPT) -pa $(EBIN_DIRS) -noshell -run outlier_detection distr $(args) $(HALT)

outlier_detection_greedy:
	$(ERL) $(NAME_OPT) -pa $(EBIN_DIRS) -noshell -run outlier_detection experiment_greedy $(args) $(HALT)

stream_table_join_example_greedy:
	$(ERL) $(NAME_OPT) -pa $(EBIN_DIRS) -noshell -run stream_table_join_example greedy_big $(args) $(HALT)


exec:
	$(ERL) $(NAME_OPT) -pa $(EBIN_DIRS) -noshell -run util exec $(args) $(HALT)

tests:
	mkdir -p logs
	@$(ERL) -pa $(EBIN_DIRS) -noshell -run abexample test $(HALT)
	@$(ERL) -pa $(EBIN_DIRS) -noshell -run taxiexample test $(HALT)
	@$(ERL) -pa $(EBIN_DIRS) -noshell -run smart_home_example test $(HALT)

prepare_dialyzer:
	dialyzer --build_plt --apps erts stdlib kernel

clean:
	rm -f $(EBIN_DIR)/*
