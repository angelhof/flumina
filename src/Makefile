ERL_FILES  = $(wildcard *.erl)
BEAM_FILES = $(patsubst %.erl,$(EBIN_DIR)/%.beam,$(ERL_FILES))
INCLUDES = $(wildcard $(I_DIR2)/*.hrl)

.PHONY: all clean

all: $(BEAM_FILES)

$(EBIN_DIR)/%.beam: %.erl $(INCLUDES)
	$(ERLC) $(ERL_COMPILE_FLAGS) -I $(I_DIR1) -I $(I_DIR2) -o $(EBIN_DIR) $<

$(BEAM_FILES): | $(EBIN_DIR)

$(EBIN_DIR):
	mkdir -p $(EBIN_DIR)

clean:
	$(RM) -rf $(EBIN_DIR)
