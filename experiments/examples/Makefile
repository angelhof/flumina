ERL_FILES  = $(wildcard *.erl)
BEAM_FILES = $(patsubst %.erl,$(EBIN_DIR)/%.beam,$(ERL_FILES))

.PHONY: all clean

all: $(BEAM_FILES)

$(EBIN_DIR)/%.beam: %.erl
	$(ERLC) $(ERL_COMPILE_FLAGS) -pa $(LIBRARIES) -I $(I_DIR1) -o $(EBIN_DIR) $<

$(BEAM_FILES): | $(EBIN_DIR)

$(EBIN_DIR):
	mkdir -p $(EBIN_DIR)

clean:
	$(RM) -rf $(EBIN_DIR)
