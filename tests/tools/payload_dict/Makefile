CPPFLAGS += -I../../..
CFLAGS ?= -O2 -g
LDFLAGS += -lzstd

TOOLS = payload_dict_train

all: $(TOOLS)

payload_dict_train: payload_dict_train.c
	$(CC) $(CPPFLAGS) $(CFLAGS) payload_dict_train.c -o $@ $(LDFLAGS)

clean:
	rm -f $(TOOLS)

.PHONY: all clean
