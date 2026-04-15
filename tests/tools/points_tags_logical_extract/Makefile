CPPFLAGS += -I../../..
CFLAGS ?= -O2 -g
LDFLAGS += -lzstd

TOOLS = points_tags_logical_extract

all: $(TOOLS)

points_tags_logical_extract: points_tags_logical_extract.c
	$(CC) $(CPPFLAGS) $(CFLAGS) points_tags_logical_extract.c -o $@ $(LDFLAGS)

clean:
	rm -f $(TOOLS)

.PHONY: all clean
