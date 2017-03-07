include /home/system/Development/Switch/Makefile.dfl 
CPPFLAGS:=$(CPPFLAGS_SANITY) -fsanitize=address

OBJS:=queries.o exec.o google_codec.o segments.o docidupdates.o app.o indexer.o docwordspace.o terms.o

all : $(OBJS)
	$(CC) $(OBJS) -o T $(LDFLAGS_SANITY) -lswitch -lpthread $(SWITCH_TLS_LDFLAGS) -lz -fsanitize=address

clean:
	rm -f *.o

.PHONY: clean
