include /home/system/Development/Switch/Makefile.dfl 
#CPPFLAGS:=$(CPPFLAGS_SANITY) $(OPTIMIZER_CFLAGS)
CPPFLAGS:=$(CPPFLAGS_SANITY) #-fsanitize=address

OBJS:=queries.o exec.o google_codec.o docidupdates.o app.o indexer.o docwordspace.o terms.o segment_index_source.o index_source.o merge.o matches.o

all : $(OBJS)
	$(CC) $(OBJS) -o T $(LDFLAGS_SANITY) -lswitch -lpthread $(SWITCH_TLS_LDFLAGS) -lz #-fsanitize=address

clean:
	rm -f *.o

.PHONY: clean
