include /home/system/Development/Switch/Makefile.dfl 
#CPPFLAGS:=$(CPPFLAGS_SANITY) $(OPTIMIZER_CFLAGS)
CPPFLAGS:=$(CPPFLAGS_SANITY) #-fsanitize=address

OBJS:=utils.o codecs.o queries.o exec.o google_codec.o docidupdates.o indexer.o docwordspace.o terms.o segment_index_source.o index_source.o merge.o lucene_codec.o app.o intersect.o

all : $(OBJS)
	$(CC) $(OBJS) -o T $(LDFLAGS_SANITY) -lswitch -lpthread $(SWITCH_TLS_LDFLAGS) -lz -L /home/system/Development/Switch/ext/FastPFor   -lFastPFor -L /home/system/Development/Switch/ext/MaskedVByte -lmaskedvbyte #-fsanitize=address

clean:
	rm -f *.o

.PHONY: clean
