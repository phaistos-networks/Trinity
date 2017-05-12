HOST:=$(shell hostname)

ifeq ($(HOST), origin)
# When building on our dev.system
	include /home/system/Development/Switch/Makefile.dfl
	CPPFLAGS:=$(CPPFLAGS_SANITY) $(OPTIMIZER_CFLAGS)
	#CPPFLAGS:=$(CPPFLAGS_SANITY) #-fsanitize=address
	SWITCH_OBJS:=$(SWITCH_BASE)/ext/FastPFor/libFastPFor.a
	SWITCH_OBJS:=$(SWITCH_BASE)/ext/FastPFor/CMakeFiles/FastPFor.dir/src/bitpacking.cpp.o $(SWITCH_BASE)/ext/FastPFor/CMakeFiles/FastPFor.dir/src/bitpackingaligned.cpp.o $(SWITCH_BASE)/ext/FastPFor/CMakeFiles/FastPFor.dir/src/bitpackingunaligned.cpp.o $(SWITCH_BASE)/ext/FastPFor/CMakeFiles/FastPFor.dir/src/horizontalbitpacking.cpp.o $(SWITCH_BASE)/ext/FastPFor/CMakeFiles/FastPFor.dir/src/simdunalignedbitpacking.cpp.o $(SWITCH_BASE)/ext/FastPFor/CMakeFiles/FastPFor.dir/src/simdbitpacking.cpp.o $(SWITCH_BASE)/ext/FastPFor/CMakeFiles/FastPFor.dir/src/varintdecode.c.o $(SWITCH_BASE)/ext/FastPFor/CMakeFiles/FastPFor.dir/src/streamvbyte.c.o
else
# Lean switch bundled in this repo
	CXX:=clang++
	CXXFLAGS:=-std=c++1z -Wstrict-aliasing=2 -Wsequence-point -Warray-bounds -Wextra -Winit-self -Wformat=2 -Wno-format-nonliteral -Wformat-security \
		-Wunused-variable -Wunused-value -Wreturn-type -Wparentheses -Wmissing-braces -Wno-invalid-source-encoding -Wno-invalid-offsetof \
		-Wno-unknown-pragmas -Wno-missing-field-initializers -Wno-unused-parameter -Wno-sign-compare -Wno-invalid-offsetof   \
		-fno-rtti -ffast-math  -D_REENTRANT -DREENTRANT  -g3 -ggdb -fno-omit-frame-pointer   \
		-fno-strict-aliasing    -DLEAN_SWITCH  -ISwitch/ -Wno-uninitialized -Wno-unused-function -Wno-uninitialized -funroll-loops  -O3
	LDFLAGS:=-ldl -ffunction-sections -lpthread -ldl -lz -LSwitch/ext_snappy/ -lsnappy
	SWITCH_LIB:=
	SWITCH_OBJS:=Switch/ext/FastPFor/CMakeFiles/FastPFor.dir/src/bitpacking.cpp.o Switch/ext/FastPFor/CMakeFiles/FastPFor.dir/src/bitpackingaligned.cpp.o Switch/ext/FastPFor/CMakeFiles/FastPFor.dir/src/bitpackingunaligned.cpp.o Switch/ext/FastPFor/CMakeFiles/FastPFor.dir/src/horizontalbitpacking.cpp.o Switch/ext/FastPFor/CMakeFiles/FastPFor.dir/src/simdunalignedbitpacking.cpp.o Switch/ext/FastPFor/CMakeFiles/FastPFor.dir/src/simdbitpacking.cpp.o Switch/ext/FastPFor/CMakeFiles/FastPFor.dir/src/varintdecode.c.o Switch/ext/FastPFor/CMakeFiles/FastPFor.dir/src/streamvbyte.c.o
endif

OBJS:=utils.o codecs.o queries.o exec.o google_codec.o docidupdates.o indexer.o docwordspace.o terms.o segment_index_source.o index_source.o merge.o lucene_codec.o intersect.o

ifeq ($(HOST), origin)
all : app lib
app:  app.o lib
	$(CC) app.o -o T $(LDFLAGS_SANITY) -lswitch -lpthread $(SWITCH_TLS_LDFLAGS) -lz -L /home/system/Development/Switch/ext/MaskedVByte -lmaskedvbyte -L./ -lthe_trinity #-fsanitize=address
else
all: switch lib

switch:
	make -C Switch/ext_snappy/ 
	make -C Switch/ext/FastPFor/
endif


lib: $(OBJS) 
	rm -f libthe_trinity.a
	ar rcs libthe_trinity.a $(SWITCH_OBJS) $(OBJS) 

clean:
	rm -f *.o T *.a Switch/ext_snappy/*o Switch/ext_snappy/*.a

.PHONY: clean
