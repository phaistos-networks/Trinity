HOST:=$(shell hostname)
# Please see lucene_codec.h comments
# Your LUCENE_ENCODING_SCHEME value should match the defined LUCENE_USE_X macro set in lucene_codec.h
LUCENE_ENCODING_SCHEME:=pfor
EXTRA_CFLAGS:=


ifeq ($(HOST), origin)
# When building on our dev.system
	include /home/system/Development/Switch/Makefile.dfl
	CPPFLAGS:=$(CPPFLAGS_SANITY) $(OPTIMIZER_CFLAGS) $(EXTRA_CFLAGS)
	#CPPFLAGS:=$(CPPFLAGS_SANITY) -fsanitize=address
	#CPPFLAGS:=$(CPPFLAGS_SANITY) 

	ifeq ($(LUCENE_ENCODING_SCHEME),streamvbyte)
		SWITCH_OBJS += $(SWITCH_BASE)/ext/streamvbyte/streamvbyte.o $(SWITCH_BASE)/ext/streamvbyte/streamvbytedelta.o
	else ifeq ($(LUCENE_ENCODING_SCHEME),maskedvbyte)
		# make sure you link against maskedvybte; -lmaskedvbyte
	else
		SWITCH_OBJS:=$(SWITCH_BASE)/ext/FastPFor/libFastPFor.a
		SWITCH_OBJS:=$(SWITCH_BASE)/ext/FastPFor/CMakeFiles/FastPFor.dir/src/bitpacking.cpp.o $(SWITCH_BASE)/ext/FastPFor/CMakeFiles/FastPFor.dir/src/bitpackingaligned.cpp.o $(SWITCH_BASE)/ext/FastPFor/CMakeFiles/FastPFor.dir/src/bitpackingunaligned.cpp.o $(SWITCH_BASE)/ext/FastPFor/CMakeFiles/FastPFor.dir/src/horizontalbitpacking.cpp.o $(SWITCH_BASE)/ext/FastPFor/CMakeFiles/FastPFor.dir/src/simdunalignedbitpacking.cpp.o $(SWITCH_BASE)/ext/FastPFor/CMakeFiles/FastPFor.dir/src/simdbitpacking.cpp.o $(SWITCH_BASE)/ext/FastPFor/CMakeFiles/FastPFor.dir/src/varintdecode.c.o $(SWITCH_BASE)/ext/FastPFor/CMakeFiles/FastPFor.dir/src/streamvbyte.c.o
	endif

else
# Lean switch bundled in this repo
	CXX:=clang++
	CXXFLAGS:=-std=c++1z -Wstrict-aliasing=2 -Wsequence-point -Warray-bounds -Wextra -Winit-self -Wformat=2 -Wno-format-nonliteral -Wformat-security \
		-Wunused-variable -Wunused-value -Wreturn-type -Wparentheses -Wmissing-braces -Wno-invalid-source-encoding -Wno-invalid-offsetof \
		-Wno-unknown-pragmas -Wno-missing-field-initializers -Wno-unused-parameter -Wno-sign-compare -Wno-invalid-offsetof   \
		-fno-rtti -ffast-math  -D_REENTRANT -DREENTRANT  -g3 -ggdb -fno-omit-frame-pointer   \
		-fno-strict-aliasing    -DLEAN_SWITCH  -ISwitch/ -Wno-uninitialized -Wno-unused-function -Wno-uninitialized -funroll-loops  -Ofast $(EXTRA_CFLAGS)
	LDFLAGS:=-ldl -ffunction-sections -lpthread -ldl -lz -LSwitch/ext_snappy/ -lsnappy
	SWITCH_LIB:=

	ifeq ($(LUCENE_ENCODING_SCHEME),streamvbyte)
		SWITCH_OBJS += Switch/ext/streamvbyte/streamvbyte.o Switch/ext/streamvbyte/streamvbytedelta.o
	else ifeq ($(LUCENE_ENCODING_SCHEME),maskedvbyte)
		# make sure you link against maskedvybte; -lmaskedvbyte
	else
		SWITCH_OBJS:=Switch/ext/FastPFor/CMakeFiles/FastPFor.dir/src/bitpacking.cpp.o Switch/ext/FastPFor/CMakeFiles/FastPFor.dir/src/bitpackingaligned.cpp.o Switch/ext/FastPFor/CMakeFiles/FastPFor.dir/src/bitpackingunaligned.cpp.o Switch/ext/FastPFor/CMakeFiles/FastPFor.dir/src/horizontalbitpacking.cpp.o Switch/ext/FastPFor/CMakeFiles/FastPFor.dir/src/simdunalignedbitpacking.cpp.o Switch/ext/FastPFor/CMakeFiles/FastPFor.dir/src/simdbitpacking.cpp.o Switch/ext/FastPFor/CMakeFiles/FastPFor.dir/src/varintdecode.c.o Switch/ext/FastPFor/CMakeFiles/FastPFor.dir/src/streamvbyte.c.o
	endif	
endif

OBJS:=percolator.o compilation_ctx.o similarity.o docset_iterators_scorers.o google_codec.o docset_spans.o lucene_codec.o queryexec_ctx.o docset_iterators.o utils.o codecs.o queries.o exec.o docidupdates.o indexer.o docwordspace.o terms.o segment_index_source.o index_source.o merge.o intersect.o

ifeq ($(HOST), origin)
all : lib #app
app:  app.o lib
	$(CXX) app.o -o T $(LDFLAGS_SANITY) -lswitch -lpthread $(SWITCH_TLS_LDFLAGS) -lz -L /home/system/Development/Switch/ext/MaskedVByte -lmaskedvbyte -L./ -lthe_trinity -lswitch #-fsanitize=address
else
all: switch lib

switch:
	make -C Switch/ext_snappy/ 
	make -C Switch/ext/FastPFor/
	make -C Switch/ext/streamvbyte/
endif


lib: $(OBJS) 
	rm -f libthe_trinity.a
	ar rcs libthe_trinity.a $(SWITCH_OBJS) $(OBJS) 

clean:
	rm -f *.o T *.a Switch/ext_snappy/*o Switch/ext_snappy/*.a

.PHONY: clean
