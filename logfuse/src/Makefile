
all: logfuse leven_dis.so

logfuse:logfuse.cpp logger.h
	g++ -O3 -D_FILE_OFFSET_BITS=64 -Wall -lpthread  -lfuse logfuse.cpp -o $@
	cp $@ ../
logptrace:logfuse.cpp logger.h
	g++ -O3 -D_FILE_OFFSET_BITS=64 -Wall -lpthread logptrace.cpp -o $@
leven_dis.so:leven_dis_lib.cpp
	g++ -O3 -fPIC -c $< -o leven_dis.lo
	g++ -shared -Wl,-soname,leven_dis.so leven_dis.lo -o $@
	cp $@ ../tools
clean:
	-rm logfuse
	-rm logptrace
	-rm leven_dis.so leven_dis.lo
	-rm leven_dis
