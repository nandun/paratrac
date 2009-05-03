## Process this file with automake to produce Makefile.in

bin_PROGRAMS = fuse

fuse_SOURCES = fuse.c

fuse_LDADD = $(FUSE_PROBE_LIBS)
fuse_CFLAGS = $(FUSE_PROBE_CFLAGS)
fuse_CPPFLAGS = -D_REENTRANT -DFUSE_USE_VERSION=26 -DLIBDIR=\"$(libdir)\"

