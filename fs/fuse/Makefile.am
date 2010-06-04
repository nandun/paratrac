## Process this file with automake to produce Makefile.in

bin_PROGRAMS = ftrac

ftrac_SOURCES = ftrac.c

ftrac_LDADD = $(FUSE_PROBE_LIBS) -lulockmgr
ftrac_CFLAGS = $(FUSE_PROBE_CFLAGS)
ftrac_CPPFLAGS = -D_REENTRANT -DFUSE_USE_VERSION=26 -DLIBDIR=\"$(libdir)\"

