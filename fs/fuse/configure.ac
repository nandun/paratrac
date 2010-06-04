#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ(2.61)
AC_INIT(ftrac, 0.4, dunnan@yl.is.s.u-tokyo.ac.jp,ftrac,
http://paratrac.googlecode.com)
AM_INIT_AUTOMAKE
AC_CONFIG_SRCDIR([ftrac.c])
AC_CONFIG_HEADER([config.h])

# Checks for programs.
AC_PROG_CC
AM_PROG_CC_C_O
CFLAGS="$CFLAGS -Wall -W"

# Checks for libraries.
#AC_CHECK_LIB([ulockmgr])

# Checks for header files.
AC_HEADER_DIRENT
AC_HEADER_STDC
AC_HEADER_TIME
AC_CHECK_HEADERS([fcntl.h string.h sys/time.h unistd.h sys/socket.h \
utime.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_C_CONST
AC_C_INLINE
AC_TYPE_MODE_T
AC_TYPE_OFF_T
AC_TYPE_SIZE_T
AC_TYPE_UID_T
AC_TYPE_PID_T

# Checks for library functions.
AC_FUNC_MALLOC
AC_FUNC_REALLOC
AC_FUNC_CLOSEDIR_VOID
AC_FUNC_LSTAT
AC_FUNC_LSTAT_FOLLOWS_SLASHED_SYMLINK
AC_FUNC_UTIME_NULL
AC_FUNC_SETVBUF_REVERSED
AC_CHECK_FUNCS([fdatasync ftruncate lchown memset memmove mkdir \ 
mkfifo rmdir strchr strdup socket strerror utime clock_gettime \
realpath uname])

# Checking FUSE
export PKG_CONFIG_PATH=/usr/local/lib/pkgconfig:$PKG_CONFIG_PATH
PKG_CHECK_MODULES(FUSE_PROBE, [fuse >= 2.7 glib-2.0 gthread-2.0])
have_fuse_opt_parse=no
AC_CHECK_FUNC([fuse_opt_parse], [have_fuse_opt_parse=yes])
if test "$have_fuse_opt_parse" = no; then
	CFLAGS="$CFLAGS -Icompat"
fi
AM_CONDITIONAL(FUSE_OPT_COMPAT, test "$have_fuse_opt_parse" = no)

AC_CONFIG_FILES([Makefile])
AC_OUTPUT
