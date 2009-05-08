/****************************************************************************
 * ParaTrac: Scalable Tracking Tools for Parallel Applications
 * Copyright (C) 2009  Nan Dun <dunnan@yl.is.s.u-tokyo.ac.jp>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * This program can be distributed under the terms of the GNU GPL.
 * See the file COPYING.
 ***************************************************************************/

/*
FUSE tool for file system calls tracking
 * Takes only the lastest snapshot of the system calls, never keeps the call
   history for system call or file.
 * The tracing granularity is determined by polling rate.
*/

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#ifdef linux
/* For pread()/pwrite() */
#define _XOPEN_SOURCE 500
#endif

#include <fuse.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <errno.h>
#include <pwd.h>
#include <utime.h>
#include <assert.h>
#include <semaphore.h>
#include <pthread.h>
#include <signal.h>
#include <sys/time.h>
#include <sys/times.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#ifdef HAVE_SETXATTR
#include <sys/xattr.h>
#endif
#include <glib.h>

#if FUSE_VERSION < 23
#error FUSE_VERSION < 23
#endif

/* 
 * system call number
 * like the definitions in <syscall.h>
 * use paratrac its own definition
 */

#define SYSC_LSTAT          84
#define SYSC_ACCESS         33
#define SYSC_READLINK       85
#define SYSC_READDIR        89
#define SYSC_MKNOD          14
#define SYSC_MKDIR          39
#define SYSC_UNLINK         10
#define SYSC_RMDIR          40
#define SYSC_SYMLINK        83
#define SYSC_RENAME         38
#define SYSC_LINK           9
#define SYSC_CHMOD          15
#define SYSC_CHOWN          16      /* lchown() */
#define SYSC_TRUNCATE       92
#define SYSC_UTIME          30
#define SYSC_OPEN           5
#define SYSC_READ           3
#define SYSC_WRITE          4
#define SYSC_STATFS         99
#define SYSC_FSYNC          118
#ifdef HAVE_SETXATTR        
#define SYSC_SETXATTR       201
#define SYSC_GETXATTR       202
#define SYSC_LISTXATTR      203
#define SYSC_REMOVEXATTR    204
#endif

#define	PATH_PREFIX	"/tmp"
#define SERVER_MAX_CONN 8
#define SERVER_MAX_BUFF	256

#define FTRAC_MODE_POLL     0
#define FTRAC_MODE_TRACK    1

#define FTRAC_TRACE_SYSTEM
#define FTRAC_TRACE_FILE
#define FTRAC_TRACE_PROC

#define POLL_STAT 		'1'
#define POLL_FILESYSTEM '2'
#define POLL_FILE		'3'
#define POLL_DIRECTORY 	'4'
#define POLL_FINISH		'5'
#define POLL_UNKNOWN 	'x'

/* system call statistics */
typedef struct stat_sc {
	time_t atime;			/* last accessed time */
	unsigned long cnt;		/* total called count */
	double elapsed;			/* last call elapsed */
	pid_t pid;				/* caller's pid */
} * stat_sc_t;

/* input/output statistics */
typedef struct stat_io {
	time_t atime;			/* last accessed time */
	unsigned long cnt;		/* total called count */
	double elapsed;			/* last call elapsed */
	pid_t pid;				/* caller's pid */

	size_t size;			/* bytes size in last call */
	off_t offset;			/* offset for file access */
	unsigned long bytes;	/* total bytes */
} * stat_io_t;

/* system call record */
typedef struct rec_sc {
    unsigned sysc;          /* system call number */
    time_t atime;
    double elapsed;
    pid_t pid;              /* caller's pid */
} * rec_sc_t;

/* filesystem global statistics */
typedef struct stat_filesystem {
	struct stat_sc stat;
	struct stat_sc access;
	struct stat_sc readlink;
	struct stat_sc readdir;
	struct stat_sc mknod;
	struct stat_sc mkdir;
	struct stat_sc symlink;
	struct stat_sc unlink;
	struct stat_sc rmdir;
	struct stat_sc rename;
	struct stat_sc link;
	struct stat_sc chmod;
	struct stat_sc chown;
	struct stat_sc truncate;
	struct stat_sc utime;
	struct stat_sc open;
	struct stat_sc statfs;
	struct stat_sc release;
	struct stat_sc fsync;
#ifdef HAVE_SETXATTR
	struct stat_sc setxattr;
	struct stat_sc getxattr;
	struct stat_sc listxattr;
	struct stat_sc removexattr;
#endif
	
	struct stat_io read;
	struct stat_io write;

} * stat_filesystem_t;

/* file statistics */
#ifdef FTRAC_TRACE_FILE
typedef struct stat_file {
	struct stat_sc stat;
	struct stat_sc access;
	struct stat_sc readlink;
	struct stat_sc readdir;
	struct stat_sc mknod;
	struct stat_sc mkdir;
	struct stat_sc symlink;
	struct stat_sc unlink;
	struct stat_sc rmdir;
	struct stat_sc rename;
	struct stat_sc link;
	struct stat_sc chmod;
	struct stat_sc chown;
	struct stat_sc truncate;
	struct stat_sc utime;
	struct stat_sc open;
	struct stat_sc statfs;
	struct stat_sc release;
	struct stat_sc fsync;
#ifdef HAVE_SETXATTR
	struct stat_sc setxattr;
	struct stat_sc getxattr;
	struct stat_sc listxattr;
	struct stat_sc removexattr;
#endif
	
	struct stat_io read;
	struct stat_io write;
	
	time_t born;	/* time stamp when file is created */
	time_t dead;	/* time stamp when file is deleted */
    
    time_t atime;   /* access time, for cleanup */

	pthread_mutex_t lock;
} * stat_file_t;
#endif

/* record table */
typedef struct hash_table {
	GHashTable *table;
	pthread_mutex_t lock;
} * hash_table_t;

struct ftrac {
	char *progname;
	char *mountpoint;
	char *username;
	int foreground;
	uid_t uid;
	gid_t gid;
	pid_t pid;
    int mode;       /* running mode: poll/track */

	/* server */
	char *sessiondir;
	char *sockpath;
	int sockfd;
	int serv_started;
	pthread_t serv_thread;

	/* statistics */
	time_t start;
	time_t end;
	struct stat_filesystem fs;
#ifdef FTRAC_TRACE_FILE
	struct hash_table files;
#endif
#ifdef FTRAC_TRACE_PROC
	struct hash_table procs;
#endif

    /* hash_table managment */
    long cache_timeout;
};

static struct ftrac ftrac;

#define FTRAC_OPT(t, p, v) { t, offsetof(struct ftrac, p), v }

enum {
	KEY_HELP,
	KEY_VERSION,
	KEY_FOREGROUND,
};

static struct fuse_opt ftrac_opts[] = {
	FTRAC_OPT("sessiondir=%s",	    sessiondir, 0),
	FTRAC_OPT("cache_timeout=%ld",	cache_timeout, 0),

	FUSE_OPT_KEY("-V",			KEY_VERSION),
	FUSE_OPT_KEY("--version",	KEY_VERSION),
	FUSE_OPT_KEY("-h",			KEY_HELP),
	FUSE_OPT_KEY("--help", 		KEY_HELP),
	FUSE_OPT_KEY("debug",       KEY_FOREGROUND),
	FUSE_OPT_KEY("-d",          KEY_FOREGROUND),
	FUSE_OPT_KEY("-f",          KEY_FOREGROUND),
	FUSE_OPT_END
};

/*********** statistic processing routines **********/
static inline void stat_sc_update(stat_sc_t sc,	time_t timestamp, 
	double elapsed, pid_t pid)
{
	sc->atime = timestamp;
	sc->elapsed = elapsed;
	sc->pid = pid;
	sc->cnt ++;
}

static inline void stat_io_update(stat_io_t io, time_t timestamp,
	double elapsed, pid_t pid, size_t size, off_t offset)
{
	io->atime = timestamp;
	io->cnt ++;
	io->elapsed = elapsed;
	io->pid = pid;

	io->size = size;
	io->offset = offset;
	io->bytes += size;
}

static char * stat_filesystem_to_dictstr(stat_filesystem_t fs)
{
	char *buf = g_strdup_printf("{"
		"'stat':(%ld,%lu,%f,%d),"
	 	"'access':(%ld,%lu,%f,%d),"
		"'readlink':(%ld,%lu,%f,%d),"
		"'readdir':(%ld,%lu,%f,%d),"
		"'mknod':(%ld,%lu,%f,%d),"
		"'mkdir':(%ld,%lu,%f,%d),"
		"'symlink':(%ld,%lu,%f,%d),"
		"'unlink':(%ld,%lu,%f,%d),"
		"'rmdir':(%ld,%lu,%f,%d),"
		"'rename':(%ld,%lu,%f,%d),"
		"'link':(%ld,%lu,%f,%d),"
		"'chmod':(%ld,%lu,%f,%d),"
		"'chown':(%ld,%lu,%f,%d),"
		"'truncate':(%ld,%lu,%f,%d),"
		"'utime':(%ld,%lu,%f,%d),"
		"'open':(%ld,%lu,%f,%d),"
		"'statfs':(%ld,%lu,%f,%d),"
		"'release':(%ld,%lu,%f,%d),"
		"'fsync':(%ld,%lu,%f,%d)"
#ifdef HAVE_SETXATTR
		",'setxattr':(%ld,%lu,%f,%d),"
		"'getxattr':(%ld,%lu,%f,%d),"
		"'listxattr':(%ld,%lu,%f,%d),"
		"'removexattr':(%ld,%lu,%f,%d)"
#endif
		",'read':(%ld,%lu,%f,%d,%lu,%lu,%lu),"
		"'write':(%ld,%lu,%f,%d,%lu,%lu,%lu)"
		"}",
		fs->stat.atime, fs->stat.cnt, fs->stat.elapsed, fs->stat.pid,
		fs->access.atime, fs->access.cnt, fs->access.elapsed, fs->access.pid,
		fs->readlink.atime, fs->readlink.cnt, fs->readlink.elapsed, \
		fs->readlink.pid,
		fs->readdir.atime,	fs->readdir.cnt, fs->readdir.elapsed, \
		fs->readdir.pid,
		fs->mknod.atime, fs->mknod.cnt, fs->mknod.elapsed, fs->mknod.pid,
		fs->mkdir.atime, fs->mkdir.cnt, fs->mkdir.elapsed, fs->mkdir.pid,
		fs->symlink.atime,	fs->symlink.cnt, fs->symlink.elapsed, \
		fs->symlink.pid,
		fs->unlink.atime, fs->unlink.cnt, fs->unlink.elapsed, fs->unlink.pid,
		fs->rmdir.atime, fs->rmdir.cnt, fs->rmdir.elapsed, fs->rmdir.pid,
		fs->rename.atime, fs->rename.cnt, fs->rename.elapsed, fs->rename.pid,
		fs->link.atime, fs->link.cnt, fs->link.elapsed, fs->link.pid,
		fs->chmod.atime, fs->chmod.cnt, fs->chmod.elapsed, fs->chmod.pid,
		fs->chown.atime, fs->chown.cnt, fs->chown.elapsed, fs->chown.pid,
		fs->truncate.atime, fs->truncate.cnt, fs->truncate.elapsed, \
		fs->truncate.pid,
		fs->utime.atime, fs->utime.cnt, fs->utime.elapsed, fs->utime.pid,
		fs->open.atime, fs->open.cnt, fs->open.elapsed, fs->open.pid,
		fs->statfs.atime, fs->statfs.cnt, fs->statfs.elapsed, fs->statfs.pid,
		fs->release.atime, fs->release.cnt, fs->release.elapsed, \
		fs->release.pid,
		fs->fsync.atime, fs->fsync.cnt, fs->fsync.elapsed, fs->fsync.pid
#ifdef HAVE_SETXATTR
		,fs->setxattr.atime, fs->setxattr.cnt, fs->setxattr.elapsed, \
		fs->setxattrpid,
		fs->getxattr.atime, fs->getxattr.cnt, fs->getxattr.elapsed, \
		fs->setxattr.pid,
		fs->listxattr.atime, fs->listxattr.cnt, fs->listxattr.elapsed, \
		fs->listxattr.pid,
		fs->removexattr.atime, fs->removexattr.cnt, fs->removexattr.elapsed, \
		fs->removesetxattr.pid
#endif
		,fs->read.atime, fs->read.cnt, fs->read.elapsed, fs->read.pid, \
		fs->read.size, fs->read.offset, fs->read.bytes,
		fs->write.atime, fs->write.cnt, fs->write.elapsed, fs->write.pid, \
		fs->write.size, fs->write.offset, fs->write.bytes
		);
	return buf;
}

static char * stat_file_to_dictstr(stat_file_t f)
{
	char *buf = g_strdup_printf("{"
		"'stat':(%ld,%lu,%f,%d),"
	 	"'access':(%ld,%lu,%f,%d),"
		"'readlink':(%ld,%lu,%f,%d),"
		"'readdir':(%ld,%lu,%f,%d),"
		"'mknod':(%ld,%lu,%f,%d),"
		"'mkdir':(%ld,%lu,%f,%d),"
		"'symlink':(%ld,%lu,%f,%d),"
		"'unlink':(%ld,%lu,%f,%d),"
		"'rmdir':(%ld,%lu,%f,%d),"
		"'rename':(%ld,%lu,%f,%d),"
		"'link':(%ld,%lu,%f,%d),"
		"'chmod':(%ld,%lu,%f,%d),"
		"'chown':(%ld,%lu,%f,%d),"
		"'truncate':(%ld,%lu,%f,%d),"
		"'utime':(%ld,%lu,%f,%d),"
		"'open':(%ld,%lu,%f,%d),"
		"'statfs':(%ld,%lu,%f,%d),"
		"'release':(%ld,%lu,%f,%d),"
		"'fsync':(%ld,%lu,%f,%d)"
#ifdef HAVE_SETXATTR
		",'setxattr':(%ld,%lu,%f,%d),"
		"'getxattr':(%ld,%lu,%f,%d),"
		"'listxattr':(%ld,%lu,%f,%d),"
		"'removexattr':(%ld,%lu,%f,%d)"
#endif
		",'read':(%ld,%lu,%f,%d,%lu,%lu,%lu),"
		"'write':(%ld,%lu,%f,%d,%lu,%lu,%lu),"
		"'born':%ld,"
		"'dead':%ld"
		"}",
		f->stat.atime, f->stat.cnt, f->stat.elapsed, f->stat.pid,
		f->access.atime, f->access.cnt, f->access.elapsed, f->access.pid,
		f->readlink.atime, f->readlink.cnt, f->readlink.elapsed, \
		f->readlink.pid,
		f->readdir.atime,	f->readdir.cnt, f->readdir.elapsed, \
		f->readdir.pid,
		f->mknod.atime, f->mknod.cnt, f->mknod.elapsed, f->mknod.pid,
		f->mkdir.atime, f->mkdir.cnt, f->mkdir.elapsed, f->mkdir.pid,
		f->symlink.atime,	f->symlink.cnt, f->symlink.elapsed, \
		f->symlink.pid,
		f->unlink.atime, f->unlink.cnt, f->unlink.elapsed, f->unlink.pid,
		f->rmdir.atime, f->rmdir.cnt, f->rmdir.elapsed, f->rmdir.pid,
		f->rename.atime, f->rename.cnt, f->rename.elapsed, f->rename.pid,
		f->link.atime, f->link.cnt, f->link.elapsed, f->link.pid,
		f->chmod.atime, f->chmod.cnt, f->chmod.elapsed, f->chmod.pid,
		f->chown.atime, f->chown.cnt, f->chown.elapsed, f->chown.pid,
		f->truncate.atime, f->truncate.cnt, f->truncate.elapsed, \
		f->truncate.pid,
		f->utime.atime, f->utime.cnt, f->utime.elapsed, f->utime.pid,
		f->open.atime, f->open.cnt, f->open.elapsed, f->open.pid,
		f->statfs.atime, f->statfs.cnt, f->statfs.elapsed, f->statfs.pid,
		f->release.atime, f->release.cnt, f->release.elapsed, \
		f->release.pid,
		f->fsync.atime, f->fsync.cnt, f->fsync.elapsed, f->fsync.pid
#ifdef HAVE_SETXATTR
		,f->setxattr.atime, f->setxattr.cnt, f->setxattr.elapsed, \
		f->setxattrpid,
		f->getxattr.atime, f->getxattr.cnt, f->getxattr.elapsed, \
		f->setxattr.pid,
		f->listxattr.atime, f->listxattr.cnt, f->listxattr.elapsed, \
		f->listxattr.pid,
		f->removexattr.atime, f->removexattr.cnt, f->removexattr.elapsed, \
		f->removesetxattr.pid
#endif
		,f->read.atime, f->read.cnt, f->read.elapsed, f->read.pid, \
		f->read.size, f->read.offset, f->read.bytes,
		f->write.atime, f->write.cnt, f->write.elapsed, f->write.pid, \
		f->write.size, f->write.offset, f->write.bytes,
		f->born,
		f->dead
		);
	return buf;
}

static void stat_file_accumulate(stat_file_t f1, stat_file_t f2)
{	
	/* stat_sc */
	f1->stat.cnt		+= f2->stat.cnt;
	f1->access.cnt 		+= f2->access.cnt;
	f1->readlink.cnt 	+= f2->readlink.cnt;
	f1->readdir.cnt 	+= f2->readdir.cnt;
	f1->mknod.cnt 		+= f2->mknod.cnt;
	f1->mkdir.cnt 		+= f2->mkdir.cnt;
	f1->symlink.cnt 	+= f2->symlink.cnt;
	f1->unlink.cnt 		+= f2->unlink.cnt;
	f1->rmdir.cnt	 	+= f2->rmdir.cnt;
	f1->rename.cnt	 	+= f2->rename.cnt;
	f1->link.cnt 		+= f2->link.cnt;
	f1->chmod.cnt	 	+= f2->chmod.cnt;
	f1->chown.cnt 		+= f2->chown.cnt;
	f1->truncate.cnt 	+= f2->truncate.cnt;
	f1->utime.cnt 		+= f2->utime.cnt;
	f1->open.cnt 		+= f2->open.cnt;
	f1->statfs.cnt 		+= f2->statfs.cnt;
	f1->release.cnt 	+= f2->release.cnt;
	f1->fsync.cnt 		+= f2->fsync.cnt;
#ifdef HAVE_SETXATTR
	f1->setxattr.cnt 	+= f2->setxattr.cnt;
	f1->getxattr.cnt 	+= f2->getxattr.cnt;
	f1->listxattr.cnt 	+= f2->listxattr.cnt;
	f1->removexattr.cnt += f2->removexattr.cnt;
#endif
	
	f1->stat.elapsed 		+= f2->stat.elapsed;
	f1->access.elapsed 		+= f2->access.elapsed;
	f1->readlink.elapsed 	+= f2->readlink.elapsed;
	f1->readdir.elapsed 	+= f2->readdir.elapsed;
	f1->mknod.elapsed 		+= f2->mknod.elapsed;
	f1->mkdir.elapsed 		+= f2->mkdir.elapsed;
	f1->symlink.elapsed 	+= f2->symlink.elapsed;
	f1->unlink.elapsed 		+= f2->unlink.elapsed;
	f1->rmdir.elapsed	 	+= f2->rmdir.elapsed;
	f1->rename.elapsed	 	+= f2->rename.elapsed;
	f1->link.elapsed 		+= f2->link.elapsed;
	f1->chmod.elapsed	 	+= f2->chmod.elapsed;
	f1->chown.elapsed 		+= f2->chown.elapsed;
	f1->truncate.elapsed 	+= f2->truncate.elapsed;
	f1->utime.elapsed 		+= f2->utime.elapsed;
	f1->open.elapsed 		+= f2->open.elapsed;
	f1->statfs.elapsed 		+= f2->statfs.elapsed;
	f1->release.elapsed 	+= f2->release.elapsed;
	f1->fsync.elapsed 		+= f2->fsync.elapsed;
#ifdef HAVE_SETXATTR
	f1->setxattr.elapsed 	+= f2->setxattr.elapsed;
	f1->getxattr.elapsed 	+= f2->getxattr.elapsed;
	f1->listxattr.elapsed 	+= f2->listxattr.elapsed;
	f1->removexattr.elapsed	+= f2->removexattr.elapsed;
#endif
	
	f1->stat.atime		= f1->stat.atime < f2->stat.atime ? 
					 	  f2->stat.atime : f1->stat.atime;
	f1->access.atime 	= f1->access.atime < f2->access.atime ? 
					 	  f2->access.atime : f1->access.atime;
	f1->readlink.atime 	= f1->readlink.atime < f2->readlink.atime ? 
					 	  f2->readlink.atime : f1->readlink.atime;
	f1->mknod.atime 	= f1->mknod.atime < f2->mknod.atime ? 
					 	  f2->mknod.atime : f1->mknod.atime;
	f1->mkdir.atime		= f1->mkdir.atime < f2->mkdir.atime ? 
					 	  f2->mkdir.atime : f1->mkdir.atime;
	f1->symlink.atime	= f1->symlink.atime < f2->symlink.atime ? 
					 	  f2->symlink.atime : f1->symlink.atime;
	f1->unlink.atime	= f1->unlink.atime < f2->unlink.atime ? 
					 	  f2->unlink.atime : f1->unlink.atime;
	f1->rmdir.atime		= f1->rmdir.atime < f2->rmdir.atime ? 
					 	  f2->rmdir.atime : f1->rmdir.atime;
	f1->rename.atime	= f1->rename.atime < f2->rename.atime ? 
					 	  f2->rename.atime : f1->rename.atime;
	f1->link.atime		= f1->link.atime < f2->link.atime ? 
					 	  f2->link.atime : f1->link.atime;
	f1->chmod.atime		= f1->chmod.atime < f2->chmod.atime ? 
					 	  f2->chmod.atime : f1->chmod.atime;
	f1->chown.atime		= f1->chown.atime < f2->chown.atime ? 
					 	  f2->chown.atime : f1->chown.atime;
	f1->truncate.atime	= f1->truncate.atime < f2->truncate.atime ? 
					 	  f2->truncate.atime : f1->truncate.atime;
	f1->utime.atime		= f1->utime.atime < f2->utime.atime ? 
					 	  f2->utime.atime : f1->utime.atime;
	f1->open.atime		= f1->open.atime < f2->open.atime ? 
					 	  f2->open.atime : f1->open.atime;
	f1->read.atime		= f1->read.atime < f2->read.atime ? 
					 	  f2->read.atime : f1->read.atime;
	f1->write.atime		= f1->write.atime < f2->write.atime ? 
					 	  f2->write.atime : f1->write.atime;
	f1->statfs.atime	= f1->statfs.atime < f2->statfs.atime ? 
					 	  f2->statfs.atime : f1->statfs.atime;
	f1->release.atime	= f1->release.atime < f2->release.atime ? 
					 	  f2->release.atime : f1->release.atime;
	f1->fsync.atime		= f1->fsync.atime < f2->fsync.atime ? 
					 	  f2->fsync.atime : f1->fsync.atime;
#ifdef HAVE_SETXATTR
	f1->setxattr.atime	= f1->setxattr.atime < f2->setxattr.atime ? 
					 	  f2->setxattr.atime : f1->setxattr.atime;
	f1->getxattr.atime	= f1->getxattr.atime < f2->getxattr.atime ? 
					 	  f2->getxattr.atime : f1->getxattr.atime;
	f1->listxattr.atime	= f1->listxattr.atime < f2->listxattr.atime ? 
					 	  f2->listxattr.atime : f1->listxattr.atime;
	f1->removexattr.atime= f1->removexattr.atime < f2->removexattr.atime ? 
					 	  f2->removexattr.atime : f1->removexattr.atime;
#endif
	
	/* stat_io */
	f1->read.cnt		+= f2->read.cnt;
	f1->write.cnt		+= f2->read.cnt;
	f1->read.elapsed	+= f2->read.elapsed;
	f1->write.elapsed	+= f2->write.elapsed;
	f1->read.atime		= f1->read.atime < f2->read.atime ? 
					 	  f2->read.atime : f1->read.atime;
	f1->write.atime		= f1->write.atime < f2->write.atime ? 
					 	  f2->write.atime : f1->write.atime;
	f1->read.bytes	+= f2->read.bytes;
	f1->write.bytes	+= f2->write.bytes;

}
/*********** hashtable processing routines **********/
static int hash_table_init(hash_table_t hashtable)
{
	hashtable->table = g_hash_table_new_full(g_str_hash, g_str_equal,
		g_free, g_free);
	if (!hashtable->table) {
		fprintf(stderr, "failed to create hash table\n");
		return -1;
	}

	pthread_mutex_init(&hashtable->lock, NULL);
	return 0;
}

static void hash_table_destroy(hash_table_t hashtable)
{
	g_hash_table_destroy(hashtable->table);
	pthread_mutex_destroy(&hashtable->lock);
}

#ifdef FTRAC_TRACE_FILE
static stat_file_t files_retrieve(hash_table_t hashtable, const char *path)
{
	stat_file_t p = (stat_file_t) g_hash_table_lookup(hashtable->table, path);
	if (!p) {
		p = (stat_file_t) g_new0(struct stat_file, 1);
		pthread_mutex_lock(&hashtable->lock);
		g_hash_table_insert(hashtable->table, g_strdup(path), p);
		pthread_mutex_unlock(&hashtable->lock);
	}
    p->atime = time(NULL);
	return p;
}

static inline stat_file_t files_lookup(hash_table_t hashtable, const char *path)
{
    stat_file_t p = (stat_file_t) g_hash_table_lookup(hashtable->table, path);
    if (p)
        p->atime = time(NULL);
    return p;
}

static stat_file_t files_accumulate(hash_table_t hashtable, const char *path)
{
	GHashTableIter iter;
	gpointer key, value;
	int found = 0;

	stat_file_t file = (stat_file_t) g_new0(struct stat_file, 1);
	g_hash_table_iter_init(&iter, hashtable->table);
	while (g_hash_table_iter_next(&iter, &key, &value)) {
		if (g_str_has_prefix(key, path)) {
			stat_file_accumulate(file, (stat_file_t) value);
			found++;
		}
	}

	if (!found) {
		g_free(file);
		file = NULL;
	}
	return file;
}

struct nowandtimeout {
    time_t now;
    time_t timeout;
};

static inline int if_entry_old(void *key, void *data, void* user_data)
{
    stat_file_t file = (stat_file_t) data;
    struct nowandtimeout *pair = (struct nowandtimeout *) user_data;
    (void) key;
    return pair->now - file->atime > pair->timeout ? 1 : 0;
}

static void files_cleanup(hash_table_t hashtable, time_t timeout)
{
    struct nowandtimeout pair;
    pair.now = time(NULL);
    pair.timeout = timeout;

	pthread_mutex_lock(&hashtable->lock);
    g_hash_table_foreach_remove(hashtable->table, if_entry_old,
        (void *) &pair);
	pthread_mutex_unlock(&hashtable->lock);
}
#endif

/*********** polling server routines **********/
typedef struct polling_process_data {
	int sockfd;
} * polling_process_data_t;

static int polling_stat(int sockfd, char *buf);
static int polling_filesystem(int sockfd, char *buf);
static int polling_file(int sockfd, char *buf);
static int polling_directory(int sockfd, char *buf);
static int polling_unknown(int sockfd, char *buf);

static void * polling_process_func(void *data)
{
	polling_process_data_t p = (polling_process_data_t) data;
	int sockfd = p->sockfd;
	char recvbuf[SERVER_MAX_BUFF];
	int res, err;
	g_free(p);

	do {
		memset(recvbuf, 0, SERVER_MAX_BUFF);
		res = recv(sockfd, recvbuf, SERVER_MAX_BUFF, 0);
		if (res <= 0) {
			fprintf(stderr, "receive failed, %s\n", strerror(errno));
			err = 1;
			goto out;
		}
        switch(recvbuf[0]) {
            case POLL_STAT:
                err = polling_stat(sockfd, recvbuf);
                break;
			
			case POLL_FILESYSTEM:
				err = polling_filesystem(sockfd, recvbuf);
				break;
			
			case POLL_FILE:
				err = polling_file(sockfd, recvbuf);
				break;

			case POLL_DIRECTORY:
				err = polling_directory(sockfd, recvbuf);
				break;
		    
            case POLL_FINISH:
				err = 0;
                goto out;
			
            default:
				err = polling_unknown(sockfd, recvbuf);
				goto out;
        }

	} while (res >= 0);

  out:
    close(sockfd);
	err ? pthread_exit((void *) 1) : pthread_exit((void *) 0);	
}

static void * polling_process(void *data)
{
	int sockfd, len;
	struct sockaddr_un clntaddr;
	pthread_t thread_id;
    time_t last_cleanup = time(NULL);
	(void) data;
	
	len = sizeof(struct sockaddr_un);
	do {
		sockfd = accept(ftrac.sockfd, (struct sockaddr *) &clntaddr,
					(socklen_t *) &len);
		if (sockfd == -1) {
			fprintf(stderr, "socket accept failed, %s\n", strerror(errno));
		} else {
			polling_process_data_t thread_dat = 
				g_new(struct polling_process_data, 1);
			thread_dat->sockfd = sockfd;
			if (pthread_create(&thread_id, NULL, polling_process_func, 
				thread_dat) != 0) {
				fprintf(stderr, "create thread failed\n");
				exit(1);
			}
			pthread_detach(thread_id);
		}

#ifdef FTRAC_TRACE_FILE
        /* reuse polling server thread to do cleanup */
        if (time(NULL) - last_cleanup > ftrac.cache_timeout) {
            files_cleanup(&ftrac.files, ftrac.cache_timeout);
            last_cleanup = time(NULL);
        }
#endif
        
	} while(1);

	return NULL;
}

/* polling operations */
static int polling_stat(int sockfd, char *buf)
{
	int res, err = 0;
	char *sendbuf;
	time_t elapsed = time(NULL) - ftrac.start;
	(void) buf;

	/* send back as python dic string */
	sendbuf = g_strdup_printf("{"
		"'tracker':'ftrac',"
		"'username':'%s','mountpoint':'%s',"
		"'start':'%ld','elapsed':'%ld'"
		"}",
		ftrac.username, ftrac.mountpoint, ftrac.start, elapsed);
    
	res = send(sockfd, sendbuf, strlen(sendbuf), 0);
	if (res < 0) {
		fprintf(stderr, "send failed, %s\n", strerror(errno));
		err = 1;
	}
	g_free(sendbuf);
	return err;
}

static int polling_filesystem(int sockfd, char *buf)
{
	int res, err = 0;
	char *sendbuf;
	(void) buf;

	sendbuf = stat_filesystem_to_dictstr(&ftrac.fs);
	res = send(sockfd, sendbuf, strlen(sendbuf), 0);
	if (res < 0) {
        fprintf(stderr, "send failed, %s\n", strerror(errno));
        err = 1;
    }
    g_free(sendbuf);
    return err;
}

static int polling_file(int sockfd, char *buf)
{
	int res, err = 0;
	char *sendbuf;
    
	stat_file_t file = files_lookup(&ftrac.files, buf+2);
	if (file)
		sendbuf = stat_file_to_dictstr(file);
	else
		sendbuf = g_strdup("None");
	res = send(sockfd, sendbuf, strlen(sendbuf), 0);
	if (res < 0) {
        fprintf(stderr, "send failed, %s\n", strerror(errno));
        err = 1;
    }
    g_free(sendbuf);
    return err;
}

static int polling_directory(int sockfd, char *buf)
{
	int res, err = 0;
	char *sendbuf;
	
	stat_file_t file = files_accumulate(&ftrac.files, buf+2);
	if (file) {
		sendbuf = stat_file_to_dictstr(file);
		g_free(file);
	} else
		sendbuf = g_strdup("None");
	res = send(sockfd, sendbuf, strlen(sendbuf), 0);
	if (res < 0) {
        fprintf(stderr, "send failed, %s\n", strerror(errno));
        err = 1;
    }
    g_free(sendbuf);
    return err;
}

static int polling_unknown(int sockfd, char *buf)
{
	int res, err = 0;
	char *sendbuf = g_strdup_printf("%c%s", POLL_UNKNOWN, buf);

    res = send(sockfd, sendbuf, strlen(sendbuf), 0);
    if (res < 0) {
        fprintf(stderr, "send failed, %s\n", strerror(errno));
        err = 1;
    }
    g_free(sendbuf);
    return err;
}

/*********** FUSE interfaces **********/
#define get_elapsed(start, end) ((double) (end.tv_sec - start.tv_sec) + \
			  (((double) (end.tv_usec - start.tv_usec)) * 0.000001))

static int ftrac_getattr(const char *path, struct stat *stbuf)
{
	int res;
	struct timeval start, end;
	double elapsed;
	time_t stamp;
	pid_t pid = fuse_get_context()->pid;
	
	stamp = time(NULL);
	gettimeofday(&start, NULL);
	
	res = lstat(path, stbuf);
	
	gettimeofday(&end, NULL);
	elapsed = get_elapsed(start, end);

#ifdef FTRAC_TRACE_SYSTEM
	stat_sc_update(&ftrac.fs.stat, stamp, elapsed, pid);
#endif
	
	if (res == -1)
		return -errno;
	
	/* only record when file exists */
#ifdef FTRAC_TRACE_FILE
	stat_file_t file = files_retrieve(&ftrac.files, path);
	pthread_mutex_lock(&file->lock);
	stat_sc_update(&file->stat, stamp, elapsed, pid);
	pthread_mutex_unlock(&file->lock);
#endif

	return 0;
}

static int ftrac_access(const char *path, int mask)
{
	int res;
	struct timeval start, end;
	double elapsed;
	time_t stamp;
	pid_t pid = fuse_get_context()->pid;

	stamp = time(NULL);
	gettimeofday(&start, NULL);
    
	res = access(path, mask);
	
	gettimeofday(&end, NULL);
	elapsed = get_elapsed(start, end);

#ifdef FTRAC_TRACE_SYSTEM
	stat_sc_update(&ftrac.fs.access, stamp, elapsed, pid);
#endif

#ifdef FTRAC_TRACE_FILE
	stat_file_t file = files_retrieve(&ftrac.files, path);
	pthread_mutex_lock(&file->lock);
	stat_sc_update(&file->access, stamp, elapsed, pid);
	pthread_mutex_unlock(&file->lock);
#endif
	
	if (res == -1)
		return -errno;
	return 0;
}

static int ftrac_readlink(const char *path, char *buf, size_t size)
{
	int res;
	struct timeval start, end;
	double elapsed;
	time_t stamp;
	pid_t pid = fuse_get_context()->pid;

	stamp = time(NULL);
	gettimeofday(&start, NULL);
	
	res = readlink(path, buf, size - 1);
	
	gettimeofday(&end, NULL);
	elapsed = get_elapsed(start, end);

#ifdef FTRAC_TRACE_SYSTEM
	stat_sc_update(&ftrac.fs.readlink, stamp, elapsed, pid);
#endif

#ifdef FTRAC_TRACE_FILE
    stat_file_t file = files_retrieve(&ftrac.files, path);
	pthread_mutex_lock(&file->lock);
	stat_sc_update(&file->readlink, stamp, elapsed, pid);
	pthread_mutex_unlock(&file->lock);
#endif
	
	if (res == -1)
		return -errno;

	buf[res] = '\0';
	return 0;
}

static int ftrac_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
	off_t offset, struct fuse_file_info *fi)
{
	DIR *dp;
	struct dirent de, *dep;
	struct timeval start, end;
	double elapsed;
	time_t stamp;
	int res;
	pid_t pid = fuse_get_context()->pid;
	(void) offset;
	(void) fi;

	dp = opendir(path);
	if (dp == NULL)
		return -errno;
	
	do {
		stamp = time(NULL);
		gettimeofday(&start, NULL);
		res = readdir_r(dp, &de, &dep);
		gettimeofday(&end, NULL);
		elapsed = get_elapsed(start, end);
#ifdef FTRAC_TRACE_SYSTEM
		stat_sc_update(&ftrac.fs.readdir, stamp, elapsed, pid);
#endif

#ifdef FTRAC_TRACE_FILE
		stat_file_t file = files_retrieve(&ftrac.files, path);
		pthread_mutex_lock(&file->lock);
		stat_sc_update(&file->stat, stamp, elapsed, pid);
		pthread_mutex_unlock(&file->lock);
#endif
		if (res == 0) {
			struct stat st;
			memset(&st, 0, sizeof(st));
			st.st_ino = de.d_ino;
			st.st_mode = de.d_type << 12;
			if (filler(buf, de.d_name, &st, 0))
				break;
		}
	} while (dep);

	closedir(dp);
	return 0;
}

static int ftrac_mknod(const char *path, mode_t mode, dev_t rdev)
{
	int res;
	struct timeval start, end;
	double elapsed;
	time_t stamp;
	pid_t pid = fuse_get_context()->pid;

	stamp = time(NULL);
	gettimeofday(&start, NULL);

#ifdef linux
	res = mknod(path, mode, rdev);
#else
	/* On Linux this could just be 'mknod(path, mode, rdev)' but this
	   is more portable */
	if (S_ISREG(mode)) {
		res = open(path, O_CREAT | O_EXCL | O_WRONLY, mode);
		if (res >= 0)
			res = close(res);
	} else if (S_ISFIFO(mode))
		res = mkfifo(path, mode);
	else
		res = mknod(path, mode, rdev);
#endif

	gettimeofday(&end, NULL);
	elapsed = get_elapsed(start, end);

#ifdef FTRAC_TRACE_SYSTEM
	stat_sc_update(&ftrac.fs.mknod, stamp, elapsed, pid);
#endif

	if (res == -1)
		return -errno;

#ifdef FTRAC_TRACE_FILE
    stat_file_t file = files_retrieve(&ftrac.files, path);
	pthread_mutex_lock(&file->lock);
	stat_sc_update(&file->mknod, stamp, elapsed, pid);
	file->born = stamp;		/* file created */
	pthread_mutex_unlock(&file->lock);
#endif

	return 0;
}

static int ftrac_mkdir(const char *path, mode_t mode)
{
	int res;
	struct timeval start, end;
	double elapsed;
	time_t stamp;
	pid_t pid = fuse_get_context()->pid;
	
	stamp = time(NULL);
	gettimeofday(&start, NULL);
	
	res = mkdir(path, mode);
	
	gettimeofday(&end, NULL);
	elapsed = get_elapsed(start, end);

#ifdef FTRAC_TRACE_SYSTEM
	stat_sc_update(&ftrac.fs.mkdir, stamp, elapsed, pid);
#endif
	
	if (res == -1)
		return -errno;

#ifdef FTRAC_TRACE_FILE
    stat_file_t file = files_retrieve(&ftrac.files, path);
	pthread_mutex_lock(&file->lock);
	stat_sc_update(&file->mkdir, stamp, elapsed, pid);
	file->born = stamp;		/* directory created */
	pthread_mutex_unlock(&file->lock);
#endif

	return 0;
}

static int ftrac_unlink(const char *path)
{
	int res;
	struct timeval start, end;
	double elapsed;
	time_t stamp;
	pid_t pid = fuse_get_context()->pid;

	stamp = time(NULL);
	gettimeofday(&start, NULL);
	
	res = unlink(path);
	
	gettimeofday(&end, NULL);
	elapsed = get_elapsed(start, end);

#ifdef FTRAC_TRACE_SYSTEM
	stat_sc_update(&ftrac.fs.unlink, stamp, elapsed, pid);
#endif

	if (res == -1)
		return -errno;

#ifdef FTRAC_TRACE_FILE
	stat_file_t file = files_retrieve(&ftrac.files, path);
	pthread_mutex_lock(&file->lock);
	stat_sc_update(&file->unlink, stamp, elapsed, pid);
	file->dead = stamp;		/* file deleted */
	pthread_mutex_unlock(&file->lock);
#endif

	return 0;
}

static int ftrac_rmdir(const char *path)
{
	int res;
	struct timeval start, end;
	double elapsed;
	time_t stamp;
	pid_t pid = fuse_get_context()->pid;

	stamp = time(NULL);
	gettimeofday(&start, NULL);
	
	res = rmdir(path);
	
	gettimeofday(&end, NULL);
	elapsed = get_elapsed(start, end);

#ifdef FTRAC_TRACE_SYSTEM
	stat_sc_update(&ftrac.fs.rmdir, stamp, elapsed, pid);
#endif

	if (res == -1)
		return -errno;

#ifdef FTRAC_TRACE_FILE
	stat_file_t file = files_retrieve(&ftrac.files, path);
	pthread_mutex_lock(&file->lock);
	stat_sc_update(&file->rmdir, stamp, elapsed, pid);
	file->dead = stamp;
	pthread_mutex_unlock(&file->lock);
#endif

	return 0;
}

static int ftrac_symlink(const char *from, const char *to)
{
	int res;
	struct timeval start, end;
	double elapsed;
	time_t stamp;
	pid_t pid = fuse_get_context()->pid;

	stamp = time(NULL);
	gettimeofday(&start, NULL);
	
	res = symlink(from, to);
	
	gettimeofday(&end, NULL);
	elapsed = get_elapsed(start, end);

#ifdef FTRAC_TRACE_SYSTEM
	stat_sc_update(&ftrac.fs.symlink, stamp, elapsed, pid);
#endif
	
	if (res == -1)
		return -errno;

#ifdef FTRAC_TRACE_FILE
    /* TODO: which one should count? or both? */
	stat_file_t file = files_retrieve(&ftrac.files, to);
	pthread_mutex_lock(&file->lock);
	stat_sc_update(&file->symlink, stamp, elapsed, pid);
	file->born = stamp;
	pthread_mutex_unlock(&file->lock);
#endif

	return 0;
}

static int ftrac_rename(const char *from, const char *to)
{
	int res;
	struct timeval start, end;
	double elapsed;
	time_t stamp;
	pid_t pid = fuse_get_context()->pid;

	stamp = time(NULL);
	gettimeofday(&start, NULL);
    
	res = rename(from, to);
	
	gettimeofday(&end, NULL);
	elapsed = get_elapsed(start, end);

#ifdef FTRAC_TRACE_SYSTEM
	stat_sc_update(&ftrac.fs.rename, stamp, elapsed, pid);
#endif

	if (res == -1)
		return -errno;

#ifdef FTRAC_TRACE_FILE
    /* TODO: which one should count? migrate data? */
	stat_file_t filefrom = files_retrieve(&ftrac.files, from);
	pthread_mutex_lock(&filefrom->lock);
	stat_sc_update(&filefrom->rename, stamp, elapsed, pid);
	filefrom->dead = stamp;
	pthread_mutex_unlock(&filefrom->lock);
	
	stat_file_t fileto = files_retrieve(&ftrac.files, to);
	pthread_mutex_lock(&fileto->lock);
	stat_sc_update(&fileto->rename, stamp, elapsed, pid);
	fileto->dead = stamp;
	pthread_mutex_unlock(&fileto->lock);
#endif

	return 0;
}

static int ftrac_link(const char *from, const char *to)
{
	int res;
	struct timeval start, end;
	double elapsed;
	time_t stamp;
	pid_t pid = fuse_get_context()->pid;

	stamp = time(NULL);
	gettimeofday(&start, NULL);
    
	res = link(from, to);
	
	gettimeofday(&end, NULL);
	elapsed = get_elapsed(start, end);

#ifdef FTRAC_TRACE_SYSTEM
	stat_sc_update(&ftrac.fs.link, stamp, elapsed, pid);
#endif

	if (res == -1)
		return -errno;

#ifdef FTRAC_TRACE_FILE
	stat_file_t file = files_retrieve(&ftrac.files, from);
	pthread_mutex_lock(&file->lock);
	stat_sc_update(&file->link, stamp, elapsed, pid);
	file->born = stamp;
	pthread_mutex_unlock(&file->lock);
#endif
	
	return 0;
}

static int ftrac_chmod(const char *path, mode_t mode)
{
	int res;
	struct timeval start, end;
	double elapsed;
	time_t stamp;
	pid_t pid = fuse_get_context()->pid;

	stamp = time(NULL);
	gettimeofday(&start, NULL);
	
	res = chmod(path, mode);
	
	gettimeofday(&end, NULL);
	elapsed = get_elapsed(start, end);

#ifdef FTRAC_TRACE_SYSTEM
	stat_sc_update(&ftrac.fs.chmod, stamp, elapsed, pid);
#endif

#ifdef FTRAC_TRACE_FILE
	stat_file_t file = files_retrieve(&ftrac.files, path);
	pthread_mutex_lock(&file->lock);
	stat_sc_update(&file->chmod, stamp, elapsed, pid);
	pthread_mutex_unlock(&file->lock);
#endif
	
	if (res == -1)
		return -errno;
	return 0;
}

static int ftrac_chown(const char *path, uid_t uid, gid_t gid)
{
	int res;
	struct timeval start, end;
	double elapsed;
	time_t stamp;
	pid_t pid = fuse_get_context()->pid;

	stamp = time(NULL);
	gettimeofday(&start, NULL);
	
	res = lchown(path, uid, gid);
	
	gettimeofday(&end, NULL);
	elapsed = get_elapsed(start, end);

#ifdef FTRAC_TRACE_SYSTEM
	stat_sc_update(&ftrac.fs.chown, stamp, elapsed, pid);
#endif

#ifdef FTRAC_TRACE_FILE
	stat_file_t file = files_retrieve(&ftrac.files, path);
	pthread_mutex_lock(&file->lock);
	stat_sc_update(&file->chown, stamp, elapsed, pid);
	pthread_mutex_unlock(&file->lock);
#endif
	
	if (res == -1)
		return -errno;
	return 0;
}

static int ftrac_truncate(const char *path, off_t size)
{
	int res;
	struct timeval start, end;
	double elapsed;
	time_t stamp;
	pid_t pid = fuse_get_context()->pid;

	stamp = time(NULL);
	gettimeofday(&start, NULL);
    
	res = truncate(path, size);
	
	gettimeofday(&end, NULL);
	elapsed = get_elapsed(start, end);

#ifdef FTRAC_TRACE_SYSTEM
	stat_sc_update(&ftrac.fs.truncate, stamp, elapsed, pid);
#endif

#ifdef FTRAC_TRACE_FILE
	stat_file_t file = files_retrieve(&ftrac.files, path);
	pthread_mutex_lock(&file->lock);
	stat_sc_update(&file->truncate, stamp, elapsed, pid);
	pthread_mutex_unlock(&file->lock);
#endif
	
	if (res == -1)
		return -errno;
	return 0;
}

#if FUSE_VERSION >= 26
static int ftrac_utimens(const char *path, const struct timespec ts[2])
{
	struct timeval tv[2];
	int res;
	struct timeval start, end;
	double elapsed;
	time_t stamp;
	pid_t pid = fuse_get_context()->pid;
	
	stamp = time(NULL);
	gettimeofday(&start, NULL);
	
	tv[0].tv_sec = ts[0].tv_sec;
	tv[0].tv_usec = ts[0].tv_nsec / 1000;
	tv[1].tv_sec = ts[1].tv_sec;
	tv[1].tv_usec = ts[1].tv_nsec / 1000;
	res = utimes(path, tv);

	gettimeofday(&end, NULL);
	elapsed = get_elapsed(start, end);

#ifdef FTRAC_TRACE_SYSTEM
	stat_sc_update(&ftrac.fs.utime, stamp, elapsed, pid);
#endif

#ifdef FTRAC_TRACE_FILE
	stat_file_t file = files_retrieve(&ftrac.files, path);
	pthread_mutex_lock(&file->lock);
	stat_sc_update(&file->utime, stamp, elapsed, pid);
	pthread_mutex_unlock(&file->lock);
#endif
	
	if (res == -1)
		return -errno;
	return 0;
}
#else
static int ftrac_utime(const char *path, struct utimbuf *buf)
{
	int res;
	struct timeval start, end;
	double elapsed;
	time_t stamp;
	pid_t pid = fuse_get_context()->pid;
	
	stamp = time(NULL);
	gettimeofday(&start, NULL);
    
	res = utime(path, buf);
	
	gettimeofday(&end, NULL);
	elapsed = get_elapsed(start, end);

#ifdef FTRAC_TRACE_SYSTEM
	stat_sc_update(&ftrac.fs.utime, stamp, elapsed, pid);
#endif

#ifdef FTRAC_TRACE_FILE
	stat_file_t file = files_retrieve(&ftrac.files, path);
	pthread_mutex_lock(&file->lock);
	stat_sc_update(&file->utime, stamp, elapsed, pid);
	pthread_mutex_unlock(&file->lock);
#endif

	if (res == -1)
		return -errno;
	return 0;
}
#endif

static int ftrac_open(const char *path, struct fuse_file_info *fi)
{
	int res;
	struct timeval start, end;
	double elapsed;
	time_t stamp;
	pid_t pid = fuse_get_context()->pid;

	stamp = time(NULL);
	gettimeofday(&start, NULL);
    
	res = open(path, fi->flags);
	
	gettimeofday(&end, NULL);
	elapsed = get_elapsed(start, end);

#ifdef FTRAC_TRACE_SYSTEM
	stat_sc_update(&ftrac.fs.open, stamp, elapsed, pid);
#endif

#ifdef FTRAC_TRACE_FILE
	stat_file_t file = files_retrieve(&ftrac.files, path);
	pthread_mutex_lock(&file->lock);
	stat_sc_update(&file->open, stamp, elapsed, pid);
	pthread_mutex_unlock(&file->lock);
#endif
	
	if (res == -1)
		return -errno;

	close(res);
	return 0;
}

static int ftrac_read(const char *path, char *buf, size_t size, off_t offset,
		    struct fuse_file_info *fi)
{
	int fd, res;
	struct timeval start, end;
	double elapsed;
	time_t stamp;
	pid_t pid = fuse_get_context()->pid;
	(void) fi;
	
	/* TODO: remove open() and close() here */
	fd = open(path, O_RDONLY);
	if (fd == -1)
		return -errno;
	
	stamp = time(NULL);
	gettimeofday(&start, NULL);
	
	res = pread(fd, buf, size, offset);
	
	gettimeofday(&end, NULL);
	elapsed = get_elapsed(start, end);

#ifdef FTRAC_TRACE_SYSTEM
	stat_io_update(&ftrac.fs.read, stamp, elapsed, pid, size, offset);
#endif

#ifdef FTRAC_TRACE_FILE
	stat_file_t file = files_retrieve(&ftrac.files, path);
	pthread_mutex_lock(&file->lock);
	stat_io_update(&file->read, stamp, elapsed, pid, size, offset);
	pthread_mutex_unlock(&file->lock);
#endif

	if (res == -1)
		res = -errno;

	close(fd);
	return res;
}

static int ftrac_write(const char *path, const char *buf, size_t size,
		     off_t offset, struct fuse_file_info *fi)
{
	int fd, res;
	struct timeval start, end;
	double elapsed;
	time_t stamp;
	pid_t pid = fuse_get_context()->pid;
	(void) fi;

	fd = open(path, O_WRONLY);
	if (fd == -1)
		return -errno;

	stamp = time(NULL);
	gettimeofday(&start, NULL);
	
	res = pwrite(fd, buf, size, offset);
	
	gettimeofday(&end, NULL);
	elapsed = get_elapsed(start, end);

#ifdef FTRAC_TRACE_SYSTEM
	stat_io_update(&ftrac.fs.write, stamp, elapsed, pid, size, offset);
#endif
#ifdef FTRAC_TRACE_FILE
	stat_file_t file = files_retrieve(&ftrac.files, path);
	pthread_mutex_lock(&file->lock);
	stat_io_update(&file->write, stamp, elapsed, pid, size, offset);
	pthread_mutex_unlock(&file->lock);
#endif

	if (res == -1)
		res = -errno;

	close(fd);
	return res;
}

static int ftrac_statfs(const char *path, struct statvfs *stbuf)
{
	int res;
	struct timeval start, end;
	double elapsed;
	time_t stamp;
	pid_t pid = fuse_get_context()->pid;
	
	stamp = time(NULL);
	gettimeofday(&start, NULL);
    
	res = statvfs(path, stbuf);
	
	gettimeofday(&end, NULL);
	elapsed = get_elapsed(start, end);
	
#ifdef FTRAC_TRACE_SYSTEM
	stat_sc_update(&ftrac.fs.statfs, stamp, elapsed, pid);
#endif

#ifdef FTRAC_TRACE_FILE
	stat_file_t file = files_retrieve(&ftrac.files, path);
	pthread_mutex_lock(&file->lock);
	stat_sc_update(&file->statfs, stamp, elapsed, pid);
	pthread_mutex_unlock(&file->lock);
#endif

	if (res == -1)
		return -errno;
	return 0;
}

static int ftrac_release(const char *path, struct fuse_file_info *fi)
{
	/* Just a stub.	 This method is optional and can safely be left
	   unimplemented */

	struct timeval start, end;
	double elapsed;
	time_t stamp;
	pid_t pid = fuse_get_context()->pid;
	(void) path;
	(void) fi;
	
	stamp = time(NULL);
	gettimeofday(&start, NULL);
   	
	/* release */
	
	gettimeofday(&end, NULL);
	elapsed = get_elapsed(start, end);

#ifdef FTRAC_TRACE_SYSTEM
	stat_sc_update(&ftrac.fs.release, stamp, elapsed, pid);
#endif

#ifdef FTRAC_TRACE_FILE
	stat_file_t file = files_retrieve(&ftrac.files, path);
	pthread_mutex_lock(&file->lock);
	stat_sc_update(&file->release, stamp, elapsed, pid);
	pthread_mutex_unlock(&file->lock);
#endif

	return 0;
}

static int ftrac_fsync(const char *path, int isdatasync,
		     struct fuse_file_info *fi)
{
	/* Just a stub.	 This method is optional and can safely be left
	   unimplemented */

	struct timeval start, end;
	double elapsed;
	time_t stamp;
	pid_t pid = fuse_get_context()->pid;
	(void) path;
	(void) isdatasync;
	(void) fi;

	stamp = time(NULL);
	gettimeofday(&start, NULL);
    
	/* fsync */

	gettimeofday(&end, NULL);
	elapsed = get_elapsed(start, end);

#ifdef FTRAC_TRACE_SYSTEM
	stat_sc_update(&ftrac.fs.fsync, stamp, elapsed, pid);
#endif

#ifdef FTRAC_TRACE_FILE
	stat_file_t file = files_retrieve(&ftrac.files, path);
	pthread_mutex_lock(&file->lock);
	stat_sc_update(&file->fsync, stamp, elapsed, pid);
	pthread_mutex_unlock(&file->lock);
#endif

	return 0;
}

#ifdef HAVE_SETXATTR
/* xattr operations are optional and can safely be left unimplemented */
static int ftrac_setxattr(const char *path, const char *name, 
	const char *value, size_t size, int flags)
{
	int res;
	struct timeval start, end;
	double elapsed;
	time_t stamp;
	pid_t pid = fuse_get_context()->pid;
	
	stamp = time(NULL);
	gettimeofday(&start, NULL);
    
	res = lsetxattr(path, name, value, size, flags);
	
	gettimeofday(&end, NULL);
	elapsed = get_elapsed(start, end);

#ifdef FTRAC_TRACE_SYSTEM
	stat_sc_update(&ftrac.fs.setxattr, stamp, elapsed, pid);
#endif

#ifdef FTRAC_TRACE_FILE
	stat_file_t file = files_retrieve(&ftrac.files, path);
	pthread_mutex_lock(&file->lock);
	stat_sc_update(&file->setxattr, stamp, elapsed, pid);
	pthread_mutex_unlock(&file->lock);
#endif

	if (res == -1)
		return -errno;
	return 0;
}

static int ftrac_getxattr(const char *path, const char *name, char *value,
			size_t size)
{
	int res;
	struct timeval start, end;
	double elapsed;
	time_t stamp;
	pid_t pid = fuse_get_context()->pid;
    
	stamp = time(NULL);
	gettimeofday(&start, NULL);
    
	res = lgetxattr(path, name, value, size);
	
	gettimeofday(&end, NULL);
	elapsed = get_elapsed(start, end);

#ifdef FTRAC_TRACE_SYSTEM
	stat_sc_update(&ftrac.fs.getxattr, stamp, elapsed, pid);
#endif

#ifdef FTRAC_TRACE_FILE
	stat_file_t file = files_retrieve(&ftrac.files, path);
	pthread_mutex_lock(&file->lock);
	stat_sc_update(&file->getxattr, stamp, elapsed, pid);
	pthread_mutex_unlock(&file->lock);
#endif
	
	if (res == -1)
		return -errno;
	return res;
}

static int ftrac_listxattr(const char *path, char *list, size_t size)
{
	int res;
	struct timeval start, end;
	double elapsed;
	time_t stamp;
	pid_t pid = fuse_get_context()->pid;
    
	stamp = time(NULL);
	gettimeofday(&start, NULL);
    
	res = llistxattr(path, list, size);
	
	gettimeofday(&end, NULL);
	elapsed = get_elapsed(start, end);
	elapsed = (double) (end.tv_sec - start.tv_sec) +
			  (((double) (end.tv_usec - start.tv_usec)) * 0.000001);

#ifdef FTRAC_TRACE_SYSTEM
	stat_sc_update(&ftrac.fs.listxattr, stamp, elapsed, pid);
#endif

#ifdef FTRAC_TRACE_FILE
	stat_file_t file = files_retrieve(&ftrac.files, path);
	pthread_mutex_lock(&file->lock);
	stat_sc_update(&file->listxattr, stamp, elapsed, pid);
	pthread_mutex_unlock(&file->lock);
#endif
	
	if (res == -1)
		return -errno;
	return res;
}

static int ftrac_removexattr(const char *path, const char *name)
{
	int res;
	struct timeval start, end;
	double elapsed;
	time_t stamp;
	pid_t pid = fuse_get_context()->pid;
    
	stamp = time(NULL);
	gettimeofday(&start, NULL);
	
	res = lremovexattr(path, name);
	
	gettimeofday(&end, NULL);
	elapsed = get_elapsed(start, end);
	elapsed = (double) (end.tv_sec - start.tv_sec) +
			  (((double) (end.tv_usec - start.tv_usec)) * 0.000001);

#ifdef FTRAC_TRACE_SYSTEM
	stat_sc_update(&ftrac.fs.removexattr, stamp, elapsed, pid);
#endif

#ifdef FTRAC_TRACE_FILE
	stat_file_t file = files_retrieve(&ftrac.files, path);
	pthread_mutex_lock(&file->lock);
	stat_sc_update(&file->removexattr, stamp, elapsed, pid);
	pthread_mutex_unlock(&file->lock);
#endif
	
	if (res == -1)
		return -errno;
	return 0;
}
#endif /* HAVE_SETXATTR */

#if FUSE_VERSION >= 26
static void * ftrac_init(struct fuse_conn_info *conn)
#else
static void * ftrac_init(void)
#endif
{
	(void) conn;
	struct ftrac *ft = &ftrac;
	struct sockaddr_un servaddr;
	pthread_t thread_id;
	int len, err;

	/* initial frac structure */
	memset(&ft->fs, 0, sizeof(struct stat_filesystem));
#ifdef FTRAC_TRACE_FILE
	hash_table_init(&ft->files);
#endif
#ifdef FTRAC_TRACE_PROC
	hash_table_init(&ft->procs);
#endif
	
	/* startup polling server */
	ft->sockfd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (ft->sockfd == -1) {
		fprintf(stderr, "create socket failed, %s\n", strerror(errno));
		exit(1);
	}
	servaddr.sun_family = AF_UNIX;
	assert(strlen(ft->sockpath) <= 108);
	strcpy(servaddr.sun_path, ft->sockpath);
	unlink(servaddr.sun_path);
	len = strlen(servaddr.sun_path) + sizeof(servaddr.sun_family);
	err = bind(ft->sockfd, (struct sockaddr *) &servaddr, len);
	if (err == -1) {
		fprintf(stderr, "failed to bind, %s\n", strerror(errno));
		exit(1);
	}
	err = listen(ft->sockfd, SERVER_MAX_CONN);
	if (err == -1) {
		fprintf(stderr, "failed to listen with connection %d, %s\n",
				SERVER_MAX_CONN, strerror(errno));
		exit(1);
	}
	err = pthread_create(&thread_id, NULL, polling_process, NULL);
	if (err != 0) {
		fprintf(stderr, "failed to create thread, %s\n", strerror(err));
		exit(1);
	}
	pthread_detach(thread_id);
	ft->serv_thread = thread_id;
	ft->serv_started = 1;
    ft->start = time(NULL);
	
	/* create debug session */
	/*
	if (ftrac.foreground) {
		char *tmp = g_strdup_printf("%s/debug", ftrac.sessiondir);
		err = mkdir(tmp, S_IRUSR | S_IWUSR | S_IXUSR);
		if (err == -1 && errno != EEXIST) {
			fprintf(stderr, "failed to create directory %s, %s\n", 
					tmp, strerror(errno));
			exit(1);
		}
		g_free(tmp);
		
		tmp = g_strdup_printf("%s/debug/ftrac", ftrac.sessiondir); 
		unlink(tmp);
		symlink(ftrac.sockpath, tmp);
		g_free(tmp);

		tmp = g_strdup_printf("%s/debug/mountpoint", ftrac.sessiondir); 
		unlink(tmp);
		symlink(ftrac.mountpoint, tmp);
		g_free(tmp);
	}
	*/
		
	return NULL;
}

static void ftrac_destroy(void *data_)
{
	(void) data_;
 
#ifdef FTRAC_TRACE_FILE
    hash_table_destroy(&ftrac.files);
#endif
#ifdef FTRAC_TRACE_PROC
    hash_table_destroy(&ftrac.procs);
#endif
	pthread_cancel(ftrac.serv_thread);
	close(ftrac.sockfd);
	remove(ftrac.sockpath);
	remove(ftrac.sessiondir);
    g_free(ftrac.username);
	g_free(ftrac.sockpath);
	g_free(ftrac.sessiondir);
	g_free(ftrac.mountpoint);
}

/*********** FUSE setup routines **********/
static struct fuse_operations ftrac_oper = {
	.init			= ftrac_init,
	.destroy		= ftrac_destroy,
	.getattr		= ftrac_getattr,
	.access			= ftrac_access,
	.readlink		= ftrac_readlink,
	.readdir		= ftrac_readdir,
	.mknod			= ftrac_mknod,
	.mkdir			= ftrac_mkdir,
	.symlink		= ftrac_symlink,
	.unlink			= ftrac_unlink,
	.rmdir			= ftrac_rmdir,
	.rename			= ftrac_rename,
	.link			= ftrac_link,
	.chmod			= ftrac_chmod,
	.chown			= ftrac_chown,
	.truncate		= ftrac_truncate,
#if FUSE_VERSION >= 26
	.utimens		= ftrac_utimens,
#else
	.utime			= ftrac_utime,
#endif
	.open			= ftrac_open,
	.read			= ftrac_read,
	.write			= ftrac_write,
	.statfs			= ftrac_statfs,
	.release		= ftrac_release,
	.fsync			= ftrac_fsync,
#ifdef HAVE_SETXATTR
	.setxattr		= ftrac_setxattr,
	.getxattr		= ftrac_getxattr,
	.listxattr		= ftrac_listxattr,
	.removexattr	= ftrac_removexattr,
#endif
};

static void usage(const char *progname)
{
	fprintf(stderr,
"usage: %s mountpoint [options]\n"
"\n"
"general options:\n"
"    -o opt,[opt...]        mount options\n"
"    -h   --help            print help\n"
"    -V   --version         print version\n"
"    -o sessiondir=PATH     session directory\n"
"    -o cache_timeout=NUM   cache timeout (default: 3600 seconds)\n"
"\n", progname);
}

static int ftrac_fuse_main(struct fuse_args *args)
{
#if FUSE_VERSION >= 26
	return fuse_main(args->argc, args->argv, &ftrac_oper, NULL);
#else
	return fuse_main(args->argc, args->argv, &ftrac_oper);
#endif
}

#if FUSE_VERSION == 25
static int fuse_opt_insert_arg(struct fuse_args *args, int pos, const char *arg)
{
	assert(pos <= args->argc);
	if (fuse_opt_add_arg(args, arg) == -1)
		return -1;

	if (pos != args->argc - 1) {
		char *newarg = args->argv[args->argc - 1];
		memmove(&args->argv[pos + 1], &args->argv[pos],
			sizeof(char *) * (args->argc - pos - 1));
		args->argv[pos] = newarg;
	}
	return 0;
}
#endif

static int ftrac_opt_proc(void *data, const char *arg, int key, struct fuse_args *outargs)
{
	(void) data;
	(void) arg;
	
	switch(key) {
		case FUSE_OPT_KEY_OPT:
			return 1;
		
		case FUSE_OPT_KEY_NONOPT:
			return 1;

		case KEY_HELP:
			usage(outargs->argv[0]);
			fuse_opt_add_arg(outargs, "-ho");
			ftrac_fuse_main(outargs);
			exit(1);

		case KEY_VERSION:
			fprintf(stderr, "FUSETrack version %s\n", PACKAGE_VERSION);
#if FUSE_VERSION >= 25
			fuse_opt_add_arg(outargs, "--version");
			ftrac_fuse_main(outargs);
#endif
			exit(0);

		case KEY_FOREGROUND:
			ftrac.foreground = 1;
			return 1;

		default:
			fprintf(stderr, "internal error\n");
			abort();
	}
}

/* use self-defined string hash to release library dependency */
static unsigned int ftrac_string_hash(char *str)
{
	unsigned int hash = 0;
	int i = 0;
	
	while (str[i]) {
		hash += str[i] * (i + 1);
		i++;
	}

	return hash;
}

/*********** main entry **********/
int main(int argc, char * argv[])
{
	struct passwd *pwd;
	struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
	char *fsname, *tmp;
	int libver, res;

    ftrac.cache_timeout = 3600;
    
	if (!g_thread_supported())
        g_thread_init(NULL);
	
	ftrac.progname = argv[0];
	
	if (fuse_opt_parse(&args, &ftrac, ftrac_opts, ftrac_opt_proc) == -1)
		exit(1);
	
	/* get mountpoint */
	if (fuse_parse_cmdline(&args, &(ftrac.mountpoint), NULL, NULL) == -1)
		exit(1);
	if (!ftrac.mountpoint) {
		fprintf(stderr, "%s: missing mount point\n", ftrac.progname);
		fprintf(stderr, "see `%s -h' for usage\n", ftrac.progname);
		fuse_opt_free_args(&args);
		exit(1);
	}
	/* ftrac.mountpoint is already absolute path here */
	fuse_opt_insert_arg(&args, 1, ftrac.mountpoint);
    
	/* setup environments */
	ftrac.uid = getuid();
	pwd = getpwuid(ftrac.uid);
	if (!pwd) {
		fprintf(stderr, "failed to get username for uid %d\n", ftrac.uid);
		exit(1);
	}

    ftrac.username = g_strdup(pwd->pw_name);
	if (!ftrac.sessiondir)
		ftrac.sessiondir = g_strdup_printf("%s/ftrac-%s-%u", PATH_PREFIX, 
			ftrac.username, ftrac_string_hash(ftrac.mountpoint));
    res = mkdir(ftrac.sessiondir, S_IRUSR | S_IWUSR | S_IXUSR);
    if (res == -1 && errno != EEXIST) {
		fprintf(stderr, "failed to create directory %s, %s\n", 
				ftrac.sessiondir, strerror(errno));
		return -1;
    }
	
	ftrac.sockpath = g_strdup_printf("%s/ftrac.sock", ftrac.sessiondir);
	
	fsname = g_strdup("profiler");
#if FUSE_VERSION >= 27
	libver = fuse_version();
	assert(libver >= 27);
	tmp = g_strdup_printf("-osubtype=fuse,fsname=%s", fsname);
#else
	tmp = g_strdup_printf("-ofsname=fuse#%s", fsname);
#endif
	fuse_opt_insert_arg(&args, 1, tmp);
	g_free(tmp);
    
	res = ftrac_fuse_main(&args);
	
	fuse_opt_free_args(&args);
	g_free(fsname);
	
	return 0;
}
