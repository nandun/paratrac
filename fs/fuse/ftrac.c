/****************************************************************************
 * ParaTrac: Scalable Tracking Tools for Parallel Applications
 * Copyright (C) 2009,2010  Nan Dun <dunnan@yl.is.s.u-tokyo.ac.jp>
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
 * ftrac.c
 * FUSE tool for file system calls and processes tracking
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
#include <ctype.h>
#include <fcntl.h>
#include <dirent.h>
#include <errno.h>
#include <pwd.h>
#include <utime.h>
#include <assert.h>
#include <semaphore.h>
#include <signal.h>
#include <pthread.h>
#include <signal.h>
#include <wait.h>
#include <time.h>
#include <sys/time.h>
#include <sys/times.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/utsname.h>
#include <sys/ptrace.h>
#ifdef HAVE_SETXATTR
#include <sys/xattr.h>
#endif
#include <glib.h>

#ifdef linux
#include <linux/version.h>
#include <linux/genetlink.h>
#include <linux/taskstats.h>
#include <linux/cgroupstats.h>
#endif

#if FUSE_VERSION < 23
#error FUSE_VERSION < 23
#endif

#if GLIB_CHECK_VERSION(2, 16, 0)
#define GLIB_HASHTABLE_HAS_ITER
#endif

/* PTRACE_OPTIONS forgotten by some glibs */
#define PTRACE_O_TRACESYSGOOD   0x00000001
#define PTRACE_O_TRACEFORK      0x00000002
#define PTRACE_O_TRACEVFORK     0x00000004
#define PTRACE_O_TRACECLONE     0x00000008
#define PTRACE_O_TRACEEXEC      0x00000010
#define PTRACE_O_TRACEVFORKDONE 0x00000020
#define PTRACE_O_TRACEEXIT      0x00000040

#define PTRACE_O_MASK           0x0000007f

#define PTRACE_EVENT_FORK       1
#define PTRACE_EVENT_VFORK      2
#define PTRACE_EVENT_CLONE      3
#define PTRACE_EVENT_EXEC       4
#define PTRACE_EVENT_VFORK_DONE 5
#define PTRACE_EVENT_EXIT       6

/* 
 * system call number
 * like the definitions in <syscall.h>
 * use paratrac its own definition
 */

#define SYSC_FS_LSTAT          84
#define SYSC_FS_FSTAT          28
#define SYSC_FS_ACCESS         33
#define SYSC_FS_READLINK       85
#define	SYSC_FS_OPENDIR        205
#define SYSC_FS_READDIR        89
#define SYSC_FS_CLOSEDIR       206
#define SYSC_FS_MKNOD          14
#define SYSC_FS_MKDIR          39
#define SYSC_FS_UNLINK         10
#define SYSC_FS_RMDIR          40
#define SYSC_FS_SYMLINK        83
#define SYSC_FS_RENAME         38
#define SYSC_FS_LINK           9
#define SYSC_FS_CHMOD          15
#define SYSC_FS_CHOWN          16      /* lchown() */
#define SYSC_FS_TRUNCATE       92
#define SYSC_FS_UTIME          30
#define SYSC_FS_CREAT          8
#define SYSC_FS_OPEN           5
#define SYSC_FS_CLOSE          6
#define SYSC_FS_READ           3
#define SYSC_FS_WRITE          4
#define SYSC_FS_STATFS         99
#define SYSC_FS_FLUSH          203
#define SYSC_FS_FSYNC          118

#ifdef HAVE_SETXATTR        
#define SYSC_FS_SETXATTR       201
#define SYSC_FS_GETXATTR       202
#define SYSC_FS_LISTXATTR      203
#define SYSC_FS_REMOVEXATTR    204
#endif

#define	PATH_PREFIX     "/tmp"
#define SERVER_MAX_CONN 8
#define SERVER_MAX_BUFF 256
#define MAX_LINE        1024
#define MAX_RAND_MIN    0
#define MAX_RAND_MAX    100
#define MAX_BUF_LEN     1048576

#define FTRAC_SIGNAL_INIT SIGUSR1

#define FTRAC_CLOCK CLOCK_REALTIME

/* Tracing code control */
#define FTRAC_TRACE_ENABLED
#define FTRAC_TRACE_USING_PTRACE
#define FTRAC_TRACE_USING_TASKSTAT
#define FTRAC_TRACE_LOG_BUFSIZE	1048576

#define N_CHECK_INFINITE -256

#define CTRL_OK			0
#define CTRL_FINISH		1
#define CTRL_POLL_STAT	2
#define CTRL_FLUSH		3

/*
 * Generic macros for dealing with netlink sockets. Might be duplicated
 * elsewhere. It is recommended that commercial grade applications use
 * libnl or libnetlink and use the interfaces provided by the library
 */
#define GENLMSG_DATA(glh)	((void *)(NLMSG_DATA(glh) + GENL_HDRLEN))
#define GENLMSG_PAYLOAD(glh)	(NLMSG_PAYLOAD(glh, 0) - GENL_HDRLEN)
#define NLA_DATA(na)		((void *)((char*)(na) + NLA_HDRLEN))
#define NLA_PAYLOAD(len)	(len - NLA_HDRLEN)

#define MAX_MSG_SIZE    4096   /* small buffer may lead to msg lost */
#define MAX_CPUS        32

#define ERROR(format, args...) \
	do {fprintf(ftrac.fp_err, format, args);} while(0)
#define DEBUG(format, args...) \
	do {if (ftrac.debug) fprintf(ftrac.fp_dbg, format, args);} while(0)

/* See /proc/[pid]/stat in man proc(5) */
typedef struct proc_stat {
	int pid;
	char comm[64];
	char state;
	int ppid;
	int pgrp;
	int session;
	int tty_nr;
	int tpgid;
	unsigned long flags;
	unsigned long minflt;
	unsigned long cminflt;
	unsigned long majflt;
	unsigned long cmajflt;
	unsigned long utime;
	unsigned long stime;
	unsigned long cutime;
	unsigned long cstime;
	long priority;
	long nice;
	long num_threads; /* hard coded as 0 before kernel 2.6 */
	long itrealvalue;
	unsigned long starttime;
	unsigned long vsize;
	long rss;
	unsigned long rsslim;
	unsigned long startcode;
	unsigned long endcode;
	unsigned long startstack;
	unsigned long kstkesp;
	unsigned long kstkeip;
	unsigned long signal;
	unsigned long blocked;
	unsigned long sigignore;
	unsigned long sigcatch;
	unsigned long wchan;
	unsigned long nswap;
	unsigned long cnswap;
#ifdef linux
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2,1,22)
	int exit_signal;
#endif
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2,2,28)
	int processor;
#endif
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2,5,19)
	unsigned long rt_priority;
	unsigned long policy;
#endif
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2,6,18)
	unsigned long long delayacct_blkio_ticks;
#endif
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2,6,24)
	unsigned long guest_time;
	long cguest_time;
#endif
#endif
} * proc_stat_t;

/* record table */
typedef struct hash_table {
	GHashTable *table;
	pthread_mutex_t lock;
	pthread_t thread;
} * hash_table_t;

/* process table entry */
typedef struct proctab_entry {
	int pid;
	int ptime;
	int live;
	int last_sysc;
	double last_stamp;
	char *environ;
	char *cmdline;
	int n_envchk;
	int n_cmdchk;
	struct proc_stat stat;
	pthread_t trace_thread;
	int thread_started;
	int traced;
	pthread_mutex_t lock;
} * proctab_entry_t;

/* used to pass pid, especially in close() and closedir() */
typedef struct ftrac_file {
	unsigned long fd;
	pid_t pid;
} * ftrac_file_t;

/* process acounting netlink structure */
typedef struct netlink_channel {
	int sock;
	pthread_t thread;
	char cpumask[32];
} * netlink_channel_t;

struct ftrac {
	char *cmdline;
	char *progname;
	char *mountpoint;
	char *cwd;
	char *username;
	int foreground;
	uid_t uid;
	gid_t gid;
	pid_t pid;
	int iid;	/* instance id */
	struct timespec itime; /* instance start time */

	/* control server */
	char *sessiondir;
	char *sockpath;
	int sockfd;
	int serv_started;
	pthread_t serv_thread;

	/* general logging */
	char *logdir;
	char *logbuf;
	size_t logbufsize;
	struct hash_table proctab;
	struct hash_table filetab;
	
	FILE *fp_err;
	FILE *fp_dbg;
	FILE *fp_env;
	FILE *fp_sysc;
	FILE *fp_file;
	FILE *fp_proc;
	FILE *fp_taskstat;
	FILE *fp_ptrace;
	
	/* process logging */
	char **env_vars;
	int env_nvars;
	char *environ;
	int stop_envchk;
	int stop_cmdchk;

	/* process accounting */
	int ncpus;
	int familyid;
	int nlsock;
	size_t nlbufsize;
	netlink_channel_t nlarr;

    /* misc usage */
    pid_t notify_pid;
	int table_timeout;
	int debug;
	int debug_log;
};

static struct ftrac ftrac;

#define FTRAC_OPT(t, p, v) { t, offsetof(struct ftrac, p), v }

typedef struct msgtemplate {
	struct nlmsghdr n;
	struct genlmsghdr g;
	char buf[MAX_MSG_SIZE];
} * msgtemplate_t;

enum {
	KEY_HELP,
	KEY_VERSION,
	KEY_FOREGROUND,
};

static struct fuse_opt ftrac_opts[] = {
	/* General options */
	FTRAC_OPT("sessiondir=%s",      sessiondir, 0),
	FTRAC_OPT("logdir=%s",          logdir, 0),
	FTRAC_OPT("logbufsize=%lu",     logbufsize, 0),
	FTRAC_OPT("notify_pid=%d",      notify_pid, 0),
	FTRAC_OPT("ftrac_debug",        debug, 1),
	FTRAC_OPT("debug_log",          debug_log, 1),

	/* FUSE logging options */ 
	FTRAC_OPT("table_timeout=%s",   table_timeout, 0),
	
	/* Process logging options */ 
	FTRAC_OPT("environ=%s",         environ, 0),
	FTRAC_OPT("stop_envchk",        stop_envchk, 0),
	FTRAC_OPT("stop_cmdchk",        stop_cmdchk, 0),

	/* Taskstat logging options */
	FTRAC_OPT("nlsock=%d",          nlsock, 0),
	FTRAC_OPT("nlbufsize=%d",       nlbufsize, 0),
	
	/* Misc options */
	FUSE_OPT_KEY("-V",              KEY_VERSION),
	FUSE_OPT_KEY("--version",       KEY_VERSION),
	FUSE_OPT_KEY("-h",              KEY_HELP),
	FUSE_OPT_KEY("--help",          KEY_HELP),
	FUSE_OPT_KEY("debug",           KEY_FOREGROUND),
	FUSE_OPT_KEY("-d",              KEY_FOREGROUND),
	FUSE_OPT_KEY("-f",              KEY_FOREGROUND),
	FUSE_OPT_END
};

/*************************************************
 * Functions declarations
 ************************************************/
static inline void proc_logging_write(proctab_entry_t proc, int flag);
static void * ptrace_process(void *data);
static void ptrace_thread_spawn(pthread_t *thread, pid_t pid);

/*************************************************
 * Functions Implementations
 ************************************************/
static inline int ftrac_rand_range(int min, int max)
{
	return min + (rand() % (max - min + 1));
}

/* Use self-defined string hash to release library dependency */
static inline unsigned int ftrac_string_hash(const char *str)
{
	unsigned int hash = 0;
	int i = 0;
	
	while (str[i]) {
		hash += str[i] * (i + 1);
		i++;
	}

	return hash;
}

/* Copy file contents to a string byte by byte */
static inline char * get_file(const char *file, size_t *len)
{
	FILE *fp = fopen(file, "rb");
	if (fp == NULL) {
		fprintf(stderr, "open file %s failed\n", file);
		return NULL;
	}
	
	int c;
	size_t i = 0, buflen;
	char *buf;
	buf = g_malloc0(MAX_BUF_LEN);
	buflen = MAX_BUF_LEN;
	while ((c = getc(fp)) != EOF) {
		buf[i++] = c;
		if (i >= buflen) {
			buflen += MAX_BUF_LEN;
			buf = g_realloc(buf, buflen);
		}
	}
	fclose(fp);
	buf[i] = '\0';
	*len = i;

	return buf;
}

/*
 * /proc file system utilities 
 */

static char * procfs_get_cmdline(pid_t pid)
{
	size_t size, i;
	char path[PATH_MAX], line[MAX_LINE], *linep;
	FILE *fp;

	snprintf(path, PATH_MAX, "/proc/%d/cmdline", pid);
	fp = fopen(path, "rb");
	if (fp == NULL) {
		DEBUG("error: open file %s failed: %s\n", path, strerror(errno));
		return NULL;
	}
	
	size = fread(line, sizeof(char), MAX_LINE - 1, fp);

	/* replace nulls and newlines in string */
	for (i=0; i < size; i++) {
		if (line[i] == '\0' || line[i] == '\n')
			line[i] = ' ';
	}
	if (i < MAX_LINE)
		line[i-1] = '\0';
	
	linep = g_strdup(line);
	
	fclose(fp);
	return linep;
}

static char * procfs_get_environ(pid_t pid)
{
	char path[PATH_MAX];
	snprintf(path, PATH_MAX, "/proc/%d/environ", pid);
	if (access(path, R_OK) != 0)
		return NULL;
	
	/* get all environ once for convienience */
	char *environ;
	size_t length, i;
	
	environ = get_file(path, &length);
	if (environ == NULL) {
		fprintf(stderr, "failed to get environ of file %s\n", path);
		return NULL;
	}
	/* replacing special characters */
	for (i = 0; i < length; i++) {
		if (environ[i] == '\0' || environ[i] == '\n')
			environ[i] = ' ';
	}
	environ[length - 1] = '\0';
		
	/* extracting vars from environ */
	char **vars = ftrac.env_vars;
	int nvars = ftrac.env_nvars;
	if (vars) {
		int i, j, len;
		char *ptr, *new_environ;

		len = 0;
		nvars = g_strv_length(vars);
		new_environ = g_new0(char, MAX_LINE);
		for (i = 0; i < nvars; i++) {
			ptr = g_strrstr(environ, vars[i]);
			j = 0;
			while(ptr && ptr[j] != '\0' && ptr[j] != ' ') {
				new_environ[len] = ptr[j];
				len++;
				j++;
				if (len >= MAX_LINE)
					new_environ = g_realloc(new_environ, len + MAX_LINE);
			}
			if (j > 0) {	/* only output when env exist */
				new_environ[len] = ' ';
				len++;
				if (len >= MAX_LINE)
					new_environ = g_realloc(new_environ, len + MAX_LINE);
			}
		}
		new_environ[len] = '\0';
		g_free(environ);
		environ = new_environ;
	}

	return environ;
}

static void procfs_get_stat(pid_t pid, proc_stat_t stat)
{
	char path[PATH_MAX];
	snprintf(path, PATH_MAX, "/proc/%d/stat", pid);
	
	FILE *fp = fopen(path, "rb");
	if (fp == NULL) {
		DEBUG("error: open file %s failed: %s\n", path, strerror(errno));
		return;
	}

	/* reference: linux-source/fs/proc/array.c */
	int res = fscanf(fp, 
		"%d %s %c %d %d "
		"%d %d %d %lu "
		"%lu %lu %lu %lu "
		"%lu %lu %lu %lu "
		"%ld %ld %ld %ld "
		"%lu %lu %ld %lu "
		"%lu %lu %lu %lu "
		"%lu %lu %lu %lu "
		"%lu %lu %lu %lu "
		"%lu"
#ifdef linux
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2,1,22)
		" %d"
#endif
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2,2,28)
		" %d"
#endif
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2,5,19)
		" %lu %lu"
#endif
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2,6,18)
		" %llu"
#endif
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2,6,24)
		" %lu %ld"
#endif
#endif
		,
		&stat->pid, stat->comm, &stat->state, &stat->ppid, &stat->pgrp,	
		&stat->session, &stat->tty_nr, &stat->tpgid, &stat->flags,
		&stat->minflt, &stat->cminflt, &stat->majflt, &stat->cmajflt,
		&stat->utime, &stat->stime, &stat->cutime,	&stat->cstime,
		&stat->priority, &stat->nice,	&stat->num_threads, &stat->itrealvalue,
		&stat->starttime, &stat->vsize, &stat->rss, &stat->rsslim,
		&stat->startcode, &stat->endcode, &stat->startstack, &stat->kstkesp,
		&stat->kstkesp, &stat->kstkeip, &stat->signal,	&stat->blocked,
		&stat->sigignore, &stat->sigcatch, &stat->wchan, &stat->nswap,
		&stat->cnswap
#ifdef linux
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2,1,22)
		, &stat->exit_signal
#endif
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2,2,28)
		, &stat->processor
#endif
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2,5,19)
		, &stat->rt_priority, &stat->policy
#endif
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2,6,18)
		, &stat->delayacct_blkio_ticks
#endif
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2,6,24)
		, &stat->guest_time, &stat->cguest_time
#endif
#endif
		);

	if (res == EOF) {
		DEBUG("error: parsing file %s\n", path);
		memset(stat, 0x0, sizeof(struct proc_stat));
	}
	fclose(fp);
}

/* 
 * Process table routines
 */
static void proctab_entry_free(gpointer data)
{
	proctab_entry_t proc = (proctab_entry_t) data;
	if (proc->live && proc->traced) {
		DEBUG("proctab: cancel trace thread for process %d\n", proc->pid);
		pthread_cancel(proc->trace_thread);
	}
	proc_logging_write(proc, 1);
	g_free(proc->cmdline);
	g_free(proc->environ);
	pthread_mutex_destroy(&proc->lock);
	g_free(proc);
}

static int proctab_entry_check(void *key, void *value, void *data)
{
	(void) key;
	(void) data;

	proctab_entry_t proc = (proctab_entry_t) value;
	if (proc->live) {
		/* For those quicly exited process, ptrace might not have a chance
		to attach (proc->traced = 0). In addition, if taskstat is not
		available, we should find out these process (mark live=0), and
		leave it to next cleanup. */
//#ifndef FTRAC_TRACE_USING_TASKSTAT
		char path[PATH_MAX];
		snprintf(path, PATH_MAX, "/proc/%d", proc->pid);
		if (access(path, F_OK) != 0) {
			/* process is actually dead */
			pthread_mutex_lock(&proc->lock);
			proc->live = 0;
			pthread_mutex_unlock(&proc->lock);
			DEBUG("proctab: process %d is actually dead\n", proc->pid);
		}
//#endif /* FTRAC_TRACE_USING_TASKSTAT */
		return 0;	/* FALSE */
	} else {
		DEBUG("proctab: process %d will be removed from table\n", proc->pid);
		return 1;	/* TRUE */
	}
	
	/* shoud not get here */
	return 0;
}

static void * proctab_cleanup(void *data)
{
	(void) data;
	int secs = ftrac.table_timeout;
	
	while (1) {
		sleep(secs);
		/* cleanup exited processes */
		DEBUG("protab: cleanup process table after %d seconds\n", secs);
		pthread_mutex_lock(&ftrac.proctab.lock);
		g_hash_table_foreach_remove(ftrac.proctab.table, 
			proctab_entry_check, NULL);
		pthread_mutex_unlock(&ftrac.proctab.lock);
	}
	pthread_exit((void *) 0);
}

static int proctab_init(void)
{
	ftrac.proctab.table = g_hash_table_new_full(g_int_hash, g_int_equal,
		g_free, proctab_entry_free);
	if (!ftrac.proctab.table) {
		fprintf(stderr, "failed to create hash table\n");
		return -1;
	}

	pthread_mutex_init(&ftrac.proctab.lock, NULL);
	
	int err = pthread_create(&ftrac.proctab.thread, NULL, 
		proctab_cleanup, NULL);
	if (err) {
		DEBUG("error: failed to create thread: %s\n", strerror(err));
		return -1;
	}
	return 0;
}

static void proctab_destroy(void)
{
	pthread_cancel(ftrac.proctab.thread);
	pthread_mutex_destroy(&ftrac.proctab.lock);
	g_hash_table_destroy(ftrac.proctab.table);
}

static inline proctab_entry_t proctab_lookup(pid_t pid)
{
	return (proctab_entry_t) g_hash_table_lookup(ftrac.proctab.table, &pid);
}

static inline void proctab_insert(pid_t pid, proctab_entry_t entry)
{
	pid_t *pidp = g_new(pid_t, 1);
	*pidp = pid;
	pthread_mutex_lock(&ftrac.proctab.lock);
	g_hash_table_insert(ftrac.proctab.table, pidp, entry);
	pthread_mutex_unlock(&ftrac.proctab.lock);
}

/*********** file table processing routines **********/
static int filetab_init(void)
{
	ftrac.filetab.table = g_hash_table_new_full(g_str_hash, g_str_equal,
		g_free, NULL);
	if (!ftrac.filetab.table) {
		fprintf(stderr, "failed to create hash table\n");
		return -1;
	}

	pthread_mutex_init(&ftrac.filetab.lock, NULL);
	return 0;
}

static void filetab_destroy(void)
{
	g_hash_table_destroy(ftrac.filetab.table);
	pthread_mutex_destroy(&ftrac.filetab.lock);
}

static unsigned int filetab_lookup_insert(const char *path)
{
	gpointer p = g_hash_table_lookup(ftrac.filetab.table, path);
	if (!p) {
		pthread_mutex_lock(&ftrac.filetab.lock);
		guint size = g_hash_table_size(ftrac.filetab.table) + 1;
		g_hash_table_insert(ftrac.filetab.table, g_strdup(path), 
			GUINT_TO_POINTER(size));
		pthread_mutex_unlock(&ftrac.filetab.lock);
		fprintf(ftrac.fp_file, "%u:%s\n", size, path);
		return size;
	}
	return GPOINTER_TO_UINT(p);
}

/*********** system call logging routines **********/
static inline double get_timespec(const struct timespec *time) 
{
	return ((double) time->tv_sec + \
		((double) time->tv_nsec) * 0.000000001);
}

static inline double get_elapsed(const struct timespec *start, 
	const struct timespec *end)
{
	return ((double) (end->tv_sec - start->tv_sec) + \
		(((double) (end->tv_nsec - start->tv_nsec)) * 0.000000001));
}

static inline void sysc_logging(int sysc, struct timespec *start, 
	struct timespec *end, int pid, int res, const char *path)
{
	double stamp = get_timespec(start);
	double elapsed = get_elapsed(start, end);
	unsigned long fid = filetab_lookup_insert(path);
	
	/* logging
	   csv format: stamp,pid,sysc,fid,res,elapsed */
	/* TODO: hash pid and ppid ptime */
	fprintf(ftrac.fp_sysc, "%.9f,%d,%d,%lu,%d,%.9f,0,0\n",
		stamp, pid, sysc, fid, res, elapsed);
}

static inline void sysc_logging_openclose(int sysc, struct timespec *start, 
	struct timespec *end, int pid, int res, const char *path)
{
	double stamp = get_timespec(start);
	double elapsed = get_elapsed(start, end);
	unsigned long fid = filetab_lookup_insert(path);
	
	/* logging file size */
	struct stat stbuf;
	off_t fsize = stat(path, &stbuf) == 0 ? stbuf.st_size : -1;

	/* TODO: use hash pid and ppid */
	fprintf(ftrac.fp_sysc, "%.9f,%d,%d,%lu,%d,%.9f,%lu,0\n", 
		stamp, pid, sysc, fid, res, elapsed, fsize);
}

static inline void sysc_logging_io(int sysc, struct timespec *start, 
	struct timespec *end, int pid, int res,
	const char *path, size_t size, off_t offset)
{
	double stamp = get_timespec(start);
	double elapsed = get_elapsed(start, end);
	unsigned long fid = filetab_lookup_insert(path);
	
	/* TODO: use hash pid and ppid */
	fprintf(ftrac.fp_sysc, "%.9f,%d,%d,%lu,%d,%.9f,%lu,%lu\n", 
		stamp, pid, sysc, fid, res, elapsed, size, offset);
}

static inline void sysc_logging_link(int sysc, 
	struct timespec *start, struct timespec *end, int pid, int res,
	const char *from, const char *to)
{
	double stamp = get_timespec(start);
	double elapsed = get_elapsed(start, end);
	unsigned long fid_from = filetab_lookup_insert(from);
	unsigned long fid_to = filetab_lookup_insert(to);
	
	fprintf(ftrac.fp_sysc, "%.9f,%d,%d,%lu,%d,%.9f,%lu,0\n", 
		stamp, pid, sysc, fid_from, res, elapsed, fid_to);
}

/*
 * Process logging routines 
 */

static inline void proc_logging_write(proctab_entry_t proc, int flag)
{
	fprintf(ftrac.fp_proc,
		"%.9f,%d,%d,%d,"
		"%lu,%lu,%lu,"
		"%s,%s\n",
		proc->last_stamp, proc->last_sysc, proc->pid, flag, 
		proc->stat.utime, proc->stat.stime, proc->stat.starttime,
		proc->cmdline,proc->environ);
}
	
static void proc_logging(int sysc, struct timespec *stamp, pid_t pid)
{
	/* return point of recursive logging */
	if (pid == 0)
		return;
	
	proctab_entry_t proc = proctab_lookup(pid);
	if (!proc) {
		proc = g_new0(struct proctab_entry, 1);
		proc->pid = pid;
		proc->live = 1;
		proc->last_sysc = sysc;
		proc->last_stamp = get_timespec(stamp);
		/* TODO: proc->ptime = ?*/
		
		/* not all /proc/[pid]/environ are accessible */
		proc->environ = procfs_get_environ(pid);
		if (proc->environ == NULL)
			/* if failed, we won't check in future */
			proc->n_envchk = -1;
		else
			proc->n_envchk = 1;
		proc->cmdline = procfs_get_cmdline(pid);
		proc->n_cmdchk = 1;
		procfs_get_stat(pid, &proc->stat);
		proc->traced = 0;
		pthread_mutex_init(&proc->lock, NULL);
		proctab_insert(pid, proc);
		
		/* keep a initial record */
		proc_logging_write(proc, 0);
		ptrace_thread_spawn(&proc->trace_thread, pid);
		proc_logging(sysc, stamp, proc->stat.ppid);
	} else if (!proc->traced) {
		/* check cmdline, n_cmdchk may be update in 
		ptrace_process() */
		if (proc->n_cmdchk > 0) {
			char *cmdline = procfs_get_cmdline(pid);
			if (proc->cmdline && cmdline) {
				char *tmp; 
				switch (g_strcmp0(proc->cmdline, cmdline)) {
					case 0:	/* environ does not change */
						g_free(cmdline);
						break;
					default:
						tmp = proc->cmdline;
						pthread_mutex_lock(&proc->lock);
						proc->cmdline = cmdline;
						if (ftrac.stop_cmdchk)
							proc->n_cmdchk = -1;
						pthread_mutex_unlock(&proc->lock);
						g_free(tmp);
				}
			}
		}
		
		/* check environ */
		if (proc->n_envchk > 0) {
			char *environ = procfs_get_environ(pid);
			if (proc->environ && environ) {
				char *tmp;
				switch (g_strcmp0(proc->environ, environ)) {
					case 0:
						g_free(environ);
						break;
					default:
						tmp = proc->environ;
						pthread_mutex_lock(&proc->lock);
						proc->environ = environ;
						if (ftrac.stop_envchk)
							proc->n_envchk = -1;
						pthread_mutex_unlock(&proc->lock);
						g_free(tmp);
				}
			}
		}
		
		/* update other fields */
		proc->last_sysc = sysc;
		proc->last_stamp = get_timespec(stamp);
		procfs_get_stat(pid, &proc->stat);
	}
}

/*
 * Initial and Destory routines
 */
static void logging_init(void)
{
	int res;
	char file[PATH_MAX];
	struct ftrac *ft = &ftrac;
	
	memset(file, 0, PATH_MAX);
	snprintf(file, PATH_MAX, "%s/error.log", ft->logdir);
	ft->fp_err = fopen(file, "wb");
	if (ft->fp_err == NULL) {
		fprintf(stderr, "open file %s failed\n", file);
		exit(1);
	}
	ft->fp_dbg = ftrac.debug_log ? ft->fp_err : stderr;

	memset(file, 0, PATH_MAX);
	snprintf(file, PATH_MAX, "%s/env.log", ft->logdir);
	ft->fp_env = fopen(file, "wb");
	if (ft->fp_env == NULL) {
		ERROR("error: open file %s: %s", file, strerror(errno));
		exit(1);
	}

	/* set large stream buffer for system call log */
	memset(file, 0, PATH_MAX);
	snprintf(file, PATH_MAX, "%s/sysc.log", ft->logdir);
	ft->fp_sysc = fopen(file, "wb");
	if (ft->fp_sysc == NULL) {
		fprintf(stderr, "open file %s failed\n", file);
		exit(1);
	}
	
	ft->logbuf = malloc(ft->logbufsize);
	if (ft->logbuf == NULL) {
		fprintf(stderr, "malloc failed\n");
		exit(1);
	}
	res = setvbuf(ft->fp_sysc, ft->logbuf, _IOFBF, ft->logbufsize);
	if (res != 0) {
		fprintf(stderr, "setvbuf failed\n");
		exit(1);
	}

	memset(file, 0, PATH_MAX);
	snprintf(file, PATH_MAX, "%s/proc.log", ftrac.logdir);
	ft->fp_proc = fopen(file, "wb");
	if (ft->fp_proc == NULL) {
		fprintf(stderr, "open file %s failed\n", file);
		exit(1);
	}
	
	memset(file, 0, PATH_MAX);
	snprintf(file, PATH_MAX, "%s/ptrace.log", ftrac.logdir);
	ft->fp_ptrace = fopen(file, "wb");
	if (ft->fp_ptrace == NULL) {
		fprintf(stderr, "open file %s failed\n", file);
		exit(1);
	}
	
	memset(file, 0, PATH_MAX);
	snprintf(file, PATH_MAX, "%s/taskstat.log", ftrac.logdir);
	ft->fp_taskstat = fopen(file, "wb");
	if (ft->fp_taskstat == NULL) {
		fprintf(stderr, "open file %s failed\n", file);
		exit(1);
	}
	
	memset(file, 0, PATH_MAX);
	snprintf(file, PATH_MAX, "%s/file.log", ft->logdir);
	ft->fp_file = fopen(file, "wb");
	if (ft->fp_file == NULL) {
		fprintf(stderr, "open file %s failed\n", file);
		exit(1);
	}
	
	/* process table and file table */
	res = proctab_init();
	if (res != 0) {
		fprintf(stderr, "initial process table failed\n");
		exit(1);
	}

	res = filetab_init();
	if (res != 0) {
		fprintf(stderr, "initial file table failed\n");
		exit(1);
	}
	
	if (ft->environ) {
		ft->env_vars = g_strsplit(ft->environ, ":", 0);
		ft->env_nvars = g_strv_length(ft->env_vars);
	}
	

	/* start logging */
	struct utsname platform;
	if (uname(&platform) == -1)
		ERROR("warning: failed to get platform info: %s\n", 
			strerror(errno));
	
	ft->cmdline = procfs_get_cmdline(ft->pid);
	fprintf(ft->fp_env,
		"version:%s\n"
		"platform:%s %s %s\n"
		"hostname:%s\n"
		"cmdline:%s\n"
		"mountpoint:%s\n"
		"user:%s\n"
		"uid:%d\n"
		"pid:%d\n"
		"iid:%d\n"
		"start:%.9f\n"
		,
		PACKAGE_VERSION,
		platform.sysname, platform.release, platform.version,
		platform.nodename,
		ft->cmdline,
		ft->mountpoint,
		ft->username,
		ft->uid,
		ft->pid,
		ft->iid,
		get_timespec(&ft->itime)
		);
	fflush(ft->fp_env);
}

static void logging_destroy(struct ftrac *ft)
{
	/* finish logging */
	struct timespec end;
	clock_gettime(FTRAC_CLOCK, &end);
	fprintf(ft->fp_env, "end:%.9f", ((double) end.tv_sec + \
		((double) end.tv_nsec) * 0.000000001));

	/* finalize */
	proctab_destroy();
	filetab_destroy();
	fclose(ft->fp_taskstat);
	fclose(ft->fp_ptrace);
	fclose(ft->fp_proc);
	fclose(ft->fp_file);
	fclose(ft->fp_sysc);
	fclose(ft->fp_env);
	fclose(ft->fp_err);
	if (ft->environ)
		g_strfreev(ft->env_vars);
	free(ft->logbuf);
	g_free(ft->logdir);
}

/*********** control server routines **********/
typedef struct ctrl_process_data {
	int sockfd;
} * ctrl_process_data_t;

static void * ctrl_process(void *data);
static int ctrl_flush(int sockfd);

static void ctrl_server_init(void)
{
	struct sockaddr_un servaddr;
	pthread_t thread_id;
	int len, err;
	struct ftrac *ft = &ftrac;

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
	err = pthread_create(&thread_id, NULL, ctrl_process, NULL);
	if (err != 0) {
		fprintf(stderr, "failed to create thread, %s\n", strerror(err));
		exit(1);
	}
	pthread_detach(thread_id);
	ft->serv_thread = thread_id;
	ft->serv_started = 1;
}

static void ctrl_server_destroy(struct ftrac *ft)
{
	pthread_cancel(ft->serv_thread);
	close(ft->sockfd);
	remove(ft->sockpath);
	g_free(ftrac.sockpath);
}

static void * ctrl_process_func(void *data)
{
	ctrl_process_data_t p = (ctrl_process_data_t) data;
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
		switch(atoi(recvbuf)) {
			case CTRL_FLUSH:
				fprintf(stderr, "in ctrl flush, %s\n", recvbuf);
				err = ctrl_flush(sockfd);
				break;
			case CTRL_FINISH:
				err = 0;
				goto out;
			default:
				err = 1;
				goto out;
		}
	} while (res >= 0);

  out:
    close(sockfd);
	err ? pthread_exit((void *) 1) : pthread_exit((void *) 0);	
}

static void * ctrl_process(void *data)
{
	int sockfd, len;
	struct sockaddr_un clntaddr;
	pthread_t thread_id;
	(void) data;
	
	len = sizeof(struct sockaddr_un);
	do {
		sockfd = accept(ftrac.sockfd, (struct sockaddr *) &clntaddr,
					(socklen_t *) &len);
		if (sockfd == -1) {
			fprintf(stderr, "socket accept failed, %s\n", strerror(errno));
		} else {
			ctrl_process_data_t thread_dat = 
				g_new(struct ctrl_process_data, 1);
			thread_dat->sockfd = sockfd;
			if (pthread_create(&thread_id, NULL, ctrl_process_func, 
				thread_dat) != 0) {
				fprintf(stderr, "create thread failed\n");
				exit(1);
			}
			pthread_detach(thread_id);
		}
	} while(1);

	return NULL;
}

/* control operations */
static int ctrl_flush(int sockfd)
{
	int res, err = 0;
	char *sendbuf = g_strdup_printf("%d", CTRL_OK);

	fflush(ftrac.fp_sysc);
	fflush(ftrac.fp_taskstat);
	fflush(ftrac.fp_file);

    res = send(sockfd, sendbuf, strlen(sendbuf), 0);
    if (res <= 0) {
        fprintf(stderr, "send failed, %s\n", strerror(errno));
        err = 1;
    }
    g_free(sendbuf);
    return err;
}

#ifdef FTRAC_TRACE_USING_PTRACE
/*************************************************
 * Ptrace logging routines 
 ************************************************/
static void ptrace_logging(pid_t pid)
{
	char *env, *cmd;
	struct proc_stat st;
	
	env = procfs_get_environ(pid);
	cmd = procfs_get_cmdline(pid);
	procfs_get_stat(pid, &st);
	
	fprintf(ftrac.fp_ptrace,
		"%d,%lu,%lu,%lu,%s,%s\n",
		pid, st.utime, st.stime, st.starttime, cmd, env);

	g_free(env);
	g_free(cmd);
}

/* ptrace threading function */
void ptrace_process_cleanup(void *data)
{	
	proctab_entry_t proc = (proctab_entry_t) data;

	/* make sure we detach all traced processes */
	ptrace(PTRACE_DETACH, proc->pid, NULL, NULL);
	pthread_mutex_lock(&proc->lock);
	proc->traced = 0;
	pthread_mutex_unlock(&proc->lock);
}

static void * ptrace_process(void *data)
{
	int status;
	pid_t *pidp = (pid_t *) data;
	pid_t pid = *pidp;
	g_free(data);
			
	if (ptrace(PTRACE_ATTACH, pid, NULL, NULL)) {
		DEBUG("faild to attach process %d: %s\n", pid, strerror(errno));
		pthread_exit((void *) 1);
	}
	
	DEBUG("waiting process %d to be attached\n", pid);
	waitpid(pid, &status, 0);

	if (ptrace(PTRACE_SETOPTIONS, pid, NULL, PTRACE_O_TRACEEXIT)) {
		DEBUG("failed to set TRACE_EXIT on process %d: %s\n", 
			pid, strerror(errno));
		pthread_exit((void *) 1);
	}

	proctab_entry_t proc = proctab_lookup(pid);
	if (proc) {
		/* since we successfully attached to process,
		stop checking by fuse immediately */
		pthread_mutex_lock(&proc->lock);
		proc->traced = 1;
		pthread_mutex_unlock(&proc->lock);
	} else {
		DEBUG("error: process %d not in process table\n", pid);
	}
	
	/* pthread_cleanup_push() must have a matching pthead_cleanup_pop(),
	and they must by at the same lexical level within the code */
	pthread_cleanup_push(ptrace_process_cleanup, proc);
	
	if (ptrace(PTRACE_CONT, pid, NULL, NULL)) {
		DEBUG("failed to PTRACE_CONT process %d: %s\n",
			pid, strerror(errno));
		pthread_exit((void *) 1);
	}
	
	while (1) {
		DEBUG("ptrace: waiting process %d on EVEN_EXIT\n", pid);
		waitpid(pid, &status, 0);	/* cancellation point */
		
		/* check if incoming signal is SIGTRAP and exit_flag is set */
		if ((WSTOPSIG(status) == SIGTRAP) && 
			(status & (PTRACE_EVENT_EXIT << 8)))
			break;

		/* pass	original signal to child */
		if (ptrace(PTRACE_CONT, pid, 0, WSTOPSIG(status))) {
			/* process may quit by receiving other signal,
			ptrace_process_cleanup() is also called here. */
			DEBUG("ptrace: failed to PTRACE_CONT %d on signal %d: %s\n",
				pid, WSTOPSIG(status), strerror(errno));
			pthread_exit((void *) 1);
		}
	}
	
	pthread_mutex_lock(&proc->lock);
	proc->live = 0;
	pthread_mutex_unlock(&proc->lock);
	ptrace_logging(pid);

	if (ptrace(PTRACE_DETACH, pid, NULL, NULL)) {
		DEBUG("error: PTRACE_CONT process %d: %s\n",
			pid, strerror(errno));
		pthread_exit((void *) 1);
	}
	
	pthread_cleanup_pop(0);
	pthread_exit((void *) 0);
}

static void ptrace_thread_spawn(pthread_t *thread, pid_t pid)
{
	pid_t *pidp = g_new0(pid_t, 1);
	*pidp = pid;
	
	int err = pthread_create(thread, NULL, ptrace_process, (void *) pidp);
	if (err) {
		DEBUG("error: failed to create thread: %s\n", strerror(err));
		exit(1);
	}
	pthread_detach(*thread);
}
#endif	/* FTRAC_TRACE_USING_PTRACE */

/*************************************************
 * Taskstat logging routines 
 ************************************************/
static int get_cpus_num(void)
{
  char buf[MAX_LINE];
  int id, max_id = 0;
  FILE *fp = fopen("/proc/stat", "r");
  while(fgets(buf, MAX_LINE, fp)){
    if(!strncmp(buf, "cpu", 3) && isdigit(buf[3])){
      sscanf(buf+3, "%d", &id);
      if(max_id < id)
        max_id = id;
    }
  }
  fclose(fp);
  return max_id + 1;
}

static int create_netlink_sock(size_t recvbufsize)
{
	struct sockaddr_nl local;
	int fd, res = 0;

	fd = socket(AF_NETLINK, SOCK_RAW, NETLINK_GENERIC);
	if (fd < 0) {
		fprintf(stderr, "failed to create netlink socket\n");
		return -1;
	}
	
	if (recvbufsize)
		res = setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &recvbufsize, 
			sizeof(recvbufsize));
		if (res < 0) {
			fprintf(stderr, "failed to set recieve buffer size "
					"to %lu\n", recvbufsize);
			return -1;
		}

	memset(&local, 0, sizeof(local));
	local.nl_family = AF_NETLINK;

	res = bind(fd, (struct sockaddr *) &local, sizeof(local));
	if (res < 0) {
		fprintf(stderr, "failed to bind socket\n");
		close(fd);
		return -1;
	}
	return fd;
}

int send_netlink_cmd(int sd, __u16 nlmsg_type, __u32 nlmsg_pid,
	__u8 genl_cmd, __u16 nla_type, void *nla_data, int nla_len)
{
	struct nlattr *na;
	struct sockaddr_nl nladdr;
	int r, buflen;
	char *buf;

	struct msgtemplate msg;

	msg.n.nlmsg_len = NLMSG_LENGTH(GENL_HDRLEN);
	msg.n.nlmsg_type = nlmsg_type;
	msg.n.nlmsg_flags = NLM_F_REQUEST;
	msg.n.nlmsg_seq = 0;
	msg.n.nlmsg_pid = nlmsg_pid;
	msg.g.cmd = genl_cmd;
	msg.g.version = 0x1;
	na = (struct nlattr *) GENLMSG_DATA(&msg);
	na->nla_type = nla_type;
	na->nla_len = nla_len + 1 + NLA_HDRLEN;
	memcpy(NLA_DATA(na), nla_data, nla_len);
	msg.n.nlmsg_len += NLMSG_ALIGN(na->nla_len);

	buf = (char *) &msg;
	buflen = msg.n.nlmsg_len ;
	memset(&nladdr, 0, sizeof(nladdr));
	nladdr.nl_family = AF_NETLINK;
	while ((r = sendto(sd, buf, buflen, 0, (struct sockaddr *) &nladdr,
			   sizeof(nladdr))) < buflen) {
		if (r > 0) {
			buf += r;
			buflen -= r;
		} else if (errno != EAGAIN)
			return -1;
	}
	return 0;
}

static int get_family_id(int sd)
{
	int id = 0, rc;
	struct nlattr *na;
	int rep_len;
	char name[100];
	
	struct msgtemplate ans;

	strncpy(name, TASKSTATS_GENL_NAME, 100);
	rc = send_netlink_cmd(sd, GENL_ID_CTRL, getpid(), CTRL_CMD_GETFAMILY,
			CTRL_ATTR_FAMILY_NAME, (void *)name,
			strlen(TASKSTATS_GENL_NAME)+1);
	
	rep_len = recv(sd, &ans, sizeof(ans), 0);
	if (ans.n.nlmsg_type == NLMSG_ERROR ||
	    (rep_len < 0) || !NLMSG_OK((&ans.n), (unsigned) rep_len))
		return 0;

	na = (struct nlattr *) GENLMSG_DATA(&ans); /* warning? */
	na = (struct nlattr *) ((char *) na + NLA_ALIGN(na->nla_len));
	if (na->nla_type == CTRL_ATTR_FAMILY_ID) {
		id = *(__u16 *) NLA_DATA(na);
	}
	return id;
}

static void proc_log_task(pid_t tid, struct taskstats *st, int live)
{
	proctab_entry_t proc = proctab_lookup(tid);
	if (!proc)
		return;
	
	/* Thread exit */
	if (st->ac_pid == 0 && st->ac_btime == 0) {
		DEBUG("taskstat: last thread of task group %d exited.\n", tid);
		return;
	}
	
	/*
	DEBUG("pid=%d, ppid=%d, live=%d, res=%d, btime=%d, etime=%llu, cmd=%s\n", 
		st->ac_pid, st->ac_ppid, live, st->ac_exitcode, 
		st->ac_btime, st->ac_etime, st->ac_comm);
	*/

	/* task/process still running */
	fprintf(ftrac.fp_taskstat, 
		"%d,%d,%d,%d,%d,%llu,%llu,%llu,%s\n",
		st->ac_pid, st->ac_ppid, live, st->ac_exitcode, st->ac_btime, 
		st->ac_etime, st->ac_utime, st->ac_stime, st->ac_comm);
	
	if (!live) {
		/* since process has exited, mark it as dead,
		 * DO NOT remove dead process from table, it is not safe */
		pthread_mutex_lock(&proc->lock);
		proc->live = 0;
		pthread_mutex_unlock(&proc->lock);
	}
}

static int recv_netlink(int sock, int liveness)
{
	struct msgtemplate msg;
	struct nlattr *na;
	int rep_len, res, i;
	int len, len2, aggr_len, count;
	pid_t rtid = 0;
	
	res = recv(sock, &msg, sizeof(msg), 0);
	if (res < 0) {
		fprintf(stderr, "receive error: %d\n", errno);
		return 0;
	}
	if (msg.n.nlmsg_type == NLMSG_ERROR ||
		!NLMSG_OK((&msg.n), (unsigned) res)) {
		struct nlmsgerr *error = NLMSG_DATA(&msg);
		ERROR("warning: receive error: %s\n", strerror(error->error));
		return 1;
	}

	//DEBUG("nlmsghdr size=%zu, nlmsg_len=%d, rep_len=%d\n",
	//	sizeof(struct nlmsghdr), msg.n.nlmsg_len, res);
	
	rep_len = GENLMSG_PAYLOAD(&msg.n);
	na = (struct nlattr *) GENLMSG_DATA(&msg);
	len = i = 0;
	while (len < rep_len) {
		len += NLA_ALIGN(na->nla_len);
		switch (na->nla_type) {
		case TASKSTATS_TYPE_AGGR_TGID:
			/* Fall through */
		case TASKSTATS_TYPE_AGGR_PID:
			aggr_len = NLA_PAYLOAD(na->nla_len);
			len2 = 0;
			/* For nested attributes, na follows */
			na = (struct nlattr *) NLA_DATA(na);
			while (len2 < aggr_len) {
				switch (na->nla_type) {
				case TASKSTATS_TYPE_PID:
					rtid = *(int *) NLA_DATA(na);
					/* DEBUG("TASKSTATS_TYPE_PID %d\n", rtid); */
					break;
				case TASKSTATS_TYPE_TGID:
					rtid = *(int *) NLA_DATA(na);
					/* DEBUG("TASKSTATS_TYPE_TGID %d\n", rtid); */
					break;
				case TASKSTATS_TYPE_STATS:
					count++;
					proc_log_task(rtid, (struct taskstats *) NLA_DATA(na), 
						liveness);
					break;
				default:
					fprintf(stderr, "unknown nested nla_type %d\n",
						na->nla_type);
					break;
				}
				len2 += NLA_ALIGN(na->nla_len);
				na = (struct nlattr *) ((char *) na + len2);
			}
			break;
		default:
			fprintf(stderr, "unknown nla_type %d\n", na->nla_type);
			break;
		}
		na = (struct nlattr *) (GENLMSG_DATA(&msg) + len);
	}
	return 0;
}

static void liveproc_log(gpointer key, gpointer value, gpointer data)
{
	int sock, *sockp, res;
	pid_t *pidp, pid;
	pidp = (pid_t *) key;
	pid = *pidp;
	sockp = (int *) data;
	sock = *sockp;
	proctab_entry_t entry = (proctab_entry_t) value;
	
	if (entry->live) {
		res = send_netlink_cmd(sock, ftrac.familyid, ftrac.pid,
			TASKSTATS_CMD_GET, TASKSTATS_CMD_ATTR_PID, &pid, sizeof(__u32));
		if (res < 0)
			fprintf(stderr, "failed to send tid cmd\n");
		
		res = recv_netlink(sock, 1);
	}
}

static void * procacc_process(void *data)
{
	int err = 0;
	netlink_channel_t nl = (netlink_channel_t) data;

	do {
		err = recv_netlink(nl->sock, 0);
	} while (!err);

	err ? pthread_exit((void *) 1) : pthread_exit((void *) 0);	
}

static void procacc_init(void)
{
	int res, i;
	pthread_t thread_id;
	struct ftrac *ft = &ftrac;

	ft->ncpus = get_cpus_num();

	/* properly set the number of listeners
	To avoid losing statistics, userspace should do one or more of the
	following:
		- increase the receive buffer sizes for the netlink sockets opened by
		  listeners to receive exit data.
		- create more listeners and reduce the number of cpus being listened to
		  by each listener. In the extreme case, there could be one listener
		  for each cpu. Users may also consider setting the cpu affinity of the
		  listener to the subset of cpus to which it listens, especially if
		  they are listening to just one cpu. */
	if (ft->nlsock == 0 || ft->nlsock > ft->ncpus)
		ft->nlsock = ft->ncpus;
	DEBUG("cpus: %d, nlsocks: %d\n", ft->ncpus, ft->nlsock);

	/* create netlink socket array */
	ft->nlarr = g_new0(struct netlink_channel, ft->nlsock);
	for (i = 0; i < ft->nlsock; i++) {
		ft->nlarr[i].sock = create_netlink_sock(ft->nlbufsize);
		if (ft->nlarr[i].sock < 0) {
			fprintf(stderr, "faild to the %dth netlink socket\n", i);
			exit(1);
		}
	}

	/* get family id */
	ft->familyid = get_family_id(ft->nlarr[0].sock);

	/* assign cpus to listeners */
	int q = ft->ncpus / ft->nlsock;
	int r = ft->ncpus % ft->nlsock;
	int start = 0, end;
	for (i = 0; i < ft->nlsock; i++) { /* figure out the assignment */ end = start + q; if (r > 0) { end += 1; r -= 1; } if (start == end - 1) snprintf(ft->nlarr[i].cpumask, 32, "%d", start); else snprintf(ft->nlarr[i].cpumask, 32, "%d-%d", start, end-1);
		
		DEBUG("cpumask[%d]: %s\n", i, ft->nlarr[i].cpumask);
		
		res = send_netlink_cmd(ft->nlarr[i].sock, ft->familyid, ft->pid, 
			TASKSTATS_CMD_GET, TASKSTATS_CMD_ATTR_REGISTER_CPUMASK, 
			&ft->nlarr[i].cpumask, strlen(ft->nlarr[i].cpumask) + 1);
		if (res < 0) {
			fprintf(stderr, "failed to register cpumask\n");
			exit(1);
		}
		start = end;
		
		/* start listening thread */
		res = pthread_create(&thread_id, NULL, procacc_process, 
			(void *) &ft->nlarr[i]);
		if (res != 0) {
			fprintf(stderr, "failed to create thread, %s\n", strerror(res));
			exit(1);
		}
		pthread_detach(thread_id);
		ft->nlarr[i].thread = thread_id;
	}
}

static void procacc_destroy(struct ftrac *ft)
{
	int res, i;
	
	for (i = 0; i < ft->nlsock; i++) {
		res = send_netlink_cmd(ft->nlarr[i].sock, ft->familyid, ft->pid, 
			TASKSTATS_CMD_GET, TASKSTATS_CMD_ATTR_DEREGISTER_CPUMASK, 
			&(ft->nlarr[i].cpumask), strlen(ft->nlarr[i].cpumask) + 1);
		if (res < 0)
			fprintf(stderr, "failed to deregister cpumask\n");
		close(ft->nlarr[i].sock);
		
		res = pthread_cancel(ft->nlarr[i].thread);
		if (res != 0)
			fprintf(stderr, "failed to cancel thread[%d]\n", i);
		
	}

	g_free(ft->nlarr);
	
	/* log all living processes */
	int sock = create_netlink_sock(ft->nlbufsize);
	if (sock < 0) {
		fprintf(stderr, "faild to the netlink socket\n");
		return;
	}
	g_hash_table_foreach(ft->proctab.table, liveproc_log, &sock);
	close(sock);
}

/*************************************************
 * FUSE APIs
 ************************************************/
static int ftrac_getattr(const char *path, struct stat *stbuf)
{
	int res, orig_errno;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	pid_t pid = fuse_get_context()->pid;

	clock_gettime(FTRAC_CLOCK, &start);
#endif
	
	res = lstat(path, stbuf);
	orig_errno = errno;
	
#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sysc_logging(SYSC_FS_LSTAT, &start, &end, pid, res, path);
	proc_logging(SYSC_FS_LSTAT, &start, pid);
#endif
	
	return res == -1 ? -orig_errno : 0;
}

static int ftrac_fgetattr(const char *path, struct stat *stbuf,
	struct fuse_file_info *fi)
{
	int fd, res, orig_errno;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	pid_t pid = fuse_get_context()->pid;
	ftrac_file_t ff = (ftrac_file_t) (uintptr_t) fi->fh;
	fd = ff->fd;
	clock_gettime(FTRAC_CLOCK, &start);
#else
	fd = fi->fh;
	(void) path;
#endif
	
	res = fstat(fd, stbuf);
	orig_errno = errno;
	
#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sysc_logging(SYSC_FS_FSTAT, &start, &end, pid, res, path);
	proc_logging(SYSC_FS_FSTAT, &start, pid);
#endif

	return res == -1 ? -orig_errno : 0;
}

static int ftrac_access(const char *path, int mask)
{
	int res, orig_errno;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	pid_t pid = fuse_get_context()->pid;
	
	clock_gettime(FTRAC_CLOCK, &start);
#endif
    
	res = access(path, mask);
	orig_errno = errno;

#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);

	sysc_logging(SYSC_FS_ACCESS, &start, &end, pid, res, path);
	proc_logging(SYSC_FS_ACCESS, &start, pid);
#endif

	return res == -1 ? -orig_errno : 0;
}

static int ftrac_readlink(const char *path, char *buf, size_t size)
{
	int res, orig_errno;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	pid_t pid = fuse_get_context()->pid;
	
	clock_gettime(FTRAC_CLOCK, &start);
#endif
	
	res = readlink(path, buf, size - 1);
	orig_errno = errno;
	
#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);

	sysc_logging(SYSC_FS_READLINK, &start, &end, pid, res, path);
	proc_logging(SYSC_FS_READLINK, &start, pid);
#endif

	if (res == -1)
		return -orig_errno;

	buf[res] = '\0';
	return 0;
}
static int ftrac_opendir(const char *path, struct fuse_file_info *fi)
{
	int orig_errno;

#ifdef FTRAC_TRACE_ENABLED
	int res;
	struct timespec start, end;
	struct ftrac_file *ff;
	pid_t pid = fuse_get_context()->pid;
	
	clock_gettime(FTRAC_CLOCK, &start);
#endif

	DIR *dp = opendir(path);
	orig_errno = errno;

#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	res = dp == NULL ? -1 : 0;
	sysc_logging(SYSC_FS_OPENDIR, &start, &end, pid, res, path);
	proc_logging(SYSC_FS_OPENDIR, &start, pid);
#endif


	if (dp == NULL)
		return -orig_errno;

#ifdef FTRAC_TRACE_ENABLED
	ff = g_new0(struct ftrac_file, 1);
	ff->fd = (unsigned long) dp;
	ff->pid = pid;
	fi->fh = (unsigned long) ff;
#else
	fi->fh = (unsigned long) dp;
#endif

	return 0;
}

static int ftrac_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
	off_t offset, struct fuse_file_info *fi)
{
	int res;
	DIR *dp;
	struct dirent de, *dep;
	(void) offset;
	(void) fi;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	struct ftrac_file *ff = (struct ftrac_file *) (uintptr_t) fi->fh;
	pid_t pid = fuse_get_context()->pid;
	
	dp = (DIR *) ff->fd;
#else
	dp = (DIR *) (uintptr_t) fi->fh;
	(void) path;
#endif
	
	do {
#ifdef FTRAC_TRACE_ENABLED
		clock_gettime(FTRAC_CLOCK, &start);
#endif

		res = readdir_r(dp, &de, &dep);

#ifdef FTRAC_TRACE_ENABLED
		clock_gettime(FTRAC_CLOCK, &end);
		
		sysc_logging(SYSC_FS_READDIR, &start, &end, pid, res, path);
		proc_logging(SYSC_FS_READDIR, &start, pid);
#endif
		if (dep) {
			struct stat st;
			memset(&st, 0, sizeof(st));
			st.st_ino = de.d_ino;
			st.st_mode = de.d_type << 12;
			if (filler(buf, de.d_name, &st, 0))
				break;
		}
	} while (dep);

	return 0;
}

static int ftrac_closedir(const char *path, struct fuse_file_info *fi)
{
	DIR *dp;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	struct ftrac_file *ff = (struct ftrac_file *) (uintptr_t) fi->fh;
	dp = (DIR *) ff->fd;
	pid_t pid = ff->pid;
	g_free(ff);	/* new() in ftrac_opendir() */
	
	clock_gettime(FTRAC_CLOCK, &start);
#else
	dp = (DIR *) (uintptr_t) fi->fh;
	(void) path;
#endif

	closedir(dp);

#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sysc_logging(SYSC_FS_CLOSEDIR, &start, &end, pid, 0, path);
	proc_logging(SYSC_FS_CLOSEDIR, &start, pid);
#endif

	return 0;
}

static int ftrac_mknod(const char *path, mode_t mode, dev_t rdev)
{
	int res, orig_errno;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	pid_t pid = fuse_get_context()->pid;
	
	clock_gettime(FTRAC_CLOCK, &start);
#endif

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
	orig_errno = errno;

#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sysc_logging(SYSC_FS_MKNOD, &start, &end, pid, res, path);
	proc_logging(SYSC_FS_MKNOD, &start, pid);
#endif

	return res == -1 ? -orig_errno : 0;
}

static int ftrac_mkdir(const char *path, mode_t mode)
{
	int res, orig_errno;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	pid_t pid = fuse_get_context()->pid;
	
	clock_gettime(FTRAC_CLOCK, &start);
#endif
	
	res = mkdir(path, mode);
	orig_errno = errno;

#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sysc_logging(SYSC_FS_MKDIR, &start, &end, pid, res, path);
	proc_logging(SYSC_FS_MKDIR, &start, pid);
#endif
	
	return res == -1 ? -orig_errno : 0;
}

static int ftrac_unlink(const char *path)
{
	int res, orig_errno;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	pid_t pid = fuse_get_context()->pid;
	
	clock_gettime(FTRAC_CLOCK, &start);
#endif
	
	res = unlink(path);
	orig_errno = errno;
	
#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sysc_logging(SYSC_FS_UNLINK, &start, &end, pid, res, path);
	proc_logging(SYSC_FS_UNLINK, &start, pid);
#endif

	return res == -1 ? -orig_errno : 0;
}

static int ftrac_rmdir(const char *path)
{
	int res, orig_errno;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	pid_t pid = fuse_get_context()->pid;
	
	clock_gettime(FTRAC_CLOCK, &start);
#endif
	
	res = rmdir(path);
	orig_errno = errno;
	
#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sysc_logging(SYSC_FS_RMDIR, &start, &end, pid, res, path);
	proc_logging(SYSC_FS_RMDIR, &start, pid);
#endif

	return res == -1 ? -orig_errno : 0;
}

static int ftrac_symlink(const char *from, const char *to)
{
	int res, orig_errno;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	pid_t pid = fuse_get_context()->pid;
	
	clock_gettime(FTRAC_CLOCK, &start);
#endif
	
	res = symlink(from, to);
	orig_errno = errno;

#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sysc_logging_link(SYSC_FS_SYMLINK, &start, &end, pid, res, from, to);
	proc_logging(SYSC_FS_SYMLINK, &start, pid);
#endif

	return res == -1 ? -orig_errno : 0;
}

static int ftrac_rename(const char *from, const char *to)
{
	int res, orig_errno;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	pid_t pid = fuse_get_context()->pid;
	
	clock_gettime(FTRAC_CLOCK, &start);
#endif
    
	res = rename(from, to);
	orig_errno = errno;
	
#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sysc_logging_link(SYSC_FS_RENAME, &start, &end, pid, res, from, to);
	proc_logging(SYSC_FS_RENAME, &start, pid);
#endif

	return res == -1 ? -orig_errno : 0;
}

static int ftrac_link(const char *from, const char *to)
{
	int res, orig_errno;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	pid_t pid = fuse_get_context()->pid;
	
	clock_gettime(FTRAC_CLOCK, &start);
#endif
    
	res = link(from, to);
	orig_errno = errno;
	
#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sysc_logging_link(SYSC_FS_LINK, &start, &end, pid, res, from, to);
	proc_logging(SYSC_FS_LINK, &start, pid);
#endif
	
	return res == -1 ? -orig_errno : 0;
}

static int ftrac_chmod(const char *path, mode_t mode)
{
	int res, orig_errno;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	pid_t pid = fuse_get_context()->pid;
	
	clock_gettime(FTRAC_CLOCK, &start);
#endif
	
	res = chmod(path, mode);
	orig_errno = errno;
	
#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sysc_logging(SYSC_FS_CHMOD, &start, &end, pid, res, path);
	proc_logging(SYSC_FS_CHMOD, &start, pid);
#endif

	return res == -1 ? -orig_errno : 0;
}

static int ftrac_chown(const char *path, uid_t uid, gid_t gid)
{
	int res, orig_errno;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	pid_t pid = fuse_get_context()->pid;
	
	clock_gettime(FTRAC_CLOCK, &start);
#endif
	
	res = lchown(path, uid, gid);
	orig_errno = errno;
	
#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sysc_logging(SYSC_FS_CHOWN, &start, &end, pid, res, path);
	proc_logging(SYSC_FS_CHOWN, &start, pid);
#endif

	return res == -1 ? -orig_errno : 0;
}

static int ftrac_truncate(const char *path, off_t size)
{
	int res, orig_errno;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	pid_t pid = fuse_get_context()->pid;
	
	clock_gettime(FTRAC_CLOCK, &start);
#endif
    
	res = truncate(path, size);
	orig_errno = errno;
	
#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sysc_logging(SYSC_FS_TRUNCATE, &start, &end, pid, res, path);
	proc_logging(SYSC_FS_TRUNCATE, &start, pid);
#endif

	return res == -1 ? -orig_errno : 0;
}

static int ftrac_ftruncate(const char *path, off_t size,
	struct fuse_file_info *fi)
{
	int fd, res, orig_errno;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	pid_t pid = fuse_get_context()->pid;
	ftrac_file_t ff = (ftrac_file_t) (uintptr_t) fi->fh;
	
	fd = ff->fd;
	clock_gettime(FTRAC_CLOCK, &start);
#else
	fd = fi->fh;
	(void) path;
#endif
    
	res = ftruncate(fd, size);
	orig_errno = errno;
	
#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sysc_logging(SYSC_FS_TRUNCATE, &start, &end, pid, res, path);
	proc_logging(SYSC_FS_TRUNCATE, &start, pid);
#endif

	return res == -1 ? -orig_errno : 0;
}

#if FUSE_VERSION >= 26
static int ftrac_utimens(const char *path, const struct timespec ts[2])
{
	struct timeval tv[2];
	int res, orig_errno;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	pid_t pid = fuse_get_context()->pid;

	clock_gettime(FTRAC_CLOCK, &start);
#endif
	
	tv[0].tv_sec = ts[0].tv_sec;
	tv[0].tv_usec = ts[0].tv_nsec / 1000;
	tv[1].tv_sec = ts[1].tv_sec;
	tv[1].tv_usec = ts[1].tv_nsec / 1000;
	res = utimes(path, tv);
	orig_errno = errno;

#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sysc_logging(SYSC_FS_UTIME, &start, &end, pid, res, path);
	proc_logging(SYSC_FS_UTIME, &start, pid);
#endif

	return res == -1 ? -orig_errno : 0;
}
#else
static int ftrac_utime(const char *path, struct utimbuf *buf)
{
	int res, orig_errno;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	pid_t pid = fuse_get_context()->pid;
	
	clock_gettime(FTRAC_CLOCK, &start);
#endif
    
	res = utime(path, buf);
	orig_errno = errno;

#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sysc_logging(SYSC_FS_UTIME, &start, &end, pid, res, path);
	proc_logging(SYSC_FS_UTIME, &start, pid);
#endif
	
	return res == -1 ? -orig_errno : 0;
}
#endif

static int ftrac_create(const char *path, mode_t mode, struct fuse_file_info
*fi)
{
	int fd, orig_errno;

#ifdef FTRAC_TRACE_ENABLED
	int res;
	struct timespec start, end;
	pid_t pid = fuse_get_context()->pid;
	
	clock_gettime(FTRAC_CLOCK, &start);
#endif
    
	fd = open(path, fi->flags, mode);
	orig_errno = errno;
	
#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	res = fd == -1 ? -1 : 0;
	sysc_logging(SYSC_FS_CREAT, &start, &end, pid, res, path);
	proc_logging(SYSC_FS_CREAT, &start, pid);
#endif

	if (fd == -1)
		return -orig_errno;

#ifdef FTRAC_TRACE_ENABLED
	struct ftrac_file *ff = g_new0(struct ftrac_file, 1);
	ff->fd = fd;
	ff->pid = pid;
	fi->fh = (unsigned long) ff;
#else
	fi->fh = fd;
#endif
	
	return 0;
}

static int ftrac_open(const char *path, struct fuse_file_info *fi)
{
	int fd, orig_errno;

#ifdef FTRAC_TRACE_ENABLED
	int res;
	struct timespec start, end;
	pid_t pid = fuse_get_context()->pid;
	
	clock_gettime(FTRAC_CLOCK, &start);
#endif
    
	fd = open(path, fi->flags);
	orig_errno = errno;
	
#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	res = fd == -1 ? -1 : 0;
	sysc_logging_openclose(SYSC_FS_OPEN, &start, &end, pid, res, path);
	proc_logging(SYSC_FS_OPEN, &start, pid);
#endif

	if (fd == -1)
		return -orig_errno;

#ifdef FTRAC_TRACE_ENABLED
	struct ftrac_file *ff = g_new0(struct ftrac_file, 1);
	ff->fd = fd;
	ff->pid = pid;
	fi->fh = (unsigned long) ff;
#else
	fi->fh = fd;
#endif
	
	return 0;
}

static int ftrac_read(const char *path, char *buf, size_t size, off_t offset,
	struct fuse_file_info *fi)
{
	int fd, res, orig_errno;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	pid_t pid = fuse_get_context()->pid;
	ftrac_file_t ff = (ftrac_file_t) (uintptr_t) fi->fh;
	
	fd = ff->fd;
	clock_gettime(FTRAC_CLOCK, &start);
#else
	fd = fi->fh;
	(void) path;
#endif
	
	res = pread(fd, buf, size, offset);
	orig_errno = errno;
	
#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	/* should not log size in arguments, but actual read bytes */
	size_t bytes = res == -1 ? 0 : res;
	sysc_logging_io(SYSC_FS_READ, &start, &end, pid, res, 
		path, bytes, offset);
	proc_logging(SYSC_FS_READ, &start, pid);
#endif

	return res == -1 ? -orig_errno : res;
}

static int ftrac_write(const char *path, const char *buf, size_t size,
		     off_t offset, struct fuse_file_info *fi)
{
	int fd, res, orig_errno;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	pid_t pid = fuse_get_context()->pid;
	ftrac_file_t ff = (ftrac_file_t) (uintptr_t) fi->fh;
	
	fd = ff->fd;
	clock_gettime(FTRAC_CLOCK, &start);
#else
	fd = fi->fh;	
	(void) path;
#endif
	
	res = pwrite(fd, buf, size, offset);
	orig_errno = errno;
	
#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	/* should not log size in arguments, but actual write bytes */
	size_t bytes = res == -1 ? 0 : res;
	sysc_logging_io(SYSC_FS_WRITE, &start, &end, pid, res, 
		path, bytes, offset);
	proc_logging(SYSC_FS_WRITE, &start, pid);
#endif

	return res == -1 ? -orig_errno : res;
}

static int ftrac_statfs(const char *path, struct statvfs *stbuf)
{
	int res, orig_errno;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	pid_t pid = fuse_get_context()->pid;

	clock_gettime(FTRAC_CLOCK, &start);
#endif
    
	res = statvfs(path, stbuf);
	orig_errno = errno;
	
#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sysc_logging(SYSC_FS_STATFS, &start, &end, pid, res, path);
	proc_logging(SYSC_FS_STATFS, &start, pid);
#endif
	
	return res == -1 ? -orig_errno : 0;
}

static int ftrac_flush(const char *path, struct fuse_file_info *fi)
{
	int fd, res, orig_errno;
	
#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	pid_t pid = fuse_get_context()->pid;
	ftrac_file_t ff = (ftrac_file_t) (uintptr_t) fi->fh;
	
	fd = ff->fd;
	clock_gettime(FTRAC_CLOCK, &start);
#else
	fd = fi->fh;
	(void) path;
#endif
	
	/* This is called from every close on an open file, so call the
	   close on the underlying filesystem.	But since flush may be
	   called multiple times for an open file, this must not really
	   close the file.  This is important if used on a network
	   filesystem like NFS which flush the data/metadata on close() */
	res = close(dup(fd));
	orig_errno = errno;

#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sysc_logging(SYSC_FS_FLUSH, &start, &end, pid, res, path);
	proc_logging(SYSC_FS_FLUSH, &start, pid);
#endif

	return res == -1 ? -orig_errno : 0;
}

static int ftrac_close(const char *path, struct fuse_file_info *fi)
{
	int fd;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	ftrac_file_t ff = (ftrac_file_t) (uintptr_t) fi->fh;
	
	fd = ff->fd;
	pid_t pid = ff->pid;
	g_free(ff);
	clock_gettime(FTRAC_CLOCK, &start);
#else
	fd = fi->fh;
	(void) path;
#endif
	
	close(fd);
	
#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sysc_logging_openclose(SYSC_FS_CLOSE, &start, &end, pid, 0, path);
	proc_logging(SYSC_FS_CLOSE, &start, pid);
#endif

	return 0;
}

static int ftrac_fsync(const char *path, int isdatasync,
		     struct fuse_file_info *fi)
{
	int fd, res, orig_errno;
	(void) path;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	pid_t pid = fuse_get_context()->pid;
	ftrac_file_t ff = (ftrac_file_t) (uintptr_t) fi->fh;
	
	fd = ff->fd;
	clock_gettime(FTRAC_CLOCK, &start);
#else
	fd = fi->fh;
	(void) path;
#endif

#ifndef HAVE_FDATASYNC
	(void) isdatasync;
#else
	if (isdatasync)
		res = fdatasync(fd);
	else
#endif
	
	res = fsync(fd);
	orig_errno = errno;

#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sysc_logging(SYSC_FS_FSYNC, &start, &end, pid, res, path);
	proc_logging(SYSC_FS_FSYNC, &start, pid);
#endif
    
	return res == -1 ? -orig_errno : 0;
}

#ifdef HAVE_SETXATTR
/* xattr operations are optional and can safely be left unimplemented */
static int ftrac_setxattr(const char *path, const char *name, 
	const char *value, size_t size, int flags)
{
	int res, orig_errno;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	pid_t pid = fuse_get_context()->pid;

	clock_gettime(FTRAC_CLOCK, &start);
#endif
    
	res = lsetxattr(path, name, value, size, flags);
	orig_errno = errno;
	
#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sysc_logging(SYSC_FS_SETXATTR, &start, &end, pid, res, path);
	proc_logging(SYSC_FS_SETXATTR, &start, pid);
#endif

	return res == -1 ? -orig_errno : 0;
}

static int ftrac_getxattr(const char *path, const char *name, char *value,
			size_t size)
{
	int res, orig_errno;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	pid_t pid = fuse_get_context()->pid;
	
	clock_gettime(FTRAC_CLOCK, &start);
#endif
    
	res = lgetxattr(path, name, value, size);
	orig_errno = errno;
	
#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sysc_logging(SYSC_FS_GETXATTR, &start, &end, pid, res, path);
	proc_logging(SYSC_FS_GETXATTR, &start, pid);
#endif

	return res == -1 ? -orig_errno : res;
}

static int ftrac_listxattr(const char *path, char *list, size_t size)
{
	int res, orig_errno;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	pid_t pid = fuse_get_context()->pid;
	
	clock_gettime(FTRAC_CLOCK, &start);
#endif
    
	res = llistxattr(path, list, size);
	orig_errno = errno;
	
#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sysc_logging(SYSC_FS_LISTXATTR, &start, &end, pid, res, path);
	proc_logging(SYSC_FS_LISTXATTR, &start, pid);
#endif

	return res == -1 ? -orig_errno : res;
}

static int ftrac_removexattr(const char *path, const char *name)
{
	int res, orig_errno;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	pid_t pid = fuse_get_context()->pid;
	
	clock_gettime(FTRAC_CLOCK, &start);
#endif
	
	res = lremovexattr(path, name);
	orig_errno = errno;

#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sysc_logging(SYSC_FS_REMOVEXATTR, &start, &end, pid, res, path);
	proc_logging(SYSC_FS_REMOVEXATTR, &start, pid);
#endif

	return res == -1 ? -orig_errno : 0;
}
#endif /* HAVE_SETXATTR */

#if FUSE_VERSION >= 26
static void * ftrac_init(struct fuse_conn_info *conn)
#else
static void * ftrac_init(void)
#endif
{
	(void) conn;

	/* set the seed using the current time and the process ID */
	srand(time(NULL) + getpid());

	/* initial instance */
	clock_gettime(FTRAC_CLOCK, &ftrac.itime);
	ftrac.uid = getuid();
	ftrac.pid = getpid();
	ftrac.iid = ftrac_rand_range(MAX_RAND_MIN, MAX_RAND_MAX); 

	/* initial log file */
	logging_init();

	/* startup control server */
	ctrl_server_init();

	/* initial process accounting */
	procacc_init();
	
    /* 
     * notify the caller we are done for initialization
     * this is necessary when ftrac is exectued by another process
     */
    if (ftrac.notify_pid) {
        assert(ftrac.notify_pid > 0);
        kill(ftrac.notify_pid, FTRAC_SIGNAL_INIT);
    }
        
	return NULL;
}

static void ftrac_destroy(void *data_)
{
	(void) data_;
	
	procacc_destroy(&ftrac); /* must before logging_destroy */
	logging_destroy(&ftrac);
	ctrl_server_destroy(&ftrac);
	remove(ftrac.sessiondir);
	g_free(ftrac.cmdline);
	g_free(ftrac.cwd);
	g_free(ftrac.username);
	g_free(ftrac.environ);
	g_free(ftrac.sessiondir);
	g_free(ftrac.mountpoint);
}

/*********** FUSE setup routines **********/
static struct fuse_operations ftrac_oper = {
	.init			= ftrac_init,
	.destroy		= ftrac_destroy,
	.getattr		= ftrac_getattr,
	.fgetattr		= ftrac_fgetattr,
	.access			= ftrac_access,
	.readlink		= ftrac_readlink,
	.opendir		= ftrac_opendir,
	.readdir		= ftrac_readdir,
	.releasedir		= ftrac_closedir,
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
	.ftruncate		= ftrac_ftruncate,
#if FUSE_VERSION >= 26
	.utimens		= ftrac_utimens,
#else
	.utime			= ftrac_utime,
#endif
	.create			= ftrac_create,
	.open			= ftrac_open,
	.read			= ftrac_read,
	.write			= ftrac_write,
	.statfs			= ftrac_statfs,
	.flush			= ftrac_flush,
	.release		= ftrac_close,
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
"Usage: %s mountpoint [options]\n"
"\n"
"General options:\n"
"    -o opt,[opt...]        mount options\n"
"    -h   --help            print help\n"
"    -V   --version         print version\n"
"    -o sessiondir=PATH     session directory\n"
"    -o logdir=PATH         log directory\n"
"    -o logbufsize=PATH     log buffer size\n"
"    -o notify_pid=PID      process to notify on entering fuse loop\n"
"    -o ftrac_debug         print debug information\n"
"    -o debug_log           redirect debug information to log file\n"
"\nFUSE logging options:\n"
"    -o table_timeout       cache timeout for table entries (60)\n"
"\nProcess logging options:\n"
"    -o environ=VAR:VAR     environment variables (all)\n"
"    -o stop_envchk         stop checking environ once updated (on)\n"
"    -o stop_cmdchk         stop checking cmdline once updated (on)\n"
"\nTaskstats logging options:\n"
"    -o nlsock=NUM          number of netlink sockets (1)\n"
"    -o nlbufsize=NUM       netlink buffer size (4096)\n"
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
static int fuse_opt_insert_arg(struct fuse_args *args, int pos, 
	const char *arg)
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

static int ftrac_opt_proc(void *data, const char *arg, int key, 
	struct fuse_args *outargs)
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
			fprintf(stderr, "FUSETracer version %s\n", PACKAGE_VERSION);
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

/* Remove commas from fsname, as it confuses the fuse option parser. */
static void fsname_remove_commas(char *fsname)
{
	if (strchr(fsname, ',') != NULL) {
		char *s = fsname;
		char *d = s;

		for (; *s; s++) {
			if (*s != ',')
				*d++ = *s;
		}
		*d = *s;
	}
}

#if FUSE_VERSION >= 27
static char *fsname_escape_commas(char *fsnameold)
{
	char *fsname = g_malloc(strlen(fsnameold) * 2 + 1);
	char *d = fsname;
	char *s;

	for (s = fsnameold; *s; s++) {
		if (*s == '\\' || *s == ',')
			*d++ = '\\';
		*d++ = *s;
	}
	*d = '\0';
	g_free(fsnameold);

	return fsname;
}
#endif

/*********** main entry **********/
int main(int argc, char * argv[])
{
	struct passwd *pwd;
	struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
	char *fsname, *tmp;
	int libver, res;

	if (!g_thread_supported())
        g_thread_init(NULL);
	
	ftrac.progname = argv[0];
	ftrac.logbufsize = FTRAC_TRACE_LOG_BUFSIZE;
	ftrac.table_timeout = 60;
	ftrac.stop_envchk = 1;
	ftrac.stop_cmdchk = 1;
	ftrac.nlsock = 1;
	ftrac.nlbufsize = MAX_MSG_SIZE;

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

	ftrac.cwd = g_get_current_dir();

	if (!ftrac.sessiondir)
		ftrac.sessiondir = g_strdup_printf("%s/ftrac-%s-%u", PATH_PREFIX, 
			ftrac.username, ftrac_string_hash(ftrac.mountpoint));
    res = mkdir(ftrac.sessiondir, S_IRUSR | S_IWUSR | S_IXUSR);
    if (res == -1 && errno != EEXIST) {
		fprintf(stderr, "failed to create directory %s, %s\n", 
				ftrac.sessiondir, strerror(errno));
		return -1;
    }
	
	if (!ftrac.logdir)
		ftrac.logdir = g_strdup_printf("%s/ftrac-%s-%u", ftrac.cwd, 
			ftrac.username, ftrac_string_hash(ftrac.mountpoint));
	else {
		char logdir[PATH_MAX];
		struct stat stbuf;
		if (realpath(ftrac.logdir, logdir) == NULL &&
			stat(ftrac.logdir, &stbuf) == 0) {
			fprintf(stderr, "bad log directory %s\n", logdir);
			exit(1);
		}
		free(ftrac.logdir);
		ftrac.logdir = g_strdup(logdir);
	}
    res = mkdir(ftrac.logdir, S_IRUSR | S_IWUSR | S_IXUSR);
    if (res == -1 && errno != EEXIST) {
		fprintf(stderr, "failed to create log directory %s, %s\n", 
				ftrac.logdir, strerror(errno));
		return -1;
    }

	ftrac.sockpath = g_strdup_printf("%s/ftrac.sock", ftrac.sessiondir);
	
	fsname = g_strdup_printf("ftrac-%d", ftrac_string_hash(ftrac.mountpoint));
#if FUSE_VERSION >= 27
	libver = fuse_version();
	assert(libver >= 27);
	if (libver >= 28)
		fsname = fsname_escape_commas(fsname);
	else
		fsname_remove_commas(fsname);
	tmp = g_strdup_printf("-osubtype=fstrac,fsname=%s", fsname);
#else
	tmp = g_strdup_printf("-ofsname=fstrac#%s", fsname);
#endif
	fuse_opt_insert_arg(&args, 1, tmp);
	g_free(tmp);
	g_free(fsname);

	res = ftrac_fuse_main(&args);
	
	fuse_opt_free_args(&args);
	
	return res;
}
