/****************************************************************************
 * ParaTrac: Scalable Tracking Tools for Parallel Applications
 * Copyright (C) 2008,2009,2010  Nan Dun <dunnan@yl.is.s.u-tokyo.ac.jp>
 * Department of Computer Science, The University of Tokyo
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

#define FUSE_USE_VERSION 26

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#define _GNU_SOURCE

#define FTRAC_TRACE_ENABLED             1
#define FTRAC_TRACE_SYSC_ENABLED        1
#define FTRAC_TRACE_PROC_ENABLED        1
#define FTRAC_TRACE_PROC_TASKSTAT       1
#define FTRAC_TRACE_PROC_PTRACE         0

#include <fuse.h>
#include <ulockmgr.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <ctype.h>
#include <fcntl.h>
#include <dirent.h>
#include <errno.h>
#include <assert.h>
#include <pwd.h>
#include <pthread.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/utsname.h>
#ifdef HAVE_SETXATTR
#error
#include <sys/xattr.h>
#endif

#include <glib.h>

#ifdef linux
#include <linux/version.h>
#if FTRAC_TRACE_PROC_TASKSTAT
#include <linux/genetlink.h>
#include <linux/taskstats.h>
#include <linux/cgroupstats.h>
#endif
#endif

#if FUSE_VERSION < 23
#error "FUSE version < 2.3 not supported"
#endif

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

#define FTRAC_MAX_BUF_LEN 1048576
#define FTRAC_MAX_LINE    4096

struct proc_stat {
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
};

struct netlink_channel {
	int sock;
	pthread_t thread;
	char cpumask[32];
};

struct hash_table {
	GHashTable *table;
	pthread_mutex_t lock;
	pthread_t thread;
};

struct proctab_entry {
	int ptime;
	int live;
	double stamp;
	char *environ;
	char *cmdline;
	int n_envchk;
	int n_cmdchk;
	struct proc_stat stat;
	pthread_t trace_thread;
	int thread_started;
	int traced;
	pthread_mutex_t lock;
};

struct ftrac_filep {
	unsigned long fd;
	pid_t pid;
};

struct ftrac_dirp {
	DIR *dp;
	struct dirent *entry;
	off_t offset;
};

struct ftrac {
	char *progname;
	char *cmdline;
	char *cwd;
	char *username;
	char *mntpoint;
	char *logdir;
	struct utsname platform;
	uid_t uid;
	gid_t gid;
	pid_t pid;
	int iid;
	struct timespec itime;
	long sys_clk_tck;
	long sys_btime;

#if FTRAC_TRACE_ENABLED && FTRAC_TRACE_SYSC_ENABLED
	struct hash_table filetab;
	FILE *sysc_stream;
	FILE *file_stream;
#endif

#if FTRAC_TRACE_ENABLED && FTRAC_TRACE_PROC_ENABLED
	struct hash_table proctab;
	FILE *proc_stream;
	char *environ;
	char **environ_vars;
	int environ_nvars;
	int cache_timeout;

#if FTRAC_TRACE_PROC_TASKSTAT
	FILE *taskstat_stream;
	int ncpus;
	int familyid;
	int nlsock;
	size_t nlbufsize;
	struct netlink_channel *nlarr;
#endif

#endif /* FTRAC_TRACE_ENABLED && FTRAC_TRACE_PROC_ENABLED */
	
	FILE *runtime_stream;
	FILE *err_stream;
	int foreground;
	int debug;
	int dump;
};

static struct ftrac ftrac;

/*
 * Utilities
 */
#define ERROR(format, args...) \
	fprintf(ftrac.err_stream, "error: "format, args)
#define WARNING(format, args...) \
	fprintf(ftrac.err_stream, "warning: "format, args)
#define DEBUG(format, args...) \
	if (ftrac.debug) {fprintf(ftrac.err_stream, "debug: "format, args);}

#define TIMING(time) clock_gettime(CLOCK_REALTIME, &time)

static inline void * util_malloc(size_t size)
{
	char *ptr = malloc(size);
	if (ptr == NULL) {
		ERROR("fatal: allocate memory with size %lu\n", size);
		abort();
	}
	return ptr;
}

static inline void * util_malloc0(size_t size)
{
	char *ptr = malloc(size);
	if (ptr == NULL) {
		ERROR("fatal: allocate memory with size %lu\n", size);
		abort();
	}
	memset(ptr, 0x0, size);
	return ptr;
}

static inline void * util_realloc(void * ptr, size_t size)
{
	ptr = realloc(ptr, size);
	if (ptr == NULL) {
		ERROR("fatal: reallocate memory to size %lu\n", size);
		abort();
	}
	return ptr;
}

static inline void * util_realloc0(void * ptr, size_t size)
{
	ptr = realloc(ptr, size);
	if (ptr == NULL) {
		ERROR("fatal: reallocate memory to size %lu\n", size);
		abort();
	}
	memset(ptr, 0x0, size);
	return ptr;
}

static inline unsigned int util_str_hash(const char *str)
{
	unsigned int hash = 0;
	int i = 0;
	
	while (str[i]) {
		hash += str[i] * (i + 1);
		i++;
	}

	return hash;
}

static inline double util_get_timespec(const struct timespec *time) 
{
	return ((double) time->tv_sec + \
		((double) time->tv_nsec) * 0.000000001);
}

static inline double util_get_elapsed(const struct timespec *start, 
	const struct timespec *end)
{
	return ((double) (end->tv_sec - start->tv_sec) + \
		(((double) (end->tv_nsec - start->tv_nsec)) * 0.000000001));
}

static inline char * util_get_file(const char *file, size_t *len, int escape)
{
	FILE *fp = fopen(file, "rb");
	if (fp == NULL) {
		fprintf(stderr, "open file %s failed\n", file);
		return NULL;
	}
	
	int c;
	size_t i = 0, buflen = FTRAC_MAX_BUF_LEN;
	char *buf;
	buf = g_malloc0(FTRAC_MAX_BUF_LEN);
	while ((c = getc(fp)) != EOF) {
		buf[i++] = c;
		if (i >= buflen) {
			buflen += FTRAC_MAX_BUF_LEN;
			buf = g_realloc(buf, buflen);
		}
	}
	fclose(fp);

	buf[i] = '\0';
	*len = i;

	if (escape) {
		for (i = 0; i < *len; i++) {
			if (buf[i] == '\0' || buf[i] == '\n')
				buf[i] = ' ';
		}
		buf[*len - 1] = '\0';
	}
	
	return buf;
}

/* prof file system utilities */
static inline int procfs_pid_exists(pid_t pid)
{
	char path[PATH_MAX];
	snprintf(path, PATH_MAX, "/proc/%d", pid);
	return access(path, F_OK);
}

static char * procfs_get_cmdline(pid_t pid)
{
	char path[PATH_MAX];
	char *cmdline = NULL;
	size_t length;
	
	memset(path, 0x0, PATH_MAX);
	snprintf(path, PATH_MAX, "/proc/%d/cmdline", pid);
	cmdline = util_get_file(path, &length, 1);
	if (cmdline == NULL) {
		ERROR("error: get file %s\n", path);
		return NULL;
	}
	
	return cmdline;
}

static long procfs_get_sys_btime(void)
{
	long btime = 0;
	char buf[FTRAC_MAX_LINE];
	FILE *fp = fopen("/proc/stat", "r");
	if (fp == NULL) {
		ERROR("error: open file /proc/stat: %s\n", strerror(errno));
		return btime;
	}
	
	while(fgets(buf, FTRAC_MAX_LINE, fp)){
		if(!strncmp(buf, "btime", 5)){
			sscanf(buf + 5, "%ld", &btime);
			break;
		}
	}

	fclose(fp);
	return btime;
}

#if FTRAC_TRACE_ENABLED && FTRAC_TRACE_PROC_ENABLED
static char * procfs_get_environ(pid_t pid)
{
	char path[PATH_MAX];
	
	snprintf(path, PATH_MAX, "/proc/%d/environ", pid);
	if (access(path, R_OK) != 0) {
		return NULL;
	}
	
	/* get all environ once for convienience */
	char *environ;
	size_t length;
	
	environ = util_get_file(path, &length, 1);
	if (environ == NULL) {
		ERROR("get environ of file %s\n", path);
		return NULL;
	}
		
	/* extracting vars from environ */
	char **vars = ftrac.environ_vars;
	int nvars = ftrac.environ_nvars;
	if (vars) {
		int i, j, len;
		char *ptr, *new_environ;

		len = 0;
		nvars = g_strv_length(vars);
		new_environ = g_new0(char, FTRAC_MAX_LINE);
		for (i = 0; i < nvars; i++) {
			ptr = g_strrstr(environ, vars[i]);
			j = 0;
			while(ptr && ptr[j] != '\0' && ptr[j] != ' ') {
				new_environ[len] = ptr[j];
				len++;
				j++;
				if (len >= FTRAC_MAX_LINE)
					new_environ = 
						g_realloc(new_environ, len + FTRAC_MAX_LINE);
			}
			if (j > 0) {	/* only output when env exist */
				new_environ[len] = ' ';
				len++;
				if (len >= FTRAC_MAX_LINE)
					new_environ = 
						g_realloc(new_environ, len + FTRAC_MAX_LINE);
			}
		}
		new_environ[len] = '\0';
		g_free(environ);
		environ = new_environ;
	}

	return environ;
}

static void procfs_get_stat(pid_t pid, struct proc_stat *stat)
{
	char path[PATH_MAX];
	struct proc_stat st;
	
	memset(path, 0x0, PATH_MAX);
	snprintf(path, PATH_MAX, "/proc/%d/stat", pid);
	
	FILE *fp = fopen(path, "rb");
	if (fp == NULL) {
		ERROR("open file %s: %s\n", path, strerror(errno));
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
		&st.pid, st.comm, &st.state, &st.ppid, &st.pgrp,	
		&st.session, &st.tty_nr, &st.tpgid, &st.flags,
		&st.minflt, &st.cminflt, &st.majflt, &st.cmajflt,
		&st.utime, &st.stime, &st.cutime,	&st.cstime,
		&st.priority, &st.nice,	&st.num_threads, &st.itrealvalue,
		&st.starttime, &st.vsize, &st.rss, &st.rsslim,
		&st.startcode, &st.endcode, &st.startstack, &st.kstkesp,
		&st.kstkesp, &st.kstkeip, &st.signal, &st.blocked,
		&st.sigignore, &st.sigcatch, &st.wchan, &st.nswap,
		&st.cnswap
#ifdef linux
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2,1,22)
		, &st.exit_signal
#endif
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2,2,28)
		, &st.processor
#endif
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2,5,19)
		, &st.rt_priority, &st.policy
#endif
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2,6,18)
		, &st.delayacct_blkio_ticks
#endif
#if LINUX_VERSION_CODE >= KERNEL_VERSION(2,6,24)
		, &st.guest_time, &st.cguest_time
#endif
#endif
		);
	
	fclose(fp);
	
	if (res == EOF) 
		ERROR("parsing file %s\n", path);
	else
		memcpy(stat, &st, sizeof(struct proc_stat));
}

#if FTRAC_TRACE_PROC_TASKSTAT
static int procfs_get_cpus_num(void)
{
	char buf[FTRAC_MAX_LINE];
	int id, max_id = 0;
	FILE *fp = fopen("/proc/stat", "r");
	
	while(fgets(buf, FTRAC_MAX_LINE, fp)){
		if(!strncmp(buf, "cpu", 3) && isdigit(buf[3])){
			sscanf(buf+3, "%d", &id);
			if(max_id < id)
				max_id = id;
		}
	}
	
	fclose(fp);
	return max_id + 1;
}
#endif
#endif /* FTRAC_TRACE_ENABLED && FTRAC_TRACE_PROC_ENABLED */

/* 
 * System call trace routines
 */
#if FTRAC_TRACE_ENABLED && FTRAC_TRACE_SYSC_ENABLED
/* Forward declarations */
static inline void file_logging(int index, const char *path);

/* File table structure and routines */
static int filetab_init(void)
{
	ftrac.filetab.table = g_hash_table_new_full(g_str_hash, 
		g_str_equal, g_free, NULL);
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
		file_logging(size, path);
		return size;
	}
	return GPOINTER_TO_UINT(p);
}

static void sysc_logging_init(void)
{
	char file[PATH_MAX];
	
	assert(ftrac.logdir);

	memset(file, 0, PATH_MAX);
	snprintf(file, PATH_MAX, "%s/sysc.log", ftrac.logdir);
	ftrac.sysc_stream = fopen(file, "wb");
	if (ftrac.sysc_stream == NULL) {
		ERROR("open file %s: %s\n", file, strerror(errno));
		exit(1);
	}

	/* TODO: set large buffer for sysc log stream */
	
	memset(file, 0, PATH_MAX);
	snprintf(file, PATH_MAX, "%s/file.log", ftrac.logdir);
	ftrac.file_stream = fopen(file, "wb");
	if (ftrac.file_stream == NULL) {
		ERROR("open file %s: %s\n", file, strerror(errno));
		exit(1);
	}

	filetab_init();
}

static void sysc_logging_destroy(void)
{
	filetab_destroy();

	fclose(ftrac.sysc_stream);
	fclose(ftrac.file_stream);
}

static inline void sysc_logging(int sysc, struct timespec *start, 
	struct timespec *end, int pid, int res, const char *path)
{
	fprintf(ftrac.sysc_stream, "%.9f,%d,%d,%u,%d,%.9f,0,0\n",
		util_get_timespec(end), pid, sysc, 
		filetab_lookup_insert(path), res, 
		util_get_elapsed(start, end));
}

static inline void sysc_logging_openclose(int sysc, struct timespec *start, 
	struct timespec *end, int pid, int res, const char *path)
{
	struct stat stbuf;
	off_t fsize = stat(path, &stbuf) == 0 ? stbuf.st_size : -1;

	fprintf(ftrac.sysc_stream, "%.9f,%d,%d,%u,%d,%.9f,%lu,0\n", 
		util_get_timespec(end), pid, sysc, 
		filetab_lookup_insert(path), res, 
		util_get_elapsed(start, end), fsize);
}

static inline void sysc_logging_io(int sysc, struct timespec *start, 
	struct timespec *end, int pid, int res,
	const char *path, size_t size, off_t offset)
{
	fprintf(ftrac.sysc_stream, "%.9f,%d,%d,%u,%d,%.9f,%lu,%lu\n", 
		util_get_timespec(end), pid, sysc, 
		filetab_lookup_insert(path), res, 
		util_get_elapsed(start, end), size, offset);
}

static inline void sysc_logging_link(int sysc, 
	struct timespec *start, struct timespec *end, int pid, int res,
	const char *from, const char *to)
{
	fprintf(ftrac.sysc_stream, "%.9f,%d,%d,%u,%d,%.9f,%u,0\n", 
		util_get_timespec(end), pid, sysc, 
		filetab_lookup_insert(from), res, 
		util_get_elapsed(start, end), 
		filetab_lookup_insert(to));
}

static inline void file_logging(int index, const char *path)
{
	fprintf(ftrac.file_stream, "%u:%s\n", index, path);
}

#endif /* FTRAC_TRACE_ENABLED && FTRAC_TRACE_SYSC_ENABLED */

/*
 * Process trace routiens
 */
#if FTRAC_TRACE_ENABLED && FTRAC_TRACE_PROC_ENABLED

#if FTRAC_TRACE_PROC_TASKSTAT
static void taskstat_init(void);
static void taskstat_destroy(void);
#endif

enum {
	PROC_LOG_INIT = 0,
	PROC_LOG_CMDLINE_UPDATED = 1,
	PROC_LOG_ENVIRON_UPDATED = 2,
	PROC_LOG_CMD_ENV_UPDATED = 3,
	PROC_LOG_FINAL = 4,
};

static inline void proc_logging_write(struct proctab_entry *proc, int flag)
{
	/* TAG: file_format */
	fprintf(ftrac.proc_stream,
		"%d|#|%d|#|%d|#|%lu|#|%.9f|#|%lu|#|%lu|#|%s|#|%s\n",
		flag, proc->stat.pid, proc->stat.ppid,
		proc->stat.starttime, proc->stamp,
		proc->stat.utime, proc->stat.stime, 
		proc->cmdline, proc->environ);
}

static void proctab_entry_free(gpointer data)
{
	struct proctab_entry *proc = (struct proctab_entry *) data;
	
	proc_logging_write(proc, PROC_LOG_FINAL);

	g_free(proc->cmdline);
	g_free(proc->environ);
	pthread_mutex_destroy(&proc->lock);
	g_free(proc);
}

static int proctab_entry_check(void *key, void *value, void *data)
{
	(void) key;
	(void) data;

	struct proctab_entry *proc = (struct proctab_entry *) value;
	if (proc->live) {
		if (procfs_pid_exists(proc->stat.pid) == -1) {
			/* process is actually dead, mark it as dead and
			   leave it to next cleaup run */
			pthread_mutex_lock(&proc->lock);
			proc->live = 0;
			pthread_mutex_unlock(&proc->lock);
		}
		return 0;
	} else {
		return 1;
	}
}

static void * proctab_cleanup(void *data)
{
	(void) data;
	
	while (1) {
		sleep(ftrac.cache_timeout);
		pthread_mutex_lock(&ftrac.proctab.lock);
		g_hash_table_foreach_remove(ftrac.proctab.table, 
			proctab_entry_check, NULL);
		pthread_mutex_unlock(&ftrac.proctab.lock);
	}

	pthread_exit((void *) 0);
}

static void proctab_init(void)
{
	int err;

	ftrac.proctab.table = g_hash_table_new_full(g_int_hash, g_int_equal,
		g_free, proctab_entry_free);
	if (!ftrac.proctab.table) {
		fprintf(stderr, "failed to create hash table\n");
		exit(1);
	}

	pthread_mutex_init(&ftrac.proctab.lock, NULL);

	err = pthread_create(&ftrac.proctab.thread, NULL,
		proctab_cleanup, NULL);
	if (err) {
		ERROR("create thread: %s\n", strerror(err));
		exit(1);
	}
}

static void proctab_destroy(void)
{
	pthread_cancel(ftrac.proctab.thread);
	pthread_mutex_destroy(&ftrac.proctab.lock);
	g_hash_table_destroy(ftrac.proctab.table);
}

static inline struct proctab_entry * proctab_lookup(pid_t pid)
{
	return (struct proctab_entry *) 
		g_hash_table_lookup(ftrac.proctab.table, &pid);
}

static void proc_logging_init(void)
{
	char file[PATH_MAX];

	ftrac.cache_timeout = 600;
	
	assert(ftrac.logdir);

	memset(file, 0, PATH_MAX);
	snprintf(file, PATH_MAX, "%s/proc.log", ftrac.logdir);
	ftrac.proc_stream = fopen(file, "wb");
	if (ftrac.proc_stream == NULL) {
		ERROR("open file %s failed\n", file);
		exit(1);
	}

	if (ftrac.environ) {
		ftrac.environ_vars = g_strsplit(ftrac.environ, ":", 0);
		ftrac.environ_nvars = g_strv_length(ftrac.environ_vars);
	}

	proctab_init();

#if FTRAC_TRACE_PROC_TASKSTAT
	taskstat_init();
#endif
	
}

static void proc_logging_destroy(void)
{

#if FTRAC_TRACE_PROC_TASKSTAT
	taskstat_destroy();
#endif

	proctab_destroy();

	if (ftrac.environ)
		g_strfreev(ftrac.environ_vars);

	fclose(ftrac.proc_stream);
}

static void proc_logging(int sysc, struct timespec *stamp, pid_t pid)
{
	/* parent process may already exit */
	if (pid == 0 || procfs_pid_exists(pid) == -1)
		return;
	
	struct proctab_entry *proc = proctab_lookup(pid);
	/* Be careful about race condition here */
	if (proc) {
		int cmd_updated = 0, env_updated = 0;
		pthread_mutex_lock(&proc->lock);
		if (proc->n_cmdchk > 0) {
			char *cmdline = procfs_get_cmdline(pid);
			if (cmdline && proc->cmdline && strlen(cmdline) > 0 &&
				g_strcmp0(cmdline, proc->cmdline)) {
				g_free(proc->cmdline);
				proc->cmdline = cmdline;
				cmd_updated = 1;
			} else 
				g_free(cmdline);
		}
		if (proc->n_envchk > 0) {
			char *environ = procfs_get_environ(pid);
			if (environ && proc->environ && strlen(environ) > 0 &&
				strcmp(environ, proc->environ)) {
				g_free(proc->environ);
				proc->environ = environ;
				env_updated = 1;
			} else
				g_free(environ);
		}
		procfs_get_stat(pid, &proc->stat);
		proc->stamp = util_get_timespec(stamp);
		pthread_mutex_unlock(&proc->lock);
		
		/* keep a record when updated */
		if (cmd_updated && env_updated)
			proc_logging_write(proc, PROC_LOG_CMD_ENV_UPDATED);
		else if (cmd_updated)
			proc_logging_write(proc, PROC_LOG_CMDLINE_UPDATED);
		else if (env_updated)
			proc_logging_write(proc, PROC_LOG_ENVIRON_UPDATED);

	} else {
		pthread_mutex_lock(&ftrac.proctab.lock);
		proc = proctab_lookup(pid);
		if (proc) {
			DEBUG("Other thread already inserted process %d\n", pid);
			pthread_mutex_unlock(&ftrac.proctab.lock);
			return;
		} else {
			/* Ready to insert new entry */
			proc = g_malloc(sizeof(struct proctab_entry));
			pid_t *key = g_malloc(sizeof(pid_t));
			*key = pid;
			g_hash_table_insert(ftrac.proctab.table, key, proc);
		}
		pthread_mutex_unlock(&ftrac.proctab.lock);
		
		/* Only one thread should reach here */
		pthread_mutex_init(&proc->lock, NULL);
		proc->environ = procfs_get_environ(pid);
		proc->n_envchk = proc->environ == NULL ? -1 : 1;
		proc->cmdline = procfs_get_cmdline(pid);
		proc->n_cmdchk = 1;
		proc->live = 1;
		proc->traced = 0;
		procfs_get_stat(pid, &proc->stat);
		proc->stamp = util_get_timespec(stamp);
		
		proc_logging_write(proc, PROC_LOG_INIT);
		proc_logging(sysc, stamp, proc->stat.ppid);
	}	
}

#if FTRAC_TRACE_PROC_TASKSTAT
/*
 * Generic macros for dealing with netlink sockets. Might be duplicated
 * elsewhere. It is recommended that commercial grade applications use
 * libnl or libnetlink and use the interfaces provided by the library
 */
#define GENLMSG_DATA(glh)       ((void *)(NLMSG_DATA(glh) + GENL_HDRLEN))
#define GENLMSG_PAYLOAD(glh)    (NLMSG_PAYLOAD(glh, 0) - GENL_HDRLEN)
#define NLA_DATA(na)            ((void *)((char*)(na) + NLA_HDRLEN))
#define NLA_PAYLOAD(len)        (len - NLA_HDRLEN)

#define TASKSTAT_MAX_MSG_SIZE    4096   /* small buffer may lead to msg lost */
#define TASKSTAT_MAX_CPUS        32

struct msgtemplate {
	struct nlmsghdr n;
	struct genlmsghdr g;
	char buf[TASKSTAT_MAX_MSG_SIZE];
};

static int taskstat_nl_create(size_t recvbufsize)
{
	struct sockaddr_nl local;
	int fd, res = 0;

	fd = socket(AF_NETLINK, SOCK_RAW, NETLINK_GENERIC);
	if (fd < 0) {
		ERROR("create netlink socket: %s\n", strerror(errno));
		return -1;
	}
	
	if (recvbufsize) {
		res = setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &recvbufsize, 
			sizeof(recvbufsize));
		if (res < 0) {
			ERROR("set recieve buffer size %lu: %s\n", recvbufsize,
				strerror(errno));
			return -1;
		}
	}

	memset(&local, 0, sizeof(local));
	local.nl_family = AF_NETLINK;

	res = bind(fd, (struct sockaddr *) &local, sizeof(local));
	if (res < 0) {
		ERROR("bind socket: %s\n", strerror(errno));
		close(fd);
		return -1;
	}
	return fd;
}

int taskstat_nl_send(int sd, __u16 nlmsg_type, __u32 nlmsg_pid,
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

static int taskstat_get_familyid(int sd)
{
	int id = 0, rc;
	struct nlattr *na;
	int rep_len;
	char name[100];
	
	struct msgtemplate ans;

	strncpy(name, TASKSTATS_GENL_NAME, 100);
	rc = taskstat_nl_send(sd, GENL_ID_CTRL, getpid(), CTRL_CMD_GETFAMILY,
			CTRL_ATTR_FAMILY_NAME, (void *)name,
			strlen(TASKSTATS_GENL_NAME)+1);
	
	rep_len = recv(sd, &ans, sizeof(ans), 0);
	if (ans.n.nlmsg_type == NLMSG_ERROR ||
	    (rep_len < 0) || !NLMSG_OK((&ans.n), (unsigned) rep_len))
		return 0;
	
	int dummy;	/* To avoid strict-aliasing warning */
	memset(&dummy, 0, sizeof(int));
	na = (struct nlattr *) (GENLMSG_DATA(&ans) + dummy);
	na = (struct nlattr *) ((char *) na + NLA_ALIGN(na->nla_len));
	if (na->nla_type == CTRL_ATTR_FAMILY_ID) {
		id = *(__u16 *) NLA_DATA(na);
	}
	return id;
}

static void taskstat_logging(pid_t tid, struct taskstats *st, int live)
{
	struct proctab_entry *proc = proctab_lookup(tid);
	if (!proc)
		return;
	
	/* Thread exit */
	if (st->ac_pid == 0 && st->ac_btime == 0) {
		DEBUG("taskstat: last thread of task group %d exited.\n", tid);
		return;
	}
	
	/* TAG: file_format */
	fprintf(ftrac.taskstat_stream, 
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

static int taskstat_nl_recv(int sock, int live)
{
	struct msgtemplate msg;
	struct nlattr *na;
	int rep_len, res, i;
	int len, len2, aggr_len, count;
	pid_t rtid = 0;
	
	res = recv(sock, &msg, sizeof(msg), 0);
	if (res < 0) {
		ERROR("receive error: %d\n", errno);
		return 0;
	}
	if (msg.n.nlmsg_type == NLMSG_ERROR ||
		!NLMSG_OK((&msg.n), (unsigned) res)) {
		struct nlmsgerr *error = NLMSG_DATA(&msg);
		ERROR("receive error: %d\n", error->error);
		return 1;
	}
	
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
						taskstat_logging(rtid, 
							(struct taskstats *) NLA_DATA(na), live);
						break;
					default:
						ERROR("unknown nested nla_type %d\n", na->nla_type);
						break;
					}
					len2 += NLA_ALIGN(na->nla_len);
					na = (struct nlattr *) ((char *) na + len2);
				}
				break;
			default:
				ERROR("unknown nla_type %d\n", na->nla_type);
				break;
		}
		na = (struct nlattr *) (GENLMSG_DATA(&msg) + len);
	}
	return 0;
}

static void taskstat_query(void *key, void *value, void *data)
{
	int sock, *sockp, res;
	pid_t *pidp, pid;
	pidp = (pid_t *) key;
	pid = *pidp;
	sockp = (int *) data;
	sock = *sockp;
	struct proctab_entry *entry = (struct proctab_entry *) value;
	
	if (entry->live) {
		res = taskstat_nl_send(sock, ftrac.familyid, ftrac.pid,
			TASKSTATS_CMD_GET, TASKSTATS_CMD_ATTR_PID, &pid, sizeof(__u32));
		if (res < 0)
			ERROR("netlink send for %d\n", ftrac.pid);
		
		res = taskstat_nl_recv(sock, 1);
	}
}

static void * taskstat_process(void *data)
{
	int err = 0;
	struct netlink_channel *nl = (struct netlink_channel *) data;

	do {
		err = taskstat_nl_recv(nl->sock, 0);
	} while (!err);

	err ? pthread_exit((void *) 1) : pthread_exit((void *) 0);	
}

static void taskstat_init(void)
{
	int res, i;
	pthread_t thread_id;
	char file[PATH_MAX];
	
	assert(ftrac.logdir);
	memset(file, 0, PATH_MAX);
	snprintf(file, PATH_MAX, "%s/taskstat.log", ftrac.logdir);
	ftrac.taskstat_stream = fopen(file, "wb");
	if (ftrac.taskstat_stream == NULL) {
		ERROR("open file %s failed\n", file);
		exit(1);
	}

	ftrac.ncpus = procfs_get_cpus_num();

	/* properly set the number of listeners
	To avoid losing statistics, userspace should do one or more of the
	following:
		- increase the receive buffer sizes for the netlink sockets opened by
		  listeners to receive exit data.
		- create more listeners and reduce the number of cpus being 
		  listened to by each listener. In the extreme case, there could 
		  be one listener for each cpu. Users may also consider setting 
		  the cpu affinity of the
		  listener to the subset of cpus to which it listens, especially if
		  they are listening to just one cpu. */
	if (ftrac.nlsock == 0 || ftrac.nlsock > ftrac.ncpus)
		ftrac.nlsock = ftrac.ncpus;
	
	DEBUG("taskstat: cpus=%d, nlsocks=%d\n", ftrac.ncpus, ftrac.nlsock);

	/* create netlink socket array */
	ftrac.nlarr = g_new0(struct netlink_channel, ftrac.nlsock);
	for (i = 0; i < ftrac.nlsock; i++) {
		ftrac.nlarr[i].sock = taskstat_nl_create(ftrac.nlbufsize);
		if (ftrac.nlarr[i].sock < 0) {
			ERROR("create the %dth netlink socket\n", i);
			exit(1);
		}
	}

	ftrac.familyid = taskstat_get_familyid(ftrac.nlarr[0].sock);

	/* assign cpus to listeners */
	int q = ftrac.ncpus / ftrac.nlsock;
	int r = ftrac.ncpus % ftrac.nlsock;
	int start = 0, end;
	for (i = 0; i < ftrac.nlsock; i++) { /* figure out the assignment */
		end = start + q; 
		if (r > 0) { 
			end += 1; 
			r -= 1; 
		} 
		if (start == end - 1) 
			snprintf(ftrac.nlarr[i].cpumask, 32, "%d", start);
		else 
			snprintf(ftrac.nlarr[i].cpumask, 32, "%d-%d", start, end-1);
		
		DEBUG("taskstat: cpumask[%d]: %s\n", i, ftrac.nlarr[i].cpumask);
		
		res = taskstat_nl_send(ftrac.nlarr[i].sock, ftrac.familyid, ftrac.pid, 
			TASKSTATS_CMD_GET, TASKSTATS_CMD_ATTR_REGISTER_CPUMASK, 
			&ftrac.nlarr[i].cpumask, strlen(ftrac.nlarr[i].cpumask) + 1);
		if (res < 0) {
			ERROR("register cpumask: %s\n", strerror(errno));
			exit(1);
		}
		start = end;
		
		/* start listening thread */
		res = pthread_create(&thread_id, NULL, taskstat_process, 
			(void *) &ftrac.nlarr[i]);
		if (res != 0) {
			ERROR("failed to create thread, %s\n", strerror(res));
			exit(1);
		}
		pthread_detach(thread_id);
		ftrac.nlarr[i].thread = thread_id;
	}
}

static void taskstat_destroy(void)
{
	int res, i;
	
	for (i = 0; i < ftrac.nlsock; i++) {
		res = taskstat_nl_send(ftrac.nlarr[i].sock, ftrac.familyid, ftrac.pid, 
			TASKSTATS_CMD_GET, TASKSTATS_CMD_ATTR_DEREGISTER_CPUMASK, 
			&(ftrac.nlarr[i].cpumask), strlen(ftrac.nlarr[i].cpumask) + 1);
		if (res < 0)
			ERROR("deregister cpumask: %s\n", strerror(errno));
		close(ftrac.nlarr[i].sock);
		
		res = pthread_cancel(ftrac.nlarr[i].thread);
		if (res != 0)
			ERROR("failed to cancel thread[%d]\n", i);
	}

	g_free(ftrac.nlarr);
	
	/* log all living processes */
	int sock = taskstat_nl_create(ftrac.nlbufsize);
	if (sock < 0) {
		ERROR("create netlink socket with bufsize=%lu: %s\n",
			ftrac.nlbufsize, strerror(errno));
		return;
	}
	g_hash_table_foreach(ftrac.proctab.table, taskstat_query, &sock);
	close(sock);

	fclose(ftrac.taskstat_stream);
}
#endif /* FTRAC_TRACE_PROC_TASKSTAT */

#endif /* FTRAC_TRACE_ENABLED && FTRAC_TRACE_PROC_ENABLED */

/*
 * Runtime logging routines
 */
static void runtime_init(void)
{
	struct passwd *pwd;
	char file[PATH_MAX];
//	int res;
    
	srand(time(NULL) + getpid());
	
	TIMING(ftrac.itime);
	ftrac.pid = getpid();
	ftrac.uid = getuid();
	if (!(pwd = getpwuid(ftrac.uid))) {
		ERROR("failed to get username for uid %d\n", ftrac.uid);
		exit(1);
	}
    ftrac.username = g_strdup(pwd->pw_name);
	ftrac.gid = pwd->pw_gid;
	ftrac.cmdline = procfs_get_cmdline(ftrac.pid);
	if (uname(&ftrac.platform) == -1) {
		ERROR("warning: failed to get platform info: %s\n", 
			strerror(errno));
	}
	ftrac.sys_clk_tck = sysconf(_SC_CLK_TCK);
	if (ftrac.sys_clk_tck == -1) {
		ERROR("get _SC_CLK_TK: %s\n", strerror(errno));
		ftrac.sys_clk_tck = 100;
	}
	ftrac.sys_btime = procfs_get_sys_btime();
	if (ftrac.sys_btime == -1) {
		ERROR("get system btime %s\n", strerror(errno));
		exit(1);
	}
	
	if (!ftrac.foreground || ftrac.dump) {
		memset(file, 0, PATH_MAX);
		snprintf(file, PATH_MAX, "%s/error.log", ftrac.logdir);
		ftrac.err_stream = fopen(file, "wb");
		if (ftrac.err_stream == NULL) {
			ERROR("open file %s: %s\n", file, strerror(errno));
			exit(1);
		}
	}
	
	memset(file, 0, PATH_MAX);
	snprintf(file, PATH_MAX, "%s/runtime.log", ftrac.logdir);
	ftrac.runtime_stream = fopen(file, "wb");
	if (ftrac.runtime_stream == NULL) {
		ERROR("open file %s: %s", file, strerror(errno));
		exit(1);
	}
	
	/* logging runtime */
	fprintf(ftrac.runtime_stream,
		"version:%s\n"
		"platform:%s %s %s\n"
		"hostname:%s\n"
		"clktck:%lu\n"
		"sysbtime:%lu\n"
		"cmdline:%s\n"
		"mountpoint:%s\n"
		"user:%s\n"
		"uid:%d\n"
		"gid:%d\n"
		"pid:%d\n"
		"iid:%d\n"
		"start:%.9f\n"
		,
		PACKAGE_VERSION,
		ftrac.platform.sysname, ftrac.platform.release, 
		ftrac.platform.version, ftrac.platform.nodename,
		ftrac.sys_clk_tck,
		ftrac.sys_btime,
		ftrac.cmdline,
		ftrac.mntpoint,
		ftrac.username,
		ftrac.uid,
		ftrac.gid,
		ftrac.pid,
		ftrac.iid,
		util_get_timespec(&ftrac.itime)
		);
	fflush(ftrac.runtime_stream);
	
}

static void runtime_destroy(void)
{
	struct timespec stamp; 
	
	g_free(ftrac.cwd);
	g_free(ftrac.cmdline);
	g_free(ftrac.username);
	g_free(ftrac.mntpoint);
	
	if (!ftrac.foreground || ftrac.dump)
		fclose(ftrac.err_stream);

	TIMING(stamp);
	fprintf(ftrac.runtime_stream, "end:%.9f", util_get_timespec(&stamp));
	fclose(ftrac.runtime_stream);
}

/*
 * FUSE routines
 */

#if FTRAC_TRACE_SYSC_ENABLED
#define SYSC_LOGGING \
	sysc_logging(_SYSC, &start, &end, ctxt->pid, res, path)
#define SYSC_LOGGING_LINK \
	sysc_logging_link(_SYSC, &start, &end, ctxt->pid, res, from, to)
#define SYSC_LOGGING_OPENCLOSE \
	sysc_logging_openclose(_SYSC, &start, &end, ctxt->pid, res, path);
#define SYSC_LOGGING_IO \
	sysc_logging_io(_SYSC, &start, &end, ctxt->pid, res, path, bytes, offset);
#else
#define SYSC_LOGGING do {(void) ctxt;} while (0)
#define SYSC_LOGGING_LINK do {(void) ctxt;} while (0)
#define SYSC_LOGGING_OPENCLOSE do {(void) ctxt;} while (0)
#define SYSC_LOGGING_IO do {(void) ctxt; (void) bytes;} while (0)
#endif

#if FTRAC_TRACE_PROC_ENABLED
#define PROC_LOGGING proc_logging(_SYSC, &end, ctxt->pid)
#else
#define PROC_LOGGING do {(void) ctxt;} while (0)
#endif

#define _SYSC SYSC_FS_LSTAT
static int ftrac_getattr(const char *path, struct stat *stbuf)
{
	int res, orig_errno;

#if FTRAC_TRACE_ENABLED
	struct timespec start, end;
	struct fuse_context *ctxt = fuse_get_context();
	TIMING(start);
#endif
	
	res = lstat(path, stbuf);
	orig_errno = errno;
	
#if FTRAC_TRACE_ENABLED
	TIMING(end);
	SYSC_LOGGING;
	PROC_LOGGING;
#endif
	
	return res == -1 ? -orig_errno : 0;
}

#undef _SYSC
#define _SYSC SYSC_FS_FSTAT
static int ftrac_fgetattr(const char *path, struct stat *stbuf,
	struct fuse_file_info *fi)
{
	int fd, res, orig_errno;
	(void) path;

#if FTRAC_TRACE_ENABLED
	struct timespec start, end;
	struct fuse_context *ctxt = fuse_get_context();
	struct ftrac_filep *filep = (struct ftrac_filep *) (uintptr_t) fi->fh;
	fd = filep->fd;
	TIMING(start);
#else
	fd = fi->fh;
#endif
	
	res = fstat(fd, stbuf);
	orig_errno = errno;
	
#if FTRAC_TRACE_ENABLED
	TIMING(end);
	SYSC_LOGGING;
	PROC_LOGGING;
#endif

	return res == -1 ? -orig_errno : 0;
}

#undef _SYSC
#define _SYSC SYSC_FS_ACCESS
static int ftrac_access(const char *path, int mask)
{
	int res, orig_errno;

#if FTRAC_TRACE_ENABLED
	struct timespec start, end;
	struct fuse_context *ctxt = fuse_get_context();
	TIMING(start);
#endif
    
	res = access(path, mask);
	orig_errno = errno;

#if FTRAC_TRACE_ENABLED
	TIMING(end);
	SYSC_LOGGING;
	PROC_LOGGING;
#endif

	return res == -1 ? -orig_errno : 0;
}

#undef _SYSC
#define _SYSC SYSC_FS_READLINK
static int ftrac_readlink(const char *path, char *buf, size_t size)
{
	int res, orig_errno;

#if FTRAC_TRACE_ENABLED
	struct timespec start, end;
	struct fuse_context *ctxt = fuse_get_context();
	TIMING(start);
#endif
	
	res = readlink(path, buf, size - 1);
	orig_errno = errno;
	
#if FTRAC_TRACE_ENABLED
	TIMING(end);
	SYSC_LOGGING;
	PROC_LOGGING;
#endif

	if (res == -1)
		return -orig_errno;

	buf[res] = '\0';
	return 0;
}

#undef _SYSC
#define _SYSC SYSC_FS_OPENDIR
static int ftrac_opendir(const char *path, struct fuse_file_info *fi)
{
	int orig_errno;

#if FTRAC_TRACE_ENABLED
	int res;
	struct timespec start, end;
	struct fuse_context *ctxt = fuse_get_context();
	TIMING(start);
#endif
	
	struct ftrac_dirp *dirp = g_malloc(sizeof(struct ftrac_dirp));
	dirp->dp = opendir(path);
	orig_errno = errno;

#if FTRAC_TRACE_ENABLED
	TIMING(end);
	res = dirp->dp == NULL ? -1 : 0;
	SYSC_LOGGING;
	PROC_LOGGING;
#endif

	if (dirp->dp == NULL) {
		g_free(dirp);
		return -orig_errno;
	}
	dirp->offset = 0;
	dirp->entry = NULL;
	
	fi->fh = (unsigned long) dirp;
	return 0;
}

#undef _SYSC
#define _SYSC SYSC_FS_READDIR
static int ftrac_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
	off_t offset, struct fuse_file_info *fi)
{
	int res = 0;
	struct ftrac_dirp *dirp = (struct ftrac_dirp *) (uintptr_t) fi->fh;
	struct stat st;
	off_t nextoff;	
	(void) path;

#if FTRAC_TRACE_ENABLED
	struct timespec start, end;
	struct fuse_context *ctxt = fuse_get_context();
#endif
	
	if (offset != dirp->offset) {
		seekdir(dirp->dp, offset);
		dirp->entry = NULL;
		dirp->offset = offset;
	}

	while (1) {
#if FTRAC_TRACE_ENABLED
		TIMING(start);
#endif
		if (!dirp->entry) {
			dirp->entry = readdir(dirp->dp);
			if (!dirp->entry)
				break;
		}

#if FTRAC_TRACE_ENABLED
		TIMING(end);
		SYSC_LOGGING;
		PROC_LOGGING;
#endif
		memset(&st, 0, sizeof(struct stat));
		st.st_ino = dirp->entry->d_ino;
		st.st_mode = dirp->entry->d_type << 12;
		nextoff = telldir(dirp->dp);
		if (filler(buf, dirp->entry->d_name, &st, nextoff))
			break;
		
		dirp->entry = NULL;
		dirp->offset = nextoff;
	}
	
	if (res != 0)
		ERROR("readdir failed: %s\n", strerror(res));
	
	return 0;
}

#undef _SYSC
#define _SYSC SYSC_FS_CLOSEDIR
static int ftrac_releasedir(const char *path, struct fuse_file_info *fi)
{
	struct ftrac_dirp *dirp = (struct ftrac_dirp *) (uintptr_t) fi->fh;
	(void) path;

#if FTRAC_TRACE_ENABLED
	struct timespec start, end;
	struct fuse_context *ctxt = fuse_get_context();
	TIMING(start);
#endif

	closedir(dirp->dp);

#if FTRAC_TRACE_ENABLED
	TIMING(end);
  #if FTRAC_TRACE_SYSC_ENABLED
	sysc_logging(_SYSC, &start, &end, ctxt->pid, 0, path);
  #else
  	do { (void) ctxt; } while (0);
  #endif
	PROC_LOGGING;
#endif

	g_free(dirp);
	
	return 0;
}

#undef _SYSC
#define _SYSC SYSC_FS_MKNOD
static int ftrac_mknod(const char *path, mode_t mode, dev_t rdev)
{
	int res, orig_errno;

#if FTRAC_TRACE_ENABLED
	struct timespec start, end;
	struct fuse_context *ctxt = fuse_get_context();
	TIMING(start);
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

#if FTRAC_TRACE_ENABLED
	TIMING(end);
	SYSC_LOGGING;
	PROC_LOGGING;
#endif

	return res == -1 ? -orig_errno : 0;
}

#undef _SYSC
#define _SYSC SYSC_FS_MKDIR
static int ftrac_mkdir(const char *path, mode_t mode)
{
	int res, orig_errno;

#if FTRAC_TRACE_ENABLED
	struct timespec start, end;
	struct fuse_context *ctxt = fuse_get_context();
	TIMING(start);
#endif
	
	res = mkdir(path, mode);
	orig_errno = errno;

#if FTRAC_TRACE_ENABLED
	TIMING(end);
	SYSC_LOGGING;
	PROC_LOGGING;
#endif
	
	return res == -1 ? -orig_errno : 0;
}

#undef _SYSC
#define _SYSC SYSC_FS_UNLINK
static int ftrac_unlink(const char *path)
{
	int res, orig_errno;

#if FTRAC_TRACE_ENABLED
	struct timespec start, end;
	struct fuse_context *ctxt = fuse_get_context();
	TIMING(start);
#endif
	
	res = unlink(path);
	orig_errno = errno;
	
#if FTRAC_TRACE_ENABLED
	TIMING(end);
	SYSC_LOGGING;
	PROC_LOGGING;
#endif

	return res == -1 ? -orig_errno : 0;
}

#undef _SYSC
#define _SYSC SYSC_FS_RMDIR
static int ftrac_rmdir(const char *path)
{
	int res, orig_errno;

#if FTRAC_TRACE_ENABLED
	struct timespec start, end;
	struct fuse_context *ctxt = fuse_get_context();
	TIMING(start);
#endif
	
	res = rmdir(path);
	orig_errno = errno;
	
#if FTRAC_TRACE_ENABLED
	TIMING(end);
	SYSC_LOGGING;
	PROC_LOGGING;
#endif

	return res == -1 ? -orig_errno : 0;
}

#undef _SYSC
#define _SYSC SYSC_FS_SYMLINK
static int ftrac_symlink(const char *from, const char *to)
{
	int res, orig_errno;

#if FTRAC_TRACE_ENABLED
	struct timespec start, end;
	struct fuse_context *ctxt = fuse_get_context();
	TIMING(start);
#endif
	
	res = symlink(from, to);
	orig_errno = errno;

#if FTRAC_TRACE_ENABLED
	TIMING(end);
	SYSC_LOGGING_LINK;
	PROC_LOGGING;
#endif

	return res == -1 ? -orig_errno : 0;
}

#undef _SYSC
#define _SYSC SYSC_FS_RENAME
static int ftrac_rename(const char *from, const char *to)
{
	int res, orig_errno;

#if FTRAC_TRACE_ENABLED
	struct timespec start, end;
	struct fuse_context *ctxt = fuse_get_context();
	TIMING(start);
#endif
    
	res = rename(from, to);
	orig_errno = errno;
	
#if FTRAC_TRACE_ENABLED
	TIMING(end);
	SYSC_LOGGING_LINK;
	PROC_LOGGING;
#endif

	return res == -1 ? -orig_errno : 0;
}

#undef _SYSC
#define _SYSC SYSC_FS_LINK
static int ftrac_link(const char *from, const char *to)
{
	int res, orig_errno;

#if FTRAC_TRACE_ENABLED
	struct timespec start, end;
	struct fuse_context *ctxt = fuse_get_context();
	TIMING(start);
#endif
    
	res = link(from, to);
	orig_errno = errno;
	
#if FTRAC_TRACE_ENABLED
	TIMING(end);
	SYSC_LOGGING_LINK;
	PROC_LOGGING;
#endif
	
	return res == -1 ? -orig_errno : 0;
}

#undef _SYSC
#define _SYSC SYSC_FS_CHMOD
static int ftrac_chmod(const char *path, mode_t mode)
{
	int res, orig_errno;

#if FTRAC_TRACE_ENABLED
	struct timespec start, end;
	struct fuse_context *ctxt = fuse_get_context();
	TIMING(start);
#endif
	
	res = chmod(path, mode);
	orig_errno = errno;
	
#if FTRAC_TRACE_ENABLED
	TIMING(end);
	SYSC_LOGGING;
	PROC_LOGGING;
#endif

	return res == -1 ? -orig_errno : 0;
}

#undef _SYSC
#define _SYSC SYSC_FS_CHOWN
static int ftrac_chown(const char *path, uid_t uid, gid_t gid)
{
	int res, orig_errno;

#if FTRAC_TRACE_ENABLED
	struct timespec start, end;
	struct fuse_context *ctxt = fuse_get_context();
	TIMING(start);
#endif
	
	res = lchown(path, uid, gid);
	orig_errno = errno;
	
#if FTRAC_TRACE_ENABLED
	TIMING(end);
	SYSC_LOGGING;
	PROC_LOGGING;
#endif

	return res == -1 ? -orig_errno : 0;
}

#undef _SYSC
#define _SYSC SYSC_FS_TRUNCATE
static int ftrac_truncate(const char *path, off_t size)
{
	int res, orig_errno;

#if FTRAC_TRACE_ENABLED
	struct timespec start, end;
	struct fuse_context *ctxt = fuse_get_context();
	TIMING(start);
#endif
    
	res = truncate(path, size);
	orig_errno = errno;
	
#if FTRAC_TRACE_ENABLED
	TIMING(end);
	SYSC_LOGGING;
	PROC_LOGGING;
#endif

	return res == -1 ? -orig_errno : 0;
}

#undef _SYSC
#define _SYSC SYSC_FS_TRUNCATE
static int ftrac_ftruncate(const char *path, off_t size,
	struct fuse_file_info *fi)
{
	int fd, res, orig_errno;
	(void) path;

#if FTRAC_TRACE_ENABLED
	struct timespec start, end;
	struct fuse_context *ctxt = fuse_get_context();
	struct ftrac_filep *filep = (struct ftrac_filep *) (uintptr_t) fi->fh;
	fd = filep->fd;
	TIMING(start);
#else
	fd = fi->fh;
#endif
    
	res = ftruncate(fd, size);
	orig_errno = errno;
	
#if FTRAC_TRACE_ENABLED
	TIMING(end);
	SYSC_LOGGING;
	PROC_LOGGING;
#endif

	return res == -1 ? -orig_errno : 0;
}

#undef _SYSC
#define _SYSC SYSC_FS_UTIME
#if FUSE_VERSION >= 26
static int ftrac_utimens(const char *path, const struct timespec ts[2])
{
	struct timeval tv[2];
	int res, orig_errno;

#if FTRAC_TRACE_ENABLED
	struct timespec start, end;
	struct fuse_context *ctxt = fuse_get_context();
	TIMING(start);
#endif
	
	tv[0].tv_sec = ts[0].tv_sec;
	tv[0].tv_usec = ts[0].tv_nsec / 1000;
	tv[1].tv_sec = ts[1].tv_sec;
	tv[1].tv_usec = ts[1].tv_nsec / 1000;
	res = utimes(path, tv);
	orig_errno = errno;

#if FTRAC_TRACE_ENABLED
	TIMING(end);
	SYSC_LOGGING;
	PROC_LOGGING;
#endif

	return res == -1 ? -orig_errno : 0;
}
#else
static int ftrac_utime(const char *path, struct utimbuf *buf)
{
	int res, orig_errno;

#if FTRAC_TRACE_ENABLED
	struct timespec start, end;
	struct fuse_context *ctxt = fuse_get_context();
	TIMING(start);
#endif
    
	res = utime(path, buf);
	orig_errno = errno;

#if FTRAC_TRACE_ENABLED
	TIMING(end);
	SYSC_LOGGING;
	PROC_LOGGING;
#endif
	
	return res == -1 ? -orig_errno : 0;
}
#endif

#undef _SYSC
#define _SYSC SYSC_FS_CREAT
static int ftrac_create(const char *path, mode_t mode, 
	struct fuse_file_info *fi)
{
	int fd, orig_errno;

#if FTRAC_TRACE_ENABLED
	int res;
	struct timespec start, end;
	struct fuse_context *ctxt = fuse_get_context();
	TIMING(start);
#endif
    
	fd = open(path, fi->flags, mode);
	orig_errno = errno;
	
#if FTRAC_TRACE_ENABLED
	TIMING(end);
	res = fd == -1 ? -1 : 0;
	SYSC_LOGGING;
	PROC_LOGGING;
#endif

	if (fd == -1)
		return -orig_errno;

#if FTRAC_TRACE_ENABLED
	struct ftrac_filep *filep = g_malloc(sizeof(struct ftrac_filep));
	filep->fd = fd;
	filep->pid = ctxt->pid;
	fi->fh = (unsigned long) filep;
#else
	fi->fh = fd;
#endif
	
	return 0;
}

#undef _SYSC
#define _SYSC SYSC_FS_OPEN
static int ftrac_open(const char *path, struct fuse_file_info *fi)
{
	int fd, orig_errno;

#if FTRAC_TRACE_ENABLED
	int res;
	struct timespec start, end;
	struct fuse_context *ctxt = fuse_get_context();
	TIMING(start);
#endif
    
	fd = open(path, fi->flags);
	orig_errno = errno;
	
#if FTRAC_TRACE_ENABLED
	TIMING(end);
	res = fd == -1 ? -1 : 0;
	SYSC_LOGGING_OPENCLOSE;
	PROC_LOGGING;
#endif

	if (fd == -1)
		return -orig_errno;

#if FTRAC_TRACE_ENABLED
	struct ftrac_filep *filep = g_malloc(sizeof(struct ftrac_filep));
	filep->fd = fd;
	filep->pid = ctxt->pid;
	fi->fh = (unsigned long) filep;
#else
	fi->fh = fd;
#endif
	
	return 0;
}

#undef _SYSC
#define _SYSC SYSC_FS_READ
static int ftrac_read(const char *path, char *buf, size_t size, off_t offset,
	struct fuse_file_info *fi)
{
	int fd, res, orig_errno;
	(void) path;

#if FTRAC_TRACE_ENABLED
	struct timespec start, end;
	struct fuse_context *ctxt = fuse_get_context();
	struct ftrac_filep *filep = (struct ftrac_filep *) (uintptr_t) fi->fh;
	fd = filep->fd;
	TIMING(start);
#else
	fd = fi->fh;
#endif
	
	res = pread(fd, buf, size, offset);
	orig_errno = errno;
	
#if FTRAC_TRACE_ENABLED
	TIMING(end);
	
	/* should not log size in arguments, but actual read bytes */
	size_t bytes = res == -1 ? 0 : res;
	SYSC_LOGGING_IO;
	PROC_LOGGING;
#endif

	return res == -1 ? -orig_errno : res;
}

#undef _SYSC
#define _SYSC SYSC_FS_WRITE
static int ftrac_write(const char *path, const char *buf, size_t size,
	off_t offset, struct fuse_file_info *fi)
{
	int fd, res, orig_errno;
	(void) path;

#if FTRAC_TRACE_ENABLED
	struct timespec start, end;
	struct fuse_context *ctxt = fuse_get_context();
	struct ftrac_filep *filep = (struct ftrac_filep *) (uintptr_t) fi->fh;
	fd = filep->fd;
	TIMING(start);
#else
	fd = fi->fh;	
#endif
	
	res = pwrite(fd, buf, size, offset);
	orig_errno = errno;
	
#if FTRAC_TRACE_ENABLED
	TIMING(end);
	
	/* should log actual written bytes */
	size_t bytes = res == -1 ? 0 : res;
	SYSC_LOGGING_IO;
	PROC_LOGGING;
#endif

	return res == -1 ? -orig_errno : res;
}

#undef _SYSC
#define _SYSC SYSC_FS_STATFS
static int ftrac_statfs(const char *path, struct statvfs *stbuf)
{
	int res, orig_errno;

#if FTRAC_TRACE_ENABLED
	struct timespec start, end;
	struct fuse_context *ctxt = fuse_get_context();
	TIMING(start);
#endif
    
	res = statvfs(path, stbuf);
	orig_errno = errno;
	
#if FTRAC_TRACE_ENABLED
	TIMING(end);
	SYSC_LOGGING;
	PROC_LOGGING;
#endif
	
	return res == -1 ? -orig_errno : 0;
}

#undef _SYSC
#define _SYSC SYSC_FS_FLUSH
static int ftrac_flush(const char *path, struct fuse_file_info *fi)
{
	int fd, res, orig_errno;
	(void) path;
	
#if FTRAC_TRACE_ENABLED
	struct timespec start, end;
	struct fuse_context *ctxt = fuse_get_context();
	struct ftrac_filep *filep = (struct ftrac_filep *) (uintptr_t) fi->fh;
	fd = filep->fd;
	TIMING(start);
#else
	fd = fi->fh;
#endif
	
	/* This is called from every close on an open file, so call the
	   close on the underlying filesystem.	But since flush may be
	   called multiple times for an open file, this must not really
	   close the file.  This is important if used on a network
	   filesystem like NFS which flush the data/metadata on close() */
	res = close(dup(fd));
	orig_errno = errno;

#if FTRAC_TRACE_ENABLED
	TIMING(end);
	SYSC_LOGGING;
	PROC_LOGGING;
#endif

	return res == -1 ? -orig_errno : 0;
}

#undef _SYSC
#define _SYSC SYSC_FS_CLOSE
static int ftrac_release(const char *path, struct fuse_file_info *fi)
{
	int fd, res, orig_errno;
	(void) path;

#if FTRAC_TRACE_ENABLED
	struct timespec start, end;
	struct fuse_context *ctxt = fuse_get_context();
	struct ftrac_filep *filep = (struct ftrac_filep *) (uintptr_t) fi->fh;
	fd = filep->fd;
	TIMING(start);
#else
	fd = fi->fh;
#endif
	
	res = close(fd);
	orig_errno = errno;
	
#if FTRAC_TRACE_ENABLED
	TIMING(end);
	SYSC_LOGGING;
	PROC_LOGGING;
	g_free(filep);
#endif

	return res == -1 ? -orig_errno : 0;
}

#undef _SYSC
#define _SYSC SYSC_FS_FSYNC
static int ftrac_fsync(const char *path, int isdatasync,
	struct fuse_file_info *fi)
{
	int fd, res, orig_errno;
	(void) path;

#if FTRAC_TRACE_ENABLED
	struct timespec start, end;
	struct fuse_context *ctxt = fuse_get_context();
	struct ftrac_filep *filep = (struct ftrac_filep *) (uintptr_t) fi->fh;
	fd = filep->fd;
	TIMING(start);
#else
	fd = fi->fh;
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

#if FTRAC_TRACE_ENABLED
	TIMING(end);
	SYSC_LOGGING;
	PROC_LOGGING;
#endif
    
	return res == -1 ? -orig_errno : 0;
}

#ifdef HAVE_SETXATTR
/* xattr operations are optional and can safely be left unimplemented */

#undef _SYSC
#define _SYSC SYSC_FS_SETXATTR
static int ftrac_setxattr(const char *path, const char *name, 
	const char *value, size_t size, int flags)
{
	int res, orig_errno;

#if FTRAC_TRACE_ENABLED
	struct timespec start, end;
	struct fuse_context *ctxt = fuse_get_context();
	TIMING(start);
#endif
    
	res = lsetxattr(path, name, value, size, flags);
	orig_errno = errno;
	
#if FTRAC_TRACE_ENABLED
	TIMING(end);
	SYSC_LOGGING;
	PROC_LOGGING;
#endif

	return res == -1 ? -orig_errno : 0;
}

#undef _SYSC
#define _SYSC SYSC_FS_GETXATTR
static int ftrac_getxattr(const char *path, const char *name, char *value,
			size_t size)
{
	int res, orig_errno;

#if FTRAC_TRACE_ENABLED
	struct timespec start, end;
	struct fuse_context *ctxt = fuse_get_context();
	TIMING(start);
#endif
    
	res = lgetxattr(path, name, value, size);
	orig_errno = errno;
	
#if FTRAC_TRACE_ENABLED
	TIMING(end);
	SYSC_LOGGING;
	PROC_LOGGING;
#endif

	return res == -1 ? -orig_errno : res;
}

#undef _SYSC
#define _SYSC SYSC_FS_LISTXATTR
static int ftrac_listxattr(const char *path, char *list, size_t size)
{
	int res, orig_errno;

#if FTRAC_TRACE_ENABLED
	struct timespec start, end;
	struct fuse_context *ctxt = fuse_get_context();
	TIMING(start);
#endif
    
	res = llistxattr(path, list, size);
	orig_errno = errno;
	
#if FTRAC_TRACE_ENABLED
	TIMING(end);
	SYSC_LOGGING;
	PROC_LOGGING;
#endif

	return res == -1 ? -orig_errno : res;
}

#undef _SYSC
#define _SYSC SYSC_FS_REMOVEXATTR
static int ftrac_removexattr(const char *path, const char *name)
{
	int res, orig_errno;

#if FTRAC_TRACE_ENABLED
	struct timespec start, end;
	struct fuse_context *ctxt = fuse_get_context();
	TIMING(start);
#endif
	
	res = lremovexattr(path, name);
	orig_errno = errno;

#if FTRAC_TRACE_ENABLED
	TIMING(end);
	SYSC_LOGGING;
	PROC_LOGGING;
#endif

	return res == -1 ? -orig_errno : 0;
}
#endif /* HAVE_SETXATTR */

static int ftrac_lock(const char *path, struct fuse_file_info *fi,
	int cmd, struct flock *lock)
{
	(void) path;

	return ulockmgr_op(fi->fh, cmd, lock, &fi->lock_owner,
			   sizeof(fi->lock_owner));
}

#if FUSE_VERSION >= 26
static void * ftrac_init(struct fuse_conn_info *conn)
#else
static void * ftrac_init(void)
#endif
{
#if FUSE_VERSION >= 26
	(void) conn;
#endif
	
	runtime_init();

#if FTRAC_TRACE_ENABLED && FTRAC_TRACE_SYSC_ENABLED
	sysc_logging_init();
#endif

#if FTRAC_TRACE_ENABLED && FTRAC_TRACE_PROC_ENABLED
	proc_logging_init();
#endif

	return NULL;
}

static void ftrac_destroy(void *data_)
{
	(void) data_;

#if FTRAC_TRACE_ENABLED && FTRAC_TRACE_SYSC_ENABLED
	sysc_logging_destroy();
#endif

#if FTRAC_TRACE_ENABLED && FTRAC_TRACE_PROC_ENABLED
	proc_logging_destroy();
#endif
	
	runtime_destroy();
}

static struct fuse_operations ftrac_oper = {
	.init			= ftrac_init,
	.destroy		= ftrac_destroy,
	.getattr		= ftrac_getattr,
	.fgetattr		= ftrac_fgetattr,
	.access			= ftrac_access,
	.readlink		= ftrac_readlink,
	.opendir		= ftrac_opendir,
	.readdir		= ftrac_readdir,
	.releasedir		= ftrac_releasedir,
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
	.release		= ftrac_release,
	.fsync			= ftrac_fsync,
#ifdef HAVE_SETXATTR
	.setxattr		= ftrac_setxattr,
	.getxattr		= ftrac_getxattr,
	.listxattr		= ftrac_listxattr,
	.removexattr	= ftrac_removexattr,
#endif
	.lock			= ftrac_lock,

	.flag_nullpath_ok = 1,
};

/* 
 * Command arguments and main entry
 */
enum {
	KEY_HELP,
	KEY_VERSION,
	KEY_FOREGROUND,
};

#define FTRAC_OPT(t, p, v) { t, offsetof(struct ftrac, p), v }

static struct fuse_opt ftrac_opts[] = {
	FTRAC_OPT("logdir=%s",          logdir, 0),
	FTRAC_OPT("ftrac_debug",        debug, 1),
	FTRAC_OPT("dump",               dump, 1),

#if FTRAC_TRACE_ENABLED && FTRAC_TRACE_PROC_ENABLED
	FTRAC_OPT("cache_timeout=%d",   cache_timeout, 0),
	FTRAC_OPT("environ=%s",         environ, 0),
#endif
	
	FUSE_OPT_KEY("-V",              KEY_VERSION),
	FUSE_OPT_KEY("--version",       KEY_VERSION),
	FUSE_OPT_KEY("-h",              KEY_HELP),
	FUSE_OPT_KEY("--help",          KEY_HELP),
	FUSE_OPT_KEY("debug",           KEY_FOREGROUND),
	FUSE_OPT_KEY("-d",              KEY_FOREGROUND),
	FUSE_OPT_KEY("-f",              KEY_FOREGROUND),
	FUSE_OPT_END
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
"    -o logdir=PATH         log directory\n"
"    -o logbufsize=PATH     log buffer size\n"
"    -o notify_pid=PID      process to notify on entering fuse loop\n"
"    -o ftrac_debug         print debug information\n"
"    -o dump                redirect debug/error streams to log file\n"
"\nProcess logging options:\n"
"    -o cache_timeout=NUM   cache timeout for process table (60)\n"
"    -o environ=VAR:VAR     environment variables (all)\n"
"    -o stop_envchk         stop checking environ once updated (off)\n"
"    -o stop_cmdchk         stop checking cmdline once updated (off)\n"
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
			fprintf(stderr, "ftrac version %s\n", PACKAGE_VERSION);
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

int main(int argc, char * argv[])
{
	int libver, res;
	struct fuse_args args = FUSE_ARGS_INIT(argc, argv);
	char *fsname, *optstr;
	
	if (!g_thread_supported())
        g_thread_init(NULL);

	ftrac.progname = argv[0];
	ftrac.cwd = g_get_current_dir();
	ftrac.err_stream = stderr;
	
	if (fuse_opt_parse(&args, &ftrac, ftrac_opts, ftrac_opt_proc) == -1) {
		fuse_opt_free_args(&args);
		exit(1);
	}
	
	DEBUG("ParaTrac ftrac version: %s\n", PACKAGE_VERSION);
	
	if (fuse_parse_cmdline(&args, &(ftrac.mntpoint), NULL, NULL) == -1) {
		fuse_opt_free_args(&args);
		exit(1);
	}
	if (!ftrac.mntpoint) {
		fprintf(stderr, 
			"%s: missing mount point\n"
			"see `%s -h' for usage\n", 
			ftrac.progname, ftrac.progname);
		fuse_opt_free_args(&args);
		exit(1);
	}
	/* Insert mount point argument back,
	   mount point is already absolute path here */
	fuse_opt_insert_arg(&args, 1, ftrac.mntpoint);
	
	if (!ftrac.logdir) {
		ftrac.logdir = g_strdup_printf("%s/ftrac-%s-%u", ftrac.cwd, 
			ftrac.username, util_str_hash(ftrac.mntpoint));
	} else {
		char logdir[PATH_MAX];
		struct stat stbuf;
		if (realpath(ftrac.logdir, logdir) == NULL &&
			stat(ftrac.logdir, &stbuf) == 0) {
			ERROR("bad log directory %s\n", logdir);
			exit(1);
		}
		free(ftrac.logdir);
		ftrac.logdir = g_strdup(logdir);
	}
    res = mkdir(ftrac.logdir, S_IRUSR | S_IWUSR | S_IXUSR);
    if (res == -1 && errno != EEXIST) {
		ERROR("create directory %s, %s\n", ftrac.logdir, strerror(errno));
		exit(1);
    }
	
	
	fsname = g_strdup_printf("ftrac-%d", util_str_hash(ftrac.mntpoint));
#if FUSE_VERSION >= 27
	libver = fuse_version();
	assert(libver >= 27);
	/* Remove commas from fsname, 
	   because it many confuse fuse option parser */
	if (libver >= 28)
		fsname = fsname_escape_commas(fsname);
	else
		fsname_remove_commas(fsname);
	optstr = g_strdup_printf("-osubtype=fstrac,fsname=%s", fsname);
#else
	optstr = g_strdup_printf("-ofsname=fstrac#%s", fsname);
#endif
	fuse_opt_insert_arg(&args, 1, optstr);

	res = ftrac_fuse_main(&args);

	fuse_opt_free_args(&args);
	g_free(fsname);
	g_free(optstr);
	
	return res;
}
/* EOF */
