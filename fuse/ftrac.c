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
 * FUSE tool for file system calls tracking
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
#include <signal.h>
#include <pthread.h>
#include <signal.h>
#include <time.h>
#include <sys/time.h>
#include <sys/times.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <sys/utsname.h>
#ifdef HAVE_SETXATTR
#include <sys/xattr.h>
#endif
#include <glib.h>

#if FUSE_VERSION < 23
#error FUSE_VERSION < 23
#endif

#if GLIB_CHECK_VERSION(2, 16, 0)
#define GLIB_HASHTABLE_HAS_ITER
#endif

/* 
 * system call number
 * like the definitions in <syscall.h>
 * use paratrac its own definition
 */

#define SYSC_FS_LSTAT          84
#define SYSC_FS_FSTAT		   28
#define SYSC_FS_ACCESS         33
#define SYSC_FS_READLINK       85
#define	SYSC_FS_OPENDIR		   205
#define SYSC_FS_READDIR        89
#define SYSC_FS_CLOSEDIR	   206
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
#define SYSC_FS_CLOSE		   6
#define SYSC_FS_READ           3
#define SYSC_FS_WRITE          4
#define SYSC_FS_STATFS         99
#define SYSC_FS_FLUSH		   203
#define SYSC_FS_FSYNC          118
#ifdef HAVE_SETXATTR        
#define SYSC_FS_SETXATTR       201
#define SYSC_FS_GETXATTR       202
#define SYSC_FS_LISTXATTR      203
#define SYSC_FS_REMOVEXATTR    204
#endif

#define	PATH_PREFIX		"/tmp"
#define SERVER_MAX_CONN 8
#define SERVER_MAX_BUFF	256
#define MAX_FILENAME	255
#define MAX_LINE		1024

#define FTRAC_SIGNAL_INIT   SIGUSR1

#define FTRAC_CLOCK	CLOCK_REALTIME

/* Tracing code control */
#define FTRAC_TRACE_ENABLED
#define FTRAC_TRACE_LOG_BUFSIZE	1048576

#define CTRL_OK			0
#define CTRL_FINISH		1
#define CTRL_POLL_STAT	2
#define CTRL_FLUSH		3

/* system call statistics */
typedef struct stat_sc {
	double stamp;			/* last accessed time */
	unsigned long cnt;		/* total called count */
	double esum;			/* sum of all elapsed */
} * stat_sc_t;

/* input/output statistics */
typedef struct stat_io {
	double stamp;			/* last accessed time */
	unsigned long cnt;		/* total called count */
	double esum;			/* sum of all elapsed */

	unsigned long bytes;	/* total bytes */
} * stat_io_t;

/* filesystem global statistics */
typedef struct stat_fs {
	struct stat_sc lstat;
	struct stat_sc fstat;
	struct stat_sc access;
	struct stat_sc readlink;
	struct stat_sc opendir;
	struct stat_sc readdir;
	struct stat_sc closedir;
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
	struct stat_sc creat;
	struct stat_sc open;
	struct stat_sc statfs;
	struct stat_sc flush;
	struct stat_sc close;
	struct stat_sc fsync;
#ifdef HAVE_SETXATTR
	struct stat_sc setxattr;
	struct stat_sc getxattr;
	struct stat_sc listxattr;
	struct stat_sc removexattr;
#endif
	
	struct stat_io read;
	struct stat_io write;

} * stat_fs_t;

/* record table */
typedef struct hash_table {
	GHashTable *table;
	pthread_mutex_t lock;
} * hash_table_t;

/* used to pass pid, especially in close() and closedir() */
typedef struct ftrac_file {
	unsigned long fd;
	pid_t pid;
} * ftrac_file_t;

struct ftrac {
	char *progname;
	char *mountpoint;
	char *cwd;
	char *username;
	int foreground;
	uid_t uid;
	gid_t gid;
	pid_t pid;

	/* control server */
	char *sessiondir;
	char *sockpath;
	int sockfd;
	int serv_started;
	pthread_t serv_thread;

	/* statistics */
	struct stat_fs fs;

	/* logging */
	char *logdir;
	char *logbuf;
	size_t logbufsize;
	struct hash_table proctab;
	struct hash_table filetab;
	FILE *env;
	FILE *log;
	FILE *procmap;
	FILE *filemap;

    /* misc usage */
    pid_t notify_pid;
};

static struct ftrac ftrac;

#define FTRAC_OPT(t, p, v) { t, offsetof(struct ftrac, p), v }

enum {
	KEY_HELP,
	KEY_VERSION,
	KEY_FOREGROUND,
};

static struct fuse_opt ftrac_opts[] = {
	FTRAC_OPT("sessiondir=%s",      sessiondir, 0),
	FTRAC_OPT("logdir=%s",			logdir, 0),
	FTRAC_OPT("logbufsize=%lu",		logbufsize, 0),
	FTRAC_OPT("notify_pid=%d",      notify_pid, 0),

	FUSE_OPT_KEY("-V",			KEY_VERSION),
	FUSE_OPT_KEY("--version",	KEY_VERSION),
	FUSE_OPT_KEY("-h",			KEY_HELP),
	FUSE_OPT_KEY("--help", 		KEY_HELP),
	FUSE_OPT_KEY("debug",       KEY_FOREGROUND),
	FUSE_OPT_KEY("-d",          KEY_FOREGROUND),
	FUSE_OPT_KEY("-f",          KEY_FOREGROUND),
	FUSE_OPT_END
};

/* use self-defined string hash to release library dependency */
static unsigned int ftrac_string_hash(const char *str)
{
	unsigned int hash = 0;
	int i = 0;
	
	while (str[i]) {
		hash += str[i] * (i + 1);
		i++;
	}

	return hash;
}

/*********** process info routines **********/
static char * get_proc_cmdline(pid_t pid)
{
	size_t size, i;
	char line[MAX_LINE], *linep;
	char *filename = g_strdup_printf("/proc/%d/cmdline", pid);
	FILE *file = fopen(filename, "rb");
	if (file == NULL)
		fprintf(stderr, "open file %s failed\n", filename);
	
	size = fread(line, sizeof(char), MAX_LINE - 1, file);

	/* replace nulls and newlines in string */
	for (i=0; i < size; i++) {
		if (line[i] == '\0' || line[i] == '\n')
			line[i] = ' ';
	}
	if (i < MAX_LINE)
		line[i-1] = '\0';
	
	linep = g_strdup(line);
	
	fclose(file);
	g_free(filename);
	
	return linep;
}

static pid_t get_proc_ppid(pid_t pid)
{
	pid_t ppid;
	char line[MAX_LINE], *linep;
	char *filename = g_strdup_printf("/proc/%d/status", pid);
	FILE *file = fopen(filename, "rb");
	if (file == NULL)
		fprintf(stderr, "open file %s failed\n", filename);
	
	do {
		linep = fgets(line, MAX_LINE - 1, file);
		if (g_str_has_prefix(line, "PPid:"))
			break;
	} while (linep);
	

	if(sscanf(linep, "PPid:\t%d", &ppid) != 1)
		fprintf(stderr, "failed to get ppid of %d\n", pid);
	
	fclose(file);
	g_free(filename);
	
	return ppid;
}

/*********** hashtable processing routines **********/
static int proctab_init(hash_table_t hashtable)
{
	hashtable->table = g_hash_table_new_full(g_int_hash, g_int_equal,
		g_free, NULL);
	if (!hashtable->table) {
		fprintf(stderr, "failed to create hash table\n");
		return -1;
	}

	pthread_mutex_init(&hashtable->lock, NULL);
	return 0;
}

static void proctab_destroy(hash_table_t hashtable)
{
	g_hash_table_destroy(hashtable->table);
	pthread_mutex_destroy(&hashtable->lock);
}

static void proctab_insert(hash_table_t hashtable, int pid)
{
	if (pid == 0)	/* process 1 is init [2] */
		return;

	int *pidp = &pid;
	gpointer p = g_hash_table_lookup(hashtable->table, pidp);
	if (!p) {
		char *cmdline = get_proc_cmdline(pid);
		pid_t ppid = get_proc_ppid(pid);
		pidp = g_new(int, 1);
		*pidp = pid;
		pthread_mutex_lock(&hashtable->lock);
		g_hash_table_insert(hashtable->table, pidp, GINT_TO_POINTER(1));
		pthread_mutex_unlock(&hashtable->lock);
		fprintf(ftrac.procmap, "%d:%d:%s\n", pid, ppid, cmdline);

		/* recursively insert parent process to process table
		   this is useful to build workflow tree */
		proctab_insert(hashtable, ppid);
	}
	return;
}

static int filetab_init(hash_table_t hashtable)
{
	hashtable->table = g_hash_table_new_full(g_str_hash, g_str_equal,
		g_free, NULL);
	if (!hashtable->table) {
		fprintf(stderr, "failed to create hash table\n");
		return -1;
	}

	pthread_mutex_init(&hashtable->lock, NULL);
	return 0;
}

static void filetab_destroy(hash_table_t hashtable)
{
	g_hash_table_destroy(hashtable->table);
	pthread_mutex_destroy(&hashtable->lock);
}

static unsigned int filetab_lookup(hash_table_t hashtable, const char *path)
{
	gpointer p = g_hash_table_lookup(hashtable->table, path);
	if (!p) {
		pthread_mutex_lock(&hashtable->lock);
		guint size = g_hash_table_size(hashtable->table) + 1;
		g_hash_table_insert(hashtable->table, g_strdup(path), 
			GUINT_TO_POINTER(size));
		pthread_mutex_unlock(&hashtable->lock);
		fprintf(ftrac.filemap, "%u:%s\n", size, path);
		return size;
	}
	return GPOINTER_TO_UINT(p);
}

/*********** statistic processing routines **********/
#define get_timespec(timespec) ((double) timespec->tv_sec + \
	((double) timespec->tv_nsec) * 0.000000001)

#define get_elapsed(start, end) ((double) (end->tv_sec - start->tv_sec) + \
	(((double) (end->tv_nsec - start->tv_nsec)) * 0.000000001))

static inline void sc_log_common(stat_sc_t stat, int sysc, 
	struct timespec *start, struct timespec *end, int pid, int res,
	const char *path)
{
	double stamp = get_timespec(start);
	double elapsed = get_elapsed(start, end);
	unsigned long fid = filetab_lookup(&ftrac.filetab, path);
	
	/*
		logging
		csv format: stamp,pid,sysc,fid,res,elapsed
	*/
	fprintf(ftrac.log, "%.9f,%d,%d,%lu,%d,%.9f,0,0\n", stamp, pid, sysc, fid, 
		res, elapsed);
	proctab_insert(&ftrac.proctab, pid);

	/* internal statistics */
	stat->stamp = stamp;
	stat->cnt ++;
	stat->esum += elapsed;
}

static inline void sc_log_io(stat_io_t stat, int sysc, 
	struct timespec *start, struct timespec *end, int pid, int res,
	const char *path, size_t size, off_t offset)
{
	double stamp = get_timespec(start);
	double elapsed = get_elapsed(start, end);
	unsigned long fid = filetab_lookup(&ftrac.filetab, path);
	
	fprintf(ftrac.log, "%.9f,%d,%d,%lu,%d,%.9f,%lu,%lu\n", stamp, pid, sysc, 
		fid, res, elapsed, size, offset);
	proctab_insert(&ftrac.proctab, pid);

	stat->stamp = stamp;
	stat->cnt ++;
	stat->esum += elapsed;
	stat->bytes += size;
}

static inline void sc_log_link(stat_sc_t stat, int sysc, 
	struct timespec *start, struct timespec *end, int pid, int res,
	const char *from, const char *to)
{
	double stamp = get_timespec(start);
	double elapsed = get_elapsed(start, end);
	unsigned long fid_from = filetab_lookup(&ftrac.filetab, from);
	unsigned long fid_to = filetab_lookup(&ftrac.filetab, to);
	
	fprintf(ftrac.log, "%.9f,%d,%d,%lu,%d,%.9f,%lu,0\n", stamp, pid, sysc, 
		fid_from, res, elapsed, fid_to);
	proctab_insert(&ftrac.proctab, pid);

	stat->stamp = stamp;
	stat->cnt ++;
	stat->esum += elapsed;
}

static char * stat_fs_to_dictstr(stat_fs_t fs)
{
	char *buf = g_strdup_printf("{"
		"'lstat':(%.9f,%lu,%.9f),"
		"'fstat':(%.9f,%lu,%.9f),"
	 	"'access':(%.9f,%lu,%.9f),"
		"'readlink':(%.9f,%lu,%.9f),"
		"'opendir':(%.9f,%lu,%.9f),"
		"'readdir':(%.9f,%lu,%.9f),"
		"'closedir':(%.9f,%lu,%.9f),"
		"'mknod':(%.9f,%lu,%.9f),"
		"'mkdir':(%.9f,%lu,%.9f),"
		"'symlink':(%.9f,%lu,%.9f),"
		"'unlink':(%.9f,%lu,%.9f),"
		"'rmdir':(%.9f,%lu,%.9f),"
		"'rename':(%.9f,%lu,%.9f),"
		"'link':(%.9f,%lu,%.9f),"
		"'chmod':(%.9f,%lu,%.9f),"
		"'chown':(%.9f,%lu,%.9f),"
		"'truncate':(%.9f,%lu,%.9f),"
		"'utime':(%.9f,%lu,%.9f),"
		"'creat':(%.9f,%lu,%.9f),"
		"'open':(%.9f,%lu,%.9f),"
		"'statfs':(%.9f,%lu,%.9f),"
		"'flush':(%.9f,%lu,%.9f),"
		"'close':(%.9f,%lu,%.9f),"
		"'fsync':(%.9f,%lu,%.9f)"
#ifdef HAVE_SETXATTR
		",'setxattr':(%.9f,%lu,%.9f),"
		"'getxattr':(%.9f,%lu,%.9f),"
		"'listxattr':(%.9f,%lu,%.9f),"
		"'removexattr':(%.9f,%lu,%.9f)"
#endif
		",'read':(%.9f,%lu,%.9f,%lu),"
		"'write':(%.9f,%lu,%.9f,%lu)"
		"}",
		fs->lstat.stamp, fs->lstat.cnt, fs->lstat.esum,
		fs->fstat.stamp, fs->fstat.cnt, fs->fstat.esum,
		fs->access.stamp, fs->access.cnt, fs->access.esum,
		fs->readlink.stamp, fs->readlink.cnt, fs->readlink.esum,
		fs->opendir.stamp, fs->opendir.cnt, fs->opendir.esum,
		fs->readdir.stamp, fs->readdir.cnt, fs->readdir.esum,
		fs->closedir.stamp, fs->closedir.cnt, fs->closedir.esum,
		fs->mknod.stamp, fs->mknod.cnt, fs->mknod.esum,
		fs->mkdir.stamp, fs->mkdir.cnt, fs->mkdir.esum,
		fs->symlink.stamp, fs->symlink.cnt, fs->symlink.esum,
		fs->unlink.stamp, fs->unlink.cnt, fs->unlink.esum,
		fs->rmdir.stamp, fs->rmdir.cnt, fs->rmdir.esum,
		fs->rename.stamp, fs->rename.cnt, fs->rename.esum,
		fs->link.stamp, fs->link.cnt, fs->link.esum,
		fs->chmod.stamp, fs->chmod.cnt, fs->chmod.esum,
		fs->chown.stamp, fs->chown.cnt, fs->chown.esum,
		fs->truncate.stamp, fs->truncate.cnt, fs->truncate.esum,
		fs->utime.stamp, fs->utime.cnt, fs->utime.esum,
		fs->creat.stamp, fs->creat.cnt, fs->creat.esum,
		fs->open.stamp, fs->open.cnt, fs->open.esum,
		fs->statfs.stamp, fs->statfs.cnt, fs->statfs.esum,
		fs->flush.stamp, fs->flush.cnt, fs->flush.esum,
		fs->close.stamp, fs->close.cnt, fs->close.esum,
		fs->fsync.stamp, fs->fsync.cnt, fs->fsync.esum
#ifdef HAVE_SETXATTR
		,fs->setxattr.stamp, fs->setxattr.cnt, fs->setxattr.esum,
		fs->getxattr.stamp, fs->getxattr.cnt, fs->setxattr.esum,
		fs->listxattr.stamp, fs->listxattr.cnt, fs->listxattr.esum,
		fs->removexattr.stamp, fs->removexattr.cnt, fs->removesetxattr.esum
#endif
		,fs->read.stamp, fs->read.cnt, fs->read.esum, fs->read.bytes,
		fs->write.stamp, fs->write.cnt, fs->write.esum, fs->write.bytes
		);
	return buf;
}

/*********** log processing routines **********/
static void log_init(struct ftrac *ft)
{
	int res;
	char file[MAX_FILENAME];

	/* runtime environments */
	memset(file, 0, MAX_FILENAME);
	snprintf(file, MAX_FILENAME, "%s/env.log", ft->logdir);
	ft->env = fopen(file, "wb");
	if (ft->env == NULL) {
		fprintf(stderr, "open file %s failed\n", file);
		exit(1);
	}
	
	/* log file and buffer */
	memset(file, 0, MAX_FILENAME);
	snprintf(file, MAX_FILENAME, "%s/trace.log", ft->logdir);
	ft->log = fopen(file, "wb");
	if (ft->log == NULL) {
		fprintf(stderr, "open file %s failed\n", file);
		exit(1);
	}
	
	ft->logbuf = malloc(ft->logbufsize);
	if (ft->logbuf == NULL) {
		fprintf(stderr, "malloc failed\n");
		exit(1);
	}
	res = setvbuf(ft->log, ft->logbuf, _IOFBF, ft->logbufsize);
	if (res != 0) {
		fprintf(stderr, "setvbuf failed\n");
		exit(1);
	}

	/* process table */
	res = proctab_init(&ft->proctab);
	if (res != 0) {
		fprintf(stderr, "initial process table failed\n");
		exit(1);
	}
	
	memset(file, 0, MAX_FILENAME);
	snprintf(file, MAX_FILENAME, "%s/proc.map", ft->logdir);
	ft->procmap = fopen(file, "wb");
	if (ft->procmap == NULL) {
		fprintf(stderr, "open file %s failed\n", file);
		exit(1);
	}
	/* insert the first record for root process */
	fprintf(ft->procmap, "0:0:system init\n");

	/* file table */
	res = filetab_init(&ft->filetab);
	if (res != 0) {
		fprintf(stderr, "initial file table failed\n");
		exit(1);
	}
	
	memset(file, 0, MAX_FILENAME);
	snprintf(file, MAX_FILENAME, "%s/file.map", ft->logdir);
	ft->filemap = fopen(file, "wb");
	if (ft->filemap == NULL) {
		fprintf(stderr, "open file %s failed\n", file);
		exit(1);
	}

	/* start logging */
	struct timespec start;
	struct passwd *pwd;
	struct utsname platform;

	clock_gettime(CLOCK_REALTIME, &start);
	if (uname(&platform) == -1)
		fprintf(stderr, "get platform info failed.\n");
	pwd = getpwuid(getuid());
		
	fprintf(ft->env,
		"prog:ftrac\n"
		"start:%.9f\n"
		"platform:%s %s %s\n"
		"hostname:%s\n"
		"user:%s\n"
		"uid:%d\n"
		, 
		((double) start.tv_sec + ((double) start.tv_nsec) * 0.000000001),
		platform.sysname, platform.release, platform.version,
		platform.nodename,
		pwd->pw_name,
		pwd->pw_uid);

	fflush(ft->env);
}

static void log_destroy(struct ftrac *ft)
{
	/* finish logging */
	struct timespec end;
	clock_gettime(CLOCK_REALTIME, &end);
	fprintf(ft->env, "end:%.9f", ((double) end.tv_sec + \
		((double) end.tv_nsec) * 0.000000001));
	
	/* finalize */
	proctab_destroy(&ft->proctab);
	filetab_destroy(&ft->filetab);
	fclose(ft->procmap);
	fclose(ft->filemap);
	fclose(ft->log);
	fclose(ft->env);
	free(ft->logbuf);
	g_free(ft->logdir);
}

/*********** control server routines **********/
typedef struct ctrl_process_data {
	int sockfd;
} * ctrl_process_data_t;

static void * ctrl_process(void *data);
static int ctrl_poll_stat(int sockfd, char *buf);
static int ctrl_flush(int sockfd);

static void ctrl_server_init(struct ftrac *ft)
{
	struct sockaddr_un servaddr;
	pthread_t thread_id;
	int len, err;

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
            case CTRL_POLL_STAT:
                err = ctrl_poll_stat(sockfd, recvbuf);
                break;
			
			case CTRL_FLUSH:
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
static int ctrl_poll_stat(int sockfd, char *buf)
{
	int res, err = 0;
	char *sendbuf;
	(void) buf;

	sendbuf = stat_fs_to_dictstr(&ftrac.fs);
	res = send(sockfd, sendbuf, strlen(sendbuf), 0);
	if (res < 0) {
        fprintf(stderr, "send failed, %s\n", strerror(errno));
        err = 1;
    }
    g_free(sendbuf);
    return err;
}

static int ctrl_flush(int sockfd)
{
	int res, err = 0;
	char *sendbuf = g_strdup_printf("%c", CTRL_OK);

	fflush(ftrac.log);
	fflush(ftrac.procmap);
	fflush(ftrac.filemap);

    res = send(sockfd, sendbuf, strlen(sendbuf), 0);
    if (res < 0) {
        fprintf(stderr, "send failed, %s\n", strerror(errno));
        err = 1;
    }
    g_free(sendbuf);
    return err;
}

/*********** FUSE interfaces **********/
static int ftrac_getattr(const char *path, struct stat *stbuf)
{
	int res;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;

	clock_gettime(FTRAC_CLOCK, &start);
#endif
	
	res = lstat(path, stbuf);
	
#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sc_log_common(&ftrac.fs.lstat, SYSC_FS_LSTAT, &start, &end,
		fuse_get_context()->pid, res, path);
#endif
	
	if (res == -1)
		return -errno;
	return 0;
}

static int ftrac_fgetattr(const char *path, struct stat *stbuf,
	struct fuse_file_info *fi)
{
	int fd, res;

#ifdef FTRAC_TRACE_NONE
	(void) path;
	fd = fi->fh;
#else
	struct timespec start, end;
	ftrac_file_t ff = (ftrac_file_t) (uintptr_t) fi->fh;
	fd = ff->fd;
	clock_gettime(FTRAC_CLOCK, &start);
#endif
	
	res = fstat(fd, stbuf);
	
#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sc_log_common(&ftrac.fs.fstat, SYSC_FS_FSTAT, &start, &end,
		fuse_get_context()->pid, res, path);
#endif

	if (res == -1)
		return -errno;
	return 0;
}

static int ftrac_access(const char *path, int mask)
{
	int res;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	
	clock_gettime(FTRAC_CLOCK, &start);
#endif
    
	res = access(path, mask);

#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);

	sc_log_common(&ftrac.fs.access, SYSC_FS_ACCESS, &start, &end,
		fuse_get_context()->pid, res, path);
#endif

	if (res == -1)
		return -errno;
	return 0;
}

static int ftrac_readlink(const char *path, char *buf, size_t size)
{
	int res;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	
	clock_gettime(FTRAC_CLOCK, &start);
#endif
	
	res = readlink(path, buf, size - 1);
	
#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);

	sc_log_common(&ftrac.fs.readlink, SYSC_FS_READLINK, &start, &end,
		fuse_get_context()->pid, res, path);
#endif

	if (res == -1)
		return -errno;

	buf[res] = '\0';
	return 0;
}
static int ftrac_opendir(const char *path, struct fuse_file_info *fi)
{
#ifdef FTRAC_TRACE_ENABLED
	int res;
	struct timespec start, end;
	struct ftrac_file *ff;
	pid_t pid = fuse_get_context()->pid;
	
	clock_gettime(FTRAC_CLOCK, &start);
#endif

	DIR *dp = opendir(path);

#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	res = dp == NULL ? -1 : 0;
	sc_log_common(&ftrac.fs.opendir, SYSC_FS_OPENDIR, &start, &end, pid, res, 
		path);
#endif


	if (dp == NULL)
		return -errno;

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
		
		sc_log_common(&ftrac.fs.readdir, SYSC_FS_READDIR, &start, &end,
			fuse_get_context()->pid, res, path);
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
	g_free(ff);
	
	clock_gettime(FTRAC_CLOCK, &start);
#else
	dp = (DIR *) (uintptr_t) fi->fh;
	(void) path;
#endif

	closedir(dp);

#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sc_log_common(&ftrac.fs.closedir, SYSC_FS_CLOSEDIR, &start, &end, pid, 0, 
		path);
#endif

	return 0;
}

static int ftrac_mknod(const char *path, mode_t mode, dev_t rdev)
{
	int res;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	
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

#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sc_log_common(&ftrac.fs.mknod, SYSC_FS_MKNOD, &start, &end,
		fuse_get_context()->pid, res, path);
#endif

	if (res == -1)
		return -errno;
	return 0;
}

static int ftrac_mkdir(const char *path, mode_t mode)
{
	int res;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	
	clock_gettime(FTRAC_CLOCK, &start);
#endif
	
	res = mkdir(path, mode);

#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sc_log_common(&ftrac.fs.mkdir, SYSC_FS_MKDIR, &start, &end,
		fuse_get_context()->pid, res, path);
#endif
	
	if (res == -1)
		return -errno;
	return 0;
}

static int ftrac_unlink(const char *path)
{
	int res;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	
	clock_gettime(FTRAC_CLOCK, &start);
#endif
	
	res = unlink(path);
	
#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sc_log_common(&ftrac.fs.unlink, SYSC_FS_UNLINK, &start, &end,
		fuse_get_context()->pid, res, path);
#endif

	if (res == -1)
		return -errno;
	return 0;
}

static int ftrac_rmdir(const char *path)
{
	int res;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	
	clock_gettime(FTRAC_CLOCK, &start);
#endif
	
	res = rmdir(path);
	
#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sc_log_common(&ftrac.fs.rmdir, SYSC_FS_RMDIR, &start, &end,
		fuse_get_context()->pid, res, path);
#endif

	if (res == -1)
		return -errno;
	return 0;
}

static int ftrac_symlink(const char *from, const char *to)
{
	int res;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	
	clock_gettime(FTRAC_CLOCK, &start);
#endif
	
	res = symlink(from, to);

#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sc_log_link(&ftrac.fs.symlink, SYSC_FS_SYMLINK, &start, &end,
		fuse_get_context()->pid, res, from, to);
#endif

	if (res == -1)
		return -errno;
	return 0;
}

static int ftrac_rename(const char *from, const char *to)
{
	int res;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	
	clock_gettime(FTRAC_CLOCK, &start);
#endif
    
	res = rename(from, to);
	
#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sc_log_link(&ftrac.fs.rename, SYSC_FS_RENAME, &start, &end,
		fuse_get_context()->pid, res, from, to);
#endif

	if (res == -1)
		return -errno;
	return 0;
}

static int ftrac_link(const char *from, const char *to)
{
	int res;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	
	clock_gettime(FTRAC_CLOCK, &start);
#endif
    
	res = link(from, to);
	
#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sc_log_link(&ftrac.fs.link, SYSC_FS_LINK, &start, &end,
		fuse_get_context()->pid, res, from, to);
#endif

	if (res == -1)
		return -errno;
	return 0;
}

static int ftrac_chmod(const char *path, mode_t mode)
{
	int res;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	
	clock_gettime(FTRAC_CLOCK, &start);
#endif
	
	res = chmod(path, mode);
	
#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sc_log_common(&ftrac.fs.chmod, SYSC_FS_CHMOD, &start, &end,
		fuse_get_context()->pid, res, path);
#endif

	if (res == -1)
		return -errno;
	return 0;
}

static int ftrac_chown(const char *path, uid_t uid, gid_t gid)
{
	int res;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	
	clock_gettime(FTRAC_CLOCK, &start);
#endif
	
	res = lchown(path, uid, gid);
	
#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sc_log_common(&ftrac.fs.chown, SYSC_FS_CHOWN, &start, &end,
		fuse_get_context()->pid, res, path);
#endif

	if (res == -1)
		return -errno;
	return 0;
}

static int ftrac_truncate(const char *path, off_t size)
{
	int res;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	
	clock_gettime(FTRAC_CLOCK, &start);
#endif
    
	res = truncate(path, size);
	
#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sc_log_common(&ftrac.fs.truncate, SYSC_FS_TRUNCATE, &start, &end,
		fuse_get_context()->pid, res, path);
#endif

	if (res == -1)
		return -errno;
	return 0;
}

static int ftrac_ftruncate(const char *path, off_t size,
	struct fuse_file_info *fi)
{
	int fd, res;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	ftrac_file_t ff = (ftrac_file_t) (uintptr_t) fi->fh;
	
	fd = ff->fd;
	clock_gettime(FTRAC_CLOCK, &start);
#else
	fd = fi->fh;
	(void) path;
#endif
    
	res = ftruncate(fd, size);
	
#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sc_log_common(&ftrac.fs.truncate, SYSC_FS_TRUNCATE, &start, &end,
		fuse_get_context()->pid, res, path);
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

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;

	clock_gettime(FTRAC_CLOCK, &start);
#endif
	
	tv[0].tv_sec = ts[0].tv_sec;
	tv[0].tv_usec = ts[0].tv_nsec / 1000;
	tv[1].tv_sec = ts[1].tv_sec;
	tv[1].tv_usec = ts[1].tv_nsec / 1000;
	res = utimes(path, tv);

#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sc_log_common(&ftrac.fs.utime, SYSC_FS_UTIME, &start, &end,
		fuse_get_context()->pid, res, path);
#endif

	if (res == -1)
		return -errno;
	return 0;
}
#else
static int ftrac_utime(const char *path, struct utimbuf *buf)
{
	int res;
#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	
	clock_gettime(FTRAC_CLOCK, &start);
#endif
    
	res = utime(path, buf);

#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sc_log_common(&ftrac.fs.utime, SYSC_FS_UTIME, &start, &end,
		fuse_get_context()->pid, res, path);
#endif
	
	if (res == -1)
		return -errno;
	return 0;
}
#endif

static int ftrac_create(const char *path, mode_t mode, struct fuse_file_info
*fi)
{
	int fd;

#ifdef FTRAC_TRACE_ENABLED
	int res;
	struct timespec start, end;
	pid_t pid = fuse_get_context()->pid;
	
	clock_gettime(FTRAC_CLOCK, &start);
#endif
    
	fd = open(path, fi->flags, mode);
	
#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	res = fd == -1 ? -1 : 0;
	sc_log_common(&ftrac.fs.creat, SYSC_FS_CREAT, &start, &end, pid, res, path);
#endif

	if (fd == -1)
		return -errno;

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
	int fd;

#ifdef FTRAC_TRACE_ENABLED
	int res;
	struct timespec start, end;
	pid_t pid = fuse_get_context()->pid;
	
	clock_gettime(FTRAC_CLOCK, &start);
#endif
    
	fd = open(path, fi->flags);
	
#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	res = fd == -1 ? -1 : 0;
	sc_log_common(&ftrac.fs.open, SYSC_FS_OPEN, &start, &end, pid, res, path);
#endif

	if (fd == -1)
		return -errno;

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
	int fd, res;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	ftrac_file_t ff = (ftrac_file_t) (uintptr_t) fi->fh;
	
	fd = ff->fd;
	clock_gettime(FTRAC_CLOCK, &start);
#else
	fd = fi->fh;
	(void) path;
#endif
	
	res = pread(fd, buf, size, offset);
	
#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sc_log_io(&ftrac.fs.read, SYSC_FS_READ, &start, &end, 
		fuse_get_context()->pid, res, path, size, offset);
#endif

	if (res == -1)
		res = -errno;
	return res;
}

static int ftrac_write(const char *path, const char *buf, size_t size,
		     off_t offset, struct fuse_file_info *fi)
{
	int fd, res;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	ftrac_file_t ff = (ftrac_file_t) (uintptr_t) fi->fh;
	
	fd = ff->fd;
	clock_gettime(FTRAC_CLOCK, &start);
#else
	fd = fi->fh;	
	(void) path;
#endif
	
	res = pwrite(fd, buf, size, offset);
	
#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sc_log_io(&ftrac.fs.write, SYSC_FS_WRITE, &start, &end, 
		fuse_get_context()->pid, res, path, size, offset);
#endif

	if (res == -1)
		res = -errno;
	return res;
}

static int ftrac_statfs(const char *path, struct statvfs *stbuf)
{
	int res;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;

	clock_gettime(FTRAC_CLOCK, &start);
#endif
    
	res = statvfs(path, stbuf);
	
#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sc_log_common(&ftrac.fs.statfs, SYSC_FS_STATFS, &start, &end,
		fuse_get_context()->pid, res, path);
#endif
	
	if (res == -1)
		return -errno;
	return 0;
}

static int ftrac_flush(const char *path, struct fuse_file_info *fi)
{
	int fd, res;
	
#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	ftrac_file_t ff = (ftrac_file_t) fi->fh;
	
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

#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sc_log_common(&ftrac.fs.flush, SYSC_FS_FLUSH, &start, &end,
		fuse_get_context()->pid, res, path);
#endif

	if (res == -1)
		return -errno;
	return 0;
}

static int ftrac_close(const char *path, struct fuse_file_info *fi)
{
	int fd;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	ftrac_file_t ff = (ftrac_file_t) fi->fh;
	
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
	
	sc_log_common(&ftrac.fs.close, SYSC_FS_CLOSE, &start, &end, pid, 0, path);
#endif

	return 0;
}

static int ftrac_fsync(const char *path, int isdatasync,
		     struct fuse_file_info *fi)
{
	int res;
	(void) path;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	
	clock_gettime(FTRAC_CLOCK, &start);
#endif

#ifndef HAVE_FDATASYNC
	(void) isdatasync;
#else
	if (isdatasync)
		res = fdatasync(fi->fh);
	else
#endif
		res = fsync(fi->fh);

#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sc_log_common(&ftrac.fs.fsync, SYSC_FS_FSYNC, &start, &end,
		fuse_get_context()->pid, res, path);
#endif
    
	if (res == -1)
		return -errno;

	return 0;
}

#ifdef HAVE_SETXATTR
/* xattr operations are optional and can safely be left unimplemented */
static int ftrac_setxattr(const char *path, const char *name, 
	const char *value, size_t size, int flags)
{
	int res;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;

	clock_gettime(FTRAC_CLOCK, &start);
#endif
    
	res = lsetxattr(path, name, value, size, flags);
	
#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sc_log_common(&ftrac.fs.setxattr, SYSC_FS_SETXATTR, &start, &end,
		fuse_get_context()->pid, res, path);
#endif

	if (res == -1)
		return -errno;
	return 0;
}

static int ftrac_getxattr(const char *path, const char *name, char *value,
			size_t size)
{
	int res;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	
	clock_gettime(FTRAC_CLOCK, &start);
#endif
    
	res = lgetxattr(path, name, value, size);
	
#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sc_log_common(&ftrac.fs.getxattr, SYSC_FS_GETXATTR, &start, &end,
		fuse_get_context()->pid, res, path);
#endif

	if (res == -1)
		return -errno;
	return res;
}

static int ftrac_listxattr(const char *path, char *list, size_t size)
{
	int res;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	
	clock_gettime(FTRAC_CLOCK, &start);
#endif
    
	res = llistxattr(path, list, size);
	
#ifdef FTRAC_TRACE_NONE
	clock_gettime(FTRAC_CLOCK, &end);
	
	sc_log_common(&ftrac.fs.listxattr, SYSC_FS_LISTXATTR, &start, &end,
		fuse_get_context()->pid, res, path);
#endif

	if (res == -1)
		return -errno;
	return res;
}

static int ftrac_removexattr(const char *path, const char *name)
{
	int res;

#ifdef FTRAC_TRACE_ENABLED
	struct timespec start, end;
	
	clock_gettime(FTRAC_CLOCK, &start);
#endif
	
	res = lremovexattr(path, name);

#ifdef FTRAC_TRACE_ENABLED
	clock_gettime(FTRAC_CLOCK, &end);
	
	sc_log_common(&ftrac.fs.removexattr, SYSC_FS_REMOVEXATTR, &start, &end,
		fuse_get_context()->pid, res, path);
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

	memset(&ftrac.fs, 0, sizeof(struct stat_fs));

	/* initial log file */
	log_init(&ftrac);

	/* startup control server */
	ctrl_server_init(&ftrac);
	
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
	
	log_destroy(&ftrac);
	ctrl_server_destroy(&ftrac);
	remove(ftrac.sessiondir);
	g_free(ftrac.cwd);
    g_free(ftrac.username);
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
"usage: %s mountpoint [options]\n"
"\n"
"general options:\n"
"    -o opt,[opt...]        mount options\n"
"    -h   --help            print help\n"
"    -V   --version         print version\n"
"    -o sessiondir=PATH     session directory\n"
"    -o logdir=PATH        	log directory\n"
"    -o logbufsize=PATH     log buffer size\n"
"    -o notify_pid=PID      process to notify on enter fuse loop\n"
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
		if (realpath(ftrac.logdir, logdir) == NULL) {
			fprintf(stderr, "bad log directory %s\n", logdir);
			exit(1);
		}
		free(ftrac.logdir);
		ftrac.logdir = g_strdup(logdir);
	}
    res = mkdir(ftrac.logdir, S_IRUSR | S_IWUSR | S_IXUSR);
    if (res == -1 && errno != EEXIST) {
		fprintf(stderr, "failed to create directory %s, %s\n", 
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
