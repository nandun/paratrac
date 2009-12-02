/*
  This program can be distributed under the terms of the GNU GPL.
  See the file COPYING.
  gcc -Wall `pkg-config fuse --cflags --libs` fusexmp.c -o fusexmp
  usage:  <source_dir> <mount_dir> <log_dir>
*/

#define FUSE_USE_VERSION 26

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
#include <sys/time.h>
#ifdef HAVE_SETXATTR
#include <sys/xattr.h>
#include <time.h>
#endif


#include "logger.h"

///////////////////////////////////
PathConv* conv;
Logger* log;

///////////////////////////////////


static int xmp_getattr(const char *old_path, struct stat *stbuf)
{
	char path[1024];
	conv->get_real_path(path,old_path);
	struct timespec start_time = log->get_time();
	int res = lstat(path, stbuf);
	if (res == -1)return -errno;
	log->getattr_log(old_path,fuse_get_context()->pid,start_time);

	return 0;
}

static int xmp_access(const char *old_path, int mask)
{
	char path[1024];
	conv->get_real_path(path,old_path);

	int res = access(path, mask);
	if (res == -1)
		return -errno;

	return 0;
}

static int xmp_readlink(const char *old_path, char *buf, size_t size)
{
	int res;
	char path[1024];
	conv->get_real_path(path,old_path);

	res = readlink(path, buf, size - 1);
	if (res == -1)
		return -errno;

	buf[res] = '\0';
	return 0;
}


static int xmp_readdir(const char *old_path, void *buf, fuse_fill_dir_t filler,
		       off_t offset, struct fuse_file_info *fi)
{
	char path[1024];
	conv->get_real_path(path,old_path);
	DIR *dp;
	struct dirent *de;

	(void) offset;
	(void) fi;

	struct timespec start_time = log->get_time();

	dp = opendir(path);
	if (dp == NULL)
		return -errno;


	while ((de = readdir(dp)) != NULL) {
		struct stat st;
		memset(&st, 0, sizeof(st));
		st.st_ino = de->d_ino;
		st.st_mode = de->d_type << 12;
		if (filler(buf, de->d_name, &st, 0))
			break;
	}

	closedir(dp);
	
	log -> readdir_log(old_path,fuse_get_context()->pid,start_time);
	return 0;
}

static int xmp_mknod(const char *old_path, mode_t mode, dev_t rdev)
{
	char path[1024];
	conv->get_real_path(path,old_path);
	int res;

	struct timespec start_time = log->get_time();

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
	if (res == -1)
		return -errno;

	log->mknod_log(old_path, fuse_get_context()->pid, start_time);

	return 0;
}

static int xmp_mkdir(const char *old_path, mode_t mode)
{
	char path[1024];
	conv->get_real_path(path,old_path);
	struct timespec start_time = log->get_time();
	int res = mkdir(path, mode);
	if (res == -1)
		return -errno;
	log->mkdir_log(old_path,fuse_get_context()->pid,start_time);
	return 0;
}

static int xmp_unlink(const char *old_path)
{
	char path[1024];
	conv->get_real_path(path,old_path);
	struct timespec start_time = log->get_time();
	int res = unlink(path);

	if (res == -1)
		return -errno;
	log->unlink_log(old_path,fuse_get_context()->pid,start_time);
	return 0;
}

static int xmp_rmdir(const char *old_path)
{
	int res;
	char path[1024];
	conv->get_real_path(path,old_path);
	struct timespec start_time = log->get_time();
	res = rmdir(path);
	if (res == -1)
		return -errno;
	log->rmdir_log(old_path,fuse_get_context()->pid,start_time);
	return 0;
}

static int xmp_symlink(const char *from, const char *to)
{
	int res;
	
	//	char from_r[1024];
	char to_r[1024];
	//	conv->get_real_path(from_r,from);
	conv->get_real_path(to_r,to);
	

	res = symlink(from, to_r);
	if (res == -1)
		return -errno;

	return 0;
}

static int xmp_rename(const char *from, const char *to)
{
	int res;

	char from_r[1024];
	char to_r[1024];
	conv->get_real_path(from_r,from);
	conv->get_real_path(to_r,to);

	struct timespec start_time = log->get_time();
	res = rename(from_r, to_r);
	if (res == -1)
		return -errno;
	log->rename_log(from, to,to_r,fuse_get_context()->pid,start_time);

	return 0;
}

static int xmp_link(const char *from, const char *to)
{
	int res;

	char from_r[1024];
	char to_r[1024];
	conv->get_real_path(from_r,from);
	conv->get_real_path(to_r,to);

	res = link(from_r, to_r);
	if (res == -1)
		return -errno;

	return 0;
}

static int xmp_chmod(const char *old_path, mode_t mode)
{
	char path[1024];
	conv->get_real_path(path,old_path);
	int res;

	res = chmod(path, mode);
	if (res == -1)
		return -errno;

	return 0;
}

static int xmp_chown(const char *old_path, uid_t uid, gid_t gid)
{
	char path[1024];
	conv->get_real_path(path,old_path);
	int res;

	res = lchown(path, uid, gid);
	if (res == -1)
		return -errno;

	return 0;
}

static int xmp_truncate(const char *old_path, off_t size)
{
	char path[1024];
	conv->get_real_path(path,old_path);
	int res;

	res = truncate(path, size);
	if (res == -1)
		return -errno;

	return 0;
}

static int xmp_utimens(const char *old_path, const struct timespec ts[2])
{
	char path[1024];
	conv->get_real_path(path,old_path);
	int res;
	struct timeval tv[2];

	tv[0].tv_sec = ts[0].tv_sec;
	tv[0].tv_usec = ts[0].tv_nsec / 1000;
	tv[1].tv_sec = ts[1].tv_sec;
	tv[1].tv_usec = ts[1].tv_nsec / 1000;

	res = utimes(path, tv);
	if (res == -1)
		return -errno;

	return 0;
}



static int xmp_open(const char *old_path, struct fuse_file_info *fi)
{

	int fd= -1;
	int pid = fuse_get_context()->pid;
	char path[1024];
	conv->get_real_path(path,old_path);
	
	struct timespec start = log->get_time();
	
	fd = open(path, fi->flags);
	int ret = -errno;

	log->open_log(old_path, fi->flags, fd, pid, start);

	fi->fh = fd;
	if (fd == -1){
	  return ret;
	}
	return 0;
}

static int xmp_read(const char *old_path, char *buf, size_t size, off_t offset,
		    struct fuse_file_info *fi)
{
	int fd = fi->fh;

	log->read_start(fd);

	int res = pread(fd, buf, size, offset);
	int err = errno;

	log->read_end(old_path, fd, res, err);

	return res;
}

static int xmp_write(const char *old_path, const char *buf, size_t size,
		     off_t offset, struct fuse_file_info *fi)
{
	int fd = fi->fh;

	log->write_start(fd);

	int res = pwrite(fd, buf, size, offset);
	int err = errno;

	log->write_end(old_path, fd, res, err);

	return res;
}

static int xmp_statfs(const char *old_path, struct statvfs *stbuf)
{
	char path[1024];
	conv->get_real_path(path,old_path);
	int res;

	res = statvfs(path, stbuf);
	if (res == -1)
		return -errno;

	return 0;
}

static int xmp_release(const char *old_path, struct fuse_file_info *fi)
{
    int fd = fi->fh;

    log->release_log(old_path, fd);
	close (fd);
	return 0;
}

static int xmp_fsync(const char *old_path, int isdatasync,
		     struct fuse_file_info *fi)
{
	/* Just a stub.	 This method is optional and can safely be left
	   unimplemented */
	char path[1024];
	conv->get_real_path(path,old_path);

	(void) path;
	(void) isdatasync;
	(void) fi;
	return 0;
}

#ifdef HAVE_SETXATTR
/* xattr operations are optional and can safely be left unimplemented */
static int xmp_setxattr(const char *old_path, const char *name, const char *value,
			size_t size, int flags)
{
	char path[1024];
	conv->get_real_path(path,old_path);
	int res = lsetxattr(path, name, value, size, flags);
	if (res == -1)
		return -errno;
	return 0;
}

static int xmp_getxattr(const char *old_path, const char *name, char *value,
			size_t size)
{
	char path[1024];
	conv->get_real_path(path,old_path);
	int res = lgetxattr(path, name, value, size);
	if (res == -1)
		return -errno;
	return res;
}

static int xmp_listxattr(const char *old_path, char *list, size_t size)
{
	char path[1024];
	conv->get_real_path(path,old_path);
	int res = llistxattr(path, list, size);
	if (res == -1)
		return -errno;
	return res;
}

static int xmp_removexattr(const char *old_path, const char *name)
{
	char path[1024];
	conv->get_real_path(path,old_path);
	int res = lremovexattr(path, name);
	if (res == -1)
		return -errno;
	return 0;
}
#endif /* HAVE_SETXATTR */

static struct fuse_operations xmp_oper;

void print_usage_and_exit(){
	printf("usage: ./logfuse [-detail] <source_dir> <mount_dir> <log_dir> \n");
	exit(1);
}


int main(int argc, char *argv[])
{
	
	bool detail = false;
	char dirs[3][1024];
	
	char* argv_wo_option[4];
	int j=0;
	//read arguments
	for(int i=0;i<argc;i++){
		if(argv[i][0] == '-'){
			if( strcmp((char*)argv[i], "-detail" ) == 0){
				detail=true;
			}else{
				printf("unknown flag:%s \n", argv[i]);
				print_usage_and_exit();
			}
		}else{
			if(j>3)print_usage_and_exit();
			argv_wo_option[j] = (char*)argv[i];
			++j;
		}
	}
	
	//if not absolute path, make it change to abspath.
	char* pwd = getenv("PWD");
	if(pwd != NULL){
		for(int i=0;i<3;i++){
			if(argv_wo_option[i+1][0] != '/'){
				snprintf(dirs[i],1024,"%s/%s",pwd, argv_wo_option[i+1]);
			}else{
				snprintf(dirs[i],1024,"%s", argv_wo_option[i+1]);
			}
			struct stat buf;
			if(i<2 && stat(dirs[i],&buf) !=0){
				perror(dirs[i]);
				return -1;
			}
		}
	}
	printf("src: %s\n",dirs[0]);
	printf("mnt: %s\n",dirs[1]);
	printf("log: %s\n",dirs[2]);
	
	
	
	xmp_oper.getattr	= xmp_getattr;
	xmp_oper.access		= xmp_access;
	xmp_oper.readlink	= xmp_readlink;
	xmp_oper.readdir	= xmp_readdir;
	xmp_oper.mknod		= xmp_mknod;
	xmp_oper.mkdir		= xmp_mkdir;
	xmp_oper.symlink	= xmp_symlink;
	xmp_oper.unlink		= xmp_unlink;
	xmp_oper.rmdir		= xmp_rmdir;
	xmp_oper.rename		= xmp_rename;
	xmp_oper.link		= xmp_link;
	xmp_oper.chmod		= xmp_chmod;
	xmp_oper.chown		= xmp_chown;
	xmp_oper.truncate	= xmp_truncate;
	xmp_oper.utimens	= xmp_utimens;
	xmp_oper.open		= xmp_open;
	xmp_oper.read		= xmp_read;
	xmp_oper.write		= xmp_write;
	xmp_oper.statfs		= xmp_statfs;
	xmp_oper.release	= xmp_release;
	xmp_oper.fsync		= xmp_fsync;
#ifdef HAVE_SETXATTR
	xmp_oper.setxattr	= xmp_setxattr;
	xmp_oper.getxattr	= xmp_getxattr;
	xmp_oper.listxattr	= xmp_listxattr;
	xmp_oper.removexattr	= xmp_removexattr;
#endif

	umask(0);

	char tmp_dir[1024];
	snprintf(tmp_dir,1024,"/tmp/logfuse_%d_%d", getuid(), getpid() );
	char sh_line[2048];
	snprintf(sh_line,2048,"mkdir  %s -m 755", tmp_dir);
	system(sh_line);

	conv = new PathConv(dirs[0]);
	log = new Logger(tmp_dir);
	if(detail)log->set_detail();


	argv[0] = "fuse_main";
	argv[1] = dirs[1];
//	argv[2] = "-s";


	fuse_main(2, argv, &xmp_oper, NULL);

	delete log;
	delete conv;
	snprintf(sh_line,2048,"mkdir -p %s -m 755",dirs[2]);
	system(sh_line);
	snprintf(sh_line,2048,"cp -rf %s/* %s",tmp_dir,dirs[2]);
	system(sh_line);
	snprintf(sh_line,2048,"rm -rf %s",tmp_dir);
	system(sh_line);

}


