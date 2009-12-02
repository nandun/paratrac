#include <vector>
#include <set>
#include <string>
#include <stdlib.h>
#include <unistd.h>

using namespace std;

#define minim(x,y) (x>y ? y: x)
#define maxim(x,y) (x>y ? x: y)

string itoa(int i){
	char str[32];
	memset(str,0,sizeof(str));
	snprintf(str, sizeof(str), "%d", i);
	return str;
}

int cp(const char* from, const char* to, mode_t mode){
	int fin;
	int fout;
	if( (fin = open(from,O_RDONLY)) < 0)return 1;
	if( (fout = open(to, O_CREAT | O_WRONLY, mode)) <0){
		close(fin);return 2;
	}
	char* buf[1024];
	int size;
	while((size=read(fin,&buf,1024)) > 0){
		while(size > 0){
			int ret = write(fout,&buf,size);
			if(ret <=0)break;
			size -= ret;
		}
	}
	close(fin);
	close(fout);
	return 0;
}

///////////////////////////////////////////////////////////////////////

class PathConv
{
private:
	char real_path_root[1024];
public:
	char* get_real_path(char* path, const char * pseudo_path){
		strcpy(path, real_path_root);
		strcat(path, pseudo_path);
		return path;
	}
	PathConv(const char* real_path_root){
		strcpy(this->real_path_root, real_path_root);
	}
	void set(const char* real_path_root){
		strcpy(this->real_path_root, real_path_root);
	}
};

///////////////////////////////////////////////////////////////////////

class Mutex{
private:
	pthread_mutex_t mutex;
public:
	Mutex(){
		pthread_mutex_init( &mutex, NULL );
	}
	~Mutex(){
		pthread_mutex_destroy( &mutex );
	}
	void lock(){
		pthread_mutex_lock( &mutex );
	}
	void unlock(){
		pthread_mutex_unlock( &mutex );
	}
};

//////////////////////////////////////////////////////////////////////

class UniqPid
{
public:
	unsigned long long starttime;
	int pid;
	UniqPid(unsigned long long s, int p){
		starttime = s;
		pid = p;
	}
	UniqPid(){
		starttime = 0;
		pid = 0;
	}
	bool operator <(const UniqPid& a) const{
		return starttime < a.starttime || (starttime == a.starttime && pid < a.pid);
    }
	bool operator >(const UniqPid& a) const{
		return starttime > a.starttime || (starttime == a.starttime && pid > a.pid);
    }
	bool operator ==(const UniqPid& a) const{
		return starttime == a.starttime && pid == a.pid;
    }
};

//////////////////////////////////////////////////////////////////////

class ParFID {
public:
	long long write_time;
	long long read_time;
	long long write_size;
	long long read_size;
	UniqPid up;
	int write_num;
	int read_num;
	struct timespec read_start_time;
	struct timespec write_start_time;
	int seq;

	ParFID(){
		clear();
	}
	void clear(){
		read_time = 0;
		read_num=0;
		read_size=0;
		write_time = 0;
		write_num=0;
		write_size=0;
		up = UniqPid();
		seq=0;
	}

};

//////////////////////////////////////////////////////////////////////


class Logger
{
private:
	ParFID data[1024*16];
	FILE* log_fp;
	FILE* proc_fp;
	FILE* detail_fp;
	char log_dir[1024];
	Mutex mx;
	set<UniqPid> uniq_pid_set;
	unsigned long long first_starttime;

	UniqPid get_uinq_pid(int pid){
		UniqPid up(0,pid);
		char from_str[1024];
		snprintf(from_str,1024,"/proc/%d/stat", pid );
		FILE* fp;
		if((fp = fopen(from_str, "r")) == NULL){
			fprintf(log_fp, "%d err proc stat open errno=%d where=%s\n",pid,errno, from_str);
		}else{
			int c;
			int num_of_space =0;
			while( (c=fgetc(fp)) != EOF){
				if ((char)c == ' ')num_of_space++;
				if (num_of_space == 21)break;
			}
			if( fscanf(fp, "%llu", &up.starttime) !=1 ){
				fprintf(log_fp, "%d err proc stat starttime errno=%d where=%s\n",pid,errno, from_str);
			}
			fclose(fp);
		}
		if(first_starttime==0)first_starttime = up.starttime;
		up.starttime -= first_starttime;
		return up;
	}

	bool detail;
	int file_seq;

public:
	
	void proc_log(UniqPid up){
		if(uniq_pid_set.count(up)!=0){
			return;
		}
		uniq_pid_set.insert(up);
		
		char from_str[1024];

		snprintf(from_str,1024,"/proc/%d/environ", up.pid );
		FILE* fp;
		if((fp = fopen(from_str, "r")) == NULL){
			fprintf(log_fp, "%d err proc environ errno=%d where=%s\n",up.pid,errno, from_str);
		}else{
			fprintf(proc_fp, "%llu:%d ",up.starttime, up.pid);
			int c;
			while( (c=fgetc(fp)) != EOF){
				fputc(c,proc_fp);
			}
			fprintf(proc_fp, "\n");
			fclose(fp);
		}
		return;
	}
	
	Logger(const char* log_dir_pre){
		char hostname[256];
		char sh_line[2048];
		strcpy(hostname,"hostname");
		gethostname(hostname,256);
		snprintf(log_dir,1024,"%s/%s",log_dir_pre,hostname);
		snprintf(sh_line,2048,"mkdir -p %s -m 755",log_dir);
		system(sh_line);
		//snprintf(sh_line,2048,"mkdir -p %s/proc -m 755",log_dir);
		//system(sh_line);
		snprintf(sh_line,2048,"%s/access_log",log_dir);
		log_fp = fopen(sh_line,"w");
		snprintf(sh_line,2048,"%s/proc_log",log_dir);
		proc_fp = fopen(sh_line,"w");
		first_starttime = 0;
		detail = false;
		detail_fp = NULL;
		file_seq = 0;
		mx.lock();
//		fprintf(log_fp,"#PID closed path read( time[nsec] count byte ) write( time[nsec] count byte )\n");
		struct timespec start_time_of_log = get_time();
		fprintf(log_fp,"#start time from the epoch: %ld:%ld [sec:nsec] \n",start_time_of_log.tv_sec, start_time_of_log.tv_nsec);
		mx.unlock();
	}
	
	void set_detail(){
		detail=true;
		char sh_line[2048];
		snprintf(sh_line,2048,"%s/detail_log",log_dir);
		detail_fp = fopen(sh_line,"w");
	}
	
	struct timespec get_time(){
		struct timespec st;
//		clock_gettime(CLOCK_MONOTONIC,&st);
		clock_gettime(CLOCK_REALTIME,&st);
		return st;
	}
	
	void print_log(UniqPid& up, const char* name, const char* path, struct timespec& start_time, struct timespec& end_time, const char* end_line = "\n"){
		long long nsec = ((long long)(end_time.tv_sec - start_time.tv_sec))*1000*1000*1000 + (end_time.tv_nsec - start_time.tv_nsec);
		fprintf(log_fp, "%llu:%d %s %s t=%lld%s", up.starttime, up.pid, name, path, nsec, end_line);
	}
	
	void getattr_log(const char* path, int pid, struct timespec start_time){
		struct timespec end_time = get_time();
		mx.lock();
		UniqPid up = get_uinq_pid(pid);
		print_log(up, "getattr", path, start_time, end_time);
		mx.unlock();
	}
	void readdir_log(const char* path, int pid, struct timespec start_time){
		struct timespec end_time = get_time();
		mx.lock();
		UniqPid up = get_uinq_pid(pid);
		print_log(up, "readdir", path, start_time, end_time);
		mx.unlock();
	}
	void mknod_log(const char* path, int pid, struct timespec start_time){
		struct timespec end_time = get_time();
		mx.lock();
		UniqPid up = get_uinq_pid(pid);
		proc_log(up);
		print_log(up, "created", path, start_time, end_time);
		mx.unlock();
	}
	void mkdir_log(const char* path, int pid, struct timespec start_time){
		struct timespec end_time = get_time();
		mx.lock();
		UniqPid up = get_uinq_pid(pid);
		proc_log(up);
		print_log(up, "mkdir", path, start_time, end_time);
		mx.unlock();
	}
	
	void rmdir_log(const char* path, int pid, struct timespec start_time){
		struct timespec end_time = get_time();
		mx.lock();
		UniqPid up = get_uinq_pid(pid);
		proc_log(up);
		print_log(up, "rmdir", path, start_time, end_time);
		mx.unlock();
	}
	void unlink_log(const char* path, int pid, struct timespec start_time ){
		struct timespec end_time = get_time();
		mx.lock();
		UniqPid up = get_uinq_pid(pid);
		proc_log(up);
		print_log(up, "unlink", path, start_time, end_time);
		mx.unlock();
	}
	void rename_log(const char* from, const char* to, const char* to_r, int pid, struct timespec start_time){
		struct timespec end_time = get_time();
		mx.lock();
		UniqPid up = get_uinq_pid(pid);
		struct stat buf;
		lstat(to_r,&buf);
		long long size = (long long) buf.st_size;
		proc_log(up);
		print_log(up, "rename", from, start_time, end_time, " ");
		fprintf(log_fp,"to=%s size=%lld\n",to,size);

		mx.unlock();
	}
	~Logger(){
		fclose(log_fp);
		fclose(proc_fp);
		if(detail_fp != NULL)fclose(detail_fp);
	}

	void open_log(const char* path, int flags, int fd, int pid, struct timespec start_time){
		if(fd_value_check(fd))return;
		struct timespec end_time = get_time();
		mx.lock();
		UniqPid up = get_uinq_pid(pid);
		proc_log(up);
		char flag_str[4];
		if((flags &3) == O_RDONLY){
			strcpy( flag_str, "R");
		}else if((flags &3)== O_WRONLY){
			strcpy( flag_str, "W");
		}else if((flags &3)== O_RDWR){
			strcpy( flag_str, "RW");
		}
		if (fd < 0){
		  fprintf(log_fp, "could not open!\n");
		  mx.unlock();
		  return;
		}
		if((data[fd].read_num !=0 || data[fd].write_num!=0)){
			fprintf(log_fp,"assert, duplicated processes are assigned to the same fid!\n");
		}else{
			struct stat buf;
			fstat(fd,&buf);
			long long size = (long long) buf.st_size;
			print_log(up, "opened", path, start_time, end_time, " ");
			fprintf(log_fp,"fd=%d size=%lld mode=%s seq=%d\n",fd, size, flag_str, file_seq);
		}
		data[fd].up = up;
		data[fd].seq = file_seq;
		++file_seq;
		mx.unlock();
	}

	void release_log(const char* path, int fd){
		if(fd_value_check(fd))return;
		mx.lock();
		struct stat buf;
		fstat(fd,&buf);
		long long size = (long long) buf.st_size;
		fprintf(log_fp,"%llu:%d closed %s r_time=%lld r_num=%d r_size=%lld w_time=%lld w_num=%d w_size=%lld size=%lld fid=%d\n",data[fd].up.starttime, data[fd].up.pid, path,
			data[fd].read_time , data[fd].read_num, data[fd].read_size,
			data[fd].write_time, data[fd].write_num,data[fd].write_size, size, fd );
		data[fd].clear();
		mx.unlock();
	}


	
	void read_start(int fd){
		if(fd_value_check(fd))return;
		mx.lock();
		data[fd].read_start_time= get_time();
		mx.unlock();
	}

	void read_end(const char* path, int fd, int read_byte, int err){
		if(fd_value_check(fd))return;
		mx.lock();
		if (read_byte == -1){
			fprintf(log_fp,"%d pread err: fd=%d, %s, %s \n",data[fd].up.pid, fd, path, strerror(err));
			struct stat buf;
			fstat(fd,&buf);
			fprintf(log_fp, "%d fstat:%s\n",data[fd].up.pid,strerror(errno));
			mx.unlock();
			return;
		}
		struct timespec end_time = get_time();
		long long nsec = ((long long)(end_time.tv_sec - data[fd].read_start_time.tv_sec)) * 1000*1000*1000 + end_time.tv_nsec - data[fd].read_start_time.tv_nsec;
		if(detail){
			fprintf(detail_fp, "%d read t=%lld clock=%ld:%ld size=%d\n",data[fd].seq, nsec, end_time.tv_sec, end_time.tv_nsec, read_byte);
		}
		data[fd].read_time+=nsec;
		data[fd].read_num++;
		data[fd].read_size+=(long long)read_byte;
		mx.unlock();
	}

	void write_start(int fd){
		if(fd_value_check(fd))return;
		mx.lock();
		data[fd].write_start_time =get_time();
		mx.unlock();
	}

	void write_end(const char* path, int fd, int write_byte, int err){
		if(fd_value_check(fd))return;
		mx.lock();
		if (write_byte == -1){
			fprintf(log_fp,"%d pwrite err: fd=%d, %s, %s \n",data[fd].up.pid, fd, path, strerror(err));
			struct stat buf;
			fstat(fd,&buf);
			fprintf(log_fp, "%d fstat:%s\n",data[fd].up.pid,strerror(errno));
			mx.unlock();
			return;
		}
		struct timespec end_time = get_time();
		long long nsec = ((long long)(end_time.tv_sec - data[fd].write_start_time.tv_sec)) * 1000*1000*1000 + end_time.tv_nsec - data[fd].write_start_time.tv_nsec;
		if(detail){
			fprintf(detail_fp, "%d write t=%lld clock=%ld:%ld size=%d\n",data[fd].seq, nsec, end_time.tv_sec, end_time.tv_nsec, write_byte);
		}
		data[fd].write_time+=nsec;
		data[fd].write_num++;
		data[fd].write_size+=(long long)write_byte;
		mx.unlock();
	}

	bool fd_value_check(int fd){
		if(fd >= 1024*16){
			mx.lock();
			fprintf(log_fp,"fd err: overfd %d",fd);
			mx.unlock();
			return true;
		}
		return false;
	}


};






