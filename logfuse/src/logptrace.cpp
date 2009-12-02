#include <stdio.h>
#include <unistd.h>
#include <sys/ptrace.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <asm/user.h>
#include <asm/ptrace.h>
#include <stdlib.h>
#include <map>

#include "logger.h"
#include "logptrace.h"

using namespace std;

// this is writen for x86-64 and linux

void upeek(int pid, long off, long* res){
        errno = 0;
        val = ptrace(PTRACE_PEEKUSER, pid, (char *) off, 0);
        if (val == -1 && errno) {
                char buf[60];
                sprintf(buf,"upeek: ptrace(PTRACE_PEEKUSER,%d,%lu,0)",pid,off);
                perror(buf);
                throw -1;
        }
        *res = val;
        return;
};

class Cstat{
public:
	int ret;
	struct stat m;
	Cstat(const char* path){
		memset(&s, 0, sizeof(s));
		ret = stat(path,&s);
	}
	Cstat(int fd){
		memset(&s, 0, sizeof(s));
		ret = fstat(fd,&s);
	}

};

class StateOfPid{
	int pid;
	long sys_call_no;
	int currpers;
	bool in_syscall;

	char tmp_path[1024];
	int tmp_fd;
	int tmp_flags;
	bool tmp_crate;
	Logger* logger;

	static int argreg[SUPPORTED_PERSONALITIES][MAX_ARGS] = {
		{RDI,RSI,RDX,R10,R8,R9},	/* x86-64 ABI */
		{RBX,RCX,RDX,RSI,RDI,RBP}	/* i386 ABI */
	};
	StateOfPid(int pid,const char* log_dir){
		in_syscall = false;
		sys_call_no = 0;
		this->pid = pid;
		currpers=0;
		tmp_fd =0;
		tmp_flags = 0;
		memset(tmp_path,0,sizeof(tmp_path));

		logger = new Logger(log_dir);
	}
	void do_log(){
		if(!in_syscall){
			set_scno();
			set_personality();
			switch(sys_call_no){
			ID_OPEN:
			ID_CREAT:
				tmp_flags = (sys_cal_no == ID_OPEN ?
						(int)arg(1) : O_CREAT|O_WRONLY|O_TRUNC);
				strncpy(tmp_path, (char*)arg(0), 1023);
				Cstat s(tmp_path);
				tmp_create = ( s.ret == -1 ?  true : false );
				break;
			ID_CLOSE:
				tmp_fd = (int)arg(0);
				break;
			ID_READ:
				break;
			ID_WRITE:
				break;
			}
			in_syscall=true;
		}else{
			switch(sys_call_no){
			ID_OPEN:
			ID_CREAT:
				int fd = ret_val();
				logger.open_log(tmp_path, tmp_flags, fd, pid);
				if(fd>=0 && tmp_create)logger.mknod_log(tmp_path, pid);
				break;
			ID_CLOSE:
				logger.release_log(tmp_path, tmp_fd);
				break;
			ID_READ:
				break;
			ID_WRITE:
				break;
			}
			in_syscall =false;
		}
	}
	long ret_val(){
		long ret;
		upeek(pid, 8*ORIG_RAX, &ret);
		return ret;
	}
	void set_scno(){
		upeek(pid, 8*ORIG_RAX, &sys_call_no);
	}
	long arg(int i){
		long ret = 0;
		upeek(pid, 8*argreg[currpers][i], &ret);
	}
	void set_personality(){
		/* Check CS register value. On x86-64 linux it is:
		*      0x33    for long mode (64 bit)
		*      0x23    for compatibility mode (32 bit)
		* It takes only one ptrace and thus doesn't need
		* to be cached.
		*/
		upeek(pid, 8*CS, &val);
		switch(val)
		{
		case 0x23: currpers = 1; break;
		case 0x33: currpers = 0; break;
		default:
		fprintf(stderr, "Unknown value CS=0x%02X while "
		"detecting personality of process "
		"PID=%d\n", (int)val, pid);
		currpers = current_personality;
		break;
		}
	}
};

int main(int argc, char* argv[]) {

	if(argc != 2){
		printf("usage ./strace_mod command");
		return -1;
	}


	int i;
	static char* syscallstr[1000];
	init_syscallstr(syscallstr,1000);

	pid_t pid = fork();
	if (pid==0) {

		// child
		ptrace(PTRACE_TRACEME, 0, 0, 0);
		char *const cargv[] = { "echo_rap", "/bin/echo hoge", NULL };
        char *const cenvv[] = { NULL };
	    execve("./echo_rap",cargv,cenvv); //never return, is the same process.

	} else {
		map<pid, StateOfPid*> state_of_pid;

		int status = 0;
		int pid = wait(&status); // wait for entering by 'execve'
		printf("enter execve()\n");
		state_of_pid.

		char tmp_dir[1024];
		snprintf(tmp_dir,1024,"/tmp/logptrace_%d_%d", getuid(), getpid() );
		char sh_line[2048];
		snprintf(sh_line,2048,"mkdir  %s -m 755", tmp_dir);
		system(sh_line);

		printf("%d:",pid);
		while (ptrace(PTRACE_SYSCALL, pid, 0, 0) == 0) {
		int pid_traced = wait(&status);
		printf("%d:",pid_traced);

		if(WIFEXITED(st)) {
			puts("= exit"); break;
		}
		long call_no = ptrace(PTRACE_PEEKUSER, pid, 4 * ORIG_EAX, 0);
		struct user_regs_struct regs;
		ptrace(PTRACE_GETREGS, pid, 0, &regs);
		if (in_syscall == 0) {
			in_syscall = 1;
			printf("enter %s(0x%llx) ",
			(regs.rax >= 0 && regs.rax < 1000) ? syscallstr[regs.rax] : "???", regs.rbx);
		} else {
			in_syscall = 0;
			printf("= %lld\n", regs.rax);
		}
		fflush(stdout);
	}

  }
  return 0;
}
