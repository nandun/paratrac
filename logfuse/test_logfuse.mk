
all:
	@echo ---mount---
	./logfuse src_dir mnt_dir log_hello -detail
	sleep 1
	@echo ---mount ok---
	touch mnt_dir/hoge
	cat mnt_dir/hello
	cp mnt_dir/1M mnt_dir/1M_cp
	ls mnt_dir -l
	mv mnt_dir/1M_cp mnt_dir/1M_mv
	rm mnt_dir/1M_mv
	mkdir mnt_dir/a_dir
	rm -r mnt_dir/a_dir
	@echo ---umount---
	fusermount -u mnt_dir
	@echo ---cat access_log---
	(sleep 1;cat log_hello/`hostname`/access_log;) ||\
	(sleep 1;cat log_hello/`hostname`/access_log;) ||\
	(sleep 1;cat log_hello/`hostname`/access_log;) ||\
	(sleep 1;cat log_hello/`hostname`/access_log;) ||\
	(sleep 1;cat log_hello/`hostname`/access_log;) ||\
	(sleep 1;cat log_hello/`hostname`/access_log;) ||\
	(sleep 1;cat log_hello/`hostname`/access_log;) ||\
	(sleep 1;cat log_hello/`hostname`/access_log;) ||\
	(sleep 1;cat log_hello/`hostname`/access_log;) ||\
	(sleep 1;cat log_hello/`hostname`/access_log;) ||\
	(sleep 1;cat log_hello/`hostname`/access_log;) 
	(sleep 1;cat log_hello/`hostname`/proc_log;) 


