

# Install #

**STEPS:**

To use file system tracing, first you should build and install file system
tracer where you will run your application.
```
$ hg clone http://paratrac.googlecode.com/hg/ paratrac
$ cd paratrac
$ ./prepare.sh  # auto-install required libraries for Debian/Ubuntu users
$ cd paratrac/fs/fuse
$ ./configure   # you may use --prefix to specify the install directory
$ make [OPTIONS]
$ make install  # optional
```

**OPTIONS**:

You may specify which trace should be disabled during the compilation.
  * `make FTRAC_TRACE_ENABLED=0`: disable all trace routines
  * `make FTRAC_TRACE_SYSC_ENABLED=0`: disable system call trace
  * `make FTRAC_TRACE_PROC_ENABLED=0`: disable process trace
  * `make FTRAC_TRACE_PROC_TASKSTAT=0`: disable taskstat for process trace

**NOTES and TIPS**:

**Use Python > 2.5.2 to avoid known problems**

# Start and Stop Tracer #

**STEPS:**

Start the tracer (i.e., `ftrac`) by mounting a empty directory (e.g.,
`mountpoint`).
```
$ ftrac mountpoint [OPTIONS]  # see "ftrac -h" for more options
```

Change to the working directory via the mountpoint, and run your application as usual.
```
$ cd mountpoint/path/to/working/directory
$ ./run_your_application
```

After your application finished, stop the tracer by umounting the mount point.
```
# fusermount comes with package fuse-utils in Debian/Ubuntu systems
$ fusermount -u mountpoint  
```

All tracking log will be stored under a directory named as "ftrac-username-XXXXX".

**OPTIONS:**

  * See `ftrac -h` for details of options.

**NOTES and TIPS:**

  * Using other ways to stop tracker (e.g., `kill`) may _broke_ the log data integrity.
  * Use `-o big_writes` to improve performance of I/O intensive applications.
  * Increase taskstat capacity by `-o nlsock` and `-o nlbufsize` for high parallelisim applications.

# Profile Generation #

**STEPS:**

Generate report is straightforward.
```
$ fstrac -r log_directory
```

If only database is required, use
```
$ fstrac -i log_directory
```
to import log files to database.

**OPTIONS:**

  * See `fstrac -h` for details of options.