

# Data Persistence #

## Raw CSV Logs ##

## SQL Database ##

ParaTrac uses [sqlite3](http://www.sqlite.org/) to store and retrieve trace data, which makes the trace data portable and queryable by SQL commands or APIs. There are four tables in ParaTrac trace database: _env_, _file_, _proc_, and _syscall_.

Table _env_

Table _file_ stores the paths of all files that have been accessed and their mappings from `iid` and `fid` to `path`.
  * **Column Definition**
```
iid INTEGER, fid INTEGER, path TEXT
```
    1. _iid_: Instance ID of ftrace trace.
    1. _fid_: File ID.
    1. _path_: The absolute path of the file.

Table _proc_ stores the process information traced during the application.
  * **Column Definition**
```
iid INTEGER, pid INTEGER, ppid INTEGER, live INTEGER, res INTEGER, 
btime FLOAT, elapsed FLOAT, cmdline TEXT, environ TEXT
```
    1. _iid_: Instance ID of tracer
    1. _pid_: Process ID (not _real_ _pid_, instead, hashed from _pid_ and process time.)
    1. _ppid_: Parent process ID (not _real_ _pid_, instead, hashed from _ppid_ and process time.)
    1. _live_: The liveness of process after the tracing period. 0 is dead and 1 is live.
    1. _res_: Return value of the process exit status
    1. _btime_: Began time of the process/task
    1. _elapsed_: Elapsed time of the process/task
    1. _cmdline_: Command line that starts the process/task.
    1. _environ_: Environment variables.

Table _syscall_ is used to store every system calls conducted during tracing time.
  * **Column Definition**
```
stamp DOUBLE, iid INTEGER, pid INTEGER, sysc INTEGER, fid INTEGER, res INTEGER, 
elapsed DOUBLE, aux1 INTEGER, aux2 INTEGER
```
    1. _stamp_: Timestamp when the system call was called.
    1. _iid_: Instance ID of tracer, used to identify tracers. _iid_ is used to differentiate process with the same _pid_ but resides in different machines.
    1. _pid_: Process ID of the process who invoked the system call.
    1. _sysc_: The system call number.
    1. _fid_: The file ID.
    1. _res_: The return value of system call.
    1. _elapsed_: The elapsed time (or latency) of the system call.
    1. _aux1_: Auxiliary column 1 for multiple purposes, see below:
      * If _sysc_ is `read` or `write`, then _aux1_ indicates the data size requested in the system call.
      * If _sysc_ is `link`, then _aux1_ is the _fid_ that is linked to.
      * If _sysc_ is `open` or `close`, then _aux1_ indicates the file size (in bytes) during the `open`/`close` time.
    1. _aux2_: Auxiliary column 2 for multiple purposes, see below:
      * If _sysc_ is `read` or `write`, then _aux2_ indicates the file offset (in bytes) requested in the system call.

# References and Memos #

## Process and File Tracing ##
  * Linux `/proc` filesystem: http://en.wikipedia.org/wiki/Procfs
  * Linux kernel process accouting
    * Enable process accouting: http://www.tldp.org/HOWTO/Process-Accounting/index.html
  * Task accounting: http://www.kernel.org/doc/Documentation/accounting/
    * Userspace filesystem by FUSE: http://fuse.sourceforge.net/
    * Filesystem watching by inotify: http://inotify.aiken.cz/

## Data Plotting and Graph Drawing ##
  * matplotlib Python Library: http://matplotlib.sourceforge.net/
  * NetworkX Python Library: http://networkx.lanl.gov/
  * Add interactivity to your SVG: http://www.ibm.com/developerworks/library/x-svgint/