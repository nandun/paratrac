

# Introduction #

# File System Tracking and Profiling #

ParaTrac uses several user-level techniques to perform the file system level
tracing.

| **Variables** | **Interfaces** | **Usages** |
|:--------------|:---------------|:-----------|
| Pid | FUSE | System call invoker, process specified statistics |
| PPid | `/proc` file system | Calling dependencies, process tree, process grouping in workflow analysis |
| Cmdline | `/proc` file system | Tasks names, tasks arguments, workflow annotation. |