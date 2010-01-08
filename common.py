#############################################################################
# ParaTrac: Scalable Tracking Tools for Parallel Applications
# Copyright (C) 2009  Nan Dun <dunnan@yl.is.s.u-tokyo.ac.jp>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#############################################################################

import sys
import os
import optparse
import textwrap
import time

#
# Constants
#

# Data size constants in bytes
KB = 1024
MB = 1048576
GB = 1073741824
TB = 1099511627776

# seconds constants in bytes
# start from usec
USEC = 1
MSEC = 1000
SEC  = 1000000
MIN  = 60000000
HOUR = 3600000000

# Socket constants
SOCKET_BUFSIZE = 4096

# System call number
SYSCALL = {}

SYSCALL_FILESYSTEM = {}
SYSCALL_FILESYSTEM["lstat"] = 84
SYSCALL_FILESYSTEM["fstat"] = 28
SYSCALL_FILESYSTEM["access"] = 33
SYSCALL_FILESYSTEM["readlink"] = 85
SYSCALL_FILESYSTEM["opendir"]	= 205
SYSCALL_FILESYSTEM["readdir"] = 89
SYSCALL_FILESYSTEM["closedir"] = 206
SYSCALL_FILESYSTEM["mknod"] = 14
SYSCALL_FILESYSTEM["mkdir"] = 39
SYSCALL_FILESYSTEM["unlink"] = 10
SYSCALL_FILESYSTEM["rmdir"] = 40
SYSCALL_FILESYSTEM["symlink"] = 83
SYSCALL_FILESYSTEM["rename"] = 38
SYSCALL_FILESYSTEM["link"] = 9
SYSCALL_FILESYSTEM["chmod"] = 15
SYSCALL_FILESYSTEM["chown"] = 16
SYSCALL_FILESYSTEM["truncate"] = 92
SYSCALL_FILESYSTEM["utime"] = 30
SYSCALL_FILESYSTEM["creat"] = 8
SYSCALL_FILESYSTEM["open"] = 5
SYSCALL_FILESYSTEM["close"] = 6
SYSCALL_FILESYSTEM["read"] = 3
SYSCALL_FILESYSTEM["write"] = 4
SYSCALL_FILESYSTEM["statfs"] = 99
SYSCALL_FILESYSTEM["flush"] = 203
SYSCALL_FILESYSTEM["fsync"] = 118
SYSCALL_FILESYSTEM["setxattr"] = 201
SYSCALL_FILESYSTEM["getxattr"] = 202
SYSCALL_FILESYSTEM["listxattr"] = 203
SYSCALL_FILESYSTEM["removexattr"] = 204

# reverse map
for k, v in SYSCALL_FILESYSTEM.items():
    SYSCALL_FILESYSTEM[v] = k

SYSCALL.update(SYSCALL_FILESYSTEM)

#
# Utilities
#

if sys.platform == "win32":
    timer = time.clock
else:
    timer = time.time

def parse_datasize(size):
    size = size.upper()
    if size.isdigit():
        return eval(size)
    if size.endswith('B'):
        size = size[0:-1]
    if size.endswith('K'):
        return eval(size[0:-1]) * KB
    if size.endswith('M'):
        return eval(size[0:-1]) * MB
    if size.endswith('G'):
        return eval(size[0:-1]) * GB

def smart_datasize(size):
    """ given a size in bytes, return a tuple (num, unit) """
    size = float(size)
    if size < KB:
        return (size, "B")
    if size < MB:
        return (size/KB, "KB")
    if size < GB:
        return (size/MB, "MB")
    if size < TB:
        return (size/GB, "GB")
    return (size/TB, "TB")

def smart_usec(usec):
    """ given a time in usecs, return a tuple (num, unit) """
    usec = float(usec)
    if usec < MSEC:
        return (usec, "usec")
    if usec < SEC:
        return (usec/MSEC, "msec")
    if usec < MIN:
        return (usec/SEC, "sec")
    if usec < HOUR:
        return (usec/MIN, "min")
    return (usec/HOUR, "hour")

def smart_cmdline(cmdline, verbose=0):
    """Shorten the command line based on verbose level"""
    if verbose >= 2:
        return cmdline
    elif verbose == 1:
        return cmdline.split(" ", 1)[0]
    elif verbose <= 0:
        return os.path.basename(cmdline.split(" ", 1)[0])

def smart_filename(filepath, verbose=0):
    if verbose >= 1:
        return filepath
    elif verbose <= 0:
        return os.path.basename(filepath)

def smart_makedirs(path, confirm=True):
    try: os.makedirs(path)
    except OSError, err:
        if err.errno == errno.EEXIST:
            sys.stderr.write("warning: directory %s exists\n" 
                % os.path.abspath(path))
            if confirm:
                ans = raw_input("Overwrite [Y/n/path]? ").lower()
                if ans == 'n':
                    sys.stderr.write("Aborting ...\n")
                    sys.exit(1)
                elif ans == 'y': pass
                else: return smart_makedirs(ans, confirm)
            else:
                sys.stderr.write("overwriting %s ...\n"
                    % os.path.abspath(path))
        else:
            sys.stderr.write("failed to create %s, %s\n" % \
                (path, os.strerror(err.errno)))
            sys.exit(1)
    return path

def string_hash(str):
    hash = 0
    for i in range(0, len(str)):
        hash = hash + ord(str[i]) * (i + 1);
    return hash

# list utilities
def list_unique(a):
    """ return the list with duplicate elements removed """
    return list(set(a))

def list_intersect(listoflist):
    """ return the intersection of a series of lists """
    inter = set(listoflist[0])
    for l in listoflist:
        inter = inter & set(l)
    return list(inter)

def list_union(listoflist):
    """ return the union of a series of lists """
    union = set(listoflist[0])
    for l in listoflist:
        union = union | set(l)
    return list(union)

def list_difference(listoflist):
    """ return the difference of a series of lists """
    diff = set(listoflist[0])
    for l in listoflist:
        diff = diff.difference(set(l))
    return list(set(diff))

def list_remove(fromlist, listofwithout):
    origset = set(fromlist)
    for l in listofwithout:
        origset = origset - set(l)
    return list(origset)

# class init utility
def update_opts_kw(obj, restrict, opts, kw):
    """
    Update objects's dict from opts and kw, restrict keyword in restrict
    """
    if opts is not None:
        for key in restrict:
            if obj.__dict__.has_key(key) and opts.__dict__.has_key(key):
                obj.__dict__[key] = opts.__dict__[key]

    if kw is not None:
        for key in restrict:
            if obj.__dict__.has_key(key) and kw.has_key(key):
                obj.__dict__[key] = kw[key]

