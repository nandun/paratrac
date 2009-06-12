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

#
# Constants
#

# Data size constants in bytes
KB = 1024
MB = 1048576
GB = 1073741824
TB = 1099511627776

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

SYSCALL.update(SYSCALL_FILESYSTEM)
