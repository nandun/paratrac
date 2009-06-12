#############################################################################
# ParaTrac: Scalable Tracking Tools for Parallel Applications
# Copyright (C) 2009  Nan Dun <dunnan@yl.is.s.u-tokyo.ac.jp>

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
# General tracker class
#

import os
import pwd
import socket
import sys

class Tracker:
    def __init__(self):
        # Environmental variables
        self.uid = os.getuid()
        self.pid = os.getpid()
        self.user = pwd.getpwuid(self.uid)[0]
        self.hostname = socket.gethostname()
        self.platform = " ".join(os.uname())
        self.cmdline = " ".join(sys.argv)

        # Debug variables
        self.verbosity = 0
        self.verbosestr = "[trac:%5d] %s\n"
        self.verbosecnt = 0
        self.dryrun = False

        # Streams
        self.ws = sys.stdout.write
        self.es = sys.stderr.write

    def verbose(self, msg):
        self.ws(self.verbosestr % (self.verbosecnt, msg))
        self.verbosecnt += 1
