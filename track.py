#!/usr/bin/env python

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

import commands
import errno
import optparse
import os
import pwd
import socket
import sys
import tempfile
import textwrap

from common import *

           
class Track:
    def __init__(self, opts=None, **kw):
        ## read only variables, Do NOT modify ##
        self.uid = os.getuid();
        self.pid = os.getpid();
        self.user = pwd.getpwuid(self.uid)[0]
        self.hostname = socket.gethostname()
        self.platform = " ".join(os.uname())

        ## configurable varialbes ##
        self.trace = None
        self.sessiondir = None
        self.verbosity = 0
        self.dryrun = False
        
        # fuse track variables
        self.mountpoint = None

        if opts is not None:
            for k, v in opts.__dict__.items():
                if self.__dict__.has_key(k):
                    self.__dict__[k] = v
        for k, v in kw.items():
            if self.__dict__.has_key(k):
                self.__dict__[k] = v
        
        ## runtime variables ##
        self.vcnt = 0
        self.ftrac = None
    
    def verbose(self, msg):
        ws("[paratrac:%5d] %s\n" % (self.vcnt, msg))
        
    def ftrac_prepare(self):
        self.ftrac = FUSETrac();
        self.ftrac.verbosity = self.verbosity
        self.ftrac.dryrun = self.dryrun;
        if self.mountpoint is None:
            self.mountpoint = tempfile.mkdtemp(prefix="ftrac-mp.", 
                dir="/tmp")
        if self.verbosity >= 3:
            self.verbose("ftrac_prepare: tempfile.mkdtemp(%s)" 
                % self.mountpoint)
        self.ftrac.mountpoint = self.mountpoint
        self.ftrac.mount()
    
    def ftrac_finish(self):
        self.ftrac.umount()
        if self.verbosity >= 3:
            self.verbose("ftrac_finish: os.rmdir(%s)" % self.mountpoint)
        os.rmdir(self.mountpoint)
        
    def track(self):
        # pre-processing
        if "fuse" in self.trace:
            self.ftrac_prepare()
        
        # post-processing
        if "fuse" in self.trace:
            self.ftrac_finish()

#### Standalone routines ####
def parse_argv(argv):
    usage = "usage: %prog [options]"
    parser = optparse.OptionParser(usage=usage, 
        formatter=OptionParserHelpFormatter())
    
    parser.remove_option("-h")
    parser.add_option("-h", "--help", action="store_true",
        dest="help", default=False, help="show the help message and exit")
    
    parser.add_option("-t", "--trace", action="store", type="string",
        dest="trace", metavar="TYPE,TYPE", default="fuse",
        help="tracing method\n"
             "  fuse: file system call tracing by FUSE")
    
    parser.add_option("--mountpoint", action="store", type="string",
        dest="mountpoint", metavar="PATH", default=None,
        help="mountpoint in fuse tracking")

    parser.add_option("-v", "--verbosity", action="store", type="int",
                dest="verbosity", metavar="NUM", default=0,
                help="verbosity level: 0/1/2/3 (default: 0)")
    
    parser.add_option("-d", "--dryrun", action="store_true",
                dest="dryrun", default=False,
                help="dry run, do not execute (default: disabled)")
    
    opts, args = parser.parse_args(argv)
    
    opts.print_help = parser.print_help

    opts.trace = map(lambda s:"%s" % s, opts.trace.strip(',').split(','))
    
    return opts

def main():
    opts = parse_argv(sys.argv[1:])
    if opts.help:
        opts.print_help()
        return 0
    tracker = Track(opts)
    tracker.track()
    return 0

if __name__ == "__main__":
    sys.exit(main())

