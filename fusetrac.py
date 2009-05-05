#!/usr/bin/env python

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

import commands
import errno
import optparse
import os
import pwd
import socket
import sys
import tempfile
import threading
import textwrap

from common import *

class FUSETrac:
    def __init__(self, opts=None, **kw):
        ## readonly variables ##
        self.uid = os.getuid()
        self.pid = os.getpid()
        self.user = pwd.getpwuid(self.uid)[0]
        self.hostname = socket.gethostname()
        self.platform = " ".join(os.uname())
        
        self.mode = None
        self.mountpoint = None
        self.pollpath = None
        self.verbosity = 0
        self.dryrun = False

        if opts is not None:
            for k, v in opts.__dict__.items():
                if self.__dict__.has_key(k):
                    self.__dict__[k] = v
        for k, v in kw.items():
            if self.__dict__.has_key(k):
                self.__dict__[k] = v

        # check ftrac executable
        self.ftrac = None
        res, path = commands.getstatusoutput("which ftrac")
        if res == 0:
            self.ftrac = path
        elif os.path.exists("./fuse/ftrac"):
            self.ftrac = os.path.abspath("./fuse/ftrac")
        else:
            es("error: ftrac executable not found.\n")
            sys.exit(1)
        
        self.mountpoint = os.path.abspath(self.mountpoint)
        
        self.session = None
        self.sockpath = None
        self.servsock = None
        self.vcnt = 0
        self.data = []
        self.dataproc = None
        self.dataready = threading.Condition()   # result is ready
    
    def verbose(self, msg):
        ws("[fusetrac:%5d] %s\n" % (self.vcnt, msg))
    
    # mount/umount primitivies
    def mount(self, mountpoint=None):
        if mountpoint is None:
            mountpoint = self.mountpoint
        assert os.path.isdir(mountpoint)
        if self.verbosity >= 2:
            self.verbose("mount: os.system('%s %s')" % 
                (self.ftrac, mountpoint))
        if self.dryrun:
            return
        os.system("%s %s" % (self.ftrac, mountpoint))

    def umount(self, mountpoint=None):
        if mountpoint is None:
            mountpoint = self.mountpoint
        if self.verbosity >= 2:
            self.verbose("umount: os.system('fusermount -u %s')" %
                mountpoint)
        if self.dryrun:
            return
        os.system("fusermount -u %s" % mountpoint)

    # session routines
    def sessioninit(self):
        if self.mode == "track":
            return
        elif self.mode == "poll":
            self.session = "/tmp/ftrac-%s-%u" % (self.user, 
                string_hash(self.mountpoint))
            if not os.path.isdir(self.session):
                es("error: session for mount point %s does not exist\n" % \
                    self.mountpoint)
                sys.exit(1)

        # connect to fusetrac server
        self.sockpath = "%s/ftrac.sock" % self.session
        self.servsock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        if self.verbosity >= 1:
            self.verbose("sessioninit: sock.connect(%s)" % self.sockpath)
        self.servsock.connect(self.sockpath)
        
        # start data processing thread
        self.dataproc = self.DataProcessor(self)
        #self.dataproc.start()
    
    # polling routines
    def poll(self, sock=None, path=None, rate=1.0, times="Inf", window="Inf"):
        if sock is None:
            sock = self.servsock
        if path is None:
            path = self.pollpath

        if path == "":
            op = "%s" % FTRAC_POLL_STAT
            times = 1
            rate = 1000
        elif path == "/":
            op = "%s" % FTRAC_POLL_FILESYSTEM
        elif os.path.isdir(path):
            op = "%s:%s" % (FTRAC_POLL_DIRECTORY, path)
        else:
            op = "%s:%s" % (FTRAC_POLL_FILE, path)
    
        pollsleep =  1.0 / rate
        polltimes = 0
        pollwindow = 0.0
        pollstart = timer()
        
        try:
            while (polltimes < times and pollwindow < window):
                sock.send(op)
                res = eval(sock.recv(1024))
                polltimes += 1
                pollwindow = timer() - pollstart
                self.dataprocess((op[0], res))
                #self.dataready.acquire()
                #self.data.append((op[0], res))
                #self.dataready.notify()
                #self.dataready.release()
                time.sleep(pollsleep)
        except KeyboardInterrupt:
            es("polling terminated by keyboard\n")
            sock.send("%s" % FTRAC_POLL_FINISH)
        return
    
    # data processing routines
    def dataprocess(self, data):
        self.datatoprettytxt(data)
        
    def datatoprettytxt(self, data, stream=None):
        if stream is None:
            stream = sys.stdout
        op, res = data

        if op == FTRAC_POLL_STAT:
            str = \
"""\
Paratrac FUSE tracker (version: %s, %s)
    tracker: %s
       user: %s
 mountpoint: %s
      start: %s
    elapsed: %s seconds
"""\
            % (PARATRAC_VERSION, PARATRAC_DATE,
               res["tracker"], res["username"], res["mountpoint"],
               time.strftime("%Y-%m-%d %H:%M:%S", 
               time.localtime(float(res["start"]))), 
               res["elapsed"])
        else:
            str = ""
            for c in FTRAC_SYSCALL:
                atime, cnt, elapsed, pid = res[c]
                str += "%10s: %10s %5d %15f %10u\n" % (c, time.strftime("%H:%M:%S",
                    time.localtime(atime)), cnt, elapsed, pid)
            for c in FTRAC_IOCALL:
                atime, cnt, elapsed, pid, size, offset, bytes = res[c]
                bytes, unit = smart_datasize(bytes)
                str += "%10s: %10s %5d %15f %10u %10u %10u %10u%s\n" % \
                    (c, time.strftime("%H:%M:%S",
                    time.localtime(atime)), cnt, elapsed, pid, size, offset,
                    bytes, unit)
            if op == FTRAC_POLL_FILE:
                str += "%10s: %10s\n" % ("born",
                    time.strftime("%H:%M:%S", time.localtime(res["born"])))
                str += "%10s: %10s\n" % ("dead",
                    time.strftime("%H:%M:%S", time.localtime(res["dead"])))
            str += "\n"

        stream.write(str)

    class DataProcessor(threading.Thread):
        def __init__(self, ftrac, opts=None):
            threading.Thread.__init__(self)
            self.ftrac = ftrac
            self.opts = opts
            
            self.ready = ftrac.dataready
            self.data = ftrac.data
            self.dataproc = ftrac.dataprocess

        def run(self):
            while True:
                self.ready.acquire()
                while not self.data:
                    self.ready.wait()
                self.dataproc(self.data.pop(0))
                self.ready.release()

### standalone routines ###
def parse_argv(argv):
    usage = "usage: %prog [options]"
    parser = optparse.OptionParser(usage=usage,
                 formatter=OptionParserHelpFormatter())
    
    parser.remove_option("-h")
    parser.add_option("-h", "--help", action="store_true",
                      dest="help", default=False,
                      help="show the help message and exit")
    
    parser.add_option("-m", "--mode", action="store", type="choice",
                      dest="mode", metavar="track/poll", default="track",
                      choices=["poll", "track"],
                      help="running mode (default: track)\n"
                           "  track: start and track a program\n"
                           "   poll: start polling a tracked fuse tracker")
    
    parser.add_option("-p", "--poll", action="store", type="string",
                      dest="pollpath", metavar="PATH", default="",
                      help="polling path (default: statust "")")
    
    parser.add_option("-v", "--verbosity", action="store", type="int",
                dest="verbosity", metavar="NUM", default=0,
                help="verbosity level: 0/1/2/3 (default: 0)")

    opts, args = parser.parse_args(argv)
    
    opts.print_help = parser.print_help

    if opts.mode == "poll":
        if len(args) == 0:
            es("error: missing mount point\n")
            sys.exit(1)
        opts.mountpoint = args[0]

    return opts, args

def main():
    opts, args = parse_argv(sys.argv[1:])
    if opts.help:
        opts.print_help()
        return 0

    fusetrac = FUSETrac(opts)
    fusetrac.sessioninit()
    fusetrac.poll()
    #fusetrac.pollstat(sys.argv[1])
    #fusetrac.pollfilesystem(sys.argv[1])

if __name__ == "__main__":
    sys.exit(main())
