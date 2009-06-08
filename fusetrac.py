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
import curses
import curses.textpad
import errno
import optparse
import os
import pwd
import signal
import socket
import subprocess
import sys
import tempfile
import textwrap
import threading
import traceback

from common import *

FTRAC_SOCK_BUFSIZE = 4096

# ftrac constance, keep consistent with fuse/ftrac.c
FTRAC_SYSCALL = ["stat", "access", "readlink", "opendir", "readdir", 
    "closedir", "mknod", "mkdir", "symlink", "unlink", "rmdir", "rename", 
    "link", "chmod", "chown", "truncate", "utime", "open", "statfs", 
    "flush", "close", "fsync"]
FTRAC_IOCALL = ["read", "write"]

FTRAC_STAT_SC = ["sc_count", "sc_elapsed", "sc_esum"]
FTRAC_STAT_IO = ["io_summary"]
CHART_TITLES = {}
CHART_TITLES["sc_count"] = "Total System Call Count"
CHART_TITLES["sc_elapsed"] = "Elapsed Time per System Call"
CHART_TITLES["sc_esum"] = "Total System Call Elapsed Time"
CHART_TITLES["io_summary"] = "I/O Traffic Summary"

FTRAC_PATH_PREFIX = "/tmp"
FTRAC_SIGNAL_INIT = signal.SIGUSR1
FTRAC_CTRL_OK = 0
FTRAC_CTRL_FINISH = 1
FTRAC_CTRL_POLL_STAT = 2
FTRAC_CTRL_FLUSH = 3

GV_IF_CURSES_INIT = False

class FUSETrac:
    def __init__(self, opts=None, **kw):
        ## readonly variables ##
        self.uid = os.getuid()
        self.pid = os.getpid()
        self.user = pwd.getpwuid(self.uid)[0]
        self.hostname = socket.gethostname()
        self.platform = " ".join(os.uname())
        self.cmd = None
        
        self.mode = None
        self.mountpoint = None
        self.pollpath = None
        self.pollrate = None
        self.pollratemulti = None
        self.pollcount = None
        self.pollduration = None
        self.pollfileonly = False
        self.autoumount = False
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
        self.tracstart = None
        self.vcnt = 0
        self.start = None
        self.end = None

        # data processing variables
        self.data = []
        self.dataready = threading.Condition()   # result is ready
        self.dataproc = None

        # curses variables
        self.win = None
        self.wincontrol = None
        self.winrefresh = True

        # report variables
        self.reportflush = False
    
    def verbose(self, msg):
        ws("[fusetrac:%5d] %s\n" % (self.vcnt, msg))
    
    # mount/umount primitivies
    def mount(self, mountpoint=None):
        if mountpoint is None:
            mountpoint = self.mountpoint
        if not os.path.exists(mountpoint):
            os.makedirs(mountpoint)
        elif not os.path.isdir(mountpoint):
            es("error: %s exists and is not a directory\n" % mountpoint)
            sys.exit(1)
            
        if self.dryrun:
            return
        
        stauts, output = commands.getstatusoutput("%s %s -o notify_pid=%d" 
            % (self.ftrac, mountpoint, self.pid))
        if len(output):
            es("%s\n" % output)
            sys.exit(1)
        
        # since ftrac(fuse) will spawn another process,
        # above startup process return does NOT mean fuse main loop is started,
        # we should wait ftrac to notify us the initialization is done.
        def signal_received(signum, stack):
            pass
        signal.signal(signal.SIGUSR1, signal_received)
        signal.pause()
        return 0

    def umount(self, mountpoint=None):
        if mountpoint is None:
            mountpoint = self.mountpoint
        if self.dryrun:
            return
        _, output = commands.getstatusoutput("fusermount -u %s" % mountpoint)
        if len(output):
            es("warning: %s\n" % output)
            sys.exit(1)
    
    # session routines
    def sessioninit(self):
        self.start = (time.localtime(), timer())

        self.session = "/tmp/ftrac-%s-%u" % (self.user, 
            string_hash(self.mountpoint))
        if not os.path.isdir(self.session):
            # mount ftrac
            if self.mount() != 0:
                es("error: failed to init ftrac\n")
                sys.exit(1)

        # connect to fusetrac server
        try:
            self.sockpath = "%s/ftrac.sock" % self.session
            self.servsock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            if self.verbosity >= 1:
                self.verbose("sessioninit: sock.connect(%s)" % self.sockpath)
            self.servsock.connect(self.sockpath)
        except:
            es("error: failed to connect to %s\n" % self.sockpath)
            sys.exit(1)
        
        # start data processing thread
        self.dataproc = self.DataProcessor(self)
        self.dataproc.start()

        self.wininit()
    
    def sessionfinal(self):
        self.end = (time.localtime(), timer())
        self.servsock.close()
        self.winfinal()
        if self.autoumount:
            self.umount()
    
    # curses routines
    class WinController(threading.Thread):
        def __init__(self, ftrac):
            threading.Thread.__init__(self)
            self.ftrac = ftrac 
        
        def run(self):
            while True:
                try:
                    c = chr(self.ftrac.win.getch())
                except ValueError:
                    continue
                if c in "sS":
                    curses.flash()
                    self.ftrac.winrefresh = False
                elif c in "cC":
                    self.ftrac.winrefresh = True
                elif c in "fF":
                    self.ftrac.ftrac_flush()
                    curses.flash()
                elif c == "<":
                    self.ftrac.pollrate *= self.ftrac.pollratemulti
                elif c == ">":
                    self.ftrac.pollrate /= self.ftrac.pollratemulti

    def wininit(self):
        self.win = curses.initscr()
        curses.noecho()
        curses.cbreak()
        curses.curs_set(0)
        self.wincontrol = self.WinController(self)
        self.wincontrol.start()
        GV_IF_CURSES_INIT = True

    def winfinal(self):
        curses.nocbreak()
        curses.echo()
        curses.endwin()
            
    def winoutput(self, data):
        op, res, count, duration, _ = data
        
        self.win.erase()

        summary = "Started at %s, elapsed %f seconds\n" % \
            (time.strftime("%a, %d %b %Y %H:%M:%M %Z", 
             time.localtime(self.tracstart)), timer() - self.tracstart)
        summary += "mountpoint: %s\n" % self.mountpoint
        summary += "rate: %f, count: %d, duration: %f seconds\n" % \
            (1/self.pollrate, count, duration)
        self.win.addstr(summary)
        
        formatstr = "%9s%17s%12s%18s%12s\n"
        titles = formatstr % ("OPERATION", "ACCESSED", "COUNT", "ELAPSED", 
            "BYTES")
        attr = curses.A_REVERSE | curses.A_BOLD
        self.win.addstr(titles, attr)
        
        formatstr = "%9s%17s%12d%18f\n"
        str = ""
        for c in FTRAC_SYSCALL:
            stamp, cnt, esum = res[c]
            str += formatstr % (c, 
                time.strftime("%m/%d %H:%M:%S", time.localtime(stamp)),
                cnt, esum)
        
        formatstr = "%9s%17s%12d%18f%12s\n"
        for c in FTRAC_IOCALL:
            stamp, cnt, esum, bytes = res[c]
            bytestr = "%d%s" % smart_datasize(bytes)
            str += formatstr % \
                (c, time.strftime("%m/%d %H:%M:%S", time.localtime(stamp)), cnt, 
                 esum, bytestr)

        self.win.addstr(str + "\n")
        
        self.win.addstr(
            "f: flush ftrac data, s: snapshot, c: continue\n"
            "<: rate/%.3f, >: rate*%.3f, Ctrl+c: detach.\n"
            % (self.pollratemulti, self.pollratemulti),
            curses.A_BOLD)
        
        self.win.refresh()

    # polling routines
    def poll(self, sock=None, path=None):
        if sock is None:
            sock = self.servsock
        if path is None:
            path = self.pollpath
        
        envf = open("ftrac-%s-%d/env.log" % (self.user,
            string_hash(self.mountpoint)), "rb")
        env = {}
        for line in envf.readlines():
            pair = line.strip().split(":")
            env[pair[0]] = pair[1]
        envf.close()
        self.tracstart = eval(env["start"])

        op = "%d" % FTRAC_CTRL_POLL_STAT
        count = 0
        duration = 0.0
        start = timer()
        
        try:
            while (count < self.pollcount and duration < self.pollduration):
                sock.send(op)
                res = eval(sock.recv(FTRAC_SOCK_BUFSIZE))
                count += 1
                now = timer()
                duration = now - start
                #self.dataprocess((op[0], res, count, duration, now))
                self.dataready.acquire()
                self.data.append((op[0], res, count, duration, now))
                self.dataready.notify()
                self.dataready.release()
                time.sleep(self.pollrate)
        except KeyboardInterrupt:
            pass
        
        sock.send("%d" % FTRAC_CTRL_FINISH)
        sock.close()
        return
    
    # control routines
    def ftrac_flush(self):
        sock = self.servsock
        sock.send(FTRAC_CTRL_FLUSH)
        res = sock.recv(FTRAC_SOCK_BUFSIZE)
        assert(res[0] == FTRAC_CTRL_OK)

    # data processing routines
    def dataprocess(self, data):
        if self.winrefresh:
            try:
                self.winoutput(data)
            except:
                pass
        
    class DataProcessor(threading.Thread):
        def __init__(self, ftrac):
            threading.Thread.__init__(self)
            self.ftrac = ftrac
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
    
    parser.add_option("-m", "--mode", action="store", type="choice",
                      dest="mode", metavar="track/poll", default="poll",
                      choices=["poll", "track"],
                      help="running mode (default: poll)\n"
                           "   poll: start polling a tracked fuse tracker")
    
    parser.add_option("-p", "--poll", action="store", type="string",
                      dest="pollpath", metavar="PATH", default="",
                      help="polling path (default: statust \"\")")
    
    parser.add_option("--poll-rate", action="store", type="float",
                      dest="pollrate", metavar="NUM", default=1.0,
                      help="polling rate (default: 1)")
    
    parser.add_option("--poll-rate-multi", action="store", type="float",
                      dest="pollratemulti", metavar="NUM", default=2.0,
                      help="polling rate mutiplier (default: 2)")
    
    parser.add_option("--poll-count", action="store", type="int",
                      dest="pollcount", metavar="NUM", default=-1,
                      help="polling count (default: Infinity)")
    
    parser.add_option("--poll-duration", action="store", type="int",
                      dest="pollduration", metavar="SECONDS", default=-1,
                      help="polling duration (default: Infinity)")
    
    parser.add_option("--poll-file-only", action="store_true",
                      dest="pollfileonly", default=False,
                      help="polling directory itself as file (default: off)")
    
    parser.add_option("--mount", action="store", type="string",
                      dest="mount", metavar="PATH", default=None,
                      help="use ftrac to mount")
    
    parser.add_option("--umount", action="store", type="string",
                      dest="umount", metavar="PATH", default=None,
                      help="umount ftrac")
    
    parser.add_option("--auto-umount", action="store_true",
                      dest="autoumount", default=False,
                      help="auto umount when exit")

    parser.add_option("-v", "--verbosity", action="store", type="int",
                dest="verbosity", metavar="NUM", default=0,
                help="verbosity level: 0/1/2/3 (default: 0)")

    opts, args = parser.parse_args(argv)
    
    opts.mountpoint = None
    if opts.mount:
        opts.mountpoint = opts.mount
    if opts.umount:
        opts.mountpoint = opts.umount

    if opts.mode == "poll" and opts.mountpoint is None:
        if len(args) == 0:
            es("error: missing mount point\n")
            sys.exit(1)
        opts.mountpoint = args[0]
    
    opts.pollrate = 1.0 / opts.pollrate
    if opts.pollcount == -1:
        opts.pollcount = "Inf"
    if opts.pollduration == -1:
        opts.pollduration = "Inf"

    opts.cmd = " ".join(sys.argv)
    
    return opts, args

def main():
    opts, args = parse_argv(sys.argv[1:])

    fusetrac = FUSETrac(opts)
    if opts.mount:
        return fusetrac.mount(opts.mount)
    if opts.umount:
        return fusetrac.umount(opts.umount)

    try:
        fusetrac.sessioninit()
        fusetrac.poll()
        fusetrac.sessionfinal()
    except SystemExit:
        if GV_IF_CURSES_INIT:
            curses.nocbreak()
            curses.echo()
            curses.endwin()
        return 1
    except: # do not use "else" here, will produce dummy output
        if GV_IF_CURSES_INIT:
            curses.nocbreak()
            curses.echo()
            curses.endwin()
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
