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
import socket
import sys
import tempfile
import textwrap
import threading
import traceback

from common import *

# ftrac constance, keep consistent with fuse/ftrac.c
FTRAC_SYSCALL = ["stat", "access", "readlink", "readdir", "mknod", "mkdir",
    "symlink", "unlink", "rmdir", "rename", "link", "chmod", "chown",
    "truncate", "utime", "open", "statfs", "release", "fsync"]
FTRAC_IOCALL = ["read", "write"]

#FTRAC_STAT = ["count", "elapsed", "length", "offset", "bytes"]
FTRAC_STAT_SC = ["sc_count", "sc_elapsed"]
FTRAC_STAT_IO = ["io_summary"]

FTRAC_PATH_PREFIX = "/tmp"
FTRAC_POLL_STAT = '1'
FTRAC_POLL_FILESYSTEM = '2'
FTRAC_POLL_FILE = '3'
FTRAC_POLL_DIRECTORY = '4'
FTRAC_POLL_FINISH = '5'
FTRAC_POLL_UNKNOWN = 'x'

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
        self.pollrate = None
        self.pollcount = None
        self.pollduration = None
        self.pollfileonly = False
        self.report = None
        self.reportsclog = False
        self.reportappend = False
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

        # data processing variables
        self.data = []
        self.dataready = threading.Condition()   # result is ready
        self.dataproc = None

        # curses variables
        self.win = None
        self.wincontrol = None
        self.winrefresh = True
        self.winmenu_y = None

        # report variables
        self.reportscfd = {}    # file descriptors for each system call
        self.reportstatfd = {}  # file descriptors for each statistics
    
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
        self.start = (time.localtime(), timer())

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

        # initial curses screen
        self.wininit()

        # initial report handling
        self.reportinit()
    
    def sessionfinal(self):
        self.servsock.close()
        self.reportfinal() 
        self.winfinal()
    
    # curses routines
    class WinController(threading.Thread):
        def __init__(self, ftrac):
            threading.Thread.__init__(self)
            self.ftrac = ftrac 
        
        def run(self):
            while True:
                c = chr(self.ftrac.win.getch())
                if c in 'qQ':
                    self.ftrac.pollcount = 1
                    self.ftrac.pollrate = 0.0001
                    break
                elif c in "sS":
                    curses.flash()
                    self.ftrac.winrefresh = False
                elif c in "cC":
                    self.ftrac.winrefresh = True
                elif c == "<":
                    self.ftrac.pollrate *= 10
                elif c == ">":
                    self.ftrac.pollrate /= 10

    def wininit(self):
        self.win = curses.initscr()
        curses.noecho()
        curses.cbreak()
        curses.curs_set(0)
        self.wincontrol = self.WinController(self)
        self.wincontrol.start()
        self.winmenu_y, _ = self.win.getmaxyx()
        self.winmenu_y -= 3

    def winfinal(self):
        curses.nocbreak()
        curses.echo()
        curses.endwin()
            
    def winoutput(self, data):
        op, res, count, duration, _ = data
        
        self.win.erase()
        summary = "ftrac started at %s, elapsed %f seconds\n" % \
            (time.strftime("%a, %d %b %Y %H:%M:%M %Z", 
             time.localtime(self.tracstart)), timer()-self.tracstart)
        summary += "poll: %s\n" % self.pollpath
        summary += "rate: %f, count: %d, duration: %f seconds\n" % \
            (1/self.pollrate, count, duration)
        self.win.addstr(summary)

        formatstr = "%9s%17s%9s%12s%7s%8s%8s%7s\n"
        titles = formatstr % ("OPERATION", "ACCESSED", "COUNT", 
            "ELAPSED", "PID", "LENGTH", "OFFSET", "BYTES")
        attr = curses.A_REVERSE | curses.A_BOLD
        self.win.addstr(titles, attr)
        
        if res is None:
            self.win.addstr("Statistics of %s not found, "
                "there is no such file or it has not been accessed." \
                % self.pollpath)
            self.win.refresh()
            return
        
        if op == FTRAC_POLL_STAT:
            self.win.refresh()
            return

        formatstr = "%9s%17s%9d%12f%7d\n"
        str = ""
        for c in FTRAC_SYSCALL:
            atime, cnt, elapsed, pid = res[c]
            str += formatstr % (c, 
                time.strftime("%m/%d %H:%M:%S", time.localtime(atime)),
                cnt, elapsed, pid)
        formatstr = "%9s%17s%9d%12f%7d%8d%8d%7s\n"
        for c in FTRAC_IOCALL:
            atime, cnt, elapsed, pid, size, offset, bytes = res[c]
            bytesize = "%d%s" % smart_datasize(bytes)
            str += formatstr % \
                (c, time.strftime("%m/%d %H:%M:%S",
                time.localtime(atime)), cnt, elapsed, pid, size,
                offset, bytesize)
        if op == FTRAC_POLL_FILE:
            born = res["born"]
            dead = res["dead"]
            if born == 0:
                str += "File already exists, "
            else:
                str += "File created at %s, " % \
                    time.strftime("%Y/%m/%d %H:%M:%S", time.localtime(born))
            if dead == 0:
                str += "still alive."
            else:
                str += "deleted at %s." % \
                    time.strftime("%Y/%m/%d %H:%M:%S", time.localtime(dead))

        self.win.addstr(str + "\n")

        self.win.addstr(self.winmenu_y, 0, 
            "s: snapshot, c: continue, <: rate/10, >: rate*10, q: quit.",
            curses.A_BOLD)

        self.win.refresh()

    # polling routines
    def poll(self, sock=None, path=None):
        if sock is None:
            sock = self.servsock
        if path is None:
            path = self.pollpath

        # poll stat once for info
        sock.send(FTRAC_POLL_STAT)
        res = eval(sock.recv(1024))
        self.tracstart = float(res["start"])

        if path == "":
            op = "%s" % FTRAC_POLL_STAT
        elif path == "/" and not self.pollfileonly:
            op = "%s" % FTRAC_POLL_FILESYSTEM
        elif os.path.isdir(path) and not self.pollfileonly:
            op = "%s:%s" % (FTRAC_POLL_DIRECTORY, path)
        else:
            op = "%s:%s" % (FTRAC_POLL_FILE, path)
    
        count = 0
        duration = 0.0
        start = timer()
        
        try:
            while (count < self.pollcount and duration < self.pollduration):
                sock.send(op)
                res = eval(sock.recv(1024))
                count += 1
                now = timer()
                duration = now - start
                self.dataprocess((op[0], res, count, duration, now))
                #self.dataready.acquire()
                #self.data.append((op[0], res, count, duration, now))
                #self.dataready.notify()
                #self.dataready.release()
                time.sleep(self.pollrate)
        except KeyboardInterrupt:
            es("polling terminated by keyboard\n")
        sock.send("%s" % FTRAC_POLL_FINISH)
        sock.close()
        return
    
    # report routines
    def reportinit(self):
        if self.report is None:
            self.report = os.getcwd() + \
                time.strftime("/report-%H%M%S", self.start[0])
        else:
            self.report = os.path.abspath(self.report)

        try:
            os.makedirs(self.report)
        except OSError:
            pass
        
        flag = "wb"
        #if self.reportappend:
        #    flag = "ab"
        
        # crete file for each kind of statistics
        for s in FTRAC_STAT_SC:
            f = open("%s/stat_%s.csv" % (self.report, s), flag)
            self.reportstatfd[s] = f
            f.write("Duration,"+",".join(FTRAC_SYSCALL+FTRAC_IOCALL)+"\n")
        for s in FTRAC_STAT_IO:
            f = open("%s/stat_%s.csv" % (self.report, s), flag)
            self.reportstatfd[s] = f
            f.write("Duration,RBytesinMB,RLeninKB,ROffsetinKB,"
                "WBytesinMB,WLeninKB,WOffsetinKB\n")
        
        if self.reportsclog:
            # create file for each system call being traced
            for c in FTRAC_SYSCALL:
                f = open("%s/%s.csv" % (self.report, c), flag)
                self.reportscfd[c] = f
                f.write("Index,Duration,Count,Elapsed,Pid\n")
            for c in FTRAC_IOCALL:
                f = open("%s/%s.csv" % (self.report, c), flag)
                self.reportscfd[c] = f
                f.write("Index,Duration,Count,Elapsed,Pid,Length,"
                    "Offset,Bytes\n")

    def reportfinal(self): 
        if self.reportsclog:
            for f in self.reportscfd.values(): 
                f.close()
        for f in self.reportstatfd.values():
            f.close

    def reportoutputcsv(self, data):
        op, res, count, duration, now = data
        
        if res is None or op == FTRAC_POLL_STAT:
            return
        
        stat_sc_count = [str(duration)]
        stat_sc_elapsed = [str(duration)]
        stat_io_summary = [str(duration)]
        for c in FTRAC_SYSCALL:
            atime, cnt, elapsed, pid = res[c]
            if self.reportsclog:
                self.reportscfd[c].write("%d,%f,%d,%f,%d\n" %
                    (count, duration, cnt, elapsed, pid))
            stat_sc_count.append(str(cnt))
            stat_sc_elapsed.append("%f" % elapsed)
        for c in FTRAC_IOCALL:
            atime, cnt, elapsed, pid, size, offset, bytes = res[c]
            if self.reportsclog:
                self.reportscfd[c].write("%d,%f,%d,%f,%d,%d,%d,%d\n" %
                    (count, duration, cnt, elapsed, pid, size, offset, bytes))
            stat_sc_count.append(str(cnt))
            stat_sc_elapsed.append("%f" % elapsed)
            stat_io_summary.append("%f,%f,%f" % 
                (float(bytes)/MB, float(size)/KB, float(offset)/KB))
        self.reportstatfd["sc_count"].write(",".join(stat_sc_count)+"\n")
        self.reportstatfd["sc_elapsed"].write(",".join(stat_sc_elapsed)+"\n")
        self.reportstatfd["io_summary"].write(",".join(stat_io_summary)+"\n")
    
    # data processing routines
    def dataprocess(self, data):
        if self.winrefresh:
            self.winoutput(data)
        self.reportoutputcsv(data)
        
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
    
    parser.add_option("--poll-count", action="store", type="int",
                      dest="pollcount", metavar="NUM", default=-1,
                      help="polling count (default: Infinity)")
    
    parser.add_option("--poll-duration", action="store", type="int",
                      dest="pollduration", metavar="SECONDS", default=-1,
                      help="polling duration (default: Infinity)")
    
    parser.add_option("--poll-file-only", action="store_true",
                      dest="pollfileonly", default=False,
                      help="polling directory itself as file (default: off)")
    
    parser.add_option("--report", action="store", type="string",
                      dest="report", default=None,
                      help="report name")
    
    parser.add_option("--report-syscall-log", action="store_true",
                      dest="reportsclog", default=False,
                      help="report logs of system calls (default: off)")
    
    #parser.add_option("--report-append", action="store_true",
    #                  dest="reportappend", default=False,
    #                  help="append data to previous logs instead of overwrite"
    #                  " (default: off)")
    
    parser.add_option("-v", "--verbosity", action="store", type="int",
                dest="verbosity", metavar="NUM", default=0,
                help="verbosity level: 0/1/2/3 (default: 0)")

    opts, args = parser.parse_args(argv)
    
    if opts.mode == "poll":
        if len(args) == 0:
            es("error: missing mount point\n")
            sys.exit(1)
        opts.mountpoint = args[0]
    
    opts.pollrate = 1.0 / opts.pollrate
    if opts.pollcount == -1:
        opts.pollcount = "Inf"
    if opts.pollduration == -1:
        opts.pollduration = "Inf"

    return opts, args

def main():
    opts, args = parse_argv(sys.argv[1:])

    fusetrac = FUSETrac(opts)
    try:
        fusetrac.sessioninit()
        fusetrac.poll()
        fusetrac.sessionfinal()
    except:
        curses.nocbreak()
        curses.echo()
        curses.endwin()
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
