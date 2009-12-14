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
# Tracker classes
#

__all__ = ["Tracker", "FUSETracker"]

import commands
import curses
import curses.textpad
import os
import pwd
import signal
import socket
import sys
import threading
import traceback

from common import *

class Tracker:
    """
    Tracker base class
    """
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

FUSETRAC_SYSCALL = ["lstat", "fstat", "access", "readlink", "opendir", 
    "readdir", "closedir", "mknod", "mkdir", "symlink", "unlink", "rmdir", 
    "rename", "link", "chmod", "chown", "truncate", "utime", "creat", 
    "open", "statfs", "flush", "close", "fsync", "read", "write"]

FTRAC_PATH_PREFIX = "/tmp"
FTRAC_SIGNAL_INIT = signal.SIGUSR1
FTRAC_CTRL_OK = 0
FTRAC_CTRL_FINISH = 1
FTRAC_CTRL_POLL_STAT = 2
FTRAC_CTRL_FLUSH = 3

class FUSETracker(Tracker):
    def __init__(self, opts=None, **kw):
        Tracker.__init__(self)
        
        self.mountpoint = None
        self.pollpath = None
        self.pollrate = None
        self.pollratemulti = None
        self.autoumount = False

        if opts is not None:
            for k, v in opts.__dict__.items():
                if self.__dict__.has_key(k):
                    self.__dict__[k] = v
        for k, v in kw.items():
            if self.__dict__.has_key(k):
                self.__dict__[k] = v

        self.session = None
        self.sockpath = None
        self.servsock = None
        self.tracstart = None
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
        self.winstart = False

    # mount/umount primitivies
    def check_ftracexe(self):
        # check ftrac executable
        res, path = commands.getstatusoutput("which ftrac")
        if res == 0:
            return path
        elif os.path.exists("./fuse/ftrac"):
            return os.path.abspath("./fuse/ftrac")
        else:
            self.es("error: ftrac executable not found.\n")
            sys.exit(1)
    
    def check_dirusage(self, dir):
        res, output = commands.getstatusoutput("fuser -m %s 2>&1" % dir)
        if res == 0:
            procs = output.split()
            procs.pop(0)   # remove directory
            for p in procs:
                # fuser indicator:
                # c: current directory
                # e: running executable
                # f: open file
                # r: the root directory
                # m: shared library
                pid = p.strip('cefrm')
                try:
                    fp = open("/proc/%s/cmdline" % pid, "r")
                except IOError:
                    continue;
                cmdline = fp.readline()
                fp.close()
                cmdline = cmdline.replace('\0', ' ')
                self.es("%s being used by process %s: %s\n" % (dir, pid, cmdline))
            self.es("please make sure umount is safe, or use "
               "\"--force\" to umount\n")
        
    def mount(self, mountpoint=None):
        ftracexe = self.check_ftracexe()

        if mountpoint is None:
            mountpoint = self.mountpoint
        if not os.path.exists(mountpoint):
            os.makedirs(mountpoint)
        elif not os.path.isdir(mountpoint):
            self.es("error: %s exists and is not a directory\n" % mountpoint)
            sys.exit(1)
            
        if self.dryrun:
            return
        
        stauts, output = commands.getstatusoutput("%s %s -o notify_pid=%d" 
            % (ftracexe, mountpoint, self.pid))
        if len(output):
            self.es("%s\n" % output)
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

        res, output = commands.getstatusoutput("fusermount -u %s" % mountpoint)
        if  res != 0:
            self.es("%s\n" % output)
            if output.find("busy") != -1:
                self.check_dirusage(mountpoint)
            sys.exit(1)
    
    # tracking routines
    def track(self):
        try:
            self.sessioninit()
            self.poll()
            self.sessionfinal()
        except SystemExit:
            if self.winstart:
                self.winfinal()
            return 0
        except: # using "else:" will produce dummy output
            if self.winstart:
                self.winfinal()
            traceback.print_exc()
            return 1
    
    def sessioninit(self):
        self.start = (time.localtime(), timer())

        self.session = "/tmp/ftrac-%s-%u" % (self.user, 
            string_hash(self.mountpoint))
        if not os.path.isdir(self.session):
            # mount ftrac
            if self.mount() != 0:
                self.es("error: failed to init ftrac\n")
                sys.exit(1)

        # connect to fusetrac server
        try:
            self.sockpath = "%s/ftrac.sock" % self.session
            self.servsock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            if self.verbosity >= 1:
                self.verbose("sessioninit: sock.connect(%s)" % self.sockpath)
            self.servsock.connect(self.sockpath)
        except:
            self.es("error: failed to connect to %s\n" % self.sockpath)
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
        self.winstart = True

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
        formatstr_io = "%9s%17s%12d%18f%12s\n"
        str = ""
        for c in FUSETRAC_SYSCALL:
            if c in ["read", "write"]:
                stamp, cnt, esum, bytes = res[c]
                bytestr = "%d%s" % smart_datasize(bytes)
                str += formatstr_io % \
                    (c, time.strftime("%m/%d %H:%M:%S", 
                    time.localtime(stamp)), cnt, esum, bytestr)
            else:
                stamp, cnt, esum = res[c]
                str += formatstr % (c, 
                    time.strftime("%m/%d %H:%M:%S", time.localtime(stamp)),
                    cnt, esum)
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
            while True:
                sock.send(op)
                res = eval(sock.recv(SOCKET_BUFSIZE))
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
        res = sock.recv(SOCKET_BUFSIZE)
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
