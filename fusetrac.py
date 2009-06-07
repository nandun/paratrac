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

#FTRAC_STAT = ["count", "elapsed", "length", "offset", "bytes"]
FTRAC_STAT_SC = ["sc_count", "sc_elapsed", "sc_esum"]
FTRAC_STAT_IO = ["io_summary"]
CHART_TITLES = {}
CHART_TITLES["sc_count"] = "Total System Call Count"
CHART_TITLES["sc_elapsed"] = "Elapsed Time per System Call"
CHART_TITLES["sc_esum"] = "Total System Call Elapsed Time"
CHART_TITLES["io_summary"] = "I/O Traffic Summary"

FTRAC_PATH_PREFIX = "/tmp"
FTRAC_SIGNAL_INIT = signal.SIGUSR1
FTRAC_POLL_STAT = '1'
FTRAC_POLL_FILESYSTEM = '2'
FTRAC_POLL_FILE = '3'
FTRAC_POLL_DIRECTORY = '4'
FTRAC_POLL_FINISH = '5'
FTRAC_POLL_UNKNOWN = 'x'

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
        self.report = None
        self.reportsclog = False
        self.reportappend = False
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
        self.winmenu_y = None

        # report variables
        self.reporton = False
        self.reportflush = False
        self.reportscfd = {}    # file descriptors for each system call
        self.reportstatfd = {}  # file descriptors for each statistics
    
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
        self.reportinit()
    
    def sessionfinal(self):
        self.end = (time.localtime(), timer())
        self.servsock.close()
        self.reportfinal() 
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
                elif c in "pP":
                    self.ftrac.reporton = False
                elif c in "rR":
                    self.ftrac.reporton = True
                elif c in "fF":
                    curses.flash()
                    self.ftrac.reportflush = True
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
        self.winmenu_y, _ = self.win.getmaxyx()
        self.winmenu_y -= 4
        self.winprompt_y = self.winmenu_y - 4
        GV_IF_CURSES_INIT = True

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

        formatstr = "%9s%17s%7s%9s%12s%12s%8s%8s%8s\n"
        titles = formatstr % ("OPERATION", "ACCESSED", "PID", "COUNT", 
            "ELAPSED", "ESUM", "LENGTH", "OFFSET", "BYTES")
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

        formatstr = "%9s%17s%7d%9d%12f%12f\n"
        str = ""
        for c in FTRAC_SYSCALL:
            atime, pid, cnt, elapsed, esum = res[c]
            str += formatstr % (c, 
                time.strftime("%m/%d %H:%M:%S", time.localtime(atime)),
                pid, cnt, elapsed, esum)
        formatstr = "%9s%17s%7d%9d%12f%12f%8s%8s%8s\n"
        for c in FTRAC_IOCALL:
            atime, pid, cnt, elapsed, esum, size, offset, bytes = res[c]
            sizestr = "%d%s" % smart_datasize(size)
            offsetstr = "%d%s" % smart_datasize(offset)
            bytestr = "%d%s" % smart_datasize(bytes)
            str += formatstr % \
                (c, time.strftime("%m/%d %H:%M:%S",
                time.localtime(atime)), pid, cnt, elapsed, esum, sizestr,
                offsetstr, bytestr)
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
        
        if self.reporton:
            self.win.addstr(self.winprompt_y, 0, 
                "writing record %s to %s ...\n" % (count, self.report))
        else:
            self.win.addstr(self.winprompt_y, 0, 
                "writing to %s paused.\n" % self.report, attr)
        
        self.win.addstr(self.winmenu_y, 0, 
            "p: pause reporting, r: resume reporting, f: flush report data\n"
            "s: snapshot, c: continue, <: rate/%.3f, >: rate*%.3f, Ctrl+c: quit.\n"
            % (self.pollratemulti, self.pollratemulti),
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
        res = eval(sock.recv(FTRAC_SOCK_BUFSIZE))
        self.tracstart = float(res["start"])

        op = "%s" % FTRAC_POLL_FILESYSTEM
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
                f.write("Index,Duration,Pid,Count,Elapsed,Esum\n")
            for c in FTRAC_IOCALL:
                f = open("%s/%s.csv" % (self.report, c), flag)
                self.reportscfd[c] = f
                f.write("Index,Duration,Pid,Count,Elapsed,Esum,Length,"
                    "Offset,Bytes\n")

    def reportfinal(self): 
        # generate html report
        self.reportoutputhtml()
        if self.reportsclog:
            for f in self.reportscfd.values(): 
                f.close()
        for f in self.reportstatfd.values():
            f.close

        # packing data
        basename = os.path.basename(self.report)
        if os.path.exists("%s/%s.tgz" % (self.report, basename)):
            os.remove("%s/%s.tgz" % (self.report, basename))
        cwd = os.getcwd()
        os.chdir(os.path.dirname(self.report))
        res, out = commands.getstatusoutput("tar -czf %s.tgz %s" % 
            (basename, basename))
        if res:
            es("warning: %s\n" % out)
        os.chdir(cwd)
        res, out = commands.getstatusoutput("mv %s.tgz %s" % 
            (self.report, self.report))
        if res:
            es("warning: %s\n" % out)


    def reportoutputcsv(self, data):
        op, res, count, duration, now = data
        
        if res is None or op == FTRAC_POLL_STAT:
            return
        
        stat_sc_count = [str(duration)]
        stat_sc_elapsed = [str(duration)]
        stat_sc_esum = [str(duration)]
        stat_io_summary = [str(duration)]
        for c in FTRAC_SYSCALL:
            atime, pid, cnt, elapsed, esum = res[c]
            if self.reportsclog:
                self.reportscfd[c].write("%d,%f,%d,%d,%f,%f\n" %
                    (count, duration, pid, cnt, elapsed, esum))
            stat_sc_count.append(str(cnt))
            stat_sc_elapsed.append("%f" % elapsed)
            stat_sc_esum.append("%f" % esum)
        for c in FTRAC_IOCALL:
            atime, pid, cnt, elapsed, esum, size, offset, bytes = res[c]
            if self.reportsclog:
                self.reportscfd[c].write("%d,%f,%d,%d,%f,%f,%d,%d,%d\n" %
                    (count, duration, pid, cnt, elapsed, esum, size, offset, bytes))
            stat_sc_count.append(str(cnt))
            stat_sc_elapsed.append("%f" % elapsed)
            stat_sc_esum.append("%f" % esum)
            stat_io_summary.append("%f,%f,%f" % 
                (float(bytes)/MB, float(size)/KB, float(offset)/KB))
        self.reportstatfd["sc_count"].write(",".join(stat_sc_count)+"\n")
        self.reportstatfd["sc_elapsed"].write(",".join(stat_sc_elapsed)+"\n")
        self.reportstatfd["sc_esum"].write(",".join(stat_sc_esum)+"\n")
        self.reportstatfd["io_summary"].write(",".join(stat_io_summary)+"\n")

        if self.reportflush:
            for f in self.reportstatfd.values() + self.reportscfd.values():
                f.flush()
                #os.fsync(f.fileno())
            self.reportflush = False

    def reportoutputhtml(self):
        modulesdir = "../modules"
        settingsdir = "../settings"
        datadir = "."
        basename=os.path.basename(self.report)

        start = timer()
        f = open("%s/report.html" % self.report, "wb")
        f.write(
"""\
<!-- 
  ParaTrac Report
  by %s at %s

  Prerequisite:
    * Adobe Flash Player: http://www.adobe.com/go/getflashplayer
    * SWFObject 2.0: http://code.google.com/p/swfobject/
    * amCharts: http://www.amcharts.com
-->
""" % \
        (os.path.abspath(sys.argv[0]),
         time.strftime("%a %b %d %Y %H:%M:%S %Z", self.start[0])))

        f.write(
"""\
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
<title>ParaTrac Report: %s</title>

<script type="text/javascript" src="%s/swfobject.js"></script>
<script type="text/javascript">
// Globale values
var chartWidth="980"
var chartHeight="600"
var swfVersion="8.0.0"
var swfInstall="%s/expressInstall.swf"
""" % \
        (time.strftime("%a %b %d %Y %H:%M:%S %Z", self.start[0]),\
        modulesdir, modulesdir))

        f.write(
"""\
// Chart control functions
function showAllSeries (chart_id, num){
  chart = document.getElementById(chart_id);
  var i=0;
  for (i=0;i<num;i++)
  {
    chart.showGraph(i)
  }
} 
function hideAllSeries (chart_id, num){
  chart = document.getElementById(chart_id);
  var i=0;
  for (i=0;i<num;i++)
  {
    chart.hideGraph(i)
  }
} 
function showAllValues (chart_id, num){
  chart = document.getElementById(chart_id);
  var i=0;
  for (i=0;i<num;i++)
  {
    chart.selectGraph(i)
  }
} 
function hideAllValues (chart_id, num){
  chart = document.getElementById(chart_id);
  var i=0;
  for (i=0;i<num;i++)
  {
    chart.deselectGraph(i)
  }
} 
""")
        
        # generate swfobjects here
        for c in FTRAC_STAT_SC + FTRAC_STAT_IO:
            f.write(
"""\
// Chart: %s
var vars={path:"%s/", 
settings_file:"%s/stat_%s.xml",data_file:"%s/stat_%s.csv"}
var params={}
var attrs={}
swfobject.embedSWF("%s/amline.swf", "stat_%s", chartWidth, chartHeight, swfVersion, swfInstall, vars, params, attrs);
""" % (CHART_TITLES[c], modulesdir, settingsdir, c, datadir, c, 
        modulesdir, c))
        
        f.write("</script>\n</head>\n\n<body>\n") 

        f.write(
"""\
<!-- Log: Paratrac runtime summary -->
<p align="left"><font size=5"><b>ParaTrac Tracking Report: %s</b></font><br>
<i>by ParaTrac Tools (version %s, %s)</i></p>
<table border="0">
<tr><td><b>platformn</b></td><td>%s</td></tr>
<tr><td><b>ftrac started</b></td><td>%s (%s seconds ago)</td></tr>
<tr><td><b>session began</b></td><td>%s</tr>
<tr><td><b>session end</b></td><td>%s</tr>
<tr><td><b>duration</b></td><td>%s</tr>
<tr><td><b>user</b></td><td>%s (%s)</tr>
<tr><td><b>command</b></td><td>%s</tr>
<tr><td><b>mode</b></td><td>%s (target: %s, rate: %.3f)</td></tr>
</table>
""" % \
        (basename,
        PARATRAC_VERSION, PARATRAC_DATE,
        self.platform, 
        time.strftime("%a %b %d %Y %H:%M:%S %Z",\
        time.localtime(self.tracstart)),
        start - self.tracstart,
        time.strftime("%a %b %d %Y %H:%M:%S %Z", self.start[0]),
        time.strftime("%a %b %d %Y %H:%M:%S %Z", self.end[0]),
        self.end[1] - self.start[1], self.user, self.uid,
        self.cmd, self.mode, self.pollpath, 1.0/self.pollrate))
        
        # generate chart here
        for c in FTRAC_STAT_SC + FTRAC_STAT_IO:
            f.write(
"""\
<!-- Chart: %s -->
<p align="left"><font size="3" face="Arial"><b><i>%s</i></b></font></p>
<div id="stat_%s"></div>
""" % \
        (CHART_TITLES[c], CHART_TITLES[c], c))
        
        f.flush()
        os.fsync(f.fileno())
        f.write(
"""\
<p><i>took %s seconds to generate this report. (<a href=\"%s.tgz\">%s.tgz</a>)</i></p>
<p align="right"><font size="1" face="Arial">
<a href="http://www.adobe.com/go/getflashplayer">
Get Adobe Flash Player</a></font></p>

</body>
</html>
""" % (timer()-start, basename, basename))
        
        f.close()
    
    # data processing routines
    def dataprocess(self, data):
        if self.winrefresh:
            try:
                self.winoutput(data)
            except:
                pass
        if self.reporton:
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
    
    parser.add_option("--report", action="store", type="string",
                      dest="report", default=None,
                      help="report name")
    
    parser.add_option("--report-syscall-log", action="store_true",
                      dest="reportsclog", default=False,
                      help="report logs of system calls (default: off)")
    
    parser.add_option("--mount", action="store", type="string",
                      dest="mount", metavar="PATH", default=None,
                      help="use ftrac to mount")
    
    parser.add_option("--umount", action="store", type="string",
                      dest="umount", metavar="PATH", default=None,
                      help="umount ftrac")
    
    parser.add_option("--auto-umount", action="store_true",
                      dest="autoumount", default=False,
                      help="auto umount when exit")

    #parser.add_option("--report-append", action="store_true",
    #                  dest="reportappend", default=False,
    #                  help="append data to previous logs instead of overwrite"
    #                  " (default: off)")
    
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
