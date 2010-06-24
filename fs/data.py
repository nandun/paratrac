#############################################################################
# ParaTrac: Scalable Tracking Tools for Parallel Applications
# Copyright (C) 2009,2010  Nan Dun <dunnan@yl.is.s.u-tokyo.ac.jp>

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
# fs/data.py
# Filesystem Trace Database
#

import os
import sys

from modules.utils import SYSCALL
from modules import utils
from modules import num 
from modules.data import Database as CommonDatabase

class Database(CommonDatabase):
    def __init__(self, path):
        CommonDatabase.__init__(self, path)

        # Only attributes can be accurately queried
        self.SYSC_ATTR = ["iid", "stamp", "pid", "sysc", "fid", "res",
            "elapsed", "aux1", "aux2"]
        self.FILE_ATTR = ["iid", "fid", "path"]
        self.PROC_ATTR = ["iid", "pid", "ppid", "live", 
            "res", "cmdline", "environ"]

    def _set_tabs(self):
        self.tab["runtime"] = "item TEXT, value TEXT"
        
        self.tab["file"] = "iid INTEGER, fid INTEGER, path TEXT"
        
        self.tab["sysc"] = "iid INTEGER, stamp DOUBLE, pid INTEGER, " \
            "sysc INTEGER, fid INTEGER, res INTEGER, elapsed DOUBLE, " \
            "aux1 INTEGER, aux2 INTEGER"
        
        self.tab["proc"] = "iid INTGER, pid INTEGER, ppid INTEGER, " \
            "live INTEGER, res INTEGER, btime FLOAT, elapsed FLOAT, " \
            "utime FLOAT, stime FLOAT, cmdline TEXT, environ TEXT"
        
    def import_logs(self, logdir=None):
        if logdir is None:
            logdir = os.path.dirname(self.db)

        self._create_tabs(True)

        iid = 0
        
        runtime = {}
        f = open("%s/runtime.log" % logdir)
        for l in f.readlines():
            item, val = l.strip().split(":", 1)
            self.cur.execute("INSERT INTO runtime VALUES (?,?)", (item, val))
            if val.isdigit():
                runtime[item] = eval(val)
            else:
                runtime[item] = "%s" % val
        f.close()
        
        f = open("%s/file.log" % logdir)
        for l in f.readlines():
            fid, path = l.strip().split(":", 1)
            self.cur.execute("INSERT INTO file VALUES (?,?,?)", 
                (iid, fid, path))
        f.close()
        
        f = open("%s/sysc.log" % logdir)
        btime = None
        for l in f.readlines():
            stamp,pid,sysc,fid,res,elapsed,aux1,aux2 = l.strip().split(",")
            if not btime: btime = float(stamp)
            stamp = "%f" % (float(stamp) - btime)
            self.cur.execute("INSERT INTO sysc VALUES (?,?,?,?,?,?,?,?,?)",
                (iid,stamp,pid,sysc,fid,res,elapsed,aux1,aux2))
        f.close()
        
        # import process logs according to the accuracy of information
        procs = set()
        CLK_TCK = runtime['clktck']
        SYS_BTIME = runtime['sysbtime']
        have_taskstat_log = False
        if os.path.exists("%s/taskstat.log" % logdir):
            f = open("%s/taskstat.log" % logdir)
            for l in f.readlines():
                pid,ppid,live,res,btime,elapsed,utime,stime,cmd \
                    = l.strip().split(",")
                # btime (sec), elapsed (usec), utime (usec), stime (usec)
                elapsed = float(elapsed) / 1000000.0
                utime = float(utime) / 1000000.0
                stime = float(stime) / 1000000.0
                self.cur.execute("INSERT INTO proc (iid,pid,ppid,live,res," 
                    "btime,elapsed,utime,stime) VALUES (?,?,?,?,?,?,?,?,?)",
                    (iid,pid,ppid,live,res,btime,elapsed,utime,stime))
            f.close()
            have_taskstat_log = True
        
        have_ptrace_log = False
        if os.path.exists("%s/ptrace.log" % logdir):
            f = open("%s/ptrace.log" % logdir)
            for l in f.readlines():
                pid,ppid,start,stamp,utime,stime,cmd,env \
                    = l.strip().split(",")
                if not have_taskstat_log:
                    # calculate real btime and elapsed
                    btime = SYS_BTIME + float(start) / CLK_TCK
                    elapsed = float(stamp) - btime
                    utime = float(utime) / CLK_TCK
                    stime = float(stime) / CLK_TCK
                    self.cur.execute("INSERT INTO proc (iid,pid,ppid,live,res," 
                        "btime,elapsed,utime,stime,cmdline,environ) "
                        "VALUES (?,?,?,?,?,?,?,?,?)",
                        (iid,pid,ppid,0,0,btime,elapsed,utime,stime,cmd,env))
                else:
                    self.cur.execute("UPDATE proc SET cmdline=?,environ=? WHERE "
                        "pid=%s and ppid=%s" % (pid, ppid), (cmd, env))
                procs.add(eval(pid))
            f.close()
            have_ptrace_log = True

        f = open("%s/proc.log" % logdir)
        for l in f.readlines():
            flag,pid,ppid,start,stamp,utime,stime,cmd,env \
                = l.strip().split("|#|")
            if not flag or eval(pid) in procs: # just ignore start status right now
                continue
            if not have_taskstat_log:
                btime = SYS_BTIME + float(start) / CLK_TCK
                elapsed = float(stamp) - btime
                utime = float(utime) / CLK_TCK
                stime = float(stime) / CLK_TCK
                self.cur.execute("INSERT INTO proc (iid,pid,ppid,live,res," 
                    "btime,elapsed,utime,stime,cmdline,environ) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (iid,pid,ppid,0,0,btime,elapsed,utime,stime,cmd,env))
            # TODO: integrating ptrace log
            else:
                self.cur.execute("UPDATE proc SET cmdline=?,environ=? WHERE "
                    "pid=%s and ppid=%s" % (pid, ppid), (cmd, env))
                 
        f.close()
        self.con.commit()
        
    # runtime table routines
    def runtime_sel(self, fields="*"):
        self.cur.execute("SELECT %s FROM runtime" % fields)
        return self.cur.fetchall()
    
    def runtime_get_value(self, item):
        self.cur.execute("SELECT value FROM runtime WHERE item=?", (item,))
        res = self.cur.fetchone()
        if res is None: return None
        else: return res[0]

    def runtime_values(self):
        self.cur.execute('SELECT item,value FROM runtime')
        return self.cur.fetchall()

    # syscall table routines
    def sysc_sel(self, sysc, fields="*"):
        self.cur.execute("SELECT %s FROM sysc WHERE sysc=?" 
            % fields, (sysc,))
        return self.cur.fetchall()
    
    def sysc_count(self, sysc):
        self.cur.execute("SELECT COUNT(*) FROM sysc WHERE sysc=?", (sysc,))
        return self.cur.fetchone()[0]
    
    def sysc_sum(self, sysc, field):
        cur = self.con.cursor()
        cur.execute("SELECT SUM(%s) FROM sysc WHERE sysc=?"
        "GROUP BY sysc" % field, (sysc,))
        res = cur.fetchone()
        if res is None: # No such system call
            return 0
        else:
            return res[0]
    
    def sysc_sum2(self, columns, **where):
        columns = columns.split(',')
        columns = ','.join(map(lambda s:"SUM(%s)"%s, columns))
        qstr = "SELECT %s FROM sysc" % columns
        wstr = " and ".join(map(lambda k:"%s=%s" % (k, where[k]),
            utils.list_intersect([self.SYSC_ATTR, where.keys()])))
        if wstr != "": qstr = "%s WHERE %s" % (qstr, wstr)
        self.cur.execute(qstr)
        return self.cur.fetchall()
    
    def sysc_avg(self, sysc, field):
        cur = self.con.cursor()
        cur.execute("SELECT AVG(%s) FROM sysc WHERE sysc=?"
        "GROUP BY sysc" % field, (sysc,))
        res = cur.fetchone()
        if res is None: # No such system call
            return 0
        else:
            return res[0]
    
    def sysc_std(self, sysc, field):
        cur = self.con.cursor()
        cur.execute("SELECT %s FROM sysc WHERE sysc=?" % field, (sysc,))
        vlist = map(lambda x:x[0], cur.fetchall())
        return num.num_std(vlist)
    
    def sysc_cdf(self, sysc, field, numbins=None):
        """if numbins is None, use all data"""
        self.cur.execute("SELECT %s FROM sysc WHERE sysc=?" 
            % field, (sysc,))
        vlist = map(lambda x:x[0], self.cur.fetchall())
        vlist.sort()
        total = sum(vlist)
        data = []
        curr_sum = 0.0
        for v in vlist:
            curr_sum += v
            if total == 0: ratio = 0
            else: ratio = curr_sum/total
            data.append((v, ratio))
        return data

    def sysc_sel_procs_by_file(self, iid, sysc, fid, fields="*"):
        self.cur.execute("SELECT %s FROM sysc WHERE "
            "iid=? AND sysc=? AND fid=? GROUP BY pid" % fields, 
            (iid, sysc, fid))
        return self.cur.fetchall()

    # file table routines
    def file_sel(self, columns, **where):
        qstr = "SELECT %s FROM file" % columns
        wstr = " and ".join(map(lambda k:"%s=%s" % (k, where[k]),
            utils.list_intersect([self.FILE_ATTR, where.keys()])))
        if wstr != "": qstr = "%s WHERE %s" % (qstr, wstr)
        self.cur.execute(qstr)
        return self.cur.fetchall()
    
    def files(self, **attr):
        """Return a list of files IDs that satisfy specified attributes"""
        
        qstr = "SELECT fid FROM file" # SQL query string
        if attr == {}:
            self.cur.execute(qstr)
            return map(lambda x:x[0], self.cur.fetchall())

        # Select from procs table
        qstr = "SELECT fid FROM file"
        wstr = " and ".join(map(lambda k:"%s=%s" % (k, attr[k]),
            list_intersect([self.FILE_ATTR, attr.keys()])))
        if wstr != "": qstr = "%s WHERE %s GROUP BY file" % (qstr, wstr)
        self.cur.execute(qstr)
        return map(lambda x:x[0], self.cur.fetchall())
        
        # TODO:ASAP
        # Select from sysc table

    def procs(self, **attr):
        """Return a list of processes IDs that satisfy specified attributes"""
        
        qstr = "SELECT pid FROM proc" # SQL query string
        
        if attr == {}:
            self.cur.execute(qstr)
            return map(lambda x:x[0], self.cur.fetchall())
        
        procs = []
        if "sysc" in attr.keys(): attr["sysc"] = SYSCALL[attr["sysc"]]
        # Select from syscall table
        qstr = "SELECT pid FROM syscall"
        wstr = " and ".join(map(lambda k:"%s=%s" % (k, attr[k]), 
                list_intersect([self.SYSC_ATTR, attr.keys()])))
        if wstr != "":
            qstr = "%s WHERE %s GROUP BY pid" % (qstr, wstr)
            self.cur.execute(qstr)
            procs_sc =  map(lambda x:x[0], self.cur.fetchall())
            procs.extend(procs_sc)
        
        # Select from procs table
        qstr = "SELECT pid FROM proc"
        wstr = " and ".join(map(lambda k:"%s=%s" % (k, attr[k]),
            list_intersect([self.PROC_ATTR, attr.keys()])))
        if wstr != "":
            qstr = "%s WHERE %s GROUP BY pid" % (qstr, wstr)
            self.cur.execute(qstr)
            procs_pc =  map(lambda x:x[0], self.cur.fetchall())
            if len(procs) > 0:  # procs added from syscall
                procs = list_intersect([procs, procs_pc])
            else:
                procs.extend(procs_pc)

        return procs
    
    def proc_sel(self, columns, **where):
        qstr = "SELECT %s FROM proc" % columns
        wstr = " and ".join(map(lambda k:"%s=%s" % (k, where[k]),
            utils.list_intersect([self.PROC_ATTR, where.keys()])))
        if wstr != "": qstr = "%s WHERE %s" % (qstr, wstr)
        self.cur.execute(qstr)
        return self.cur.fetchall()

    def proc_sum(self, field):
        self.cur.execute("SELECT SUM(%s) FROM proc" % field)
        res = self.cur.fetchone()
        if res is None: # No such system call
            return 0
        else:
            return res[0]
    
    def proc_avg(self, field):
        self.cur.execute("SELECT AVG(%s) FROM proc" % field)
        res = self.cur.fetchone()
        if res is None: # No such system call
            return 0
        else:
            return res[0]
    
    def proc_std(self, field):
        self.cur.execute("SELECT %s FROM proc" % field)
        vlist = map(lambda x:x[0], self.cur.fetchall())
        if len(vlist) == 0: return 0
        return num.num_std(vlist)

    def proc_sum2(self, columns, **where):
        columns = columns.split(',')
        columns = ','.join(map(lambda s:"SUM(%s)"%s, columns))
        qstr = "SELECT %s FROM proc" % columns
        wstr = " and ".join(map(lambda k:"%s=%s" % (k, where[k]),
            utils.list_intersect([self.PROC_ATTR, where.keys()])))
        if wstr != "": qstr = "%s WHERE %s" % (qstr, wstr)
        print qstr

    def proc_cmdline(self, iid, pid, fullcmd=True):
        self.cur.execute("SELECT cmdline FROM proc "
            "WHERE iid=? and pid=?", (iid, pid))
        res = self.cur.fetchone()[0]
        if fullcmd: return res
        else: return res.split(" ", 1)[0]

    def proc_io_sum_elapsed_and_bytes(self, sysc, iid, pid, fid):
        assert sysc == SYSCALL['read'] or sysc == SYSCALL['write']
        self.cur.execute("SELECT SUM(elapsed),SUM(aux1) FROM syscall "
            "WHERE sysc=? and iid=? and pid=? and fid=?",
            (sysc, iid, pid, fid))
        return self.cur.fetchone()

    def proc_stat(self, column, **attr):
        """Return (sum, avg, stddev) of column of selected processes"""
        
        qstr = "SELECT %s FROM proc" % column
        wstr = " and ".join(map(lambda k:"%s=%s" % (k, attr[k]),
            list_intersect([self.PROC_ATTR, attr.keys()])))
        if wstr != "": qstr = "%s WHERE %s" % (qstr, wstr)
        self.cur.execute(qstr)
        values = map(lambda x:x[0], self.cur.fetchall())
        return numpy.sum(values), numpy.mean(values), numpy.std(values)
    
    def proc_cdf(self, column, numbins=None, **attr):
        """Return (sum, avg, stddev) of column of selected processes"""
        
        qstr = "SELECT %s FROM proc" % column
        wstr = " and ".join(map(lambda k:"%s=%s" % (k, attr[k]),
            list_intersect([self.PROC_ATTR, attr.keys()])))
        if wstr != "": qstr = "%s WHERE %s" % (qstr, wstr)
        self.cur.execute(qstr)
        values = map(lambda x:x[0], self.cur.fetchall())
        values.sort()
        total = numpy.sum(values)
        cdf_data = []
        curr_sum = 0.0
        for v in values:
            curr_sum += v
            cdf_data.append((v, curr_sum/total))
        return cdf_data
    
    def sysc_stat(self, column, **attr):
        """Return (sum, avg, stddev) of column of selected processes"""
        
        if "sysc" in attr.keys(): attr["sysc"] = SYSCALL[attr["sysc"]]
        qstr = "SELECT %s FROM syscall" % column
        wstr = " and ".join(map(lambda k:"%s=%s" % (k, attr[k]),
            list_intersect([self.SYSC_ATTR, attr.keys()])))
        if wstr != "": qstr = "%s WHERE %s" % (qstr, wstr)
        self.cur.execute(qstr)
        values = map(lambda x:x[0], self.cur.fetchall())
        return numpy.sum(values), numpy.mean(values), numpy.std(values)

    def proc_throughput(self, iid, pid, fid, sysc):
        if sysc == "read" or sysc == "write":
            self.cur.execute("SELECT SUM(elapsed),SUM(aux1) FROM sysc"
                " WHERE iid=? and pid=? and fid=? and sysc=? GROUP BY pid", 
                (iid, pid, fid, SYSCALL[sysc]))
        else:
            self.cur.execute("SELECT SUM(elapsed),COUNT(sysc) FROM syscall"
                " WHERE iid=? and pid=? and fid=? and sysc=? GROUP BY pid", 
                (iid, pid, fid, SYSCALL[sysc]))
        return self.cur.fetchone()
