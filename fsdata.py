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
# fsdata.py
# Filesystem Trace Database
#

import os
import sqlite3
import cPickle
import scipy
import numpy

from common import *

class Database:
    def __init__(self, dbfile, initTables):
        self.dbfile = os.path.abspath(dbfile)
        self.db = sqlite3.connect(self.dbfile)
        self.cur = self.db.cursor()
        self.tables = []    # all tables in database

        if initTables: self.init_tables()
        
        # Only attributes can be accurately queried
        self.SYSC_ATTR = ["iid", "sysc"]
        self.FILE_ATTR = ["iid", "fid", "path"]
        self.PROC_ATTR = ["iid", "pid", "ppid", "live", 
            "res", "cmdline", "environ"]

    def __del__(self):
        if self.db is not None:
            self.db.commit()
            self.db.close()

    def close(self):
        self.db.commit()
        self.db.close()
        self.db = None  # mark db closed

    def cursor(self):
        return self.db.cursor()

    def init_tables(self):
        """Create tables for database
        NOTE: existing tables will be dropped"""
        # runtime
        self.cur.execute("DROP TABLE IF EXISTS runtime")
        self.cur.execute("CREATE TABLE IF NOT EXISTS runtime"
            "(item TEXT, value TEXT)")
        self.tables.append("runtime")
        
        # table: syscall
        self.cur.execute("DROP TABLE IF EXISTS syscall")
        self.cur.execute("CREATE TABLE IF NOT EXISTS syscall "
            "(stamp DOUBLE, iid INTEGER, pid INTEGER, "
            "sysc INTEGER, fid INTEGER, res INTEGER, elapsed DOUBLE, "
            "aux1 INTEGER, aux2 INTEGER)")
        self.tables.append("syscall")
        
        # table: proc
        self.cur.execute("DROP TABLE IF EXISTS proc")
        self.cur.execute("CREATE TABLE IF NOT EXISTS proc "
            "(iid INTGER, pid INTEGER, ppid INTEGER, "
            "live INTEGER, res INTEGER, btime FLOAT, elapsed FLOAT, "
            "cmdline TEXT, environ TEXT)")
        
        # table: file 
        self.cur.execute("DROP TABLE IF EXISTS file")
        self.cur.execute("CREATE TABLE IF NOT EXISTS file"
            "(iid INTEGER, fid INTEGER, path TEXT)")
        
    def import_rawdata(self, datadir=None):
        if datadir is None:
            datadir = os.path.dirname(self.dbfile)

        cur = self.cursor()

        # import runtime environment
        envFile = open("%s/env.log" % datadir)
        assert envFile.readline().startswith("#")
        for line in envFile.readlines():
            cur.execute("INSERT INTO runtime VALUES (?,?)", 
                line.strip().split(":", 1))
        envFile.close()
        
        # get instance id for further usage
        iids = str(self.runtime_get_value("iid")) # note that iids is a string
        t_allstart = float(self.runtime_get_value("start"))
        
        # import trace log data
        traceFile = open("%s/trace.log" % datadir)
        assert traceFile.readline().startswith("#")
        for line in traceFile.readlines():
            values = line.strip().split(",")
            # normalize stamp time, TODO: use trace start time first
            values[0] = "%f" % (float(values[0]) - t_allstart)
            values.insert(1, iids) # insert iid to the 2nd value
            cur.execute("INSERT INTO syscall VALUES (?,?,?,?,?,?,?,?,?)", 
                values)
        traceFile.close()

        # import process log
        procmapFile = open("%s/proc/map" % datadir)
        assert procmapFile.readline().startswith("#")
        procmap = {}
        lineno = 0
        for line in procmapFile.readlines():
            lineno += 1
            try:
                pid, ppid, cmdline = line.strip().split(":", 2)
            except ValueError:
                sys.stderr.write("Warning: line %d: %s\n" % (lineno, line))
                continue
            # WARNING: workaround to fix the cmdline of process 1
            procmap[pid] = [iids, pid, ppid, cmdline]
        procmapFile.close()
        # fix process info
        procmap["1"][2] = "1"
        procmap["1"][3] = "/sbin/init"
        
        procstatFile = open("%s/proc/stat" % datadir)
        assert procstatFile.readline().startswith("#")
        for line in procstatFile.readlines():
            pid,ppid,live,res,btime,elapsed,cmd = line.strip().split(",", 6)
            # must use ppid in proc/map
            # taskstat consider ppid of process as 1, since its parent dead
            procmap[pid].insert(3, elapsed)
            # normalize time
            procmap[pid].insert(3, "%d" % (int(btime) - t_allstart))
            procmap[pid].insert(3, res)
            procmap[pid].insert(3, live)
        procstatFile.close()

        procenvironFile = open("%s/proc/environ" % datadir)
        assert procenvironFile.readline().startswith("#")
        for line in procenvironFile.readlines():
            stamp, pid, environ = line.strip().split(",", 2)
            procmap[pid].append(environ)
        procenvironFile.close()

        for v in procmap.values():
            try:
                self.cur.execute("INSERT INTO proc VALUES (?,?,?,?,?,?,?,?,?)", v)
            except:
                print v

        # import file map data
        filemapFile = open("%s/file.map" % datadir)
        assert filemapFile.readline().startswith("#")
        for line in filemapFile.readlines():
            values = line.strip().split(":", 1)
            values.insert(0, iids)
            cur.execute("INSERT INTO file VALUES (?,?,?)", values)
        filemapFile.close()

    # runtime table routines
    def runtime_sel(self, fields="*"):
        self.cur.execute("SELECT %s FROM runtime" % fields)
        return self.cur.fetchall()
    
    def runtime_get_value(self, item):
        self.cur.execute("SELECT value FROM runtime WHERE item=?", (item,))
        res = self.cur.fetchone()
        if res is None: return None
        else: return res[0]

    # syscall table routines
    def sysc_sel(self, sysc, fields="*"):
        self.cur.execute("SELECT %s FROM syscall WHERE sysc=?" 
            % fields, (sysc,))
        return self.cur.fetchall()
    
    def sysc_count(self, sysc):
        self.cur.execute("SELECT COUNT(*) FROM syscall WHERE sysc=?", (sysc,))
        return self.cur.fetchone()[0]
    
    def sysc_sum(self, sysc, field):
        cur = self.db.cursor()
        cur.execute("SELECT SUM(%s) FROM syscall WHERE sysc=?"
        "GROUP BY sysc" % field, (sysc,))
        res = cur.fetchone()
        if res is None: # No such system call
            return 0
        else:
            return res[0]
    
    def sysc_avg(self, sysc, field):
        cur = self.db.cursor()
        cur.execute("SELECT AVG(%s) FROM syscall WHERE sysc=?"
        "GROUP BY sysc" % field, (sysc,))
        res = cur.fetchone()
        if res is None: # No such system call
            return 0
        else:
            return res[0]
    
    def sysc_stddev(self, sysc, field):
        cur = self.db.cursor()
        cur.execute("SELECT %s FROM syscall WHERE sysc=?" % field, (sysc,))
        vlist = map(lambda x:x[0], cur.fetchall())
        return numpy.std(vlist)
    
    def sysc_cdf(self, sysc, field, numbins=None):
        """if numbins is None, use all data"""
        self.cur.execute("SELECT %s FROM syscall WHERE sysc=?" 
            % field, (sysc,))
        vlist = map(lambda x:x[0], self.cur.fetchall())
        vlist.sort()
        total = sum(vlist)
        data = []
        curr_sum = 0.0
        for v in vlist:
            curr_sum += v
            data.append((v, curr_sum/total))
        return data

    def sysc_sel_procs_by_file(self, iid, sysc, fid, fields="*"):
        self.cur.execute("SELECT %s FROM syscall WHERE "
            "iid=? AND sysc=? AND fid=? GROUP BY pid" % fields, 
            (iid, sysc, fid))
        return self.cur.fetchall()

    # proc table routines
    def proc_sel(self, columns, **where):
        qstr = "SELECT %s FROM proc" % columns
        wstr = " and ".join(map(lambda k:"%s=%s" % (k, where[k]),
            list_intersect([self.PROC_ATTR, where.keys()])))
        if wstr != "": qstr = "%s WHERE %s" % (qstr, wstr)
        self.cur.execute(qstr)
        return self.cur.fetchall()
    
    # file table routines
    def file_sel(self, columns, **where):
        qstr = "SELECT %s FROM file" % columns
        wstr = " and ".join(map(lambda k:"%s=%s" % (k, where[k]),
            list_intersect([self.FILE_ATTR, where.keys()])))
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

    def proc_cmdline(self, iid, pid, fullcmd=True):
        self.cur.execute("SELECT cmdline FROM proc "
            "WHERE iid=? and pid=?", (iid, pid))
        res = self.cur.fetchone()[0]
        if fullcmd: return res
        else: return res.split(" ", 1)[0]

    def io_sum_elapsed_and_bytes(self, sysc, iid, pid, fid):
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
            self.cur.execute("SELECT SUM(elapsed),SUM(aux1) FROM syscall"
                " WHERE iid=? and pid=? and fid=? and sysc=? GROUP BY pid", 
                (iid, pid, fid, SYSCALL[sysc]))
        else:
            self.cur.execute("SELECT SUM(elapsed),COUNT(sysc) FROM syscall"
                " WHERE iid=? and pid=? and fid=? and sysc=? GROUP BY pid", 
                (iid, pid, fid, SYSCALL[sysc]))
        return self.cur.fetchone()
