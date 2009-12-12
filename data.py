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
# Data persistence
#

__all__ = ["Database", "FUSETracDB"]

import sys
import os
import sqlite3
import csv
import numpy

class Database:
    def __init__(self, dbfile):
        self.dbfile = os.path.abspath(dbfile)
        self.db = sqlite3.connect(self.dbfile)
        self.cur = self.db.cursor()
    
    def __del__(self):
        if self.db is not None:
            self.db.commit()
            self.db.close()

    def close(self):
        self.db.commit()
        self.db.close()
        self.db = None

    def cursor(self):
        return self.db.cursor()

class FUSETracDB(Database):
    def create_tables(self):
        cur = self.cur
        # table: env
        cur.execute("DROP TABLE IF EXISTS env")
        cur.execute("CREATE TABLE IF NOT EXISTS env "
            "(item TEXT, value TEXT)")

        # table: syscall
        cur.execute("DROP TABLE IF EXISTS syscall")
        cur.execute("CREATE TABLE IF NOT EXISTS syscall "
            "(stamp DOUBLE, iid INTEGER, pid INTEGER, "
            "sysc INTEGER, fid INTEGER, res INTEGER, elapsed DOUBLE, "
            "aux1 INTEGER, aux2 INTEGER)")

        # table: file
        cur.execute("DROP TABLE IF EXISTS file")
        cur.execute("CREATE TABLE IF NOT EXISTS file"
            "(iid INTEGER, fid INTEGER, path TEXT)")
        
        # table: proc
        cur.execute("DROP TABLE IF EXISTS proc")
        cur.execute("CREATE TABLE IF NOT EXISTS proc "
            "(iid INTGER, pid INTEGER, ppid INTEGER, "
            "live INTEGER, res INTEGER, btime FLOAT, elapsed FLOAT, "
            "cmdline TEXT, environ TEXT)")

    def import_data(self, datadir=None):
        if datadir is None:
            datadir = os.path.dirname(self.dbfile)

        cur = self.cursor()
        
        # import runtime environment data
        envFile = open("%s/env.log" % datadir)
        assert envFile.readline().startswith("#")
        for line in envFile.readlines():
            cur.execute("INSERT INTO env VALUES (?,?)", 
                line.strip().split(":", 1))
        envFile.close()

        # get instance id for further usage
        iids = str(self.env_get_value("iid")) # note that iids is a string
        
        # import trace log data
        traceFile = open("%s/trace.log" % datadir)
        assert traceFile.readline().startswith("#")
        for line in traceFile.readlines():
            values = line.strip().split(",")
            values.insert(1, iids) # insert iid to the 2nd value
            cur.execute("INSERT INTO syscall VALUES (?,?,?,?,?,?,?,?,?)", values)
        traceFile.close()

        # import file map data
        filemapFile = open("%s/file.map" % datadir)
        assert filemapFile.readline().startswith("#")
        for line in filemapFile.readlines():
            values = line.strip().split(":", 1)
            values.insert(0, iids)
            cur.execute("INSERT INTO file VALUES (?,?,?)", values)
        filemapFile.close()

        # import proc info data
        procmapFile = open("%s/proc/map" % datadir)
        assert procmapFile.readline().startswith("#")
        assert procmapFile.readline().startswith("0:system init")
        procmap = {}
        lineno = 0
        for line in procmapFile.readlines():
            lineno += 1
            try:
                pid, ppid, cmdline = line.strip().split(":", 2)
            except ValueError:
                sys.stderr.write("Warning: line %d: %s\n" % (lineno, line))
                continue
            procmap[pid] = [ppid, cmdline]
        procmapFile.close()

        procstatFile = open("%s/proc/stat" % datadir)
        assert procstatFile.readline().startswith("#")
        for line in procstatFile.readlines():
            values = line.strip().split(",", 6)
            pid = values[0]
            # must use ppid in proc/map
            # taskstat consider ppid of process as 1, since its parent dead
            real_ppid, cmdline = procmap[pid]
            values[1] = real_ppid
            values[6] = cmdline # check something?
            procmap[pid] = values
        procstatFile.close()

        procenvironFile = open("%s/proc/environ" % datadir)
        assert procenvironFile.readline().startswith("#")
        for line in procenvironFile.readlines():
            stamp, pid, environ = line.strip().split(",", 2)
            values = procmap[pid]
            values.insert(0, iids)
            values.append(environ)
            cur.execute("INSERT INTO proc VALUES (?,?,?,?,?,?,?,?,?)", values)
    
    # trace routines
    def env_get_value(self, item):
        cur = self.db.cursor()
        cur.execute("SELECT value FROM env WHERE item=?", (item,))
        res = cur.fetchone()
        if res is None: return None
        else: return res[0]
        
    def sysc_select(self, sysc, fields="*"):
        cur = self.db.cursor()
        cur.execute("SELECT %s FROM syscall WHERE sysc=?" % fields, (sysc,))
        return cur.fetchall()

    def sysc_select_group_by_file(self, sysc, fields="*"):
        cur = self.cur
        cur.execute("SELECT %s FROM syscall WHERE sysc=? GROUP BY fid" 
            % fields, (sysc,))
        return cur.fetchall()
    
    def sysc_select_group_by_proc(self, sysc, fields="*"):
        cur = self.cur
        cur.execute("SELECT %s FROM syscall WHERE sysc=? GROUP BY pid" 
            % fields, (sysc,))
        return cur.fetchall()
    
    def sysc_select_group_by_proc_on_fid(self, sysc, fid, fields="*"):
        cur = self.cur
        cur.execute("SELECT %s FROM syscall WHERE sysc=? AND fid=?"
            "GROUP BY pid" % fields, (sysc,fid))
        return cur.fetchall()

    def select_file(self, fid, fields):
        cur = self.db.cursor()
        cur.execute("SELECT %s FROM syscall WHERE fid=?" % fields, (fid,))
        return cur.fetchall()

    def select_sysc_on_fid(self, fid, sysc, fields="*", fetchall=True):
        cur = self.db.cursor()
        cur.execute("SELECT %s FROM syscall WHERE fid=? AND sysc=?" %
            fields, (fid, sysc))
        if fetchall: return cur.fetchall()
        else: return cur.fetchone()

    def get_first_stamp(self):
        cur = self.db.cursor()
        cur.execute("SELECT stamp FROM syscall")
        return cur.fetchone()[0]
    
    # syscall routines
    def sysc_count(self, sysc):
        cur = self.db.cursor()
        cur.execute("SELECT COUNT(*) FROM syscall WHERE sysc=?", (sysc,))
        return cur.fetchone()[0]
    
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

    def sysc_sum_by_proc(self, sysc, field, pid=-1):
        cur = self.db.cursor()
        if pid == -1:
            cur.execute("SELECT SUM(%s) FROM syscall WHERE sysc=?"
            "GROUP BY pid" % field, (sysc,))
        else:
            cur.execute("SELECT SUM(%s) FROM syscall WHERE sysc=? AND pid=?"
            "GROUP BY pid" % field, (sysc, pid))
        return cur.fetchall()
    
    def sysc_sum_by_proc_on_fid(self, sysc, fid, field, pid):
        cur = self.db.cursor()
        cur.execute("SELECT SUM(%s) FROM syscall WHERE sysc=? and fid=?"
            "GROUP BY pid" % field, (sysc, fid))
        res = cur.fetchone()
        if res: return res[0]
        else: return 0
    
    # proc map routines
    def proc_fetchall(self, fields="*", nolive=False):
        cur = self.db.cursor()
        if nolive:
            cur.execute("SELECT %s FROM proc WHERE live=0" % fields)
        else:
            cur.execute("SELECT %s FROM proc" % fields)
        return cur.fetchall()

    def proc_select_pid(self, pid, fields):
        cur = self.db.cursor()
        cur.execute("SELECT %s FROM proc WHERE pid=?" % fields, (pid,))
        return cur.fetchall()

    def proc_get_ppid(self, pid):
        cur = self.cur
        cur.execute("SELECT ppid FROM proc WHERE pid=?", (pid,))
        return cur.fetchone()[0]

    # file map routines
    def file_fetchall(self, fields="*"):
        cur = self.db.cursor()
        cur.execute("SELECT %s FROM file" % fields)
        return cur.fetchall()
