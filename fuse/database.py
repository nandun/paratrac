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

import os
import sqlite3
import csv

from paratrac.common.database import *

__all__ = ["FUSETracDB"]

class FUSETracDB(Database):
    def create_tables(self):
        cur = self.cur
        # table: env
        cur.execute("DROP TABLE IF EXISTS env")
        cur.execute("CREATE TABLE IF NOT EXISTS env "
            "(item TEXT, value TEXT)")

        # table: tracelog
        cur.execute("DROP TABLE IF EXISTS tracelog")
        cur.execute("CREATE TABLE IF NOT EXISTS tracelog "
            "(stamp DOUBLE, pid INTEGER, sysc INTEGER, fid INTEGER, "
            "res INTEGER, elapsed DOUBLE, aux1 INTEGER, aux2 INTEGER)")

        # table: filemap
        cur.execute("DROP TABLE IF EXISTS filemap")
        cur.execute("CREATE TABLE IF NOT EXISTS filemap"
            "(fid INTEGER, path TEXT)")
        
        # table: procmap
        cur.execute("DROP TABLE IF EXISTS procmap")
        cur.execute("CREATE TABLE IF NOT EXISTS procmap "
            "(pid INTEGER, ppid INTEGER, cmdline TEXT)")

    def import_data(self, datadir=None):
        if datadir is None:
            datadir = os.path.dirname(self.dbfile)

        cur = self.cursor()
        
        # import runtime environment data
        envFile = open("%s/env.log" % datadir)
        for line in envFile.readlines():
            cur.execute("INSERT INTO env VALUES (?,?)", 
                line.strip().split(":", 1))
        envFile.close()
        
        # import trace log data
        traceFile = open("%s/trace.log" % datadir)
        for line in traceFile.readlines():
            cur.execute("INSERT INTO tracelog VALUES (?,?,?,?,?,?,?,?)",
                line.strip().split(","))
        traceFile.close()

        # import file map data
        filemapFile = open("%s/file.map" % datadir)
        for line in filemapFile.readlines():
            cur.execute("INSERT INTO filemap VALUES (?,?)",
                line.strip().split(":", 1))
        filemapFile.close()

        # import proc map data
        procmapFile = open("%s/proc.map" % datadir)
        for line in procmapFile.readlines():
            cur.execute("INSERT INTO procmap VALUES (?,?,?)",
                line.strip().split(":", 2))
        procmapFile.close()
    
    # trace routines
    def select_sysc(self, sysc, fields):
        cur = self.db.cursor()
        cur.execute("SELECT %s FROM tracelog WHERE sysc=?" % fields, (sysc,))
        return cur.fetchall()

    def select_sysc_group_by_file(self, sysc, fields):
        cur = self.cur
        cur.execute("SELECT %s FROM tracelog where sysc=? group by fid" 
            % fields, (sysc,))
        return cur.fetchall()
    
    def select_file(self, fid, fields):
        cur = self.db.cursor()
        cur.execute("SELECT %s FROM tracelog WHERE fid=?" % fields, (fid,))
        return cur.fetchall()

    def get_first_stamp(self):
        cur = self.db.cursor()
        cur.execute("SELECT stamp FROM tracelog")
        return cur.fetchone()[0]
    
    # proc map routines
    def procmap_fetchall(self, fields="*"):
        cur = self.db.cursor()
        cur.execute("SELECT %s FROM procmap" % fields)
        return cur.fetchall()

    def procmap_select_pid(self, pid, fields):
        cur = self.db.cursor()
        cur.execute("SELECT %s FROM procmap WHERE pid=?" % fields, (pid,))
        return cur.fetchall()
    
    # file map routines
    def filemap_fetchall(self):
        cur = self.db.cursor()
        cur.execute("SELECT * FROM filemap")
        return cur.fetchall()
