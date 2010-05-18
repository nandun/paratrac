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
# modules/data.py
# Filesystem Trace Database
#

import os
import sqlite3
import cPickle

class Database:
    """
    Common database
    """
    def __init__(self, path):
        self.db = os.path.abspath(path)
        self.con = sqlite3.connect(self.db)
        self.cur = self.con.cursor()
        self.tab = {}
        self._set_tabs()

    def __del__(self):
        if self.con is not None:
            self.con.commit()
            self.con.close()

    def _set_tabs(self):
        return

    def _get_tabs(self):
        self.cur.execute("SELECT tbl_name FROM SQLITE_MASTER")
        return map(lambda x:x[0], self.cur.fetchall())

    def _create_tabs(self, truncate=False):
        tabs = self._get_tabs()
        for tab, spec in self.tab.items():
            if truncate and tab in tabs:
                self.cur.execute("DELETE FROM %s" % tab)
            self.cur.execute("CREATE TABLE IF NOT EXISTS %s (%s)"
                % (tab, spec))
    
    def close(self):
        self.con.commit()
        self.con.close()
        self.con = None


