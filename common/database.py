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
# General database class
#

import os
import sqlite3

__all__ = ["Database"]

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

