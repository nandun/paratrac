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

__all__ = ["Report", "FUSETracReport"]

import os
import sys

from common import *
from track import FUSETRAC_SYSCALL
from data import *

class Report():
    def __init__(self, dbfile, opts=None):
        self.datadir = os.path.dirname(dbfile)
        self.db = None
        
        self.ws = ws
        self.es = es

class FUSETracReport(Report):
    def __init__(self, dbfile, opts=None):
        Report.__init__(self, dbfile, opts)
        self.db = FUSETracDB(dbfile)
        self.reportlist = []
        self.reportseries = []

        _FUSETracReport_restrict = ["reportlist", "reportseries"]
        update_opts_kw(self, _FUSETracReport_restrict, opts, None)

    def report(self):
        # System call
        if "sysc_count" in self.reportlist or "all" in self.reportlist:
            self.report_syscall_count()
        if "sysc_elapsed" in self.reportlist or "all" in self.reportlist:
            self.report_syscall_elapsed()
        if "io_summary" in self.reportlist or "all" in self.reportlist:
            self.report_io_summary()
    
    # Report routines
    def report_syscall_count(self):
        if self.reportseries == []:
            syscalls = FUSETRAC_SYSCALL
        else:
            syscalls = list_intersect([self.reportseries, FUSETRAC_SYSCALL])
        
        stats = []
        total_cnt = 0.0
        for sc in syscalls:
            sc_num = SYSCALL[sc]
            cnt = self.db.sysc_count(sc_num)
            stats.append((sc, cnt))
            total_cnt += cnt

        self.ws("System Call Count Statistics\n")
        self.ws("%15s%20s%20s\n" % ("System Call", "Count", "Ratio"))
        for sc, cnt in stats:
            self.ws("%15s%20d%20.9f\n" % (sc, cnt, cnt/total_cnt))
        self.ws("%15s%20d%20.9f\n" % ("Total", total_cnt, 1))
    
    def report_syscall_elapsed(self):
        if self.reportseries == []:
            syscalls = FUSETRAC_SYSCALL
        else:
            syscalls = list_intersect([self.reportseries, FUSETRAC_SYSCALL])
        
        stats = []
        total_elapsed = 0.0
        for sc in syscalls:
            sc_num = SYSCALL[sc]
            sum = self.db.sysc_sum(sc_num,"elapsed")
            avg = self.db.sysc_avg(sc_num,"elapsed")
            stddev = self.db.sysc_stddev(sc_num, "elapsed")
            stats.append((sc, sum, avg, stddev))
            total_elapsed += sum
        
        self.ws("System Call Latency Statistics\n")
        self.ws("%15s%20s%20s%20s%20s\n" % 
            ("System Call", "Sum", "Ratio", "Avg", "StdDev"))
        for sc, sum, avg, stddev in stats:
            self.ws("%15s%20s%20s%20s%20s\n" % \
                (sc, sum, sum/total_elapsed, avg, stddev))
        self.ws("%15s%20.9f%20.9f\n" % ("Total", total_elapsed, 1))
    
    def report_io_summary(self):
        sc_creat = SYSCALL["creat"]
        sc_open = SYSCALL["open"]
        sc_close = SYSCALL["close"]
        sc_read = SYSCALL["read"]
        sc_write = SYSCALL["write"]
        # Read and Write bytes
        read_bytes = self.db.sysc_sum(sc_read, "aux1")
        write_bytes = self.db.sysc_sum(sc_write, "aux1")

        # Read and Write files
        n_open = 0
        n_creat = 0
        n_close = 0
        n_read_only = 0
        n_write_only = 0
        n_both = 0
        n_none = 0
       
        c_files = map(lambda x:x[0],
            self.db.sysc_select_group_by_file(sc_creat, "fid"))
        o_files = map(lambda x:x[0],
            self.db.sysc_select_group_by_file(sc_open, "fid"))
        cl_files = map(lambda x:x[0],
            self.db.sysc_select_group_by_file(sc_close, "fid"))
        r_files = map(lambda x:x[0], 
            self.db.sysc_select_group_by_file(sc_read, "fid"))
        w_files = map(lambda x:x[0], 
            self.db.sysc_select_group_by_file(sc_write, "fid"))

        n_creat = len(c_files)
        n_open = len(o_files)
        n_close = len(cl_files)
        n_accessed = n_creat + n_open - len(list_intersect([c_files,o_files]))

        n_both = len(list_intersect([r_files,w_files]))
        n_read_only = len(r_files) - n_both
        n_write_only = len(w_files) - n_both
        n_creat_only = len(list_remove(c_files,[r_files,w_files]))
        n_open_only = len(list_remove(o_files, [r_files,w_files]))
        n_none = n_creat_only + n_open_only

        self.ws("I/O Summary\n")
        self.ws("%15s%20s%20s%20s%20s%30s\n" % 
            ("Read Bytes", "Write Bytes", "Read Only Files", 
            "Written Only Files", "Read+Written Files", 
            "Created/Opened Only Files"))
        self.ws("%15s%20s%20s%20s%20s%30s\n" % \
                (read_bytes, write_bytes, n_read_only, n_write_only, 
                n_both, n_none))

        if  n_accessed != n_read_only + n_write_only + n_both + n_none:
            self.ws("Warning: create+open files: %d != accessed files: %d\n"
                % (n_accessed, n_read_only+n_write_only+n_both+n_none))
        if  n_accessed != n_close:
            self.ws("Warning: create+open files: %d != closed files: %d\n"
                % (n_accessed, n_close))
