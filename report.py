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
        if "sysc_elapsed_cdf" in self.reportlist or "all" in self.reportlist:
            self.report_syscall_elapsed_cdf()
        if "io_summary" in self.reportlist or "all" in self.reportlist:
            self.report_io_summary()
        if "io_length" in self.reportlist or "all" in self.reportlist:
            self.report_io_length()
        if "proc_io_summary" in self.reportlist or "all" in self.reportlist:
            self.report_proc_io_summary()
        if "proc_io_volume_cdf" in self.reportlist or "all" in self.reportlist:
            self.report_proc_io_volume_cdf()
        if "proc_life_cdf" in self.reportlist or "all" in self.reportlist:
            self.report_proc_life_cdf()
    
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
    
    def report_syscall_elapsed_cdf(self):
        if self.reportseries == []:
            syscalls = FUSETRAC_SYSCALL
        else:
            syscalls = list_intersect([self.reportseries, FUSETRAC_SYSCALL])

        for sc in syscalls:
            sc_num = SYSCALL[sc]
            vlist = self.db.sysc_select(sc_num, "elapsed")
            vlist = map(lambda x:x[0], vlist)
            index = 0
            self.ws("CDF of latency of %s (microsec, ratio)\n" % sc)
            for latency, ratio in stat_cdf(vlist, 0.1, 0.1):
                index += 1
                self.ws("(%.6f,%.6f) " % (latency*1000000, ratio))
                if index % 3 == 1: self.ws("\n")
            self.ws("\n\n")
    
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
    
    def report_io_length(self):
        # cdf function: read
        read_lengths = self.db.sysc_select(SYSCALL["read"], "aux1")
        total_bytes = self.db.sysc_sum(SYSCALL["read"], "aux1")
        read_lengths = map(lambda x:x[0], read_lengths)
        self.ws("Read Length CDF: (length, ratio)\n")
        index = 0
        for v in stat_cdf(read_lengths, 0.1,0.1):
            index += 1
            self.ws("(%d,%.6f) " % v)
            if index % 3 == 1: self.ws("\n")
        self.ws("\n\n")
        
        write_lengths = self.db.sysc_select(SYSCALL["write"], "aux1")
        total_bytes = self.db.sysc_sum(SYSCALL["write"], "aux1")
        write_lengths = map(lambda x:x[0], write_lengths)
        self.ws("Write Length CDF: (length, ratio)\n")
        index = 0
        for v in stat_cdf(write_lengths, 0.1, 0.1):
            index += 1
            self.ws("(%d,%.6f) " % v)
            if index % 3 == 1: self.ws("\n")
        self.ws("\n\n")

    def report_proc_io_summary(self):
        sc_creat = SYSCALL["creat"]
        sc_open = SYSCALL["open"]
        sc_close = SYSCALL["close"]
        sc_read = SYSCALL["read"]
        sc_write = SYSCALL["write"]

        # Read and Write Procs
        n_open = 0
        n_creat = 0
        n_close = 0
        n_read_only = 0
        n_write_only = 0
        n_both = 0
        n_none = 0
        
        c_procs = map(lambda x:x[0],
            self.db.sysc_select_group_by_proc(sc_creat, "pid"))
        o_procs = map(lambda x:x[0],
            self.db.sysc_select_group_by_proc(sc_open, "pid"))
        cl_procs = map(lambda x:x[0],
            self.db.sysc_select_group_by_proc(sc_close, "pid"))
        r_procs = map(lambda x:x[0], 
            self.db.sysc_select_group_by_proc(sc_read, "pid"))
        w_procs = map(lambda x:x[0], 
            self.db.sysc_select_group_by_proc(sc_write, "pid"))
        co_procs = list_remove(c_procs,[r_procs,w_procs])
        oo_procs = list_remove(o_procs, [r_procs,w_procs])
        # create and open procs
        cao_procs = list_intersect([c_procs, o_procs])
        ro_procs = list_remove(r_procs, [co_procs,oo_procs,w_procs])
        wo_procs = list_remove(w_procs, [co_procs,oo_procs,r_procs])
        rw_procs = list_intersect([r_procs,w_procs])
        
        # Shared read/write procs, inheri file descriptor
        sr_procs = list_remove(r_procs, [c_procs, o_procs])
        sw_procs = list_remove(w_procs, [c_procs, o_procs])
        srw_procs = list_intersect([sr_procs, sw_procs])
        
        n_creat = len(c_procs)
        n_open = len(o_procs)
        n_creat_and_open = len(cao_procs)
        n_close = len(cl_procs)

        n_both = len(rw_procs)
        n_read_only = len(r_procs) - n_both
        assert n_read_only == len(ro_procs)
        n_write_only = len(w_procs) - n_both
        assert n_write_only == len(wo_procs)
        n_creat_only = len(co_procs)
        n_open_only = len(oo_procs)
        n_none = n_creat_only + n_open_only
        assert n_none == len(co_procs) + len(oo_procs)
        n_accessed = n_creat + n_open - len(cao_procs) + \
            len(sr_procs) + len(sw_procs) - len(srw_procs)

        self.ws("Process I/O Summary\n")
        self.ws("%30s%30s%30s%30s\n" % 
            ("Read Only Procs (shared)", 
             "Writte Only Procs (shared)", "Read+Writte Procs (shared)", 
             "Created/Opened Only Procs"))
        self.ws("%20s (%d)%20s (%d)%20s (%d)%30s\n" % \
                (n_read_only, len(sr_procs), 
                 n_write_only, len(sw_procs),
                 n_both, len(srw_procs), n_none))

        if  n_accessed != \
            n_read_only + n_write_only + n_both + n_none: 
            self.ws("Warning: create+open procs: %d != accessed procs: %d\n"
                % (n_accessed, n_read_only+n_write_only+n_both+n_none))
        if  n_creat + n_open - n_creat_and_open != n_close:
            self.ws("Warning: create+open procs: %d != closed procs: %d\n"
                % (n_creat + n_open - n_creat_and_open, n_close))

    def report_proc_io_volume_cdf(self):
        # Read
        r_bytes = self.db.sysc_sum_by_proc(SYSCALL["read"], "aux1")
        r_bytes = map(lambda x:x[0], r_bytes)
        self.ws("CDF of process read (bytes, ratio)\n")
        index = 0
        for bytes, ratio in stat_cdf(r_bytes, 0.1, 0.1):
            index += 1
            self.ws("(%d,%.6f) " % (bytes, ratio))
            if index % 3 == 1: self.ws("\n")
        self.ws("\n\n")

        # Write
        w_bytes = self.db.sysc_sum_by_proc(SYSCALL["write"], "aux1")
        w_bytes = map(lambda x:x[0], w_bytes)
        self.ws("CDF of process write (bytes, ratio)\n")
        index = 0
        for bytes, ratio in stat_cdf(w_bytes, 0.1, 0.1):
            index += 1
            self.ws("(%d,%.6f) " % (bytes, ratio))
            if index % 3 == 1: self.ws("\n")
        self.ws("\n\n")

    def report_proc_life_cdf(self):
        # Must excluded system processes that are still alive
        lifes = self.db.proc_fetchall("elapsed", True)
        lifes = map(lambda x:x[0], lifes)
        self.ws("CDF of process life time (usec, ratio)\n")
        index = 0
        for elapsed, ratio in stat_cdf(lifes, 0.1, 0.1):
            index += 1
            self.ws("(%d,%.6f) " % (elapsed, ratio))
            if index % 3 == 1: self.ws("\n")
        self.ws("\n\n")

# Statistic functions
def stat_cdf(vlist, vthreshold=0.0, rthreshold=0.0):
    values = list(vlist)
    values.sort()
    total = sum(values)
    # Some vlist may be zero
    if total == 0: return [(0,0)]
    prev = 0
    curr_sum = 0
    prev_sum = 0
    raw_list = []
    for l in values:
        if l != prev:
            raw_list.append((prev, float(curr_sum)/total))
        curr_sum += l
        prev = l
    raw_list.append(((prev, float(curr_sum)/total)))

    # reduce the number of elements by removing error within threshold
    
    coord_list = []
    coord_list.append(raw_list[0])
    prev_value, prev_ratio = raw_list[0]
    for value, ratio in raw_list:
        if_add = False
        if (value - prev_value) > vthreshold * prev_value or \
           (ratio - prev_ratio) > rthreshold * prev_ratio:
            coord_list.append((value, ratio))
            prev_value, prev_ratio = value, ratio
            if_add = True
    if not if_add: coord_list.append((prev_value, prev_ratio))
    return coord_list
