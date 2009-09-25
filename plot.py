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
# To use plot.py, matplotlib is required
# see http://matplotlib.sourceforge.net for details
#

__all__ = ["Plot", "FUSETracPlot"]

import os
import sys
import numpy

from common import *
from track import FUSETRAC_SYSCALL
from data import *

COLOR_SERIES_1 = ["seagreen2","seashell2","skyblue2","slategray2",
    "tan1", "thistle1", "tomato1","turquoise","violet", "yellow2"]

class Plot():
    def __init__(self, dbfile, opts=None):
        self.datadir = os.path.dirname(dbfile)
        self.db = None

        self.plotinteractive=True
        self.plotformat=None
        
        _Plot_restrict = ["plotinteractive", "plotformat"]
        update_opts_kw(self, _Plot_restrict, opts, None)
        
        import matplotlib
        if not self.plotinteractive:
            if self.plotformat is None:
                self.plotformat = "png"
            if self.plotformat == "png":
                matplotlib.use("agg")
            else:
                matplotlib.use(self.plotformat)
        import matplotlib.pyplot
        import matplotlib.widgets
        
        self.pyplot = matplotlib.pyplot
        self.widgets = matplotlib.widgets
        self.requirepyplot = False
        
        self.prompt = True
        
        self.ws = ws
        self.es = es
    
    def show(self):
        if self.plotinteractive and self.requirepyplot:
            self.pyplot.show()

    def format_data_float(self, data):
        return "%.9f" % data

class FUSETracPlot(Plot):
    def __init__(self, dbfile, opts=None):
        Plot.__init__(self, dbfile, opts)
        self.db = FUSETracDB(dbfile)
        self.plotlist = []
        self.plotseries = []
        self.plotlogy = 1

        _FUSETracPlot_restrict = ["plotlist", "plotseries", "plotlogy"]
        update_opts_kw(self, _FUSETracPlot_restrict, opts, None)

    def plot(self):
        # System call
        if "sysc_count" in self.plotlist or "all" in self.plotlist:
            self.plot_syscall_count()
        if "sysc_elapsed" in self.plotlist or "all" in self.plotlist:
            self.plot_syscall_elapsed()
        if "sysc_elapsed_sum" in self.plotlist or "all" in self.plotlist:
            self.plot_syscall_elapsed_sum()
        
        # I/O
        if "io_offset" in self.plotlist or "all" in self.plotlist:
            self.plot_io_offset()
        if "io_length" in self.plotlist or "all" in self.plotlist:
            self.plot_io_length()
        if "io_bytes" in self.plotlist or "all" in self.plotlist:
            self.plot_io_bytes()

        if "proctree" in self.plotlist or "all" in self.plotlist:
            self.plot_proctree()
        if "proctree_dot" in self.plotlist or "all" in self.plotlist:
            self.plot_proctree_dot()
        if "workflow" in self.plotlist or "all" in self.plotlist:
            self.plot_workflow()
        if "workflow_dot" in self.plotlist or "all" in self.plotlist:
            self.plot_workflow_dot()
    
    # Plot routines for each type of figure
    def plot_save_figure(self, fig, file):
        output = "%s/%s.%s" % (self.datadir, file, self.plotformat)
        fig.savefig(output)
        self.ws("Plot has been saved to %s\n" % output)

    def plot_syscall_count(self):
        if self.plotseries == []:
            syscalls = FUSETRAC_SYSCALL
        else:
            syscalls = list_intersect([self.plotseries, FUSETRAC_SYSCALL])
        
        self.requirepyplot = True
        self.ws("Plotting system call count statistics... ")

        fig = self.pyplot.figure()
        ax = fig.add_subplot(1,1,1)
        ax.set_title("System Calls Counts")
        ax.set_xlabel("Time Stamp (seconds)")
        ax.set_ylabel("Operation Count")
        ax.fmt_xdata = self.format_data_float
        ax.fmt_ydata = self.format_data_float
        
        sysc_num = [SYSCALL[s] for s in syscalls]
        first_stamp = self.db.get_first_stamp()
        lines = []
        for sc in sysc_num:
            stamp = []
            count = []
            sum = 0
            for s in self.db.sysc_select(sc, "stamp"):
                sum += 1
                stamp.append(s[0] - first_stamp)
                count.append(sum)
            lines.append(ax.plot(stamp, count, linewidth=0.5, picker=5))
               
        # plot legend
        ax.legend(lines, syscalls, loc=(1.0,0.1), numpoints=1)
        legends = ax.get_legend()
        legends.draw_frame(False)
        for t in legends.get_texts():
            t.set_size("small")
        
        self.ws("Done!\n")
        
        if self.plotformat is not None:
            self.plot_save_figure(fig, "sysc_count")

    def plot_syscall_elapsed(self):
        if self.plotseries == []:
            syscalls = FUSETRAC_SYSCALL
        else:
            syscalls = list_intersect([self.plotseries, FUSETRAC_SYSCALL])
        
        self.requirepyplot = True
        self.ws("Plotting system call elapsed time statistics... ")
        
        fig = self.pyplot.figure()
        ax = fig.add_subplot(1,1,1)
        ax.set_title("System Calls Elapsed Time")
        ax.set_xlabel("Time Stamp (seconds)")
        ax.set_ylabel("Elapsed Time (seconds)")
        ax.fmt_xdata = self.format_data_float
        ax.fmt_ydata = self.format_data_float

        if self.plotlogy != 1:
            ax.semilogy(basex=self.plotlogy)

        sysc_num = [SYSCALL_FILESYSTEM[s] for s in syscalls]
        first_stamp = self.db.get_first_stamp()
        lines = []
        for sc in sysc_num:
            stamp = []
            elapsed = []
            for s,e in self.db.sysc_select(sc, "stamp,elapsed"):
                stamp.append(s - first_stamp)
                elapsed.append(e)
            lines.append(ax.plot(stamp, elapsed, linewidth=0.3))
        
        # plot legend
        ax.legend(lines, syscalls, loc=(1.0,0.1), numpoints=1)
        legends = ax.get_legend()
        legends.draw_frame(False)
        for t in legends.get_texts():
            t.set_size("small")
        
        self.ws("Done!\n")
        
        if self.plotformat is not None:
            self.plot_save_figure(fig, "sysc_elapsed")

    def plot_syscall_elapsed_sum(self):
        if self.plotseries == []:
            syscalls = FUSETRAC_SYSCALL
        else:
            syscalls = list_intersect([self.plotseries, FUSETRAC_SYSCALL])
        
        self.requirepyplot = True
        self.ws("Plotting system call elapsed time summation ... ")
        
        fig = self.pyplot.figure()
        ax = fig.add_subplot(1,1,1)
        ax.set_title("Summation of System Calls Elapsed Time")
        ax.set_xlabel("Time Stamp (seconds)")
        ax.set_ylabel("Summation of Elapsed Time (seconds)")
        ax.fmt_xdata = self.format_data_float
        ax.fmt_ydata = self.format_data_float

        if self.plotlogy != 1:
            ax.semilogy(basex=self.plotlogy)
        
        sysc_num = [SYSCALL_FILESYSTEM[s] for s in syscalls]
        first_stamp = self.db.get_first_stamp()
        lines = []
        for sc in sysc_num:
            stamp = []
            esum = []
            sum = 0
            for s,e in self.db.sysc_select(sc, "stamp,elapsed"):
                sum += e
                stamp.append(s - first_stamp)
                esum.append(sum)
            lines.append(ax.plot(stamp, esum, linewidth=1))
        
        # plot legend
        ax.legend(lines, syscalls, loc=(1.0,0.1), numpoints=1)
        legends = ax.get_legend()
        legends.draw_frame(False)
        for t in legends.get_texts():
            t.set_size("small")
        
        self.ws("Done!\n")
        
        if self.plotformat is not None:
            self.plot_save_figure(fig, "sysc_elapsed_sum")
    
    def plot_io_offset(self):
        if self.plotseries == []:
            syscalls = ["read", "write"]
        else:
            syscalls = list_intersect([self.plotseries, ["read", "write"]])
        
        self.requirepyplot = True
        self.ws("Plotting I/O offset statistics ... ")
        
        fig = self.pyplot.figure()
        ax = fig.add_subplot(1,1,1)
        ax.set_title("File Offset in I/O")
        ax.set_xlabel("Time Stamp (seconds)")
        ax.set_ylabel("Requested Offset (bytes)")
        ax.fmt_xdata = self.format_data_float
        ax.fmt_ydata = self.format_data_float
        
        if self.plotlogy != 1:
            ax.semilogy(basex=self.plotlogy)
        # db format
        # stamp, pid, sysc, fid, res, elapsed, size, offset
        sysc_num = [SYSCALL_FILESYSTEM[s] for s in syscalls]
        first_stamp = self.db.get_first_stamp()
        lines = []
        for sc in sysc_num:
            stamp = []
            offset = []
            for s,o in self.db.sysc_select(sc, "stamp,aux2"):
                stamp.append(s - first_stamp)
                offset.append(o)
            lines.append(ax.plot(stamp, offset, linewidth=0.3))
        
        # plot legend
        ax.legend(lines, syscalls, loc=(1.0,0.1), numpoints=1)
        legends = ax.get_legend()
        legends.draw_frame(False)
        for t in legends.get_texts():
            t.set_size("small")
        
        self.ws("Done!\n")

        if self.plotformat is not None:
            self.plot_save_figure(fig, "io_offset")
        
    def plot_io_length(self):
        if self.plotseries == []:
            syscalls = ["read", "write"]
        else:
            syscalls = list_intersect([self.plotseries, ["read", "write"]])
        
        self.requirepyplot = True
        self.ws("Plotting I/O length statistics ... ")
        
        fig = self.pyplot.figure()
        ax = fig.add_subplot(1,1,1)
        ax.set_title("I/O Length")
        ax.set_xlabel("Time Stamp (seconds)")
        ax.set_ylabel("Requested Length (bytes)")
        ax.fmt_xdata = self.format_data_float
        ax.fmt_ydata = self.format_data_float
        
        if self.plotlogy != 1:
            ax.semilogy(basex=self.plotlogy)
        # db format
        # stamp, pid, sysc, fid, res, elapsed, size, offset
        sysc_num = [SYSCALL_FILESYSTEM[s] for s in syscalls]
        first_stamp = self.db.get_first_stamp()
        lines = []
        for sc in sysc_num:
            stamp = []
            length = []
            for s,l in self.db.sysc_select(sc, "stamp,aux1"):
                stamp.append(s - first_stamp)
                length.append(l)
            lines.append(ax.plot(stamp, length, linewidth=0.3))
        
        # plot legend
        ax.legend(lines, syscalls, loc=(1.0,0.1), numpoints=1)
        legends = ax.get_legend()
        legends.draw_frame(False)
        for t in legends.get_texts():
            t.set_size("small")
        
        self.ws("Done!\n")

        if self.plotformat is not None:
            self.plot_save_figure(fig, "io_lenght")
    
    def plot_io_bytes(self):
        if self.plotseries == []:
            syscalls = ["read", "write"]
        else:
            syscalls = list_intersect([self.plotseries, ["read", "write"]])
        
        self.requirepyplot = True
        self.ws("Plotting I/O bytes statistics ... ")
        
        fig = self.pyplot.figure()
        ax = fig.add_subplot(1,1,1)
        ax.set_title("Total I/O Bytes")
        ax.set_xlabel("Time Stamp (seconds)")
        ax.set_ylabel("Data Volumn (bytes)")
        ax.fmt_xdata = self.format_data_float
        ax.fmt_ydata = self.format_data_float
        
        if self.plotlogy != 1:
            ax.semilogy(basex=self.plotlogy)
        # db format
        # stamp, pid, sysc, fid, res, elapsed, size, offset
        sysc_num = [SYSCALL_FILESYSTEM[s] for s in syscalls]
        first_stamp = self.db.get_first_stamp()
        lines = []
        for sc in sysc_num:
            stamp = []
            bytes = []
            sum = 0
            for s,l in self.db.sysc_select(sc, "stamp,aux1"):
                sum += l 
                stamp.append(s - first_stamp)
                bytes.append(sum)
            lines.append(ax.plot(stamp, bytes, linewidth=0.3))
        
        # plot legend
        ax.legend(lines, syscalls, loc=(1.0,0.1), numpoints=1)
        legends = ax.get_legend()
        legends.draw_frame(False)
        for t in legends.get_texts():
            t.set_size("small")
        
        self.ws("Done!\n")
        
        if self.plotformat is not None:
            self.plot_save_figure(fig, "io_bytes")

    # workflow plot using Cytoscape
    def plot_proctree(self):
        self.ws("Generating process tree ... ")
        
        sifFile = open("%s/proctree.sif" % self.datadir, "wb")
        ncsvFile = open("%s/proctree-nodes.csv" % self.datadir, "wb")
        
        ncsvFile.write("id,type,live,res,elapsed,info\n")

        for pid, ppid, live, res, elapsed, cmdline in \
            self.db.proc_fetchall("pid,ppid,live,res,elapsed,cmdline"):
            if live: elapsed = 0 # remove live process, daemons
            sifFile.write("%d call %d\n" % (ppid, pid))
            ncsvFile.write("%d,proc,%d,%d,%f,%s\n" 
                % (pid, live, res, elapsed, cmdline))
        
        sifFile.close()
        ncsvFile.close()
        
        self.ws("Done!\n")

        # prompt user
        if self.prompt:
            self.ws("Process tree graph has been created.\n"
                    "Please use Cytoscape to visualize following data:\n"
                    "  %s/proctree.sif\n"
                    "  %s/proctree-nodes.csv\n"
                    % (self.datadir, self.datadir))
    
    def plot_proctree_dot(self, restrict=[],exclude=[],fillcolor=False):
        #restrict = ["mProjectPP","mDiffFit","mConcatFit","mBgModel",
        #    "mBackground", "mImgtbl","mAdd","mShrink","mJPEG"]
        restrict = ["mDiffFit","mDiff","mFitplane"]
        #exclude = ["bash", "gnome", "python"]
        #exclude = ["make","gnome","python","bash"]
        self.ws("Generating process tree in dot language ...")
        
        colors = COLOR_SERIES_1

        procs_all = self.db.proc_fetchall("pid,ppid,live,elapsed,cmdline")
        procs_kept = []
        if len(restrict) > 0:
            for pid, _, _, _, cmdline in procs_all:
                for p in restrict:
                    if cmdline.find(p) != -1:
                        procs_kept.append(pid)
        else:
            procs_kept = map(lambda x:x[0], procs_all)
        
        procs_exclude = []
        if len(exclude) > 0:
            for pid, _, _, _, cmdline in procs_all:
                for p in exclude:
                    if cmdline.find(p) != -1:
                        procs_exclude.append(pid)
        procs_kept.sort()
        procs_exclude.append(1)
        procs_kept = list_remove(procs_kept, [procs_exclude])
       
        dotFile = open("%s/proctree.dot" % self.datadir, "wb")
        dotFile.write("digraph proctree {\n")
        procs_added = []
        for pid, ppid, live, elapsed, cmdline in procs_all:
            if pid in procs_kept and ppid in procs_kept:
                if live: elapsed = 0 # remove live process, daemons
                dotFile.write("p%d->p%d;\n" % (ppid, pid))
                procs_added.append(pid)
                procs_added.append(ppid)
            elif pid in procs_kept: procs_added.append(pid)
            elif ppid in procs_kept: procs_added.append(ppid)
            self.ws(".") # progress dots

        for pid, _, live, elapsed, cmdline in procs_all:
            if pid in procs_added:
                if live: elapsed = 0
                num, unit = smart_second(elapsed)
                dotFile.write("p%d [label=\"%s@%.2f%s\"];\n" 
                    % (pid, os.path.basename(cmdline.split(" ", 1)[0]),
                       num, unit))
        
        dotFile.write("}\n")
        self.ws("Done!\n")

        # prompt user
        if self.prompt:
            self.ws("Process tree dot flile has been created in"
                    " %s/proctree.dot\n" % self.datadir)

    def plot_workflow(self):
        self.ws("Generating workflow ... ")
        
        NEW = [SYSCALL[i] for i in ["mknod", "mkdir", "creat"]]
        DEL = [SYSCALL[i] for i in ["unlink", "rmdir"]]
        IN = [SYSCALL["read"]]
        OUT = [SYSCALL["write"]]
        TRAN = [SYSCALL[i] for i in ["symlink", "rename", "link"]]
        sifFile = open("%s/workflow.sif" % self.datadir, "wb")
        ncsvFile = open("%s/workflow-nodes.csv" % self.datadir, "wb")
        ecsvFile = open("%s/workflow-edges.csv" % self.datadir, "wb")

        ncsvFile.write("id,type,info\n")
        ecsvFile.write("id,bytes\n")
        
        pids = []
        for sysc in NEW:
            log = self.db.sysc_select(sysc, "pid,fid")
            if log is not None:
                for pid, fid in log:
                    sifFile.write("p%d %s f%d\n" % (pid, SYSCALL[sysc], fid))
                    pids.append(pid)

        for sysc in DEL:
            log = self.db.sysc_select(sysc, "pid,fid")
            if log is not None:
                syscname = SYSCALL[sysc]
                for pid, fid in log:
                    sifFile.write("f%d %s p%d\n" % (fid, syscname, pid))
                    pids.append(pid)

        for sysc in TRAN:
            log = self.db.sysc_select(sysc, "pid,fid,aux1")
            if log is not None:
                syscname = SYSCALL[sysc]
                for pid, fromfid, tofid in log:
                    sifFile.write("f%d %s p%d\n" % (fromfid, syscname, pid))
                    sifFile.write("p%d %s f%d\n" % (pid, syscname, tofid))
                    pids.append(pid)

        for sysc in IN:
            log = self.db.sysc_select_group_by_file(sysc, "pid,fid,SUM(aux1)")
            if log is not None:
                syscname = SYSCALL[sysc]
                for pid, fid, bytes in log:
                    sifFile.write("f%d %s p%d\n" % (fid, syscname, pid))
                    ecsvFile.write("f%d (%s) p%d,%d\n" 
                        % (fid, syscname, pid, bytes))
                    pids.append(pid)
        
        for sysc in OUT:
            log = self.db.sysc_select_group_by_file(sysc, "pid,fid,SUM(aux1)")
            if log is not None:
                syscname = SYSCALL[sysc]
                for pid, fid, bytes in log:
                    sifFile.write("p%d %s f%d\n" % (pid, syscname, fid))
                    ecsvFile.write("p%d (%s) f%d,%d\n" 
                        % (pid, syscname, fid, bytes))
                    pids.append(pid)
        
        for pid in pids:
            ppid = self.db.proc_get_ppid(pid)
            sifFile.write("p%d fork p%d\n" % (ppid, pid))

        # output node attributes
        for item in self.db.proc_fetchall("pid,cmdline"):
            ncsvFile.write("p%d,proc,%s\n" % item)
        for item in self.db.file_fetchall():
            ncsvFile.write("f%d,file,%s\n" % item)

        sifFile.close()
        ncsvFile.close()
        ecsvFile.close()
        
        self.ws("Done!\n")
        
        # prompt user
        if self.prompt:
            self.ws("Workflow DAG has been created.\n"
                    "Please use Cytoscape to visualize following data:\n"
                    "  %s/workflow.sif\n"
                    "  %s/workflow-nodes.csv\n"
                    "  %s/workflow-edges.csv\n"
                    % (self.datadir, self.datadir, self.datadir))

    def plot_workflow_dot(self, restrict=[], exclude=[], hasLabel=True,
        fillColor=True):
        colors = COLOR_SERIES_1
        #exclude = ["bash", "gnome", "python"]
        #exclude = ["make"]
        
        self.ws("Generating workflow in dot language...")
        
        restrict = ["mProjectPP","mDiffFit","mConcatFit","mBgModel",
        #    "mDiff","mFitplane",
            "mBackground", "mImgtbl","mAdd","mShrink","mJPEG"]
#        restrict = ["mDiffFit","mDiff","mFitplane"]
#        restrict = ["mProjectPP"]
#        restrict = ["mAdd", "mShrink", "mJPEG"]
        
        procs_all = self.db.proc_fetchall("pid,ppid,live,elapsed,cmdline")
        procs_name = {}
        for pid, _, _, _, cmdline in procs_all:
            procs_name[pid] = os.path.basename(cmdline.split(" ", 1)[0])
        procs_kept = []
        procs_parent = {}
        procs_color = {}
        if len(restrict) > 0:
            for pid, _, _, _, cmdline in procs_all:
                if fillColor: color_index = 0
                for p in restrict:
                    if cmdline.find(p) != -1: 
                        procs_kept.append(pid)
                        if fillColor and pid not in procs_color.keys(): 
                            procs_color[pid] = colors[color_index]
                    if fillColor:
                        color_index += 1
                        color_index = color_index % len(colors)

            for pid, ppid, _, _, _ in procs_all:
                if pid not in procs_kept:
                    # recursively check if its ancestor in keep list
                    while ppid != 0:
                        if ppid in procs_kept:
                            procs_parent[pid] = ppid
                            break
                        ppid = self.db.proc_get_ppid(ppid)
        else:
            procs_kept = map(lambda x:x[0], procs_all)
            if fillColor:
                for pid in procs_kept: procs_color[pid] = "gray"
        
        procs_exclude = []
        if len(exclude) > 0:
            for pid, _, _, _, cmdline in procs_all:
                for p in exclude:
                    if cmdline.find(p) != -1: procs_exclude.append(pid)
        
        procs_kept.sort()
        procs_exclude.append(1)
        procs_kept = list_remove(procs_kept, [procs_exclude])

        # Get proc info
        sc_creat = SYSCALL["creat"]
        sc_open = SYSCALL["open"]
        sc_close = SYSCALL["close"]
        sc_read = SYSCALL["read"]
        sc_write = SYSCALL["write"]
        
        file_shape = "box"
        proc_shape = "ellipse"
        dotFile = open("%s/workflow.dot" % self.datadir, "wb")
        dotFile.write("digraph workflow {\n")
        procs_added = []
        for fid, path in self.db.file_fetchall():
            fid_used = False
            c_procs = map(lambda x:x[0],
                self.db.sysc_select_group_by_proc_on_fid(sc_creat, fid, "pid"))
            o_procs = map(lambda x:x[0],
                self.db.sysc_select_group_by_proc_on_fid(sc_open, fid, "pid"))
            cl_procs = map(lambda x:x[0],
                self.db.sysc_select_group_by_proc_on_fid(sc_close, fid, "pid"))
            r_procs = map(lambda x:x[0], 
                self.db.sysc_select_group_by_proc_on_fid(sc_read, fid, "pid"))
            w_procs = map(lambda x:x[0], 
                self.db.sysc_select_group_by_proc_on_fid(sc_write, fid, "pid"))

            # read procs
            for pid in r_procs:
                if pid in procs_kept:
                    if hasLabel:
                        bytes = self.db.sysc_sum_by_proc_on_fid(sc_read, 
                            fid, "aux1", pid)
                        rvol, dunit = smart_datasize(bytes)
                        relapsed = self.db.sysc_sum_by_proc_on_fid(sc_read, 
                            fid, "elapsed", pid)
                        thpt, tunit = smart_datasize(bytes/relapsed)
                        dotFile.write("f%d->p%d [arrowhead=inv,"
                            "label=\"%.2f%s@%.2f%s/sec\"]\n" 
                            % (fid, pid, rvol, dunit, thpt, tunit))
                    else:
                        dotFile.write("f%d->p%d [arrowhead=inv]\n"%(fid, pid))
                    attr = ["style=filled","shape=%s" % proc_shape]
                    if fillColor:
                        attr.append("color=%s" % procs_color[pid])
                    if hasLabel:
                        attr.append("label=\"%s\"" % procs_name[pid])
                    dotFile.write("p%d [%s];\n" % (pid, ",".join(attr)))
                    procs_added.append(pid)
                    fid_used = True
                elif pid in procs_parent.keys():
                    ppid = procs_parent[pid]
                    dotFile.write("f%d->p%d [arrowhead=halfinv]\n" 
                        % (fid, ppid))
                    if fillColor:
                        dotFile.write("p%d [style=filled,shape=%s,color=%s];\n"
                            % (ppid, proc_shape, procs_color[ppid]))
                    else:
                        dotFile.write("p%d [style=filled,shape=%s];\n"
                            % (ppid, proc_shape))
                    fid_used = True
            
            # write procs
            for pid in w_procs:
                if pid in procs_kept:
                    if hasLabel:
                        bytes = self.db.sysc_sum_by_proc_on_fid(sc_write, 
                            fid, "aux1", pid)
                        wvol, dunit = smart_datasize(bytes)
                        relapsed = self.db.sysc_sum_by_proc_on_fid(sc_write, 
                            fid, "elapsed", pid)
                        thpt, tunit = smart_datasize(bytes/relapsed)
                        dotFile.write("p%d->f%d [arrowhead=normal,"
                            "label=\"%.2f%s@%.2f%s/sec\"]\n" 
                            % (pid, fid, wvol, dunit, thpt, tunit))
                    else:
                        dotFile.write("p%d->f%d [arrowhead=normal]\n"
                            %(pid, fid))
                    attr = ["style=filled","shape=%s" % proc_shape]
                    if fillColor:
                        attr.append("color=%s" % procs_color[pid])
                    if hasLabel:
                        attr.append("label=\"%s\"" % procs_name[pid])
                    dotFile.write("p%d [%s];\n" % (pid, ",".join(attr)))
                    procs_added.append(pid)
                    fid_used = True
                elif pid in procs_parent.keys():
                    ppid = procs_parent[pid]
                    dotFile.write("p%d->f%d [arrowhead=halfnormal]\n" 
                        % (ppid, fid))
                    if fillColor:
                        dotFile.write("p%d [style=filled,shape=%s,color=%s];\n"
                            % (ppid, proc_shape, procs_color[ppid]))
                    else:
                        dotFile.write("p%d [style=filled,shape=%s];\n"
                            % (ppid, proc_shape))
                    fid_used = True
            
            # open only
            for pid in list_remove(o_procs, [procs_added]):
                if pid in procs_kept:
                    dotFile.write(
                        "f%d->p%d [sytle=doted,arrowhead=oinv];\n" 
                        % (fid, pid))
                    attr = ["style=filled","shape=%s" % proc_shape]
                    if fillColor:
                        attr.append("color=%s" % procs_color[pid])
                    if hasLabel:
                        attr.append("label=\"%s\"" % procs_name[pid])
                    dotFile.write("p%d [%s];\n" % (pid, ",".join(attr)))
                    procs_added.append(pid)
                    fid_used = True
                elif pid in procs_parent.keys():
                    ppid = procs_parent[pid]
                    dotFile.write("f%d->p%d[sytle=doted,arrowhead=olinv];\n"
                        % (fid, ppid))
                    if fillColor:
                        dotFile.write("p%d [style=filled,shape=%s,color=%s];\n"
                            % (ppid, proc_shape, procs_color[ppid]))
                    else:
                        dotFile.write("p%d [style=filled,shape=%s];\n"
                            % (ppid, proc_shape))
                    fid_used = True
            
            # create procs
            for pid in list_remove(c_procs, [procs_added]):
                if pid in procs_kept:
                    dotFile.write("p%d->f%d [arrowhead=onormal];" % (pid, fid))
                    attr = ["style=filled","shape=%s" % proc_shape]
                    if fillColor:
                        attr.append("color=%s" % procs_color[pid])
                    if hasLabel:
                        attr.append("label=\"%s\"" % procs_name[pid])
                    dotFile.write("p%d [%s];\n" % (pid, ",".join(attr)))
                    procs_added.append(pid)
                    fid_used = True
                elif pid in procs_parent.keys():
                    ppid = procs_parent[pid]
                    dotFile.write("p%d->f%d [arrowhead=olnormal]\n" 
                        % (ppid, fid))
                    if fillColor:
                        dotFile.write("p%d [style=filled,shape=%s,color=%s];\n"
                            % (ppid, proc_shape, procs_color[ppid]))
                    else:
                        dotFile.write("p%d [style=filled,shape=%s];\n"
                            % (ppid, proc_shape))
                    fid_used = True
            
            if fid_used:
                attr = ["shape=%s" % file_shape]
                if hasLabel:
                    attr.append("label=\"%s\"" % os.path.basename(path))
                dotFile.write("f%d [%s];\n" % (fid, ",".join(attr)))
            self.ws(".") # progress dots
        
        proc_lines = []
        for pid in procs_added:
            ppid = self.db.proc_get_ppid(pid)
            if ppid in procs_added and (ppid, pid) not in proc_lines:
                dotFile.write("p%d->p%d [style=dotted];\n" % (ppid, pid))
                proc_lines.append((ppid,pid))
                
        dotFile.write("}\n")
        dotFile.close()
        self.ws("Done!\n")
