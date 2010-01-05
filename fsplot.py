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
# fsplot.py
# Plotting for File System Tracing
#
# Prerequisites:
#   * NetworkX: http://networkx.lanl.gov/
#

import os
import warnings
import Gnuplot
import pydot

import matplotlib.pyplot as pyplot
with warnings.catch_warnings():
    warnings.simplefilter("ignore")
    import networkx as nx

from common import *
import fsdata

class Plot:
    """Plot data chart"""
    def __init__(self, datadir):
        self.datadir = os.path.abspath(datadir)
        self.c = Gnuplot.Gnuplot()
        self.terminal = "png"
        self.db = fsdata.Database("%s/fstrace.db" % self.datadir, False)

    def __del__(self):
        self.db.close()

    def points_chart(self, data, prefix="points_chart", title="points_chart",
        xlabel="x label", ylabel="y label"):
        self.c.reset()
        self.c.title(title)
        self.c.xlabel(xlabel)
        self.c.ylabel(ylabel)
        self.c("set terminal %s" % self.terminal)
        self.c("set output '%s.%s'" % (prefix, self.terminal))
        self.c("set data style points")
        self.c.plot(data)
        return "%s.%s" % (prefix, self.terminal)
    
    def lines_chart(self, data, prefix="lines_chart", title="lines_chart",
        xlabel="x label", ylabel="y label"):
        self.c.reset()
        self.c.title(title)
        self.c.xlabel(xlabel)
        self.c.ylabel(ylabel)
        self.c("set terminal %s" % self.terminal)
        self.c("set output '%s.%s'" % (prefix, self.terminal))
        self.c("set data style linespoints")
        self.c.plot(data)
        return "%s.%s" % (prefix, self.terminal)
    
    def proc_tree(self, path):
        g = ProcTreeDAG(self.datadir)
        g.draw(path)

    def proc_tree_dot(self, path):
        basename = os.path.basename(path)
        suffix = basename.split(".")[-1]
        g = pydot.Dot(graph_type="digraph")
        nmap = {}
        procs_all = self.db.proc_sel("pid,ppid,live,elapsed,cmdline")
        for pid, _, live, elapsed, cmdline in procs_all:
            cmd = os.path.basename(cmdline.split(" ")[0])
            if live: fillcolor = "green"
            else: fillcolor = "white"
            n = pydot.Node("p%s" % pid, label="\"%s\"" % cmd,
            style="filled", fillcolor=fillcolor)
            nmap[pid] = n
            g.add_node(n)
        for pid, ppid, live, elapsed, cmdline in procs_all:
            if pid == 0 or ppid == 0: continue
            g.add_edge(pydot.Edge(nmap[ppid], nmap[pid]))

        if suffix != "dot":
            g.write(path, prog="fdp", format=suffix)
        g.write(os.path.splitext(path)[0] + ".dot", format="raw")
    
    def workflow(self, dir, prefix=None, format="png", prog="dot"):
        """Produce workflow DAG in various formats
        IN:
          dir: directory to store the figure
          prefix: file prefix to appear in file name
          format: image format (png/jpg/gif/ps/pdf/dot)
        OUT:
          the basename of the figure file
        """
        
        g = pydot.Dot(graph_type="digraph")

        FILE_SHAPE = "box"
        PROC_SHAPE = "ellipse"
        SC_CREAT = SYSCALL["creat"]
        SC_OPEN = SYSCALL["open"]
        SC_CLOSE = SYSCALL["close"]
        SC_READ = SYSCALL["read"]
        SC_WRITE = SYSCALL["write"]
        ARROW_READ = 'inv'

        for iid, fid, path in self.db.file_sel("iid,fid,path"):
            procs_creat = map(lambda x:x[0],
                self.db.sysc_sel_procs_by_file(iid, SC_CREAT, fid, "pid"))
            procs_open = map(lambda x:x[0],
                self.db.sysc_sel_procs_by_file(iid, SC_OPEN, fid, "pid"))
            procs_close = map(lambda x:x[0],
                self.db.sysc_sel_procs_by_file(iid, SC_CLOSE, fid, "pid"))
            procs_read = map(lambda x:x[0],
                self.db.sysc_sel_procs_by_file(iid, SC_READ, fid, "pid"))
            procs_write = map(lambda x:x[0],
                self.db.sysc_sel_procs_by_file(iid, SC_WRITE, fid, "pid"))

            # generate read relationships
            for pid in procs_read:
                elapsed, bytes = self.db.io_sum_elapsed_and_bytes(
                    SC_READ, iid, pid, fid)
                thput, tunit = smart_datasize(bytes/elapsed)
                dvol, dunit = smart_datasize(bytes)
                g.add_node(pydot.Node("f%d" % fid, label="%s" % path))
                g.add_node(pydot.Node("p%d" % pid,
                    label="%s" % self.db.proc_cmdline(iid, pid, False)))
                g.add_edge(pydot.Edge("f%d" % fid, "p%d" % pid,
                    arrowhead=ARROW_READ,
                    label='"%.2f%s@%.2f%s/sec"' % (dvol,dunit,thput,tunit)))
        
        if prefix is None or prefix == "":
            filename_prefix = "workflow"
        else:
            filename_prefix = "%s-workflow" % prefix
        basename = "%s.%s" % (filename_prefix, format)

        if format != "dot":
            g.write("%s/%s" % (dir, basename), prog=prog, format=format)
        g.write("%s/%s.dot" % (dir, filename_prefix))
        
        return basename

#
# Graph classes for plotting 
#
class ProcTreeDAG:
    def __init__(self, datadir):
        self.datadir = os.path.abspath(datadir)
        self.db = fsdata.Database("%s/fstrace.db" % self.datadir, False)
        self.g = nx.DiGraph()
        self._load()
    
    def __del__(self):
        self.db.close()

    def _load(self):
        """Generate process tree by traverse database"""
        for pid,ppid,live,cmd in self.db.proc_sel("pid,ppid,live,cmdline"):
            cmd = smart_cmdline(cmd, 0)
            self.g.add_edge(pid, ppid)

    def draw(self, path, format="png", layout="graphviz", prog="neato"):
        #ref: http://networkx.lanl.gov/reference/drawing.html
        assert layout in ["circular", "random", "spectral", "spring", 
            "shell", "graphviz"]
        draw = eval("nx.draw_%s" % layout)
        
        # Suppress warning of using os.popen3 due to old pygraphviz
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            draw(self.g, prog=prog)
        pyplot.savefig(path, format=format)

class WorkflowDAG:
    def __init__(self):
        self.g = nx.DiGraph()

    def addNode(self, node):
        self.g.add_node(node)

    def addEdge(self, src, dst):
        self.g.add_edge(src, dst)

# Drawing auxiliary utilities
def smart_cmdline(cmdline, verbose=0):
    """Shorten the command line based on verbose level"""
    if verbose >= 2:
        return cmdline
    elif verbose == 1:
        return cmdline.split(" ", 1)[0]
    elif verbose <= 0:
        return os.path.basename(cmdline.split(" ", 1)[0])
