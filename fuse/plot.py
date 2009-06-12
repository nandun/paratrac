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

import os
import sys
import numpy
import matplotlib.pyplot

from paratrac.common.consts import *
from track import FUSETRAC_SYSCALL
from database import *

__all__ = ["FUSETracPlot"]

class FUSETracPlot():
    def __init__(self, dbfile, opts=None):
        self.datadir = os.path.dirname(dbfile)
        self.db = FUSETracDB(dbfile)
        self.pyplot = matplotlib.pyplot
        self.plotlist = []
        self.usewin = False
        self.prompt = True

        if opts is not None:
            for k, v in opts.__dict__.items():
                if self.__dict__.has_key(k):
                    self.__dict__[k] = v

        self.ws = sys.stdout.write
        self.es = sys.stderr.write

    def show(self):
        if self.usewin:
            self.pyplot.show()

    def plot(self):
        #self.plot_sysc_elapsed_sum(SYSC.keys(), None)
        #self.plot_syscall_count()
        #self.plot_syscall_elapsed()
        if "proctree" in self.plotlist:
            self.plot_proctree()
    
    # Plot routines for each type of figure
    def plot_syscall_count(self):
        fig = self.pyplot.figure()
        ax = fig.add_subplot(1,1,1)
        ax.set_title("System Calls Counts")
        ax.set_xlabel("Time Stamp (seconds)")
        ax.set_ylabel("Operation Count")

        sysc_num = [SYSCALL[s] for s in FUSETRAC_SYSCALL]
        first_stamp = self.db.get_first_stamp()
        lines = []
        for sc in sysc_num:
            stamp = []
            count = []
            sum = 0
            for s in self.db.select_sysc(sc, "stamp"):
                sum += 1
                stamp.append(s[0] - first_stamp)
                count.append(sum)
            lines.append(ax.plot(stamp, count, linewidth=0.3))
        
        # plot legend
        #ax.legend(lines, FUSETRAC_SYSCALL, loc="upper right", numpoints=1)
        #legends = fig.get_legend()
        #legends.draw_frame(False)
        #fig.setp(ltext, fontsize="small")

    def plot_syscall_elapsed(self):
        fig = self.pyplot.figure()
        ax = fig.add_subplot(1,1,1)
        ax.set_title("System Calls Elapsed Time (seconds)")
        ax.set_xlabel("Time Stamp (seconds)")
        ax.set_ylabel("Elapsed Time (seconds)")

        sysc_num = [SYSCALL_FILESYSTEM[s] for s in FUSETRAC_SYSCALL]
        first_stamp = self.db.get_first_stamp()
        lines = []
        for sc in sysc_num:
            stamp = []
            elapsed = []
            for s,e in self.db.select_sysc(sc, "stamp,elapsed"):
                stamp.append(s - first_stamp)
                elapsed.append(e)
            lines.append(ax.plot(stamp, elapsed, linewidth=0.3))
        
        # plot legend
        #self.pyplot.legend(lines, sysc, loc="upper right", numpoints=1)
        #legends = self.pyplot.gca().get_legend()
        #ltext = legends.get_texts()
        #legends.draw_frame(False)
        #self.pyplot.setp(ltext, fontsize="small")

    def plot_sysc_elapsed_sum(self, sysc=[], log=10):
        # plot summary
        self.pyplot.title("Summation of System Calls Elapsed Time (seconds)")
        self.pyplot.xlabel("Time Stamp (seconds)")
        self.pyplot.ylabel("Summation of Elapsed Time (seconds)")

        if log is not None:
            self.pyplot.semilogy(basex=log)
        
        sysc_num = [SYSC[s] for s in sysc]
        first_stamp = self.db.get_first_stamp()
        lines = []
        for sc in sysc_num:
            stamp = []
            esum = []
            sum = 0
            for s,e in self.db.select_sysc(sc, "stamp,elapsed"):
                sum += e
                stamp.append(s - first_stamp)
                esum.append(sum)
            lines.append(self.pyplot.plot(stamp, esum, linewidth=0.3))
        
        # plot legend
        self.pyplot.legend(lines, sysc, loc="upper right", numpoints=1)
        legends = self.pyplot.gca().get_legend()
        ltext = legends.get_texts()
        legends.draw_frame(False)
        self.pyplot.setp(ltext, fontsize="small")

    # workflow plot using Graphviz
    def plot_proctree(self):
        gvFile = open("%s/proctree.gv" % self.datadir, "wb")
        sifFile = open("%s/proctree.sif" % self.datadir, "wb")
        attrFile = open("%s/proctree.attr" % self.datadir, "wb")
        
        gvFile.write("digraph proctree {\n")
        for pid, ppid, cmdline in self.db.procmap_fetchall():
            gvFile.write("\t%d->%d;\n" % (ppid, pid))
            sifFile.write("%d call %d\n" % (ppid, pid))
            attrFile.write("%d = %s\n" % (pid, cmdline))
        gvFile.write("}\n")
        
        gvFile.close()
        sifFile.close()
        attrFile.close()
        
        # prompt user
        if self.prompt:
            self.ws("Process tree graph has been created.\n"
                    "Please use Graphviz to visualize:\n"
                    "  %s/proctree.gv\n"
                    "Please use Cytoscape to visualize:\n"
                    "  %s/proctree.sif\n"
                    "  %s/proctree.attr\n"
                    % (self.datadir, self.datadir, self.datadir))

    def plot_workflow(self):
        gvFile = open("%s/workflow.gv" % self.datadir, "wb")
        sifFile = open("%s/workflow.sif" % self.datadir, "wb")
        attrFile = open("%s/workflow.attr" % self.datadir, "wb")
        
        gvFile.write("digraph proctree {\n")
            
        gvFile.write("}\n")
        
        gvFile.close()
        sifFile.close()
        attFile.close()
