#!/usr/bin/env python

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
import csv
import numpy
import matplotlib.pyplot

from common import *
from database import *

class Plot:
    """Basic plot class"""
    def __init__(self, database):
        self.plt = matplotlib.pyplot
        self.db = database

    def __del__(self):
        self.db.close()

    def show(self):
        self.plt.show()

class FUSETracPlot(Plot):
    """Plot FUSE tracking figures"""
    def plot(self):
        #self.plot_sysc_elapsed(SYSC.keys())
        #self.plot_sysc_elapsed_sum(SYSC.keys(), None)
        self.plot_sysc_count(SYSC.keys(), None)

    def plot_sysc_elapsed(self, sysc=[], log=10):
        # plot summary
        self.plt.title("System Calls Elapsed Time (seconds)")
        self.plt.xlabel("Time Stamp (seconds)")
        self.plt.ylabel("Elapsed Time (seconds)")

        if log is not None:
            self.plt.semilogy(basex=log)
        
        sysc_num = [SYSC[s] for s in sysc]
        first_stamp = self.db.get_first_stamp()
        lines = []
        for sc in sysc_num:
            stamp = []
            elapsed = []
            for s,e in self.db.select_sysc(sc, "stamp,elapsed"):
                stamp.append(s - first_stamp)
                elapsed.append(e)
            lines.append(self.plt.plot(stamp, elapsed, linewidth=0.3))
        
        # plot legend
        self.plt.legend(lines, sysc, loc="upper right", numpoints=1)
        legends = self.plt.gca().get_legend()
        ltext = legends.get_texts()
        legends.draw_frame(False)
        self.plt.setp(ltext, fontsize="small")

    def plot_sysc_elapsed_sum(self, sysc=[], log=10):
        # plot summary
        self.plt.title("Summation of System Calls Elapsed Time (seconds)")
        self.plt.xlabel("Time Stamp (seconds)")
        self.plt.ylabel("Summation of Elapsed Time (seconds)")

        if log is not None:
            self.plt.semilogy(basex=log)
        
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
            lines.append(self.plt.plot(stamp, esum, linewidth=0.3))
        
        # plot legend
        self.plt.legend(lines, sysc, loc="upper right", numpoints=1)
        legends = self.plt.gca().get_legend()
        ltext = legends.get_texts()
        legends.draw_frame(False)
        self.plt.setp(ltext, fontsize="small")
        return

    def plot_sysc_count(self, sysc=[], log=None):
        # plot summary
        self.plt.title("System Calls Counts")
        self.plt.xlabel("Time Stamp (seconds)")
        self.plt.ylabel("Operation Count")

        if log is not None:
            self.plt.semilogy(basex=log)
        
        sysc_num = [SYSC[s] for s in sysc]
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
            lines.append(self.plt.plot(stamp, count, linewidth=0.3))
        
        # plot legend
        self.plt.legend(lines, sysc, loc="upper right", numpoints=1)
        legends = self.plt.gca().get_legend()
        ltext = legends.get_texts()
        legends.draw_frame(False)
        self.plt.setp(ltext, fontsize="small")
        return
        
def main():
    fplot = FUSETracPlot(FUSETracDB(sys.argv[1]))
    fplot.plot()
    fplot.show()
    return 0

if __name__ == "__main__":
    sys.exit(main())
