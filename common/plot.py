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
import matplotlib.widgets

from paratrac.common.consts import *
from paratrac.common.utils import *

__all__ = ["Plot"]

class Plot():
    def __init__(self, dbfile):
        self.datadir = os.path.dirname(dbfile)
        self.db = None
        self.pyplot = matplotlib.pyplot
        self.widgets = matplotlib.widgets
        
        self.usewin = False
        self.prompt = True
        
        self.ws = sys.stdout.write
        self.es = sys.stderr.write
    
    def show(self):
        if self.usewin:
            self.pyplot.show()

    def format_data_float(self, data):
        return "%.9f" % data
