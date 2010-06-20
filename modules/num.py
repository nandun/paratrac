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
# modules/num.py
# numerical functions
#

import __builtin__
import math

HAVE_NUMPY = False
try:
    import numpy
    HAVE_NUMPY = True
except ImportError:
    HAVE_NUMPY = False

def num_average(alist):
    return sum(alist)/len(alist)

def num_std(alist):
    avg = num_average(alist)
    total = 0.0
    for x in alist:
        total += math.pow((x-avg), 2)
    return math.sqrt(total/len(alist))

if HAVE_NUMPY:
    sum = numpy.sum
    average = numpy.average
    min = numpy.min
    max = numpy.max
    std = numpy.std
else:
    sum = __builtin__.sum
    average = num_average
    min = __builtin__.min
    max = __builtin__.max
    std = num_std
