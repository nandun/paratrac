#############################################################################
# ParaTrac: Scalable Tracking Tools for Parallel Applications
# Copyright (C) 2009,2010 Nan Dun <dunnan@yl.is.s.u-tokyo.ac.jp>
#
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
# ParaTrac Version
#

PARATRAC_VERSION = 0.4
PARATRAC_DATE = "2010-05-10"
PARATRAC_AUTHOR = "Nan Dun"
PARATRAC_BUG_REPORT = "dunnan@yl.is.s.u-tokyo.ac.jp"
PARATRAC_WEB = "http://paratrac.googlecode.com/"
PARATRAC_LICENCE = "GNU General Public License"
PARATRAC_LICENCE_VERSION = "3"

PARATRAC_VERSION_STRING = """\
ParaTrac: Scalable Tracking Tools for Parallel Applications 
Version: %s, build %s
Author: %s <%s>
Web: %s

This program is free software: you can redistribute it and/or modify
it under the terms of the %s as published by
the Free Software Foundation, either version %s of the License, or
(at your option) any later version.
""" % (PARATRAC_VERSION, PARATRAC_DATE, PARATRAC_AUTHOR, PARATRAC_BUG_REPORT,
PARATRAC_WEB, PARATRAC_LICENCE, PARATRAC_LICENCE_VERSION)
