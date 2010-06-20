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
# fs/opts.py
# File system trace specifice options
#

import sys
import os

from modules.opts import Options as CommonOptions

class Options(CommonOptions):
    def __init__(self, argv=None):
        CommonOptions.__init__(self, argv)

    def _check_plot(self, option, opt_str, value, parser):
        if value == "help":
            sys.stdout.write(
"""\
Available plot types:
    
""")
            sys.exit(0)

        parser.values.plot.append(value)

    def _add_default_options(self):
        CommonOptions._add_default_options(self)
    
    def _check_opts_and_args(self):
        if self.opts.plot: 
            if len(self.args) == 1:
                sys.stdout.write("%s: missing data directory\n" 
                    % self.prog)
                sys.exit(1)
            elif not os.path.exists(self.args[1]):
                sys.stdout.write("%s: %s: No such file or directory\n"
                    % (self.prog, self.args[1]))
                sys.exit(1)
            self.opts.path = self.args[1]
