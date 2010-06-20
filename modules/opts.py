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
# modules/opts.py
# Options and cofigurations 
#

import sys
import os
import optparse
import textwrap

class Options:
    """
    Common options
    """
    def __init__(self, argv=None):
        self.optParser = optparse.OptionParser(formatter=HelpFormatter())
        self._add_default_options()
        self.opts = Values()
        self.args = None    # remaining command arguments
        self.prog = None

        if argv is not None:
            self.parse_argv(argv)

    def _check_path(self, option, opt_str, value, parser):
        if not os.path.exists(value):
            sys.stderr.write("%s: %s: No such file or directory\n" % 
                (self.prog, value))
            sys.exit(1)
        setattr(parser.values, option.dest, os.path.abspath(value))

    def _check_plot(self, option, opt_str, value, parser):
        if value == "help":
            sys.stdout.write("_check_plot funtion\n")
            sys.exit(0)
    
    def _add_default_options(self):
        self.optParser.add_option("-i", "--import", action="callback",
            type="string", dest="import_dir", metavar="PATH", default=None,
            callback=self._check_path, help="import raw logs to database")
        
        self.optParser.add_option("-r", "--report", action="callback",
            type="string", dest="report_dir", metavar="PATH", default=None,
            callback=self._check_path, 
            help="generate report for given data")
        
        self.optParser.add_option("-p", "--plot", action="callback",
            type="string", dest="plot", metavar="TYPE", default=[],
            callback=self._check_plot, 
            help="plot given type of figures, see '-p help' for details")
        
        self.optParser.add_option("-v", "--verbosity", action="store",
            type="int", dest="verbosity", metavar="NUM", default=None,
            help="verbosity level: 0/1/2/3/4/5 (default: 0)")

        self.optParser.add_option("-V", "--version", action="callback",
            callback=self.print_version, help="show version")
    
    def _check_opts_and_args(self):
        return

    def parse_argv(self, argv):
        self.prog = os.path.basename(argv[0])
        opts, self.args = self.optParser.parse_args(argv)
        self.opts.update(opts.__dict__)
        self._check_opts_and_args()
        return self.opts, self.args

    def print_version(self, option, opt, value, parser):
        from version import PARATRAC_VERSION_STRING
        sys.stdout.write(PARATRAC_VERSION_STRING)
        sys.exit(0)

class Values:
    """
    Option values container
    """
    def __init__(self, values=None):
        if isinstance(values, list):
            for k, v in values:
                setattr(self, k, v)
        elif isinstance(values, dict):
            for k, v in values.items():
                setattr(self, k, v)

    def __str__(self):
        return str(self.__dict__)

    def update(self, dict):
        self.__dict__.update(dict)

    def has_attr(self, attr):
        return self.__dict__.has_key(attr)
        
    def set_value(self, attr, val):
        self.__dict__[attr] = val
    
    def get_value(self, attr):
        if self.__dict__.has_key(attr):
            return self.__dict__[attr]
        else:
            return None

    def get_kws(self):
        return self.__dict__

    def items(self):
        return self.__dict__.items()

# OptionParser help string workaround
# adapted from Tim Chase's code from following thread
# http://groups.google.com/group/comp.lang.python/msg/09f28e26af0699b1
class HelpFormatter(optparse.IndentedHelpFormatter):
    def format_description(self, desc):
        if not desc: return ""
        desc_width = self.width - self.current_indent
        indent = " " * self.current_indent
        bits = desc.split('\n')
        formatted_bits = [
            textwrap.fill(bit, desc_width, initial_indent=indent,
                susequent_indent=indent) for bit in bits]
        result = "\n".join(formatted_bits) + "\n"
        return result

    def format_option(self, opt):
        result = []
        opts = self.option_strings[opt]
        opt_width = self.help_position - self.current_indent - 2
        if len(opts) > opt_width:
            opts = "%*s%s\n" % (self.current_indent, "", opts)
            indent_first = self.help_position
        else:
            opts = "%*s%-*s  " % (self.current_indent, "", opt_width, opts)
            indent_first = 0
        result.append(opts)
        if opt.help:
            help_text = self.expand_default(opt)
            help_lines = []
            for para in help_text.split("\n"):
                help_lines.extend(textwrap.wrap(para, self.help_width))
            result.append("%*s%s\n" % (indent_first, "", help_lines[0]))
            result.extend(["%*s%s\n" % (self.help_position, "", line)
                for line in help_lines[1:]])
        elif opts[-1] != "\n":
            result.append("\n")
        return "".join(result)
