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
# modules/verbose.py
# Verbose and user interaction
#

import sys
import threading
import time

def stdout_flush(s):
    sys.stdout.write(s)
    sys.stdout.flush()

def stderr_flush(s):
    sys.stderr.write(s)
    sys.stderr.flush()

class Progress:
    class Tick(threading.Thread):
        def __init__(self, stop, delay, func):
            threading.Thread.__init__(self)
            self.stop = stop
            self.func = func
            self.delay = delay
        
        def run(self):
            while not self.stop.isSet():
                self.func()
                self.stop.wait(self.delay)
            
    def __init__(self, smsg="Start", emsg="End", delay=1, dot='.'):
        self.smsg = smsg
        self.emsg = emsg
        self.delay = delay
        self.dot = dot
        self.stop = threading.Event()
        self.tmr = self.Tick(self.stop, self.delay, self._print_dot)

    def _print_dot(self):
        stdout_flush(self.dot)

    def start(self, msg=None):
        if msg is None: msg = self.smsg
        stdout_flush(msg)
        self.stop.clear()
        self.tmr.start()
    
    def cancel(self, msg=None):
        if msg is not None:
            stderr_flush(msg)
        self.stop.set()

    def end(self, msg=None):
        if msg is None: msg = self.emsg
        self.stop.set()
        stdout_flush(msg)
