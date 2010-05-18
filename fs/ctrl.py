"""FUSE Tracer Control

AUTHOR: Nan Dun <dunnan@yl.is.s.u-tokyo.ac.jp>
LAST UPDATE: Feb. 2, 2010
LICENCE: GNU GPL version 3 <http://www.gnu.org/licenses/>
"""

import os
import pwd
import socket
import signal
import time

from modules.utils import *

FTRAC_PATH_PREFIX = "/tmp"
FTRAC_SIGNAL_INIT = signal.SIGUSR1
FTRAC_CTRL_OK = 0
FTRAC_CTRL_FINISH = 1
FTRAC_CTRL_POLL_STAT = 2
FTRAC_CTRL_FLUSH = 3

class Control:
    def __init__(self, mountpoint):
        self.uid = os.getuid()
        self.user = pwd.getpwuid(self.uid)[0]
        self.mountpoint = os.path.abspath(mountpoint)

        self._connect_ftrac()

    def __del__(self):
        self._disconnect_ftrac()

    def vs(self, msg):
        sys.stderr.write(msg)
    
    def _connect_ftrac(self):
        self.session = "/tmp/ftrac-%s-%u" % (self.user,
            string_hash(self.mountpoint))
        try:
            self.sockpath = "%s/ftrac.sock" % self.session
            self.servsock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.servsock.connect(self.sockpath)
        except:
            self.vs("error: failed to connect to %s\n" % self.sockpath)
            sys.exit(1)

    def _disconnect_ftrac(self):
        self.servsock.send("%d" % FTRAC_CTRL_FINISH)
        self.servsock.close()

    def ftrac_flush_logs(self):
        op = "%d" % FTRAC_CTRL_FLUSH
        self.servsock.send(op)
        self.servsock.recv(SOCKET_BUFSIZE)
