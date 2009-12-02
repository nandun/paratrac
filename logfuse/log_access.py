#!/usr/bin/env python

import os, sys
import time

def err_out(msg):
    msg = '%s\n' % msg
    sys.stderr.write(msg)
    sys.exit(1)

def listup_dir(path, site):
    tmp_l = os.listdir(path)
    list = []
    for itm in tmp_l:
        if itm.find(site, 0) is 0:
            list.append(os.path.join(path, itm))
    return list

def check_access(path_l, site):
    for path in path_l:
        file_l = os.listdir(path)
        for file in file_l:
            file = os.path.join(path, file)
            if not os.access(file, os.R_OK):
                while not os.access(file, os.R_OK):
                    time.sleep(.3)
        print '%s: access checked' % path[path.find(site, 0):]


if __name__ == '__main__':
    log_dir = sys.argv[1]
    site_name = sys.argv[2]

    dir_l = listup_dir(log_dir, site_name)
#    print dir_l
    check_access(dir_l, site_name)
