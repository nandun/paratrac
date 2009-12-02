#!/usr/bin/env python

import os, sys


def err_out(msg):
    msg = '%s\n' % msg
    sys.stderr.write(msg)
    sys.exit(1)

def dict_append(dict, list):
    if dict.has_key(list[0]):
        l = dict[list[0]]
        l.append(list[1])
        dict[list[0]] = l
    else:
        dict[list[0]] = [list[1]]

def sort_f(item1, item2):
    if len(item1) < len(item2):
        return -1
    elif len(item1) > len(item2):
        return 1
    
    if int(item1[1:]) < int(item2[1:]):
        return -1
    elif int(item1[1:]) < int(item2[1:]):
        return 1
    else:
        return 0

def list_uniq(lst):
    tmp_dic = {}
    for itm in lst:
        tmp_dic[itm] = 'v'
    return tmp_dic.keys()


class Dependency:
    def __init__(self, path):
        self.dep_dat = os.path.join(path, 'dependency.dat')
        if not os.access(self.dep_dat, os.F_OK):
            err = 'There is no LOG_DIR/dependency/dependency.dat'
            err_out(err)
        self.read_dep()

    def read_dep(self):
        self.file_in_dic = {}
        self.fileout_dic = {}
        self.modules_dic = {}
        file_l = []
        mod_l = []

        dep_f = open(self.dep_dat, 'r')
        for line in dep_f:
            l = line.split()
            
            if l[0][0] is 'F':
                dict_append(self.file_in_dic, l)
                file_l.append(l[0])
            elif l[0][0] is 'J':
                dict_append(self.fileout_dic, [l[1], l[0]])
                file_l.append(l[1])
            elif l[0][0] is 'M':
                dict_append(self.modules_dic, l)
                mod_l.append(l[0])
        #END_FOR
        dep_f.close()

        file_l.sort(sort_f)
        mod_l.sort(sort_f)

        self.file_lst = list_uniq(file_l)
        self.mod_lst = list_uniq(mod_l)

class Cost:
    def __init__(self, path):
        self.cost_dat = os.path.join(path, 'cost.dat')
        if not os.access(self.cost_dat, os.F_OK):
            err = 'There is no LOG_DIR/dependency/cost.dat'
            error_out(err)
        self.read_cost()

    def read_cost(self):
        self.file_dic = {}
        self.mod_dic = {}
        self.job_dic = {}

        cost_f = open(self.cost_dat, 'r')
        for line in cost_f:
            l = line.split()
            
            if l[0][0] is 'F':
                self.file_dic[l[0]] = int(l[1])
            elif l[0][0] is 'J':
                self.job_dic[l[0]] = float(l[1])
            elif l[0][0] is 'M':
                self.mod_dic[l[0]] = int(l[1])
        #END_FOR
        cost_f.close()


def file_cost(dep, cst):
    input_cst = 0
    output_cst = 0
    module_cst = 0
    intermediate_in = 0
    intermediate_out = 0

    for f in dep.file_lst:
        f_cst = cst.file_dic[f]

        if dep.file_in_dic.has_key(f):
            cnt = len(dep.file_in_dic[f])
            if not dep.fileout_dic.has_key(f):
                input_cst += f_cst * cnt
            else:
                intermediate_in += f_cst * cnt
                intermediate_out += f_cst * len(dep.fileout_dic[f])
    
        elif dep.fileout_dic.has_key(f):
            cnt = len(dep.fileout_dic[f])
            output_cst += f_cst * cnt

    for m in dep.mod_lst:
        m_cst = cst.mod_dic[m]
        m_cnt = len(dep.modules_dic[m])
        
        module_cst += m_cst * m_cnt

    return [input_cst, output_cst, module_cst, intermediate_in, intermediate_out]

def output_fcst(path, list):
    path = os.path.join(path, 'wf_filecost.dat')
    out_f = open(path, 'w')

    str = 'Input Cost  : %d\n' % list[0]
    out_f.write(str)
    #sys.stdout.write(str)
    str = 'Output Cost : %d\n' % list[1]
    out_f.write(str)
    str = 'Module Cost : %d\n\n' % list[2]
    out_f.write(str)
    str = 'Intermediate File in Cost  : %d\n' % list[3]
    out_f.write(str)
    str = 'Intermediate File out Cost : %d\n' % list[4]
    out_f.write(str)
    
    out_f.close()


if __name__ == '__main__':
    log_dir = sys.argv[1]
    dep_dir = os.path.join(log_dir, 'dependency')

    dependency = Dependency(dep_dir)
    cost = Cost(dep_dir)

    cst_l = file_cost(dependency, cost)
    output_fcst(dep_dir, cst_l)
