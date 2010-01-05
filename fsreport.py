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
# fsdata.py
# Filesystem Trace Report
#

import sys
import os
import time
import HTMLgen

import common
import fsdata
import fsplot

FUSETRAC_SYSCALL = ["lstat", "fstat", "access", "readlink", "opendir", 
    "readdir", "closedir", "mknod", "mkdir", "symlink", "unlink", "rmdir", 
    "rename", "link", "chmod", "chown", "truncate", "utime", "creat", 
    "open", "statfs", "flush", "close", "fsync", "read", "write"]

class Report():
    def __init__(self, datadir):
        self.datadir = os.path.abspath(datadir)
        self.db = fsdata.Database("%s/fstrace.db" % self.datadir, False)
        self.plot = fsplot.Plot(datadir)
        
        # report root dir
        self.rdir = os.path.abspath("%s/report" % self.datadir)
        if not os.path.exists(self.rdir):
            common.smart_makedirs(self.rdir)
        # figures dir
        self.fdir = os.path.abspath("%s/figures" % self.rdir)
        if not os.path.exists(self.fdir):
            common.smart_makedirs(self.fdir)
        # data dir
        self.ddir = os.path.abspath("%s/data" % self.rdir)
        if not os.path.exists(self.ddir):
            common.smart_makedirs(self.ddir)

        # unit
        self.unit = {}
        self.unit["latency"] = ("usec", 1.0e06)
        self.unit["fsize"] = ("KB", 1.0e03)

    def __del__(self):
        self.db.close()
    
    #
    # HTML report
    #
    def html(self):
        HTMLgen.PRINTECHO = 0   # turn off HTMLgen verbose

        start = (time.localtime(), common.timer())

        SECTION_SIZE = 2
        SUBSECTION_SIZE = SECTION_SIZE + 1

        # heading
        headstr = "ParaTrac Filesystem Profiling Report"
        doc = HTMLgen.SimpleDocument(title=headstr)
        doc.append(HTMLgen.Heading(SECTION_SIZE, headstr))
        
        # summary
        doc.append(HTMLgen.Heading(SUBSECTION_SIZE, "Runtime Summary"))
        doc.append(self.html_summary())

        # system call statistics
        doc.append(HTMLgen.Heading(SUBSECTION_SIZE, "System Call Statistics"))
        for c in self.html_sysc_stat(): doc.append(c)
        
        # io statistics
        doc.append(HTMLgen.Heading(SUBSECTION_SIZE, "Input/Output Statistics"))
        for c in self.html_io_stat(): doc.append(c)

        # process tree
        doc.append(HTMLgen.Heading(SUBSECTION_SIZE, "Process Hierarchy"))
        for c in self.html_proc_tree(): doc.append(c)

        # workflow DAG
        doc.append(HTMLgen.Heading(SUBSECTION_SIZE, "Workflow DAG"))
        for c in self.html_workflow(): doc.append(c)
        
        # footnote
        end = (time.localtime(), common.timer())
        doc.append(self.html_footnote(start, end))
        
        doc.write("%s/fsreport.html" % self.rdir)
        sys.stdout.write("Report in %s/fsreport.html.\n" % self.rdir)
        sys.stdout.flush()
    
    def html_summary(self):
        runtime = {}
        for i, v in self.db.runtime_sel(): runtime[i] = v
        table = HTMLgen.Table(tabletitle=None,
            heading=[],
            border=0, width="100%", cell_padding=2, cell_spacing=0,
            column1_align="left", cell_align="left")
        table.body = []
        table.body.append([HTMLgen.Emphasis("FUSE Tracer"), 
            HTMLgen.Text(": v%s" % runtime["version"])])
        table.body.append([HTMLgen.Emphasis("Hostname"), 
            HTMLgen.Text(": %s" % runtime["hostname"])])
        table.body.append([HTMLgen.Emphasis("Platform"), 
            HTMLgen.Text(": %s" % runtime["platform"])])
        table.body.append([HTMLgen.Emphasis("Command Line"), 
            HTMLgen.Text(": %s" % runtime["cmdline"])])
        table.body.append([HTMLgen.Emphasis("Mountpoint"), 
            HTMLgen.Text(": %s" % runtime["mountpoint"])])
        table.body.append([HTMLgen.Emphasis("User"), 
            HTMLgen.Text(": %s (uid=%s)" % (runtime["user"], runtime["uid"]))])
        table.body.append([HTMLgen.Emphasis("Tracer Instance"), 
            HTMLgen.Text(": iid=%s (pid=%s)" 
            % (runtime["iid"], runtime["pid"]))])
        table.body.append([HTMLgen.Emphasis("Tracing Time"), 
            HTMLgen.Text(": %s --- %s (%.5f seconds)" 
            % ((time.strftime("%a %b %d %Y %H:%M:%S %Z",
               time.localtime(eval(runtime["start"])))),
              (time.strftime("%a %b %d %Y %H:%M:%S %Z",
               time.localtime(eval(runtime["end"])))),
              (eval(runtime["end"]) - eval(runtime["start"]))))])
        datastr = HTMLgen.Text(":")
        datastr.append(HTMLgen.Href("../fstrace.db", " fstrace.db"))
        datastr.append("(size=%s %s)" 
            % (os.path.getsize("%s/fstrace.db" % self.datadir) /
            self.unit["fsize"][1], self.unit["fsize"][0]))
        table.body.append([HTMLgen.Emphasis("Data"), datastr])
        # TODO: environ?
        return table

    def html_sysc_stat(self):
        """make a HTML table for system call count statistics"""
        html_contents = []

        syscalls = FUSETRAC_SYSCALL
        stats = []
        total_cnt = 0
        total_elapsed = 0.0
        unit_str = self.unit["latency"][0]
        unit_scale = self.unit["latency"][1]
        for sc in syscalls:
            sc_num = common.SYSCALL[sc]
            cnt = self.db.sysc_count(sc_num)
            if cnt == 0: continue
            elapsed_sum = self.db.sysc_sum(sc_num,"elapsed")
            elapsed_avg = self.db.sysc_avg(sc_num,"elapsed") * unit_scale
            elapsed_stddev = \
                self.db.sysc_stddev(sc_num, "elapsed") * unit_scale
            total_cnt += cnt
            total_elapsed += elapsed_sum
            
            # plot distribution and cdf
            # ATTENTION: choose stamp as x-coordinates
            distf = self.plot.points_chart(
                data=map(lambda (x,y):(x,y*unit_scale), 
                    self.db.sysc_sel(sc_num, "stamp,elapsed")),
                prefix="%s/dist-%s" % (self.fdir, sc),
                title="Distribution of Latency of %s" % sc,
                xlabel="Tracing Time (seconds)",
                ylabel="Latency (%s)" % unit_str)
            cdff = self.plot.lines_chart(
                data=map(lambda (x,y):(x*unit_scale,y), 
                    self.db.sysc_cdf(sc_num, "elapsed")),
                prefix="%s/cdf-%s" % (self.fdir, sc),
                title="Cumulative Distribution of Latency of %s" % sc,
                xlabel="Latency (%s)" % unit_str,
                ylabel="Percent")
            
            stats.append((sc, cnt, elapsed_sum, elapsed_avg, elapsed_stddev,
                distf, cdff))

        table = HTMLgen.Table(tabletitle=None, #"System Call Count",
            heading=["System Call", "Count", "", "Latency",
                "", "", "", "", ""],
            heading_align="left",
            border=0, width="100%", cell_padding=2, cell_spacing=0,
            column1_align="left", cell_align="left")
        table.body = []
        table.body.append(map(lambda s:HTMLgen.Emphasis(s),
            ["Name", "Sum", "Ratio", 
            "Sum", "Ratio", 
            "Avg (%s)" % self.unit["latency"][0], 
            "StdDev", "Dist", "CDF"]))
        for sc, cnt, e_sum, e_avg, e_stddev, distf, cdff in stats:
            distf = os.path.basename(distf)
            cdff = os.path.basename(cdff)
            table.body.append([HTMLgen.Strong(sc), cnt, float(cnt)/total_cnt,
                e_sum, e_sum/total_elapsed, e_avg, e_stddev, 
                HTMLgen.Href("figures/%s" % distf, 
                    "%s" % distf.split(".")[-1].upper()),
                HTMLgen.Href("figures/%s" % cdff, 
                    "%s" % cdff.split(".")[-1].upper())])
        table.body.append(map(lambda s:HTMLgen.Strong(s),
            ["Total", total_cnt, 1.0, total_elapsed, 1.0, 
            "n/a", "n/a", "n/a", "n/a"]))
        html_contents.append(table)
        
        notes = HTMLgen.Small("*System calls not invoked are ignored.")
        notes = HTMLgen.Emphasis(notes)
        html_contents.append(notes)

        return html_contents
    
    def html_io_stat(self):
        html_contents = []
        
        syscalls = ["read", "write"]
        stats = []
        total_bytes = 0
        for sc in syscalls:
            sc_num = common.SYSCALL[sc]
            bytes = self.db.sysc_sum(sc_num, "aux1")
            if bytes == 0: continue # ignore operation not executed
            len_avg = self.db.sysc_avg(sc_num, "aux1")
            len_stddev = self.db.sysc_stddev(sc_num, "aux1")
            off_avg = self.db.sysc_avg(sc_num, "aux2")
            off_stddev = self.db.sysc_stddev(sc_num, "aux2")
            total_bytes += bytes

            # plot
            sz_cum_data = []
            sz_sum = 0
            for s, sz in self.db.sysc_sel(sc_num, "stamp,aux1"):
                sz_sum += sz
                sz_cum_data.append((s,sz_sum))
                
            sz_cum_fig = self.plot.lines_chart(
                data=sz_cum_data,
                prefix="%s/cum-%s" % (self.fdir, sc),
                title="Summation of Request Size of %s" % sc,
                xlabel="Time (seconds)",
                ylabel="Total Data Size (bytes)")

            len_dist_fig = self.plot.points_chart(
                data=self.db.sysc_sel(sc_num, "stamp,aux1"),
                prefix="%s/len-dist-%s" % (self.fdir, sc),
                title="Distribution of Length of %s" % sc,
                xlabel="Time Stamp (seconds)",
                ylabel="Request Length (bytes)")
            
            len_cdf_fig = self.plot.lines_chart(
                data=self.db.sysc_cdf(sc_num, "aux1"),
                prefix="%s/len-cdf-%s" % (self.fdir, sc),
                title="CDF of Length of %s" % sc,
                xlabel="Request Length (bytes)",
                ylabel="Ratio")
            
            off_dist_cfg = self.plot.points_chart(
                data=self.db.sysc_sel(sc_num, "stamp,aux2"),
                prefix="%s/offset-dist-%s" % (self.fdir, sc),
                title="Distribution of Request Offset of %s" % sc,
                xlabel="Time Stamp (seconds)",
                ylabel="Request Offset (bytes)")

            off_cdf_cfg = self.plot.lines_chart(
                data=self.db.sysc_cdf(sc_num, "aux2"),
                prefix="%s/offset-cdf-%s" % (self.fdir, sc),
                title="CDF of Offset of %s" % sc,
                xlabel="Request Offset (bytes)",
                ylabel="Ratio")

            stats.append((sc, bytes, len_avg, len_stddev, 
                off_avg, off_stddev, sz_cum_fig, len_dist_fig, len_cdf_fig,
                off_dist_cfg, off_cdf_cfg))
        
        table = HTMLgen.Table(tabletitle=None, #"System Call Count",
            heading=["I/O", "Size", "", "", 
                "Length", "", "", "", 
                "Offset", "", "", ""],
            heading_align="left",
            border=0, width="100%", cell_padding=2, cell_spacing=0,
            column1_align="left", cell_align="left")
        
        table.body = []
        table.body.append(map(lambda s:HTMLgen.Emphasis(s),
            ["Name", "Sum", "Ratio", "CUM",
            "Avg", "StdDev", "Dist", "CDF",
            "Avg", "StdDev", "Dist", "CDF"]))

        for sc, bytes, len_avg, len_stddev, off_avg, off_stddev, \
            sz_cum_fig, len_dist_fig, len_cdf_fig, \
            off_dist_fig, off_cdf_fig in stats:
            table.body.append(
                [HTMLgen.Strong(sc), bytes,
                float(bytes)/total_bytes,
                html_fighref(sz_cum_fig),
                len_avg, len_stddev, 
                html_fighref(len_dist_fig), 
                html_fighref(len_cdf_fig),
                off_avg, off_stddev, 
                html_fighref(off_dist_fig),
                html_fighref(off_cdf_fig)])

        html_contents.append(table)

        return html_contents
    
    def html_proc_tree(self):
        """Produce the process tree image and corresponding HTML mark"""
        html_contents = []
        
        self.plot.proc_tree("%s/proc-tree.png" % self.fdir)
        image = HTMLgen.Image(width=400, height=247,
            align="center", border=1,
            alt="Process Tree",
            src="figures/proc-tree.png")
        
        html_contents.append(HTMLgen.Href("figures/proc-tree.png", image))

        return html_contents
    
    def html_workflow(self):
        """Produce the process tree image and corresponding HTML mark"""
        html_contents = []
        basename = self.plot.workflow(self.fdir,format="png")
        text = HTMLgen.Text()
        text.append("The workflow DAG ")
        text.append(HTMLgen.Href("figures/%s" % basename, basename))
        text.append("is generated by graphviz dot from ")
        text.append(HTMLgen.Href("figures/%s.dot" % basename.split(".")[0],
            "%s.dot" % basename.split(".")[0]))
        html_contents.append(text)
        return html_contents

    def html_footnote(self, start, end):
        text = HTMLgen.Small()
        text.append(HTMLgen.Emphasis(
            "Generated at %s, took %.5f seconds, using "
            % (time.strftime("%a %b %d %Y %H:%M:%S %Z", end[0]),
              (end[1] - start[1]))))
        
        text.append(HTMLgen.Emphasis(HTMLgen.Href("../report.conf", 
            "report.conf")))
        text.append(" by ")
        from version import PARATRAC_VERSION, PARATRAC_DATE, PARATRAC_WEB
        text.append(HTMLgen.Emphasis(HTMLgen.Href(PARATRAC_WEB, "ParaTrac")))
        text.append(HTMLgen.Emphasis(" v%s, %s.\n" 
            % (PARATRAC_VERSION, PARATRAC_DATE)))
        para = HTMLgen.Paragraph(text)
        return para

#########################################################################
# Auxiliary Utilities
#########################################################################
def html_fighref(filename, figsdir="figures"):
    """return an HTML href suffix string link to the target figure
    file"""
    basename = os.path.basename(filename)
    return HTMLgen.Href("%s/%s" % (figsdir, basename),
        "%s" % basename.split(".")[-1].upper())

##########################################################################
# Default configure string
# Hard-coded for installation convenience
##########################################################################

PARATRAC_DEFAULT_REPORT_CONFIG_STRING = """\
# ParaTrac default profile report configuration
# 2009/12/30

[report]
# report format: html
format = 'html'

[unit]
# 'sec', 'msec', 'usec'
latency='msec'

[html]
# plot tools, 'gnuplot'
plot = 'gnuplot'
imgtype = 'png'
"""

def main():
    r = Report(sys.argv[1])
    r.html()

main()
