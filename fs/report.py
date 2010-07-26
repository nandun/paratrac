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
# fs/data.py
# Filesystem Trace Report
#

import sys
import os
import time

import version
import modules.utils as utils
import modules.DHTML as DHTML
import data
import plot

FUSETRAC_SYSCALL = ["lstat", "fstat", "access", "readlink", "opendir", 
    "readdir", "closedir", "mknod", "mkdir", "symlink", "unlink", "rmdir", 
    "rename", "link", "chmod", "chown", "truncate", "utime", "creat", 
    "open", "statfs", "flush", "close", "fsync", "read", "write"]

class Report():
    def __init__(self, dbpath):
        self.datadir = os.path.dirname(dbpath)
        self.db = data.Database(dbpath)
        self.plot = plot.Plot(self.datadir)
        
        # report root dir
        self.rdir = os.path.abspath("%s/report" % self.datadir)
        if not os.path.exists(self.rdir):
            utils.smart_makedirs(self.rdir)
        # figures dir
        self.fdir = os.path.abspath("%s/figures" % self.rdir)
        if not os.path.exists(self.fdir):
            utils.smart_makedirs(self.fdir)
        # tables dir
        self.tdir = os.path.abspath("%s/tables" % self.rdir)
        if not os.path.exists(self.tdir):
            utils.smart_makedirs(self.tdir)
        # data dir
        self.ddir = os.path.abspath("%s/data" % self.rdir)
        if not os.path.exists(self.ddir):
            utils.smart_makedirs(self.ddir)

        # unit
        self.unit = {}
        self.unit["latency"] = ("usec", 1.0e06)
        self.unit["fsize"] = ("KB", 1.0e03)

    def __del__(self):
        self.db.close()
    
    def runtime_stats(self):
        runtime = {}
        for k, v in self.db.runtime_values():
            runtime[k] = v
        return runtime

    def sysc_stats(self, plot=False):
        syscalls = FUSETRAC_SYSCALL
        stats = []
        total_cnt = 0
        total_elapsed = 0.0
        unit_str = self.unit["latency"][0]
        unit_scale = self.unit["latency"][1]
        
        cdff = "N/A"
        distf = "N/A"
        for sc in syscalls:
            sc_num = utils.SYSCALL[sc]
            cnt = self.db.sysc_count(sc_num)
            if cnt == 0: continue
            elapsed_sum = self.db.sysc_sum(sc_num,"elapsed")
            elapsed_avg = self.db.sysc_avg(sc_num,"elapsed") * unit_scale
            elapsed_stddev = \
                self.db.sysc_std(sc_num, "elapsed") * unit_scale
            total_cnt += cnt
            total_elapsed += elapsed_sum
            
            if plot:
                distf = self.plot.points_chart(
                    data=map(lambda (x,y):(x,y*unit_scale), 
                        self.db.sysc_sel(sc_num, "stamp,elapsed")),
                    prefix="%s/dist-%s" % (self.fdir, sc),
                    title="Distribution of Latency of %s" % sc,
                    xlabel="Tracing Time (seconds)",
                    ylabel="Latency (%s)" % unit_str)

            if plot:
                cdff = self.plot.lines_chart(
                    data=map(lambda (x,y):(x*unit_scale,y), 
                        self.db.sysc_cdf(sc_num, "elapsed")),
                    prefix="%s/cdf-%s" % (self.fdir, sc),
                    title="Cumulative Distribution of Latency of %s" % sc,
                    xlabel="Latency (%s)" % unit_str,
                    ylabel="Percent")
            
            stats.append((sc, cnt, elapsed_sum, elapsed_avg, elapsed_stddev,
                distf, cdff))

        return stats, total_cnt, total_elapsed

    def io_stats(self, plot=False):
        syscalls = ["read", "write"]
        stats = []
        total_bytes = 0
        
        sz_cum_fig = None
        len_dist_fig = None
        len_cdf_fig = None
        off_dist_cfg = None
        off_cdf_cfg = None
        for sc in syscalls:
            sc_num = utils.SYSCALL[sc]
            bytes = self.db.sysc_sum(sc_num, "aux1")
            if bytes == 0: continue # ignore operation not executed
            len_avg = self.db.sysc_avg(sc_num, "aux1")
            len_std = self.db.sysc_std(sc_num, "aux1")
            off_avg = self.db.sysc_avg(sc_num, "aux2")
            off_std = self.db.sysc_std(sc_num, "aux2")
            total_bytes += bytes

            if plot:
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
                

            stats.append((sc, bytes, len_avg, len_std, 
                off_avg, off_std, sz_cum_fig, len_dist_fig, len_cdf_fig,
                off_dist_cfg, off_cdf_cfg))

        return stats, total_bytes

    def proc_stats(self):
        stats = []
        stats.append((
            self.db.proc_sum("elapsed"), self.db.proc_avg("elapsed"),
            self.db.proc_std("elapsed"),
            self.db.proc_sum("utime"), self.db.proc_avg("utime"),
            self.db.proc_std("utime"),
            self.db.proc_sum("stime"), self.db.proc_avg("stime"),
            self.db.proc_std("stime")
            ))
        
        return stats

class HTMLReport(Report):
    def __init__(self, dbpath):
        Report.__init__(self, dbpath)
        
        # html constants
        self.INDEX_FILE = "index.html"
        self.NAVI_FILE = "navi.html"
        self.MAIN_FILE = "index.html"
        self.CSS_FILE = "style.css"
        self.TITLE = "ParaTrac Profiling Report"
        self.TITLE_SIZE = 1
        self.SECTION_SIZE = self.TITLE_SIZE + 1
        self.SUBSECTION_SIZE = self.SECTION_SIZE + 1
        self.SIDEBAR_SIZE = 20
        self.LINK_ATTRS = {"rel":"stylesheet", "type":"text/css", 
            "href":"%s" % self.CSS_FILE}
    
    def write(self):
        self.start = utils.timer2()
        
        self.css_file()
        self.main_page()

    def main_page(self):
        doc = DHTML.HTMLDocument()

        # head
        head = doc.makeHead(title=self.TITLE)
        head.appendChild(doc.tag("link", attrs=self.LINK_ATTRS))
        doc.add(head)
        
        # body
        body = doc.tag("body")
        doc.add(body)
        
        body.appendChild(doc.H(self.TITLE_SIZE, value=self.TITLE))
        
        # runtime summary
        body.appendChild(doc.H(self.SECTION_SIZE, "Runtime Summary"))
        runtime = self.runtime_stats()
        rows = []
        rows.append(["ParaTrac", "v%s" % runtime["version"]])
        rows.append(["Platform", "%s" % runtime["platform"]])
        rows.append(["Mountpoint", "%s" % runtime["mountpoint"]])
        rows.append(
            ["Elapsed", "%s --- %s (%.2f seconds)" 
            % ((time.strftime("%a %b %d %Y %H:%M:%S %Z",
               time.localtime(eval(runtime["start"])))),
              (time.strftime("%a %b %d %Y %H:%M:%S %Z",
               time.localtime(eval(runtime["end"])))),
              (eval(runtime["end"]) - eval(runtime["start"])))])
        rows.append(["User","%s (%s)" % (runtime["user"], runtime["uid"])])
        rows.append(["Command", "%s" % runtime["cmdline"]])
        rows.append(["Data", doc.HREF("trace.sqlite", "../trace.sqlite")])
        body.appendChild(doc.table([], rows))
        
        # system call statistics
        body.appendChild(doc.H(self.SECTION_SIZE, "System Call Statistics"))
        rows = []
        stats, total_cnt, total_elapsed = self.sysc_stats(True)
        for sc, cnt, e_sum, e_avg, e_stddev, distf, cdff in stats:
            base = os.path.basename(distf)
            distfref = doc.HREF(doc.IMG("figures/%s" % base, 
                attrs={"class":"thumbnail"}), "figures/%s" % base)
            base = os.path.basename(cdff)
            cdffref = doc.HREF(doc.IMG("figures/%s" % base, 
                attrs={"class":"thumbnail"}), "figures/%s" % base)
            rows.append([sc, cnt, round(float(cnt)/total_cnt, 5),
                round(e_sum, 5), round(e_sum/total_elapsed, 5), 
                round(e_avg, 5), round(e_stddev, 5), 
                distfref, cdffref])
        body.appendChild(doc.table([
            ("Syscall", "Count:Sum", "Ratio", "Latency:Sum",
            "Ratio", "Avg", "Std", "Dist", "CDF")], rows))

        notes = doc.tag("p", value="*System calls not invoked are ignored.",
            attrs={"class":"notes"})
        body.appendChild(notes)
       
        # io statistics
        body.appendChild(doc.H(self.SECTION_SIZE, "I/O Statistics"))
        rows = []
        stats, total_bytes = self.io_stats(True)
        for sc, byts, len_avg, len_std, off_avg, off_std, \
            sz_cum_fig, len_dist_fig, len_cdf_fig, \
            off_dist_fig, off_cdf_fig in stats:
            base = os.path.basename(sz_cum_fig)
            sz_cum_fig = doc.HREF(doc.IMG("figures/%s" % base, 
                attrs={"class":"thumbnail"}), "figures/%s" % base)
            base = os.path.basename(len_dist_fig)
            len_dist_fig = doc.HREF(doc.IMG("figures/%s" % base, 
                attrs={"class":"thumbnail"}), "figures/%s" % base)
            base = os.path.basename(len_cdf_fig)
            len_cdf_fig = doc.HREF(doc.IMG("figures/%s" % base, 
                attrs={"class":"thumbnail"}), "figures/%s" % base)
            base = os.path.basename(off_dist_fig)
            off_dist_fig = doc.HREF(doc.IMG("figures/%s" % base, 
                attrs={"class":"thumbnail"}), "figures/%s" % base)
            base = os.path.basename(off_cdf_fig)
            off_cdf_fig = doc.HREF(doc.IMG("figures/%s" % base, 
                attrs={"class":"thumbnail"}), "figures/%s" % base)
            rows.append([sc, byts, round(float(byts)/total_bytes, 5),
                sz_cum_fig, round(len_avg, 5), round(len_std, 5), len_dist_fig,
                len_cdf_fig, round(off_avg, 5), round(off_std, 5), 
                off_dist_fig, off_cdf_fig])
        body.appendChild(doc.table([("Syscall", "Bytes:Sum", "Ratio", "CUM",
            "Length:Avg", "Std", "Dist", "CDF", 
            "Offset:Avg", "StdDev", "Dist", "CDF")], rows))

        # process statistics
        body.appendChild(doc.H(self.SECTION_SIZE, "Process Statistics"))
        rows = []
        for e_sum, e_avg, e_std, ut_sum, ut_avg, ut_std, \
            st_sum, st_avg, st_std in self.proc_stats():
            rows.append(["All", 
                round(e_sum, 5), round(e_avg, 5), round(e_std, 5), 
                round(ut_sum, 5), round(ut_avg, 5), round(ut_std, 5), 
                round(st_sum, 5), round(st_avg, 5), round(st_std, 5)])
        body.appendChild(doc.table([("Proc", "Elapsed:Sum", "Avg", "Std",
            "utime:Sum", "Avg", "Std", "stime:Sum", "Avg", "Std")],
            rows))
        
        # workflow
        body.appendChild(doc.H(self.SECTION_SIZE, "Workflow Statistics"))
        rows = []
        g = self.plot.workflow("%s/%s" % (self.fdir, "workflow.%s" % "png"))
        n_files, n_procs = g.nodes_count()
        d_avg, d_Cd_avg, d_Cb_avg, d_Cc_avg = g.degree_stat()
        figref = doc.HREF(doc.IMG("figures/workflow.png", 
            attrs={"class":"thumbnail"}), "figures/workflow.png")
        rows.append([n_files+n_procs, n_procs, n_files, figref,
            round(d_avg, 5), 
            round(d_Cd_avg, 5), round(d_Cb_avg, 5), round(d_Cc_avg, 5)])
        body.appendChild(doc.table([("Total", "Procs", "Files", "DAG",
            "Degree:Avg", "Centrality", "Betweeness", "Closeness")],
            rows))
        
        # footnote
        self.end = utils.timer2()
        pNode = doc.tag("p", 
            value="Generated at %s, took %.2f seconds by "
            % (time.strftime("%a %b %d %Y %H:%M:%S %Z", self.end[0]),
              (self.end[1] - self.start[1])),
            attrs={"class":"footnote"})
        pNode.appendChild(doc.HREF("ParaTrac", version.PARATRAC_WEB))
        pNode.appendChild(doc.TEXT(" (v%s, build %s)." 
            % (version.PARATRAC_VERSION, version.PARATRAC_DATE)))
        body.appendChild(pNode)
        
        htmlFile = open("%s/%s" % (self.rdir, self.MAIN_FILE), "w")
        doc.write(htmlFile)
        htmlFile.close()
    
    def css_file(self):
        cssFile = open("%s/%s" % (self.rdir, self.CSS_FILE), "w")
        cssFile.write(PARATRAC_DEFAULT_CSS_STYLE_STRING)
        cssFile.close()
    
    def fig_href(self, doc, filename, figsdir="figures"):
        basename = os.path.basename(filename)
        return doc.HREF("%s" % basename.split(".")[-1].upper(),
            "%s/%s" % (figsdir, basename))

    #
    # HTML report
    #
    def html_2(self):
        HTMLgen.PRINTECHO = 0   # turn off HTMLgen verbose

        start = (time.localtime(), timer())

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
        doc.append(HTMLgen.Heading(SUBSECTION_SIZE, "Process Statistics"))
        for c in self.html_proc_stat(): doc.append(c)

        # workflow DAG
        doc.append(HTMLgen.Heading(SUBSECTION_SIZE, "Workflow Statistics"))
        for c in self.html_workflow_stat(): doc.append(c)
        
        # footnote
        end = (time.localtime(), timer())
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
        try:
            table.body.append([HTMLgen.Emphasis("Tracing Time"), 
                HTMLgen.Text(": %s --- %s (%.5f seconds)" 
                % ((time.strftime("%a %b %d %Y %H:%M:%S %Z",
                   time.localtime(eval(runtime["start"])))),
                  (time.strftime("%a %b %d %Y %H:%M:%S %Z",
                   time.localtime(eval(runtime["end"])))),
                  (eval(runtime["end"]) - eval(runtime["start"]))))])
        except KeyError:
            pass
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
            sc_num = SYSCALL[sc]
            cnt = self.db.sysc_count(sc_num)
            if cnt == 0: continue
            elapsed_sum = self.db.sysc_sum(sc_num,"elapsed")
            elapsed_avg = self.db.sysc_avg(sc_num,"elapsed") * unit_scale
            elapsed_stddev = \
                self.db.sysc_std(sc_num, "elapsed") * unit_scale
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
            table.body.append([HTMLgen.Strong(sc), cnt, float(cnt)/total_cnt,
                e_sum, e_sum/total_elapsed, e_avg, e_stddev, 
                html_fighref(distf),
                html_fighref(cdff)])
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
            sc_num = SYSCALL[sc]
            bytes = self.db.sysc_sum(sc_num, "aux1")
            if bytes == 0: continue # ignore operation not executed
            len_avg = self.db.sysc_avg(sc_num, "aux1")
            len_stddev = self.db.sysc_std(sc_num, "aux1")
            off_avg = self.db.sysc_avg(sc_num, "aux2")
            off_stddev = self.db.sysc_std(sc_num, "aux2")
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
    
    def html_proc_stat(self):
        """Produce the process tree image and corresponding HTML mark"""
        html_contents = []
        
        #TODO:REFINE
        # Better way to select processes with different I/O operations

        # Process statistics
        total_procs = self.db.procs()
        live_procs = self.db.procs(live=1)
        dead_procs = self.db.procs(live=0)

        creat_procs = self.db.procs(sysc="creat")
        open_procs = self.db.procs(sysc="open")
        close_procs = self.db.procs(sysc="close")
        read_procs = self.db.procs(sysc="read")
        write_procs = self.db.procs(sysc="write")

        creat_only_procs = list_remove(creat_procs, [read_procs, write_procs])
        open_only_procs = list_remove(open_procs, [read_procs, write_procs])
        
        # create and open procs
        creat_and_open_procs = list_intersect([creat_procs, open_procs])
        read_only_procs = list_remove(read_procs, [creat_only_procs, 
            open_only_procs, write_procs])
        write_only_procs = list_remove(write_procs, 
            [creat_only_procs, open_only_procs, read_procs])
        read_write_procs = list_intersect([read_procs, write_procs])
        
        # shared read/write procs, inheri file descriptor
        shared_read_procs = list_remove(read_procs, 
            [creat_procs, open_procs])
        shared_write_procs = list_remove(write_procs, 
            [creat_procs, open_procs])
        shared_read_write_procs = list_intersect([shared_read_procs, 
            shared_write_procs])
        
        life_sum, life_avg, life_std = self.db.proc_stat("elapsed", live=0)
        life_sum, life_sum_unit = smart_usec(life_sum)
        life_avg, life_avg_unit = smart_usec(life_avg)
        life_std, life_std_unit = smart_usec(life_avg)
        life_scale = eval(life_avg_unit.upper())
        
        distf = self.plot.points_chart(
            data=map(lambda (x,y):(x,y/life_scale), 
                self.db.proc_sel("btime,elapsed", live=0)),
            prefix="%s/dist-proclife" % self.fdir,
            title="Distribution of Process Life Time",
            xlabel="Process Begin Time (seconds)",
            ylabel="Elapsed (%s)" % life_avg_unit)
        
        cdff = self.plot.lines_chart(
            data=map(lambda (x,y):(x/life_scale,y), 
                self.db.proc_cdf("elapsed", live=0)),
            prefix="%s/cdf-proclife" % self.fdir,
            title="Cumulative Distribution of Process Lifetime",
            xlabel="Elapsed (%s)" % life_avg_unit,
            ylabel="Percent")

        pt_basename = "proc-tree.png"
        self.plot.proc_tree("%s/%s" % (self.fdir, pt_basename))

        # Summary Table
        table = HTMLgen.Table(tabletitle=None, # Process Tree Table
            heading=["Status", "", "",
                "I/O", "", "", "",
                "Life Time*", "", "", "", "",
                "Structure"],
            heading_align="left",
            border=0, width="100%", cell_padding=2, cell_spacing=0,
            column1_align="left", cell_align="left")
        table.body = []
        table.body.append(map(lambda s:HTMLgen.Emphasis(s),
            ["Total", "Alive", "Dead",
             "R-Only", "W-Only", "R/W", "C-Only",
             "Sum (%s)" % life_sum_unit, 
             "Avg (%s)" % life_avg_unit, 
             "StdDev (%s)" % life_std_unit, 
             "Dist", "CDF", 
             "DAG"]))
        table.body.append([
            len(total_procs),       # Total
            len(live_procs),        # Alive
            len(dead_procs),        # Dead
            len(read_only_procs),   # Read
            len(write_only_procs),  # Write
            len(read_write_procs),  # Read and write
            len(creat_only_procs) + len(open_only_procs),   # No I/O
            life_sum,       # Sum
            life_avg,       # Avg
            life_std,    # StdDev
            html_fighref(distf), # Dist
            html_fighref(cdff), # CDF
            html_fighref(pt_basename)  # DAG
            ])
        html_contents.append(table)
        notes = HTMLgen.Small("*Only terminated processes.")
        notes = HTMLgen.Emphasis(notes)
        html_contents.append(notes)

        return html_contents
    
    def html_workflow_stat(self, format="svg"):
        """Produce the process tree image and corresponding HTML mark"""
        html_contents = []

        wf_basename = "workflow.%s" % format

        # Fetch networkx DAG to performe graph analysis
        g = self.plot.workflow("%s/%s" % (self.fdir, wf_basename))

        n_files, n_procs = g.nodes_count()
        
        d_avg, d_Cd_avg, d_Cb_avg, d_Cc_avg = g.degree_stat()
        
        tAll, tProc, tFile = self.html_causal_order(g)
        
        table = HTMLgen.Table(tabletitle=None, # Process Tree Table
            heading=["Summary", "", "", "",
                "Degree*", "", "", ""],
            heading_align="left",
            border=0, width="100%", cell_padding=2, cell_spacing=0,
            column1_align="left", cell_align="left")
        table.body = []
        table.body.append(map(lambda s:HTMLgen.Emphasis(s),
            ["Total", "Procs", "Files", "DAG", 
             "Avg", 
             html_sub("Avg C", "D"),  # Degree Centrality
             html_sub("Avg C", "B"),
             html_sub("Avg C", "C")]))

        table.body.append([
            html_tabhref(tAll, "%d" % (n_procs+n_files)), # Total
            html_tabhref(tProc, "%d" % n_procs),    # Processes
            html_tabhref(tFile, "%d" % n_files),    # Files
            html_fighref(wf_basename),  # DAG
            "%.2f" % d_avg,     # Avg-Degree
            "%.2f" % d_Cd_avg,  # Degree Centrality
            "%.2f" % d_Cb_avg,  # Betweeness
            "%.2f" % d_Cc_avg,  # Closeness
            ])
        html_contents.append(table)

        return html_contents
    
    def html_causal_order(self, wfg):
        """Generate process and files causal order lists"""
        
        SECTION_SIZE = 3
        headstr = "Processes and Files in Topological/Casual Order"
        docAll = HTMLgen.SimpleDocument(title=headstr)
        docAll.append(HTMLgen.Heading(SECTION_SIZE, headstr))
        
        headstr = "Processes in Topological/Casual Order"
        docProc = HTMLgen.SimpleDocument(title=headstr)
        docProc.append(HTMLgen.Heading(SECTION_SIZE, headstr))
        
        headstr = "Files in Topological/Casual Order"
        docFile = HTMLgen.SimpleDocument(title="Files Casual Order")
        docFile.append(HTMLgen.Heading(SECTION_SIZE, headstr))
        
        tableAll = HTMLgen.Table(tabletitle=None,
            heading=["Index","ID","Description"],
            heading_align="left",
            border=1, width="100%", cell_padding=1, cell_spacing=0,
            column1_align="left", cell_align="left")
        
        tableProc = HTMLgen.Table(tabletitle=None,
            heading=["Index","Pid","Command line"],
            heading_align="left",
            border=1, width="100%", cell_padding=1, cell_spacing=0,
            column1_align="left", cell_align="left")
        
        tableFile = HTMLgen.Table(tabletitle=None,
            heading=["Index","ID","Path"],
            heading_align="left",
            border=1, width="100%", cell_padding=1, cell_spacing=0,
            column1_align="left", cell_align="left")

        tableAll.body = []
        tableProc.body = []
        tableFile.body = []
        
        nAll = nProc = nFile = 0
        for n in wfg.causal_order():
            type, id = n[0], int(n[1:])
            nAll += 1
            if type == "p":
                nProc += 1
                desc = "%s" % self.db.proc_sel("cmdline", pid=id)[0]
                tableProc.body.append([nProc, id, desc])
                tableAll.body.append([nAll, id, desc])
            if type == "f":
                nFile += 1
                desc = "%s" % self.db.file_sel("path", fid=id)[0]
                tableFile.body.append([nFile, id, desc])
                f = HTMLgen.Font(color=HTMLcolors.RED1)
                tableAll.body.append([nAll, f(id), f(desc)])
        
        docAll.append(tableAll)
        docProc.append(tableProc)
        docFile.append(tableFile)

        docAllpath = "%s/causal-order-all.html" % self.tdir
        docProcpath = "%s/causal-order-procs.html" % self.tdir
        docFilepath = "%s/causal-order-files.html" % self.tdir
        docAll.write(docAllpath)
        docProc.write(docProcpath)
        docFile.write(docFilepath)
        return docAllpath, docProcpath, docFilepath

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
    return DHTML.Href("%s/%s" % (figsdir, basename),
        "%s" % basename.split(".")[-1].upper())

def html_tabhref(filename, text="HTML", tabsdir="tables"):
    """return an HTML href suffix string link to the target table 
    file"""
    basename = os.path.basename(filename)
    return HTMLgen.Href("%s/%s" % (tabsdir, basename), text)

def html_sub(maintxt, subtxt):
    t = HTMLgen.Text(maintxt)
    t.append(HTMLgen.Sub(subtxt))
    return t

##########################################################################
# Default configure string
# Hard-coded for installation convenience
##########################################################################

PARATRAC_DEFAULT_REPORT_CONFIG_STRING = """\
# ParaTrac default profile report configuration
# 2010/01/25

[report]
section = ["sysc", "io", "proc", "workflow"]
# report format: html
format = 'html'

# 'sec', 'msec', 'usec'
latency='msec'

# plot tools, 'gnuplot'
plot = 'gnuplot'
imgtype = 'png'
"""

# Be careful about browser compatibility!
# CSS selector:
#   IE: NODE.class
#   Firefox: NODE[class=value]
PARATRAC_DEFAULT_CSS_STYLE_STRING = """\
H1 {
font-family: Arial;
}

H2 {
font-family: Arial;
background-color: #C3FDB8;
}

P[class=footnote] {
font-family: Times New Roman;
font-size: 12pt;
font-style: italic;
display: block;
background-color: #C3FDB8;
}

P[class=notes] {
font-family: Times New Roman;
font-size: 12pt;
font-style: italic;
}

IMG[class=thumbnail] {
border-style: outset;
border-width: 1px;
width: 20px;
height: 20px;
}

IMG[class=demo] {
display: block;
margin-left: auto;
margin-right: auto;
width: 800px;
height: 600px;
vertical-align: middle;
border-style: outset;
border-width: 0px;
}

TABLE {
font-family: "Lucida Sans Unicode", "Lucida Grande", Sans-Serif;
font-size: 14px;
border-collapse: collapse;
text-align: left;
width: 100%;
}

TH {
font-size: 14px;
font-weight: bold;
padding: 6px 8px;
border-bottom: 2px solid;
}

TD {
padding: 2px 2px;
}

UL[class=navi] {
font-family: "Lucida Sans Unicode", "Lucida Grande", Sans-Serif;
font-size: 14px;
cursor: pointer;
}
"""
