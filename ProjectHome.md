ParaTrac is a profilier for parallel/distributed applications. It enables user to investigate their applications by non-intrusive tracing and auto-generated informative profiles.

## Features ##

  * User-level tracing for both file system call and process statistics
  * Fine-grained profiles for application investigation

## Profiling Data-Intensive Workflows ##

Data-intensive workflow are current target application of ParaTrac.

  * Low-level I/O profiles: General application behavior, I/O pattern/prediction, bottlenecks of underlying I/O systems.
  * Workflow profiles: Data-process interactions, processing pattern, inefficient scheduling detection, workflow debugging.

Here is some [exemplary profiles](http://tsukuba000.intrigger.omni.hpcc.jp/~dun/demo/) of data-intensive workflows.

## Future Directions ##

  * Memory reference profiling
  * Profile replaying

## Publications ##
Nan Dun, Kenjiro Taura and Akinori Yonezawa.  _ParaTrac: A Fine-Grained Profiler for Data-Intensive Workflows_. In Proceedings of The ACM International Symposium on High Performance Distributed Computing ([HPDC 2010](http://hpdc2010.eecs.northwestern.edu/)), to appear, Chicago, Illinois, June 2010. ([pdf](http://www.yl.is.s.u-tokyo.ac.jp/~dunnan/pub/hpdc2010.pdf), [Slides](http://www.yl.is.s.u-tokyo.ac.jp/~dunnan/talk/100623.HPDC2010.pdf))

Nan Dun, Kenjiro Taura and Akinori Yonezawa. _Fine-Grained Profiling for Data-Intensive Workflows_. In Proceedings of the 10th IEEE International Symposium on Cluster Computing and the Grid ([CCGrid 2010](http://www.gridbus.org/ccgrid2010/)). Best Poster, Melbourne, Australia, May 2010. ([pdf](http://www.yl.is.s.u-tokyo.ac.jp/~dunnan/pub/ccgrid2010.pdf), [A1 Poster](http://www.yl.is.s.u-tokyo.ac.jp/~dunnan/pub/ccgrid2010PosterA1.pdf))