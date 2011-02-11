===================================
Tips for speeding up batch indexing
===================================


Overview
========

Indexing documents tends to fall into two general patterns: adding documents
one at a time as they are created (as in a web application), and adding a bunch
of documents at once (batch indexing).

The following settings and alternate workflows can make batch indexing faster.


StemmingAnalyzer cache
======================

The stemming analyzer by default uses a least-recently-used (LRU) cache to limit
the amount of memory it uses, to prevent the cache from growing very large if
the analyzer is reused for a long period of time. However, the LRU cache can
slow down indexing by almost 200% compared to a stemming analyzer with an
"unbounded" cache.

When you're indexing in large batches with a one-shot instance of the
analyzer, consider using an unbounded cache::

    w = myindex.writer()
    # Get the analyzer object from a text field
    stem_ana = w.schema["content"].format.analyzer
    # Set the cachesize to -1 to indicate unbounded caching
    stem_ana.cachesize = -1
    # Reset the analyzer to pick up the changed attribute
    stem_ana.clear()
    
    # Use the writer to index documents...


The ``limitmb`` parameter
=========================

The ``limitmb`` parameter to :meth:`whoosh.index.Index.writer` controls the
*maximum* memory (in megabytes) the writer will use for the indexing pool. The
higher the number, the faster indexing will be.

The default value of ``32`` is actually pretty low, considering many people
have multiple gigabytes of RAM these days. Setting it higher can speed up
indexing considerably::

    from whoosh import index
    
    ix = index.open_dir("indexdir")
    writer = ix.writer(limitmb=256)

.. note::
    The actual memory used will be higher than this value because of interpreter
    overhead (up to twice as much!). It is very useful as a tuning parameter,
    but not for trying to exactly control the memory usage of Whoosh.


The ``procs`` parameter
=======================

The ``procs`` parameter to :meth:`whoosh.index.Index.writer` controls the
number of processors the writer will use for the indexing pool (via the
``multiprocessing`` module).

    from whoosh import index
    
    ix = index.open_dir("indexdir")
    writer = ix.writer(procs=4)
    
Note that when you use multiprocessing, the ``limitmb`` parameter controls the
amount of memory used by *each process*, so the actual memory used will be
``limitmb * procs``::

    # Each process will use a limit of 128 MB, for a total of 512 MB
    writer = ix.writer(procs=4, limitmb=128)


MultiSegmentWriter
==================

The ``procs`` parameter causes the default ``FileWriter`` to use multiple
processors to build the pool, but then still uses a single process to merge
the pool into a segment.

You can get much better indexing speed using the MultiSegmentWriter, which
instead of a building the pool in parallel uses entirely separate parallel
writers. The drawback is that instead of creating a single new segment,
``MultiSegmentWriter`` creates a number of new segments equal to the number of
processes you you use. For example, if you use ``procs=4``, the writer will
create four new segments.

So, while ``MultiSegmentWriter`` is much faster than a normal writer, you should
only use it for large batch indexing jobs (and perhaps only for indexing from
scratch). It should not be the only method you use for indexing, because
otherwise the number of segments will increase forever!

To use a ``MultiSegmentWriter``, construct it directly, with your Index as the
first argument::

    from whoosh import index
    from whoosh.filedb.multiproc import MultiSegmentWriter
    
    ix = index.open_dir("indexdir")
    writer = MultiSegmentWriter(ix, procs=4, limitmb=128)


Benchmarks
==========

As a single data point purely to illustrate the possible relative differences
between single processing, a multiprocessing pool, and ``MultiSegmentWriter``,
here are the indexing times for the ``benchmarks/enron.py``, indexing over 1 GB
of text in over 500 000 email messages, using the three different methods on a
Windows machine with ``limitmb=128``::

    Default Writer     procs=1 : 49m
    Default Writer     procs=4 : 32m
    MultiSegmentWriter procs=4 : 13m




