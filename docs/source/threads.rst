====================================
Concurrency, locking, and versioning
====================================

Concurrency
===========

The ``FileIndex`` object is "stateless" and should be share-able between
threads.

A ``Reader`` object (which underlies the ``Searcher`` object) wraps open files and often
individual methods rely on consistent file cursor positions (e.g. they do two
``file.read()``\ s in a row, so if another thread moves the cursor between the two
read calls Bad Things would happen). You should use one Reader/Searcher per
thread in your code.

Readers/Searchers tend to cache information (such as field caches for sorting),
so if you can share one across multiple search requests, it's a big performance
win.


Locking
=======

Only one thread/process can write to an index at a time. When you open a writer,
it locks the index. If you try to open a writer on the same index in another
thread/process, it will raise ``whoosh.store.LockError``.

In a multi-threaded or multi-process environment your code needs to be aware
that opening a writer may raise this exception if a writer is already open.
Whoosh includes a couple of example implementations
(:class:`whoosh.writing.AsyncWriter` and :class:`whoosh.writing.BufferedWriter`)
of ways to work around the write lock.

While the writer is open and during the commit, **the index is still available
for reading**. Existing readers are unaffected and new readers can open the
current index normally.


Lock files
----------

Locking the index is accomplished by acquiring an exclusive file lock on the
``<indexname>_WRITELOCK`` file in the index directory. The file is not deleted
after the file lock is released, so the fact that the file exists **does not**
mean the index is locked.


Versioning
==========

When you open a reader/searcher, the reader represents a view of the **current
version** of the index. If someone writes changes to the index, any readers
that are already open **will not** pick up the changes automatically. A reader
always sees the index as it existed when the reader was opened.

If you are re-using a Searcher across multiple search requests, you can check
whether the Searcher is a view of the latest version of the index using
:meth:`whoosh.searching.Searcher.up_to_date`. If the searcher is not up to date,
you can get an up-to-date copy of the searcher using
:meth:`whoosh.searching.Searcher.refresh`::

    # If 'searcher' is not up-to-date, replace it
    searcher = searcher.refresh()

(If the searcher has the latest version of the index, ``refresh()`` simply
returns it.)

Calling ``Searcher.refresh()`` is more efficient that closing the searcher and
opening a new one, since it will re-use any underlying readers and caches that
haven't changed.



