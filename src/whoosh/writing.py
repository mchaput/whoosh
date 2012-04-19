# Copyright 2007 Matt Chaput. All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#    1. Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#
#    2. Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY MATT CHAPUT ``AS IS'' AND ANY EXPRESS OR
# IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
# EVENT SHALL MATT CHAPUT OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
# OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
# EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
# The views and conclusions contained in the software and documentation are
# those of the authors and should not be interpreted as representing official
# policies, either expressed or implied, of Matt Chaput.

from __future__ import with_statement
import threading
import time
from contextlib import contextmanager

from whoosh.compat import abstractmethod
from whoosh.store import LockError
from whoosh.util import synchronized


# Exceptions

class IndexingError(Exception):
    pass


# Document grouping context manager

@contextmanager
def groupmanager(writer):
    writer.start_group()
    yield
    writer.end_group()


# Base class

class IndexWriter(object):
    """High-level object for writing to an index.

    To get a writer for a particular index, call
    :meth:`~whoosh.index.Index.writer` on the Index object.

    >>> writer = myindex.writer()

    You can use this object as a context manager. If an exception is thrown
    from within the context it calls :meth:`~IndexWriter.cancel` to clean up
    temporary files, otherwise it calls :meth:`~IndexWriter.commit` when the
    context exits.

    >>> with myindex.writer() as w:
    ...     w.add_document(title="First document", content="Hello there.")
    ...     w.add_document(title="Second document", content="This is easy!")
    """

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.cancel()
        else:
            self.commit()

    def group(self):
        """Returns a context manager that calls
        :meth:`~IndexWriter.start_group` and :meth:`~IndexWriter.end_group` for
        you, allowing you to use a ``with`` statement to group hierarchical
        documents::
        
            with myindex.writer() as w:
                with w.group():
                    w.add_document(kind="class", name="Accumulator")
                    w.add_document(kind="method", name="add")
                    w.add_document(kind="method", name="get_result")
                    w.add_document(kind="method", name="close")
                
                with w.group():
                    w.add_document(kind="class", name="Calculator")
                    w.add_document(kind="method", name="add")
                    w.add_document(kind="method", name="multiply")
                    w.add_document(kind="method", name="get_result")
                    w.add_document(kind="method", name="close")
        """

        return groupmanager(self)

    def start_group(self):
        """Start indexing a group of hierarchical documents. The backend should
        ensure that these documents are all added to the same segment::
        
            with myindex.writer() as w:
                w.start_group()
                w.add_document(kind="class", name="Accumulator")
                w.add_document(kind="method", name="add")
                w.add_document(kind="method", name="get_result")
                w.add_document(kind="method", name="close")
                w.end_group()
                
                w.start_group()
                w.add_document(kind="class", name="Calculator")
                w.add_document(kind="method", name="add")
                w.add_document(kind="method", name="multiply")
                w.add_document(kind="method", name="get_result")
                w.add_document(kind="method", name="close")
                w.end_group()
        
        A more convenient way to group documents is to use the
        :meth:`~IndexWriter.group` method and the ``with`` statement.
        """

        pass

    def end_group(self):
        """Finish indexing a group of hierarchical documents. See
        :meth:`~IndexWriter.start_group`.
        """

        pass

    def add_field(self, fieldname, fieldtype, **kwargs):
        """Adds a field to the index's schema.

        :param fieldname: the name of the field to add.
        :param fieldtype: an instantiated :class:`whoosh.fields.FieldType`
            object.
        """

        self.schema.add(fieldname, fieldtype, **kwargs)

    def remove_field(self, fieldname, **kwargs):
        """Removes the named field from the index's schema. Depending on the
        backend implementation, this may or may not actually remove existing
        data for the field from the index. Optimizing the index should always
        clear out existing data for a removed field.
        """

        self.schema.remove(fieldname, **kwargs)

    @abstractmethod
    def reader(self, **kwargs):
        """Returns a reader for the existing index.
        """

        raise NotImplementedError

    def searcher(self, **kwargs):
        from whoosh.searching import Searcher

        return Searcher(self.reader(), **kwargs)

    def delete_by_term(self, fieldname, text, searcher=None):
        """Deletes any documents containing "term" in the "fieldname" field.
        This is useful when you have an indexed field containing a unique ID
        (such as "pathname") for each document.

        :returns: the number of documents deleted.
        """

        from whoosh.query import Term

        q = Term(fieldname, text)
        return self.delete_by_query(q, searcher=searcher)

    def delete_by_query(self, q, searcher=None):
        """Deletes any documents matching a query object.

        :returns: the number of documents deleted.
        """

        if searcher:
            s = searcher
        else:
            s = self.searcher()

        try:
            count = 0
            for docnum in s.docs_for_query(q, for_deletion=True):
                if not self.is_deleted(docnum):
                    self.delete_document(docnum)
                    count += 1
        finally:
            if not searcher:
                s.close()

        return count

    @abstractmethod
    def delete_document(self, docnum, delete=True):
        """Deletes a document by number.
        """
        raise NotImplementedError

    @abstractmethod
    def add_document(self, **fields):
        """The keyword arguments map field names to the values to index/store::

            w = myindex.writer()
            w.add_document(path=u"/a", title=u"First doc", text=u"Hello")
            w.commit()

        Depending on the field type, some fields may take objects other than
        unicode strings. For example, NUMERIC fields take numbers, and DATETIME
        fields take ``datetime.datetime`` objects::

            from datetime import datetime, timedelta
            from whoosh import index
            from whoosh.fields import *

            schema = Schema(date=DATETIME, size=NUMERIC(float), content=TEXT)
            myindex = index.create_in("indexdir", schema)

            w = myindex.writer()
            w.add_document(date=datetime.now(), size=5.5, content=u"Hello")
            w.commit()

        Instead of a single object (i.e., unicode string, number, or datetime),
        you can supply a list or tuple of objects. For unicode strings, this
        bypasses the field's analyzer. For numbers and dates, this lets you add
        multiple values for the given field::

            date1 = datetime.now()
            date2 = datetime(2005, 12, 25)
            date3 = datetime(1999, 1, 1)
            w.add_document(date=[date1, date2, date3], size=[9.5, 10],
                           content=[u"alfa", u"bravo", u"charlie"])

        For fields that are both indexed and stored, you can specify an
        alternate value to store using a keyword argument in the form
        "_stored_<fieldname>". For example, if you have a field named "title"
        and you want to index the text "a b c" but store the text "e f g", use
        keyword arguments like this::

            writer.add_document(title=u"a b c", _stored_title=u"e f g")

        You can boost the weight of all terms in a certain field by specifying
        a ``_<fieldname>_boost`` keyword argument. For example, if you have a
        field named "content", you can double the weight of this document for
        searches in the "content" field like this::

            writer.add_document(content="a b c", _title_boost=2.0)

        You can boost every field at once using the ``_boost`` keyword. For
        example, to boost fields "a" and "b" by 2.0, and field "c" by 3.0::

            writer.add_document(a="alfa", b="bravo", c="charlie",
                                _boost=2.0, _c_boost=3.0)

        Note that some scoring algroithms, including Whoosh's default BM25F,
        do not work with term weights less than 1, so you should generally not
        use a boost factor less than 1.

        See also :meth:`Writer.update_document`.
        """

        raise NotImplementedError

    def _doc_boost(self, fields, default=1.0):
        if "_boost" in fields:
            return float(fields["_boost"])
        else:
            return default

    def _field_boost(self, fields, fieldname, default=1.0):
        boostkw = "_%s_boost" % fieldname
        if boostkw in fields:
            return float(fields[boostkw])
        else:
            return default

    def _unique_fields(self, fields):
        # Check which of the supplied fields are unique
        unique_fields = [name for name, field in self.schema.items()
                         if name in fields and field.unique]
        return unique_fields

    def update_document(self, **fields):
        """The keyword arguments map field names to the values to index/store.

        This method adds a new document to the index, and automatically deletes
        any documents with the same values in any fields marked "unique" in the
        schema::

            schema = fields.Schema(path=fields.ID(unique=True, stored=True),
                                   content=fields.TEXT)
            myindex = index.create_in("index", schema)

            w = myindex.writer()
            w.add_document(path=u"/", content=u"Mary had a lamb")
            w.commit()

            w = myindex.writer()
            w.update_document(path=u"/", content=u"Mary had a little lamb")
            w.commit()

            assert myindex.doc_count() == 1

        It is safe to use ``update_document`` in place of ``add_document``; if
        there is no existing document to replace, it simply does an add.

        You cannot currently pass a list or tuple of values to a "unique"
        field.

        Because this method has to search for documents with the same unique
        fields and delete them before adding the new document, it is slower
        than using ``add_document``.

        * Marking more fields "unique" in the schema will make each
          ``update_document`` call slightly slower.

        * When you are updating multiple documents, it is faster to batch
          delete all changed documents and then use ``add_document`` to add
          the replacements instead of using ``update_document``.

        Note that this method will only replace a *committed* document;
        currently it cannot replace documents you've added to the IndexWriter
        but haven't yet committed. For example, if you do this:

        >>> writer.update_document(unique_id=u"1", content=u"Replace me")
        >>> writer.update_document(unique_id=u"1", content=u"Replacement")

        ...this will add two documents with the same value of ``unique_id``,
        instead of the second document replacing the first.

        See :meth:`Writer.add_document` for information on
        ``_stored_<fieldname>``, ``_<fieldname>_boost``, and ``_boost`` keyword
        arguments.
        """

        # Delete the set of documents matching the unique terms
        unique_fields = self._unique_fields(fields)
        if unique_fields:
            with self.searcher() as s:
                for docnum in s._find_unique([(name, fields[name])
                                              for name in unique_fields]):
                    self.delete_document(docnum)

        # Add the given fields
        self.add_document(**fields)

    def commit(self):
        """Finishes writing and unlocks the index.
        """
        pass

    def cancel(self):
        """Cancels any documents/deletions added by this object
        and unlocks the index.
        """
        pass


class AsyncWriter(threading.Thread, IndexWriter):
    """Convenience wrapper for a writer object that might fail due to locking
    (i.e. the ``filedb`` writer). This object will attempt once to obtain the
    underlying writer, and if it's successful, will simply pass method calls on
    to it.

    If this object *can't* obtain a writer immediately, it will *buffer*
    delete, add, and update method calls in memory until you call ``commit()``.
    At that point, this object will start running in a separate thread, trying
    to obtain the writer over and over, and once it obtains it, "replay" all
    the buffered method calls on it.

    In a typical scenario where you're adding a single or a few documents to
    the index as the result of a Web transaction, this lets you just create the
    writer, add, and commit, without having to worry about index locks,
    retries, etc.

    For example, to get an aynchronous writer, instead of this:

    >>> writer = myindex.writer()

    Do this:

    >>> from whoosh.writing import AsyncWriter
    >>> writer = AsyncWriter(myindex)
    """

    def __init__(self, index, delay=0.25, writerargs=None):
        """
        :param index: the :class:`whoosh.index.Index` to write to.
        :param delay: the delay (in seconds) between attempts to instantiate
            the actual writer.
        :param writerargs: an optional dictionary specifying keyword arguments
            to to be passed to the index's ``writer()`` method.
        """

        threading.Thread.__init__(self)
        self.running = False
        self.index = index
        self.writerargs = writerargs or {}
        self.delay = delay
        self.events = []
        try:
            self.writer = self.index.writer(**self.writerargs)
        except LockError:
            self.writer = None

    def reader(self):
        return self.index.reader()

    def searcher(self, **kwargs):
        from whoosh.searching import Searcher
        return Searcher(self.reader(), fromindex=self.index, **kwargs)

    def _record(self, method, args, kwargs):
        if self.writer:
            getattr(self.writer, method)(*args, **kwargs)
        else:
            self.events.append((method, args, kwargs))

    def run(self):
        self.running = True
        writer = self.writer
        while writer is None:
            try:
                writer = self.index.writer(**self.writerargs)
            except LockError:
                time.sleep(self.delay)
        for method, args, kwargs in self.events:
            getattr(writer, method)(*args, **kwargs)
        writer.commit(*self.commitargs, **self.commitkwargs)

    def delete_document(self, *args, **kwargs):
        self._record("delete_document", args, kwargs)

    def add_document(self, *args, **kwargs):
        self._record("add_document", args, kwargs)

    def update_document(self, *args, **kwargs):
        self._record("update_document", args, kwargs)

    def add_field(self, *args, **kwargs):
        self._record("add_field", args, kwargs)

    def remove_field(self, *args, **kwargs):
        self._record("remove_field", args, kwargs)

    def delete_by_term(self, *args, **kwargs):
        self._record("delete_by_term", args, kwargs)

    def commit(self, *args, **kwargs):
        if self.writer:
            self.writer.commit(*args, **kwargs)
        else:
            self.commitargs, self.commitkwargs = args, kwargs
            self.start()

    def cancel(self, *args, **kwargs):
        if self.writer:
            self.writer.cancel(*args, **kwargs)


class BufferedWriter(IndexWriter):
    """Convenience class that acts like a writer but buffers added documents to
    a :class:`~whoosh.ramindex.RamIndex` before dumping the buffered documents
    as a batch into the actual index.

    In scenarios where you are continuously adding single documents very
    rapidly (for example a web application where lots of users are adding
    content simultaneously), using a BufferedWriter is *much* faster than
    opening and committing a writer for each document you add.

    (This class may also be useful for batches of ``update_document`` calls. In
    a normal writer, ``update_document`` calls cannot update documents you've
    added *in that writer*. With ``BufferedWriter``, this will work.)

    If you're adding a batches of documents at a time, you can just use a
    regular writer -- you're already committing a "batch" of documents, so you
    don't need this class.

    To use this class, create it from your index and *keep it open*, sharing
    it between threads.

    >>> from whoosh.writing import BufferedWriter
    >>> writer = BufferedWriter(myindex, period=120, limit=100)

    You can control how often the ``BufferedWriter`` flushes the in-memory
    index to disk using the ``period`` and ``limit`` arguments. ``period`` is
    the maximum number of seconds between commits. ``limit`` is the maximum
    number of additions to buffer between commits.

    You can read/search the combination of the on-disk index and the buffered
    documents in memory by calling ``BufferedWriter.reader()`` or
    ``BufferedWriter.searcher()``. This allows quasi-real-time search, where
    documents are available for searching as soon as they are buffered in
    memory, before they are committed to disk.

    >>> searcher = writer.searcher()

    .. tip::
        By using a searcher from the shared writer, multiple *threads* can
        search the buffered documents. Of course, other *processes* will only
        see the documents that have been written to disk. If you want indexed
        documents to become available to other processes as soon as possible,
        you have to use a traditional writer instead of a ``BufferedWriter``.

    Calling ``commit()`` on the ``BufferedWriter`` manually commits any batched
    up changes. You can continue to make changes after calling ``commit()``,
    and you can call ``commit()`` multiple times.

    .. note::
        This object keeps an underlying writer open and stores documents in
        memory, so you must explicitly call the :meth:`~BufferedWriter.close()`
        method on this object before it goes out of scope to release the
        write lock and make sure any uncommitted changes are saved.
    """

    def __init__(self, index, period=60, limit=10, writerargs=None,
                 commitargs=None, tempixclass=None):
        """
        :param index: the :class:`whoosh.index.Index` to write to.
        :param period: the maximum amount of time (in seconds) between commits.
            Set this to ``0`` or ``None`` to not use a timer. Do not set this
            any lower than a few seconds.
        :param limit: the maximum number of documents to buffer before
            committing.
        :param writerargs: dictionary specifying keyword arguments to be passed
            to the index's ``writer()`` method when creating a writer.
        :param commitargs: dictionary specifying keyword arguments to be passed
            to the writer's ``commit()`` method when committing a writer.
        """

        self.index = index
        self.period = period
        self.limit = limit
        self.writerargs = writerargs or {}
        self.commitargs = commitargs or {}
        self._sync_lock = threading.RLock()
        self._write_lock = threading.Lock()

        if tempixclass is None:
            from whoosh.ramindex import RamIndex as tempixclass
        self.tempixclass = tempixclass

        self.writer = None
        self.base = self.index.doc_count_all()
        self.bufferedcount = 0
        self.commitcount = 0
        self.ramindex = self._create_ramindex()
        if self.period:
            self.timer = threading.Timer(self.period, self.commit)

    def __del__(self):
        if hasattr(self, "writer") and self.writer:
            if not self.writer.is_closed:
                try:
                    self.writer.cancel()
                except:
                    pass
            del self.writer

    def _create_ramindex(self):
        return self.tempixclass(self.index.schema)

    def _get_writer(self):
        if self.writer is None:
            self.writer = self.index.writer(**self.writerargs)
            self.schema = self.writer.schema
            self.base = self.index.doc_count_all()
            self.bufferedcount = 0
        return self.writer

    @synchronized
    def reader(self, **kwargs):
        from whoosh.reading import MultiReader

        writer = self._get_writer()
        ramreader = self.ramindex
        if self.index.is_empty():
            return ramreader
        else:
            reader = writer.reader(**kwargs)
            if reader.is_atomic():
                reader = MultiReader([reader, ramreader])
            else:
                reader.add_reader(ramreader)
            return reader

    def searcher(self, **kwargs):
        from whoosh.searching import Searcher

        return Searcher(self.reader(), fromindex=self.index, **kwargs)

    def close(self):
        self.commit(restart=False)

    def commit(self, restart=True):
        if self.period:
            self.timer.cancel()

        # Replace the RAM index
        with self._sync_lock:
            oldramindex = self.ramindex
            self.ramindex = self._create_ramindex()

        with self._write_lock:
            if self.bufferedcount:
                self._get_writer().add_reader(oldramindex.reader())

            if self.writer:
                self.writer.commit(**self.commitargs)
                self.writer = None
                self.commitcount += 1

            if restart:
                if self.period:
                    self.timer = threading.Timer(self.period, self.commit)

    def add_reader(self, reader):
        with self._write_lock:
            self._get_writer().add_reader(reader)

    def add_document(self, **fields):
        with self._sync_lock:
            self.ramindex.add_document(**fields)
            self.bufferedcount += 1
        if self.bufferedcount >= self.limit:
            self.commit()

    @synchronized
    def update_document(self, **fields):
        self._get_writer()
        super(BufferedWriter, self).update_document(**fields)

    @synchronized
    def delete_document(self, docnum, delete=True):
        if docnum < self.base:
            return self._get_writer().delete_document(docnum, delete=delete)
        else:
            return self.ramindex.delete_document(docnum - self.base,
                                                 delete=delete)

    @synchronized
    def is_deleted(self, docnum):
        if docnum < self.base:
            return self.writer.is_deleted(docnum)
        else:
            return self.ramindex.is_deleted(docnum - self.base)

# Backwards compatibility with old name
BatchWriter = BufferedWriter
