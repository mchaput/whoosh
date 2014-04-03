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
from abc import ABCMeta, abstractmethod
from contextlib import contextmanager
from functools import wraps

from whoosh import columns
from whoosh.fields import UnknownFieldError
from whoosh.index import LockError
from whoosh.util import unclosed


# Exceptions

class IndexingError(Exception):
    pass


# Document grouping context manager

@contextmanager
def groupmanager(writer):
    writer.start_group()
    yield
    writer.end_group()


# Writer base class

class IndexWriter(object):
    """
    High-level object for writing to an index.

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

    __metaclass__ = ABCMeta

    def __init__(self, ix):
        self._ix = ix
        self.schema = ix.schema
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.closed:
            if exc_type:
                self.cancel()
            else:
                self.commit()

    def group(self):
        """
        Returns a context manager that calls
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

    @unclosed
    def start_group(self):
        """
        Start indexing a group of hierarchical documents. The backend should
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

        # This should be overridden by parallel writers to make sure that all
        # docs added between start_group() and end_group() go into the same
        # sub-index
        pass

    @unclosed
    def end_group(self):
        """
        Finish indexing a group of hierarchical documents. See
        :meth:`~IndexWriter.start_group`.
        """

        pass

    @unclosed
    def add_field(self, fieldname, fieldtype, **kwargs):
        """
        Adds a field to the index's schema.

        :param fieldname: the name of the field to add.
        :param fieldtype: an instantiated :class:`whoosh.fields.FieldType`
            object.
        """

        self.schema.add(fieldname, fieldtype, **kwargs)

    @unclosed
    def remove_field(self, fieldname, **kwargs):
        """
        Removes the named field from the index's schema. Depending on the
        backend implementation, this may or may not actually remove existing
        data for the field from the index. Optimizing the index should always
        clear out existing data for a removed field.
        """

        self.schema.remove(fieldname, **kwargs)

    @abstractmethod
    def reader(self, **kwargs):
        """
        Returns a reader for the existing index.
        """

        raise NotImplementedError

    @unclosed
    def searcher(self, **kwargs):
        from whoosh.searching import Searcher
        return Searcher(self.reader(), **kwargs)

    @unclosed
    def delete_by_term(self, fieldname, text, searcher=None):
        """
        Deletes any documents containing "term" in the "fieldname" field.
        This is useful when you have an indexed field containing a unique ID
        (such as "pathname") for each document.

        :returns: the number of documents deleted.
        """

        from whoosh.query import Term

        q = Term(fieldname, text)
        return self.delete_by_query(q, searcher=searcher)

    @unclosed
    def delete_by_query(self, q, searcher=None):
        """
        Deletes any documents matching a query object.

        :returns: the number of documents deleted.
        """

        if searcher:
            s = searcher
        else:
            s = self.searcher()

        try:
            count = 0
            for docid in s.docs_for_query(q, for_deletion=True):
                self.delete_document(docid)
                count += 1
        finally:
            if not searcher:
                s.close()

        return count

    @abstractmethod
    def delete_document(self, docid):
        """
        Deletes a document by ID.
        """
        raise NotImplementedError

    @abstractmethod
    def add_document(self, **fields):
        """
        The keyword arguments map field names to the values to index/store::

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

    @abstractmethod
    def add_reader(self, reader):
        raise NotImplementedError

    def _unique_fields(self, fields):
        # Check which of the supplied fields are unique
        return [name for name, field in self.schema.items()
                if name in fields and field.unique]

    @abstractmethod
    def update_document(self, **fields):
        """
        The keyword arguments map field names to the values to index/store.

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

        * Marking more fields "unique" in the schema will make each
          ``update_document`` call slightly slower.

        * When you are updating multiple documents, it is faster to batch
          delete all changed documents and then use ``add_document`` to add
          the replacements instead of using ``update_document``.

        See :meth:`Writer.add_document` for information on
        ``_stored_<fieldname>``, ``_<fieldname>_boost``, and ``_boost`` keyword
        arguments.
        """

        raise NotImplementedError

    @abstractmethod
    def commit(self):
        """
        Finishes writing and unlocks the index.
        """

        raise NotImplementedError

    @unclosed
    def cancel(self):
        """
        Cancels any documents/deletions added by this object
        and unlocks the index.
        """

        self.closed = True


# Codec-based Implementation

class DBWriter(IndexWriter):
    def __init__(self, txn, codec, schema, ixinfo=None):
        self._txn = txn
        self._codec = codec
        self._info = ixinfo or codec.info(txn)
        self.schema = schema

        # Create a low-level writer
        self._docwriter = self._codec.doc_writer(self._txn)
        # Create a low-level column writer
        self._colwriter = self._codec.column_writer(self._txn)

        self.closed = False
        self.merge = True
        self.optimize = False

    @unclosed
    def reader(self, **kwargs):
        from whoosh.reading import DBReader

        return DBReader(self._txn, self._codec, self.schema)

    @unclosed
    def delete_document(self, docid):
        self._docwriter.delete(docid)
        if self._colwriter:
            for fieldname, fieldobj in self.schema.items():
                if fieldobj.column_type:
                    self._colwriter.remove_value(fieldname, fieldobj, docid)

    @unclosed
    def remove_field(self, fieldname, **kwargs):
        IndexWriter.remove_field(self, fieldname, **kwargs)
        self._docwriter.remove_field_terms(fieldname)

    @unclosed
    def add_reader(self, r):
        schema = self.schema
        dw = self._docwriter
        fieldnames = set(r.indexed_field_names()).intersection(schema.names())

        # Copy per-document info
        docmap = {}
        for oldid in r.doc_ids():
            stored = r.stored_fields(oldid)

            newid = dw.next_doc_id()
            docmap[oldid] = newid
            dw.start_doc(newid)
            for fieldname in fieldnames:
                fieldobj = schema[fieldname]
                dw.start_field(fieldname, fieldobj, stored.get(fieldname), None)
                #
                dw.finish_field()
            dw.finish_doc()

            for tag in r.doc_tags(oldid):
                dw.add_tag(newid, tag)

        # TODO: do columns here
        pass

        # Copy postings
        for fieldname in fieldnames:
            fieldobj = schema[fieldname]
            for termbytes in r.lexicon(fieldname):
                m = r.matcher(fieldname, termbytes)
                dw.add_matcher(fieldname, fieldobj, termbytes, m, docmap)

    def update_document(self, **fields):
        self.add_document(_update=True, **fields)

    @unclosed
    def add_document(self, _update=False, _triggers=None, _triggered=None,
                     **fields):
        schema = self.schema
        dw = self._docwriter
        docid = dw.next_doc_id()
        colwriter = self._colwriter

        docboost = fields.get("_boost", 1.0)
        fieldnames = sorted([name for name in fields.keys()
                             if not name.startswith("_")])
        fieldboosts = {}
        for fieldname in fieldnames:
            fboost = fields.get("_%s_boost" % fieldname, 1.0)
            fieldboosts[fieldname] = fboost * docboost

        target = None
        if _triggers:
            target = {}

        dw.start_doc(docid)
        for fieldname in fieldnames:
            if fieldname not in schema:
                raise UnknownFieldError(fieldname)

            value = fields[fieldname]
            if value is None:
                continue

            for fname, fieldobj in schema.indexable_fields(fieldname):
                fieldlen = None

                if fieldobj.indexed:
                    fieldboost = fieldboosts.get(fname, 1.0)
                    fieldlen, posts = fieldobj.index(value, boost=fieldboost)

                    # If we're storing a vector or checking triggers, then we
                    # need to access the postings multiple times, so put them in
                    # a list
                    if _triggers or fieldobj.vector:
                        posts = list(posts)

                    if fieldobj.vector:
                        dw.store_vector(fieldname, fieldobj, posts)

                    if _triggers:
                        v = fieldobj.format.buffer(vector=True)
                        target[fname] = v.from_list(posts)

                    dw.add_field_postings(fname, fieldobj, fieldlen, posts)

                storedval = fields.get("_stored_" + fname, value)
                dw.add_field(fname, fieldobj, storedval, fieldlen, _update)

                if colwriter and fieldobj.column_type and storedval is not None:
                    colval = fieldobj.to_column_value(storedval)
                    colwriter.add_value(fname, fieldobj, docid, colval)

        if _triggers:
            for tq in _triggers:
                if tq.trigger(schema, target) and _triggered:
                    _triggered.append(tq)

        dw.finish_doc()

    @unclosed
    def doc_count(self):
        return self._docwriter.doc_count()

    @unclosed
    def commit(self):
        self._docwriter.tidy(self.schema)

        ixinfo = self._info
        ixinfo.schema = self.schema
        ixinfo.touch()
        self._codec.write_info(self._txn, ixinfo)

        self._docwriter.close()
        if self._colwriter:
            self._colwriter.close()
        self._txn.commit()
        self.closed = True

    @unclosed
    def cancel(self):
        self._txn.cancel()
        self.closed = True


# Writer wrappers

class AsyncWriter(threading.Thread, IndexWriter):
    """
    Convenience wrapper for a writer object that might fail due to locking
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


# # Buffered writer class
#
# class BufferedWriter(IndexWriter):
#     """
#     Convenience class that acts like a writer but buffers added documents to
#     a buffer before dumping the buffered documents as a batch into the actual
#     index.
#
#     In scenarios where you are continuously adding single documents very
#     rapidly (for example a web application where lots of users are adding
#     content simultaneously), using a BufferedWriter is *much* faster than
#     opening and committing a writer for each document you add. If you're adding
#     batches of documents at a time, you can just use a regular writer.
#
#     (This class may also be useful for batches of ``update_document`` calls. In
#     a normal writer, ``update_document`` calls cannot update documents you've
#     added *in that writer*. With ``BufferedWriter``, this will work.)
#
#     To use this class, create it from your index and *keep it open*, sharing
#     it between threads.
#
#     >>> from whoosh.writing import BufferedWriter
#     >>> writer = BufferedWriter(myindex, period=120, limit=20)
#     >>> # Then you can use the writer to add and update documents
#     >>> writer.add_document(...)
#     >>> writer.add_document(...)
#     >>> writer.add_document(...)
#     >>> # Before the writer goes out of scope, call close() on it
#     >>> writer.close()
#
#     .. note::
#         This object stores documents in memory and may keep an underlying
#         writer open, so you must explicitly call the
#         :meth:`~BufferedWriter.close` method on this object before it goes out
#         of scope to release the write lock and make sure any uncommitted
#         changes are saved.
#
#     You can read/search the combination of the on-disk index and the
#     buffered documents in memory by calling ``BufferedWriter.reader()`` or
#     ``BufferedWriter.searcher()``. This allows quasi-real-time search, where
#     documents are available for searching as soon as they are buffered in
#     memory, before they are committed to disk.
#
#     .. tip::
#         By using a searcher from the shared writer, multiple *threads* can
#         search the buffered documents. Of course, other *processes* will only
#         see the documents that have been written to disk. If you want indexed
#         documents to become available to other processes as soon as possible,
#         you have to use a traditional writer instead of a ``BufferedWriter``.
#
#     You can control how often the ``BufferedWriter`` flushes the in-memory
#     index to disk using the ``period`` and ``limit`` arguments. ``period`` is
#     the maximum number of seconds between commits. ``limit`` is the maximum
#     number of additions to buffer between commits.
#
#     You don't need to call ``commit()`` on the ``BufferedWriter`` manually.
#     Doing so will just flush the buffered documents to disk early. You can
#     continue to make changes after calling ``commit()``, and you can call
#     ``commit()`` multiple times.
#     """
#
#     def __init__(self, index, period=60, limit=10, writerargs=None,
#                  commitargs=None):
#         """
#         :param index: the :class:`whoosh.index.Index` to write to.
#         :param period: the maximum amount of time (in seconds) between commits.
#             Set this to ``0`` or ``None`` to not use a timer. Do not set this
#             any lower than a few seconds.
#         :param limit: the maximum number of documents to buffer before
#             committing.
#         :param writerargs: dictionary specifying keyword arguments to be passed
#             to the index's ``writer()`` method when creating a writer.
#         """
#
#         self.index = index
#         self.period = period
#         self.limit = limit
#         self.writerargs = writerargs or {}
#         self.commitargs = commitargs or {}
#
#         self.lock = threading.RLock()
#         self.writer = self.index.writer(**self.writerargs)
#
#         self._make_ram_index()
#         self.bufferedcount = 0
#
#         # Start timer
#         if self.period:
#             self.timer = threading.Timer(self.period, self.commit)
#             self.timer.start()
#
#     def _make_ram_index(self):
#         from whoosh.codec.memory import MemoryCodec
#
#         self.codec = MemoryCodec()
#
#     def _get_ram_reader(self):
#         return self.codec.reader(self.schema)
#
#     @property
#     def schema(self):
#         return self.writer.schema
#
#     def reader(self, **kwargs):
#         from whoosh.reading import MultiReader
#
#         reader = self.writer.reader()
#         with self.lock:
#             ramreader = self._get_ram_reader()
#
#         # If there are in-memory docs, combine the readers
#         if ramreader.doc_count():
#             if reader.is_atomic():
#                 reader = MultiReader([reader, ramreader])
#             else:
#                 reader.add_reader(ramreader)
#
#         return reader
#
#     def searcher(self, **kwargs):
#         from whoosh.searching import Searcher
#         return Searcher(self.reader(), fromindex=self.index, **kwargs)
#
#     def close(self):
#         self.commit(restart=False)
#
#     def commit(self, restart=True):
#         if self.period:
#             self.timer.cancel()
#
#         with self.lock:
#             ramreader = self._get_ram_reader()
#             self._make_ram_index()
#
#         if self.bufferedcount:
#             self.writer.add_reader(ramreader)
#         self.writer.commit(**self.commitargs)
#         self.bufferedcount = 0
#
#         if restart:
#             self.writer = self.index.writer(**self.writerargs)
#             if self.period:
#                 self.timer = threading.Timer(self.period, self.commit)
#                 self.timer.start()
#
#     def add_reader(self, reader):
#         # Pass through to the underlying on-disk index
#         self.writer.add_reader(reader)
#         self.commit()
#
#     def add_document(self, **fields):
#         with self.lock:
#             # Hijack a writer to make the calls into the codec
#             with self.codec.writer(self.writer.schema) as w:
#                 w.add_document(**fields)
#
#             self.bufferedcount += 1
#             if self.bufferedcount >= self.limit:
#                 self.commit()
#
#     def update_document(self, **fields):
#         with self.lock:
#             IndexWriter.update_document(self, **fields)
#
#     def delete_document(self, docid):
#         with self.lock:
#             base = self.index.doc_count()
#             if docid < base:
#                 self.writer.delete_document(docid)
#             else:
#                 ramsegment = self.codec.segment
#                 ramsegment.delete_document(docid - base)
#
#     def is_deleted(self, docid):
#         base = self.index.doc_count()
#         if docid < base:
#             return self.writer.is_deleted(docid)
#         else:
#             return self._get_ram_writer().is_deleted(docid - base)
#
#
# # Backwards compatibility with old name
# BatchWriter = BufferedWriter
