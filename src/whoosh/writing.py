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

import copy
import os
from collections import defaultdict
from concurrent import futures
from contextlib import contextmanager
from functools import wraps
from threading import RLock, Thread
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from whoosh import fields, index, merging
from whoosh.ifaces import codecs, readers, searchers, storage
from whoosh.compat import xrange
from whoosh.postings import PostTuple, change_docid, post_docid, TERMBYTES
from whoosh.ifaces import queries
from whoosh.util import unclosed


# Typing aliases

TermDict = Dict[str, Dict[bytes, List[PostTuple]]]


# Constants

MAX_TERM_LEN = 1 << 16


# Exceptions

class IndexingError(Exception):
    pass


# Document grouping context manager

@contextmanager
def groupmanager(writer):
    writer.start_group()
    yield
    writer.end_group()


# Decorator that raises an exception if the writer has already added a document
def before_add(f):
    @wraps(f)
    def before_add_wrapper(self, *args, **kwargs):
        if self._added:
            raise Exception("Can't call this method after adding a document")
        return f(self, *args, **kwargs)
    return before_add_wrapper


# Object for keeping track of segments and merges

class SegmentList(object):
    def __init__(self, session: 'storage.Session', schema: 'fields.Schema',
                 segments: 'Sequence[codecs.Segment]'):
        from whoosh.reading import SegmentReader

        self.session = session
        self.schema = schema
        self.readerclass = SegmentReader
        self._lock = RLock()

        self.segments = []  # type: List[codecs.Segment]

        # Keep track of the ongoing merges
        self._current_merges = []  # type: List[merging.Merge]
        # Cache readers for the segments for computing deletions
        self._cached_readers = {}  # type: Dict[str, readers.IndexReader]
        # Buffer deletes in memory before applying them to the segment
        self._buffered_deletes = {}  # type: Dict[str, Set[int]]

        for segment in segments:
            self.add(segment)

    def __len__(self):
        return len(self.segments)

    def add(self, segment: 'codecs.Segment', buffered_deletes: Set[int]=None):
        with self._lock:
            segid = segment.segment_id()
            buffered_deletes = buffered_deletes or set()

            self.segments.append(segment)
            self._buffered_deletes[segid] = buffered_deletes

    def merging_ids(self) -> Set[str]:
        with self._lock:
            out = set()
            for merge in self._current_merges:
                out.update(merge.segment_ids())
            return out

    def add_merge(self, merge: merging.Merge):
        with self._lock:
            for segment in merge.segments:
                self.save_buffered_deletes(segment)
            self._current_merges.append(merge)

    def are_merging(self, idset: Set[str]) -> bool:
        with self._lock:
            for segid in self.merging_ids():
                if segid in idset:
                    return True

    def has_segment(self, segment: 'codecs.Segment') -> bool:
        return self.has_segment_with_id(segment.segment_id())

    def has_segment_with_id(self, segid: str) -> bool:
        for seg in self.segments:
            if seg.segment_id() == segid:
                return True
        return False

    def remove_segment(self, segment: 'codecs.Segment'):
        segid = segment.segment_id()
        with self._lock:
            # Close and remove the cached reader if it exists
            if segid in self._cached_readers:
                self._cached_readers.pop(segid).close()

            # Remove the buffered deletes set. It would be nice if we could
            # detect errors by making sure it's empty, but it might legit have
            # leftover deletions if they were buffered while the segment was
            # merging
            del self._buffered_deletes[segid]

            # Remove segment from segments list
            for i in xrange(len(self.segments)):
                if self.segments[i].segment_id() == segment.segment_id():
                    del self.segments[i]
                    break
            else:
                raise KeyError("Segment %s not in list" % segid)

    def integrate(self, newsegment: 'codecs.Segment', merge_id: str):
        with self._lock:
            # Just do a simple linear search for the merge
            for i in xrange(len(self._current_merges)):
                m = self._current_merges[i]
                if m.merge_id == merge_id:
                    break
            else:
                # Didn't find the merge in the list, something's wrong!
                raise Exception("Merge %r not in merging list" % merge_id)

            # Remove this merge from the list
            del self._current_merges[i]

            # Remove the merged segments
            for i, segment in enumerate(m.segments):
                self.remove_segment(segment)

            # Add the segment
            self.add(newsegment)

            # Apply queued query deletes to the new segment
            if m.delete_queries:
                self._delete_by_query(newsegment, m.delete_queries)

    def _delete_by_query(self, segment: 'codecs.Segment',
                         qs: 'Iterable[queries.Query]'):
        from whoosh.searching import ConcreteSearcher

        r = self.reader(segment)
        s = ConcreteSearcher(r)
        delbuf = self._buffered_deletes[segment.segment_id()]
        for q in qs:
            docids = q.docs(s, deleting=True)
            delbuf.update(docids)

    def delete_by_query(self, q: 'queries.Query'):
        with self._lock:
            # For current segments, run the query and buffer the deletions
            for segment in self.segments:
                self._delete_by_query(segment, (q,))

            # For the current merges, remember to perform this deletion
            # when they're finished
            for merge in self._current_merges:
                merge.delete_queries.append(q)

    def make_reader(self, segment: 'codecs.Segment') -> 'readers.IndexReader':
        return self.readerclass(self.session.store, self.schema, segment)

    def reader(self, segment: 'codecs.Segment') -> 'readers.IndexReader':
        with self._lock:
            self.save_buffered_deletes(segment)

            segid = segment.segment_id()
            try:
                return self._cached_readers[segid]
            except KeyError:
                r = self.make_reader(segment)
                self._cached_readers[segid] = r
            return r

    def multireader(self, segments: 'Sequence[codecs.Segment]'=None,
                    ) -> 'readers.IndexReader':
        with self._lock:
            from whoosh import reading

            segments = segments or self.segments
            rs = [self.reader(seg) for seg in segments]
            assert rs
            if len(rs) == 1:
                return rs[0]
            else:
                return reading.MultiReader(rs)

    def test_is_deleted(self, segment: 'codecs.Segment', docnum: int):
        segid = segment.segment_id()
        if docnum in self._buffered_deletes[segid]:
            return True

        for seg in self.segments:
            if seg.segment_id() == segid:
                return seg.is_deleted(docnum)
        raise KeyError

    def save_buffered_deletes(self, segment: 'codecs.Segment'):
        with self._lock:
            segid = segment.segment_id()
            buffered = self._buffered_deletes[segid]
            if buffered:
                segment.delete_documents(self._buffered_deletes[segid])
                self._buffered_deletes[segid] = set()

    def save_all_buffered_deletes(self):
        with self._lock:
            for segment in self.segments:
                self.save_buffered_deletes(segment)

    def close(self):
        with self._lock:
            # Close all cached readers
            for reader in self._cached_readers.values():
                reader.close()

            self.save_all_buffered_deletes()


# Codec and segment-based writer

class SegmentWriter(object):
    def __init__(self, store: 'storage.Storage', indexname: 'str',
                 schema: 'fields.Schema', generation: int,
                 segments: 'Sequence[codecs.Segment]'=None,
                 session: 'storage.Session'=None, cdc: 'codecs.Codec'=None,
                 docbase: int=0, merge_strategy: 'merging.MergeStrategy'=None,
                 doc_limit=1000, executor: 'futures.Executor'=None,
                 is_sub_writer: bool=False):
        self.store = store
        self.indexname = indexname
        self.schema = schema
        self.generation = generation

        self._external_session = session
        self.session = session or self.store.open(indexname, writable=True)

        self._original_segments = segments if segments is not None else []
        self._segments = copy.deepcopy(self._original_segments)
        self.segments = SegmentList(self.session, self.schema, self._segments)

        if cdc is None:
            from whoosh.codec import default_codec
            cdc = default_codec()
        self.codec = cdc

        self.merge_strategy = merge_strategy or merging.TieredMergeStrategy()
        self.doc_limit = doc_limit
        self.executor = executor

        # Flags the user can set to alter the behavior of the next commit
        self.merge = True
        self.optimize = False

        self.segment = None  # type: codecs.Segment
        self._perdoc = None  # type: codecs.PerDocumentWriter
        self._terms = None # type: codecs.FieldWriter
        self._start_new_segment()

        self.closed = False
        self._docnum = self.docbase = docbase
        self._termbuffer = {}  # type: TermDict
        self._doccount = 0
        self._added = False
        self._changed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.cancel()
        else:
            self.commit()

    def _start_new_segment(self):
        self.segment = self.new_segment()

        codec = self.codec
        self._perdoc = codec.per_document_writer(self.session, self.segment)
        self._terms = codec.field_writer(self.session, self.segment)

    def group(self):
        """
        Returns a context manager that calls ``start_group`` and ``end_group``
        for you, allowing you to use a ``with`` statement to group hierarchical
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
        ``group`` method and the ``with`` statement.
        """

        pass

    def end_group(self):
        """
        Finish indexing a group of hierarchical documents. See
        :meth:`~SegmentWriter.start_group`.
        """

        pass

    def new_segment(self) -> 'codecs.Segment':
        return self.codec.new_segment(self.store, self.indexname)

    def add_field(self, fieldname: str, field: 'fields.FieldType'):
        """
        Adds a field to the index's schema.

        :param fieldname: the name of the field to add.
        :param field: an instantiated :class:`whoosh.fields.FieldType`
            object.
        """

        self.schema.add(fieldname, field)

    def remove_field(self, fieldname):
        """
        Removes the named field from the index's schema. Depending on the
        backend implementation, this may or may not actually remove existing
        data for the field from the index. Optimizing the index should always
        clear out existing data for a removed field.

        :param fieldname: the name of the field to remove.
        """

        self.schema.remove(fieldname)

    def searcher(self, **kwargs) -> 'searchers.Searcher':
        """
        Returns a searcher for the existing index.

        :param kwargs: keyword arguments passed to the index's reader() method.
        """

        from whoosh import searching

        return searching.ConcreteSearcher(self.reader(), **kwargs)

    def delete_by_term(self, fieldname: str, termbytes: bytes):
        """
        Deletes any documents containing "term" in the "fieldname" field.
        This is useful when you have an indexed field containing a unique ID
        (such as "pathname") for each document.

        :param fieldname: the name of the field containing the term.
        :param termbytes: the bytestring of the term to delete.
        :returns: the number of documents deleted.
        """

        from whoosh.query.terms import Term

        q = Term(fieldname, termbytes)
        return self.delete_by_query(q)

    @unclosed
    def reader(self, **kwargs):
        """
        Returns a reader for the existing index.

        :param kwargs: keyword arguments passed to the index's reader() method.
        """

        return self.segments.multireader()

    # Have to override add_field and remove_field to add before_add decorator
    @before_add
    @unclosed
    def add_field(self, fieldname: str, field: 'fields.FieldType'):
        self.schema.add(fieldname, field)

    @before_add
    @unclosed
    def remove_field(self, fieldname: str):
        self.schema.remove(fieldname)

    def has_deletions(self) -> bool:
        return any(s.has_deletions() for s in self.segments)

    def deleted_count(self):
        return sum(s.deleted_count() for s in self.segments)

    @unclosed
    def delete_by_query(self, q: 'queries.Query'):
        """
        Deletes any documents matching a query object.

        :param q: delete documents which match this query.
        """

        self.segments.delete_by_query(q)

    @unclosed
    def add_reader(self, reader: 'readers.IndexReader'):
        """
        Adds the contents of the given reader to this index.

        :param reader: the reader to add.
        """

        newsegment = copy_reader(
            reader, self.session, self.indexname, self.codec, self.schema
        )
        self.segments.add(newsegment)
        self.try_merging()

    def try_merging(self, expunge_deleted: bool=False):
        strategy = self.merge_strategy
        merging = self.segments.merging_ids()
        merges = strategy.get_merges(self._segments, merging,
                                     expunge_deleted=expunge_deleted)
        for merge in merges:
            self.apply_merge(merge)

    def apply_merge(self, merge: merging.Merge):
        ids_to_merge = set(seg.segment_id() for seg in merge.segments)
        if self.segments.are_merging(ids_to_merge):
            raise Exception("Trying to merge already merging segments")
        self.segments.add_merge(merge)

        if self.executor:
            future = self.executor.submit(
                perform_merge, self.store, self.indexname, self.codec,
                self.schema, merge
            )

            def merge_callback(f):
                self.segments.integrate(*f.results())

            future.add_done_callback(merge_callback)
        else:
            self._perform_merge(merge)

    def _perform_merge(self, merge: merging.Merge):
        newsegment, merge_id = perform_merge(
            self.session, self.indexname, self.codec, self.schema, merge
        )
        self.segments.integrate(newsegment, merge_id)

    def _index_field(self, fieldname: str, field: 'fields.FieldType',
                     value: Any, stored_val: Any, boost=1.0):
        if value is None:
            return

        docnum = self._docnum
        perdoc = self._perdoc

        length = 0
        if field.indexed:
            # Returns the field length and a generator of post tuples
            length, posts = field.index(value, docnum, boost=boost)
            postcount = len(posts)

            # Get the buffer for this field
            try:
                fdict = self._termbuffer[fieldname]
            except KeyError:
                self._termbuffer[fieldname] = fdict = defaultdict(list)

            # Buffer the posts
            for post in posts:
                fdict[post[TERMBYTES]].append(post)

            if field.vector:
                # If we need to add the posts as a vector, copy the
                # generator into a list so they can be used more than once
                perdoc.add_vector_postings(fieldname, field, posts)

        # Write the per-document values
        perdoc.add_field(fieldname, field, stored_val, length)
        # Write the column value
        if field.column:
            perdoc.add_column_value(fieldname, field.column,
                                    field.to_column_value(stored_val))

        return length

    @unclosed
    def add_document(self, **kwargs):
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

        index_field = self._index_field

        # The keyword argument keys are the fields to index
        fieldnames = sorted([name for name in kwargs.keys()
                             if not name.startswith("_")])
        # Look for a document-wide boost keyword argument
        doc_boost = kwargs.get("_boost", 1.0)

        # Tell the per-document writer to start a new document
        self._perdoc.start_doc(self._docnum)

        # Index each field
        for fieldname in fieldnames:
            # Get the field object from the schema
            try:
                field = self.schema[fieldname]
            except KeyError:
                raise ValueError("No %r field in schema" % fieldname)

            # Get the value from the keyword argument
            value = kwargs.get(fieldname)
            # Look for an optional "store this value" keyword argument
            stored_val = kwargs.get("_stored_" + fieldname, value)
            # Look for an optional field boost keyword argument
            field_boost = (field.field_boost *
                           doc_boost *
                           kwargs.get("_%s_boost" % fieldname, 1.0))
            # Index the field
            index_field(fieldname, field, value, stored_val, boost=field_boost)

            # If the field has sub-fields, index them with the same values
            for subname, subfield in field.subfields(fieldname):
                index_field(subname, subfield, value, stored_val)

        # Tell the per-document writer the finish the curent document
        self._perdoc.finish_doc()

        # Update writer state
        self._docnum += 1
        self._added = True
        self._changed = True

        self._doccount += 1
        if self._doccount >= self.doc_limit:
            self.flush()

    def update_document(self, **kwargs):
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

        Because this method has to search for documents with the same unique
        fields and delete them before adding the new document, it is slower
        than using ``add_document``.

        * Marking more fields "unique" in the schema will make each
          ``update_document`` call slightly slower.

        * When you are updating multiple documents, it is faster to batch
          delete all changed documents and then use ``add_document`` to add
          the replacements instead of using ``update_document``.

        Note that this method will only replace a *committed* document;
        currently it cannot replace documents you've added to the writer
        but haven't yet committed. For example, if you do this:

        >>> writer.update_document(unique_id=u"1", content=u"Replace me")
        >>> writer.update_document(unique_id=u"1", content=u"Replacement")

        ...this will add two documents with the same value of ``unique_id``,
        instead of the second document replacing the first.

        See :meth:`Writer.add_document` for information on
        ``_stored_<fieldname>``, ``_<fieldname>_boost``, and ``_boost`` keyword
        arguments.
        """

        self._delete_for_update(kwargs)

        # Add the given fields
        self.add_document(**kwargs)

    def _delete_for_update(self, kwargs):
        from whoosh.query.terms import Term

        # Delete the set of documents matching the unique terms
        for fieldname, fieldobj in self.schema.items():
            if fieldname in kwargs and fieldobj.unique:
                q = Term(fieldname, kwargs[fieldname])
                self.delete_by_query(q)

    def _merge_flushed(self, merge: bool, optimize: bool,
                       expunge_deleted: bool):
        if optimize and len(self.segments) > 1:
            # Create a merge with every segment
            m = merging.Merge(list(self.segments.segments))
            self.segments.add_merge(m)
            # Don't do the merge in the background
            self._perform_merge(m)
            assert len(self.segments) == 1

        elif merge:
            self.try_merging(expunge_deleted=expunge_deleted)

    @unclosed
    def flush(self, merge: bool=None, optimize: bool=None,
              expunge_deleted: bool=False, restart: bool=True
              ) -> 'codecs.Segment':
        """
        Flushes any queued documents to a new segment but does not close the
        writer.

        :param merge: Try to merge segments after flushing. Skipping merging
            is faster but eventually will fill up the index with small segments.
        :param optimize: Merge more aggressively.
        :param expunge_deleted: Merge segments with lots of deletions more
            aggressively.
        :param restart: setting this to False indicates this writer won't be
            used again after this flush.
        """

        merge = merge if merge is not None else self.merge
        optimize = optimize if optimize is not None else self.optimize

        # Flush the buffered terms
        self._flush_terms()
        # Close the codec writers
        self._perdoc.close()
        self._terms.close()

        # Add the current segment to the segment list
        thissegment = self.segment
        self.segments.add(thissegment)

        # TODO: what to do with _changed and _added here?
        self._changed = False
        self._added = False

        self._merge_flushed(merge, optimize, expunge_deleted)

        if restart:
            self._start_new_segment()

        return thissegment

    @unclosed
    def commit(self, merge: bool=None, optimize: bool=None):
        """
        Finishes writing and unlocks the index.

        :param merge: Try to merge segments after flushing. Skipping merging
            is faster but eventually will fill up the index with small segments.
        :param optimize: Merge more aggressively.
        """

        merge = merge if merge is not None else self.merge
        optimize = optimize if optimize is not None else self.optimize
        if optimize or self._changed:
            self.flush(merge, optimize)

        # Wait for background tasks to complete
        if self.executor:
            self.executor.shutdown(wait=True)

        # Sync the TOC to storage
        self._sync_toc(self.session)

        self._close()

    @unclosed
    def cancel(self):
        """
        Cancels any documents/deletions added by this object and unlocks the
        index.
        """

        # Close the codec writers
        self._perdoc.close()
        self._terms.close()
        self._close()

    def _close(self):
        # Release the lock if we have one
        if self.session and not self._external_session:
            self.session.close()

        self.closed = True

    def _flush_terms(self):
        schema = self.schema
        _fields = self._termbuffer
        fwriter = self._terms

        for fieldname in sorted(_fields):
            fielddict = _fields[fieldname]
            fieldobj = schema[fieldname]

            fwriter.start_field(fieldname, fieldobj)
            for termbytes in sorted(fielddict):
                fwriter.start_term(termbytes)
                posts = fielddict[termbytes]
                for post in posts:
                    fwriter.add_posting(post)
                fwriter.finish_term()
            fwriter.finish_field()
        _fields.clear()
        self._postcount = 0

    def _sync_toc(self, session):
        self.segments.save_all_buffered_deletes()
        toc = index.Toc(self.schema, self.segments.segments, self.generation)
        self.store.save_toc(session, toc)


# Merge machinery

def _copy_perdoc(schema: 'fields.Schema', reader: 'readers.IndexReader',
                 perdoc: 'codecs.PerDocumentWriter'
                 ) -> Optional[Dict[int, int]]:
    """
    Copies the per-document information from a reader into a PerDocumentWriter.

    :param schema: the schema to use for writing.
    :param reader: the reader to import the per-document data from.
    :param perdoc: the per-document writer to write to.
    :return: A dictionary mapping old doc numbers to new doc numbers, or
        None if no mapping is necessary
    """

    # If the incoming reading has deletions, we need to return a dictionary
    # to map old document numbers to new document numbers
    has_del = reader.has_deletions()
    docmap = {}  # type: Dict[int, int]

    fieldnames = list(schema.names())

    # Open all column readers
    cols = {}
    for fieldname in fieldnames:
        fieldobj = schema[fieldname]
        colobj = fieldobj.column
        if colobj and reader.has_column(fieldname):
            creader = reader.column_reader(fieldname, colobj)
            cols[fieldname] = creader

    # Iterate over the docs in the reader, getting the stored fields at
    # the same time
    newdoc = 0
    for docnum, stored in reader.iter_docs():
        if has_del:
            docmap[docnum] = newdoc

        # Copy the information between reader and writer
        perdoc.start_doc(newdoc)
        for fieldname in fieldnames:
            fieldobj = schema[fieldname]
            length = reader.doc_field_length(docnum, fieldname)

            # Copy the any stored value and length
            perdoc.add_field(fieldname, fieldobj,
                             stored.get(fieldname), length)

            # Copy any vector
            if fieldobj.vector and reader.has_vector(docnum, fieldname):
                vreader = reader.vector(docnum, fieldname)
                posts = tuple(vreader.postings())
                perdoc.add_vector_postings(fieldname, fieldobj, posts)

            # Copy any column value
            if fieldname in cols:
                colobj = fieldobj.column
                cval = cols[fieldname][docnum]
                perdoc.add_column_value(fieldname, colobj, cval)

        perdoc.finish_doc()
        newdoc += 1

    if has_del:
        return docmap


def _copy_terms(schema: 'fields.Schema', reader: 'readers.IndexReader',
                fieldnames: Set[str], fwriter: 'codecs.FieldWriter',
                docmap: Optional[Dict[int, int]]):
    """
    Copies term information from a reader into a FieldWriter.

    :param schema: the schema to use for writing.
    :param reader: the reader to import the terms from.
    :param fieldnames: the names of the fields to be included.
    :param fwriter: the FieldWriter to write to.
    :param docmap: an optional dictionary mapping document numbers in the
        incoming reader to numbers in the new segment.
    """

    last_fieldname = None
    for fieldname, termbytes in reader.all_terms():
        if fieldname not in fieldnames:
            continue

        if fieldname != last_fieldname:
            if last_fieldname is not None:
                fwriter.finish_field()
            fieldobj = schema[fieldname]
            fwriter.start_field(fieldname, fieldobj)
            last_fieldname = fieldname

        fwriter.start_term(termbytes)

        m = reader.matcher(fieldname, termbytes)
        count = 0
        for p in m.all_postings():
            if docmap:
                # Make a new posting with the doc ID updated for this segment
                newid = docmap[post_docid(p)]
                p = change_docid(p, newid)

            fwriter.add_posting(p)
            count += 1

        m.close()
        fwriter.finish_term()

    if last_fieldname is not None:
        fwriter.finish_field()


def perform_merge(session: 'storage.Session', indexname: str,
                  cdc: 'codecs.Codec', schema: 'fields.Schema',
                  merge: merging.Merge
                  ) -> 'Sequence[Tuple[codecs.Segment, str]]':
    from whoosh.reading import SegmentReader, MultiReader

    rs = [SegmentReader(session.store, schema, segment) for segment
          in merge.segments]
    assert rs
    if len(rs) == 1:
        reader = rs[0]
    else:
        reader = MultiReader(rs)

    newsegment = copy_reader(reader, session, indexname, cdc, schema)
    return newsegment, merge.merge_id


def copy_reader(reader: 'readers.IndexReader', session: 'storage.Session',
                indexname: str, cdc: 'codecs.Codec', schema: 'fields.Schema',
                ) -> 'Tuple[codecs.Segment]':
    newsegment = cdc.new_segment(session.store, indexname)

    # Create writers for the new segment
    perdoc = cdc.per_document_writer(session, newsegment)
    fwriter = cdc.field_writer(session, newsegment)

    # Field names to index
    indexednames = set(fname for fname in reader.indexed_field_names()
                       if fname in schema)

    # Add the per-document data. This returns a mapping of old docnums
    # to new docnums (if there were changes because deleted docs were
    # skipped, otherwise it's None). We'll use this mapping to rewrite
    # doc references when we import the term data.
    docmap = _copy_perdoc(schema, reader, perdoc)
    # Add the term data
    _copy_terms(schema, reader, indexednames, fwriter, docmap)

    # Close the writers
    fwriter.close()
    perdoc.close()

    return newsegment


def batch_index(batch_filename: str, count: int, storage_url: str,
                indexname: str, schema: 'fields.Schema', generation: int,
                merge_id: str, doc_limit: int) -> Tuple[codecs.Segment, str]:
    from whoosh.compat import pickle

    store = storage.from_url(storage_url)
    w = SegmentWriter(store, indexname, schema, generation, doc_limit=doc_limit,
                      is_sub_writer=True)

    with open(batch_filename, "rb") as f:
        for _ in xrange(count):
            kwargs = pickle.load(f)
            w.add_document(**kwargs)
    os.remove(batch_filename)

    segment = w.flush(merge=False, optimize=False)
    w.cancel()

    return segment, merge_id


# Multi-(threaded|processing) writer using concurrent.futures

class MultiWriter(SegmentWriter):
    def __init__(self, *args, **kwargs):
        super(MultiWriter, self).__init__(*args, **kwargs)

        self._group_level = 0
        self._buffered = 0
        self._temppath = None
        self._tempfile = None
        self._make_temp()

    def _make_temp(self):
        from tempfile import mkstemp

        fd, self._temppath = mkstemp(suffix=".pickle", prefix="multi")
        self._tempfile = os.fdopen(fd, "wb")
        self._buffered = 0

    def start_group(self):
        self._group_level += 1

    def end_group(self):
        self._group_level -= 1

    @unclosed
    def add_document(self, **kwargs):
        from whoosh.compat import pickle

        pickle.dump(kwargs, self._tempfile, -1)
        self._buffered += 1
        if self._buffered >= self.doc_limit and not self._group_level:
            self.flush()

    @unclosed
    def update_document(self, **kwargs):
        self._delete_for_update(kwargs)
        self.add_document(**kwargs)

    @unclosed
    def flush(self, merge: bool=None, optimize: bool=None,
              expunge_deleted: bool=False, restart: bool=True
              ) -> 'codecs.Segment':
        """
        Flushes any queued documents to a new segment but does not close the
        writer.

        :param merge: Try to merge segments after flushing. Skipping merging
            is faster but eventually will fill up the index with small segments.
        :param optimize: Merge more aggressively.
        :param expunge_deleted: Merge segments with lots of deletions more
            aggressively.
        :param restart: setting this to False indicates this writer won't be
            used again after this flush.
        """

        # Should we try to merge after integrating the new segment?
        merge = merge if merge is not None else self.merge
        # Should we try to merge ALL segments after integrating the new segment?
        optimize = optimize if optimize is not None else self.optimize

        # Finish buffering
        self._tempfile.close()
        # Create a merge object representing the merge-in of the new segment
        mergeobj = merging.Merge([])
        self.segments.add_merge(mergeobj)

        # Create a segment from the buffered docs in a future
        future = self.executor.submit(
            batch_index, self._buffered, self.store.as_url(), self.indexname,
            self.schema, self.generation, mergeobj.merge_id, self.doc_limit + 1,
        )

        def multi_flush_callback(f):
            self.segments.integrate(*f.results())
            self._merge_flushed(merge, optimize, expunge_deleted)
        # Add a callback to integrate the new segment
        future.add_done_callback(multi_flush_callback)

        # Restart buffering documents
        if restart:
            self._make_temp()


