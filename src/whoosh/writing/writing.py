import logging
from collections import defaultdict
from concurrent import futures
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Any, List, Sequence

from whoosh import columns, fields
from whoosh.ifaces import codecs, queries, readers, searchers, storage
from whoosh.writing import merging, reporting, segmentlist
from whoosh.postings.ptuples import PostTuple, TERMBYTES, DOCID
from whoosh.util import now, times, unclosed


logger = logging.getLogger(__name__)

EOL_FIELDNAME = "_eol"
EOL_COLUMN = columns.SparseIntColumn()


# Exceptions

class IndexingError(Exception):
    pass


# Super-simple context manager for grouping documents using writer.group()
@contextmanager
def groupmanager(writer):
    writer.start_group()
    yield
    writer.end_group()


# Low-level object for writing a single segment to storage

def posting_sort_key(post: PostTuple):
    return post[TERMBYTES], post[DOCID]


class SegmentWriter:
    def __init__(self,
                 codec: 'codecs.Codec',
                 session: 'storage.Session',
                 segment: 'codecs.Segment',
                 schema: 'fields.Schema',
                 docbase: int=0):
        self.codec = codec
        self.session = session
        self.segment = segment
        self.schema = schema
        self._docnum = self.docbase = docbase

        self._perdoc = codec.per_document_writer(session, segment)
        self._terms = codec.field_writer(session, segment)
        self._pbuffers = defaultdict(list)

        self.doc_count = 0
        self.post_count = 0

    def start_document(self):
        self._perdoc.start_doc(self._docnum)

    def index_field(self, fieldname: str, value: Any, stored_val: Any,
                    boost=1.0):
        if value is None:
            return

        docnum = self._docnum
        perdoc = self._perdoc

        length = 0
        field = self.schema[fieldname]
        if field.indexed:
            # Returns the field length and a generator of post tuples
            length, posts = field.index(value, docnum, boost=boost)
            self._pbuffers[fieldname].extend(posts)

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

    def add_column_value(self, fieldname: str, columnobj: 'columns.Column',
                         value: Any):
        self._perdoc.add_column_value(fieldname, columnobj, value)

    def finish_document(self):
        # Tell the per-document writer to finish the current document
        self._perdoc.finish_doc()

        # Update writer state
        self._docnum += 1
        self.doc_count += 1

    def _flush_terms(self):
        schema = self.schema
        pbufs = self._pbuffers
        fwriter = self._terms

        if not pbufs:
            logger.info("No terms to flush")

        logger.info("Flushing terms in %d fields", len(pbufs))
        t = now()

        for fieldname in sorted(pbufs):
            postlist = pbufs[fieldname]
            logger.info("Flushing %d posts in %r field",
                        len(postlist), fieldname)

            postlist.sort(key=posting_sort_key)
            fieldobj = schema[fieldname]
            fwriter.start_field(fieldname, fieldobj)
            tbytes = None
            for post in postlist:
                if tbytes != post[TERMBYTES]:
                    if tbytes is not None:
                        fwriter.finish_term()
                    tbytes = post[TERMBYTES]
                    fwriter.start_term(tbytes)

                fwriter.add_posting(post)
            if tbytes is not None:
                fwriter.finish_term()
            fwriter.finish_field()

        self._pbuffers.clear()

        logger.info("Flushed terms in %0.06f s", now() - t)

    def finish_segment(self) -> 'codecs.Segment':
        # Flush the buffered terms
        self._flush_terms()

        # Close the codec writers
        logger.info("Closing codec writers")
        self._perdoc.close()
        self._terms.close()

        segment = self.segment
        self.codec.finish_segment(self.session, segment)
        return segment

    def cancel(self):
        self._perdoc.close()
        self._terms.close()


# High-level writer object manages writing and merging multiple segments in an
# indexing session

class IndexWriter:
    def __init__(self,
                 codec: 'codecs.Codec',
                 store: 'storage.Storage',
                 indexname: str,
                 segments: 'List[codecs.Segment]',
                 schema: 'fields.Schema',
                 generation: int,
                 doc_limit: int=10000,
                 post_limit: int=2000000,
                 merge_strategy: merging.MergeStrategy=None,
                 executor: futures.Executor=None,
                 reporter: reporting.Reporter=None
                 ):
        self.codec = codec
        self.session = store.open(indexname, writable=True)
        self.schema = schema
        self.generation = generation

        self.doc_limit = doc_limit
        self.post_limit = post_limit
        self.merge_strategy = merge_strategy or merging.default_strategy()
        self.executor = executor
        self.reporter = reporter or reporting.default_reporter()

        # This object keeps track of the current segments, buffered deletions,
        # and ongoing merges
        self.seglist = segmentlist.SegmentList(self.session, self.schema,
                                               segments)

        # Low-level object that knows how to write a single segment
        self.segwriter = None  # type: SegmentWriter
        self._start_new_segment()

        self.closed = False
        self._group_depth = 0

        # The user can set these flags while writing to tell the writer what to
        # do when it commits
        self.merge = True
        self.optimize = False

    def _start_new_segment(self):
        segment = self.codec.new_segment(self.session)
        self.segwriter = SegmentWriter(self.codec, self.session, segment,
                                       self.schema)

    # User API

    @unclosed
    def delete_by_term(self, fieldname: str, text: str):
        """
        Deletes any documents containing "term" in the "fieldname" field.
        This is useful when you have an indexed field containing a unique ID
        (such as "pathname") for each document.

        :param fieldname: the name of the field containing the term.
        :param termbytes: the bytestring of the term to delete.
        :returns: the number of documents deleted.
        """

        from whoosh.query import Term
        return self.delete_by_query(Term(fieldname, text))

    @unclosed
    def delete_by_query(self, q: 'queries.Query'):
        """
        Deletes any documents matching a query object.

        :param q: delete documents which match this query.
        """

        # Tell the SegmentList to buffer the deletions on existing segments and
        # remember to apply the deletion to in-progress merges
        self.seglist.delete_by_query(q)

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

        # Tell the low-level segment writer we're starting a new document
        segwriter = self.segwriter
        segwriter.start_document()
        index_field = segwriter.index_field

        # This method processes data and "special" argument passed in the
        # keyword arguments and passes the actual data down to the lower-level
        # SegmentWriter

        # Separate actual field names from "special" arguments that start with
        # an underscore
        fieldnames = sorted([name for name in kwargs
                             if not name.startswith("_")])

        # You can pass _doc_boost=2.0 to multiply the boost on all fields
        doc_boost = kwargs.get("_boost", 1.0)

        eol_dt = None
        if "_ttl" in kwargs:
            eol_dt = datetime.utcnow() + timedelta(seconds=kwargs["_ttl"])
        elif "_eol" in kwargs:
            eol_dt = datetime.utcnow()
        if eol_dt is not None:
            segwriter.add_column_value(EOL_FIELDNAME, EOL_COLUMN,
                                       times.datetime_to_long(eol_dt))

        for fieldname in fieldnames:
            try:
                field = self.schema[fieldname]
            except KeyError:
                raise ValueError("No %r field in schema" % fieldname)

            # Get the value from the keyword argument
            value = kwargs.get(fieldname)
            # You can pass _stored_fieldname to set the stored value differently
            # from the indexed value
            stored_val = kwargs.get("_stored_" + fieldname, value)

            # You can pass _fieldname_boost=2.0 to set the boost for all
            # postings in the field
            boost = (field.field_boost * doc_boost *
                     kwargs.get("_%s_boost" % fieldname, 1.0))

            # Pass the information down to the SegmentWriter
            index_field(fieldname, value, stored_val, boost)

            # If the field has sub-fields, index them with the same values
            for subname, subfield in field.subfields(fieldname):
                index_field(subname, value, stored_val)

        # Tell the SegmentWriter we're done with this document
        segwriter.finish_document()

        should_flush = (
            segwriter.doc_count >= self.doc_limit or
            segwriter.post_count >= self.post_limit
        )

        if should_flush:
            self.flush_segment()

    @unclosed
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

        # Delete any documents having the same values in "unique" fields as the
        # incoming document
        self._delete_for_update(kwargs)

        # Add the given fields
        self.add_document(**kwargs)

    def _delete_for_update(self, kwargs):
        from whoosh.query import Or, Term

        # Delete the set of documents matching the unique terms
        qs = []
        for fieldname, fieldobj in self.schema.items():
            if fieldname in kwargs and fieldobj.unique:
                qs.append(Term(fieldname, kwargs[fieldname]))
        if qs:
            self.delete_by_query(Or(qs))

    @unclosed
    def add_reader(self, reader: 'readers.IndexReader'):
        """
        Adds the contents of the given reader to this index.

        :param reader: the reader to add.
        """

        newsegment = self.codec.new_segment(self.session)
        newsegment = merging.copy_reader(reader, self.codec, self.session,
                                         self.schema, newsegment)
        self.seglist.add_segment(newsegment)
        self._try_merging()

    # Flush

    @unclosed
    def flush_segment(self, merge: bool=None, optimize: bool=None,
                      expunge_deleted: bool=False, restart: bool=True):
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

        if self._group_depth:
            # Can't flush a new segment while we're in a group, since we need
            # to guarantee that all docs in the same group are in the same
            # segment
            return

        logger.info("Flushing current segment")

        # Should we try to merge after integrating the new segment?
        merge = merge if merge is not None else self.merge
        # Should we try to merge ALL segments after integrating the new segment?
        optimize = optimize if optimize is not None else self.optimize

        # The actual implementation is in a separate method so MultiWriter can
        # easily override it without having to duplicate the code above and
        # below
        self._implement_flush(merge, optimize, expunge_deleted)

        if restart:
            self._start_new_segment()

    def _implement_flush(self, merge, optimize, expunge_deleted):
        # Subclasses that flush documents differently (MultiWriter) can override
        # this method

        newsegment = self.segwriter.finish_segment()

        # Add the new segment to the segment list
        self.seglist.add_segment(newsegment)
        self._maybe_merge(merge, optimize, expunge_deleted)

    # Merge methods

    def _maybe_merge(self, merge: bool, optimize: bool,
                     expunge_deleted: bool):
        if optimize and len(self.seglist) > 1:
            logger.info("Optimizing")
            # Create a merge with every segment
            merge_obj = merging.Merge(list(self.seglist.segments))
            # Start the merge
            self._start_merge(merge_obj)

        elif merge:
            logger.info("Trying to merge after flush")
            self._try_merging(expunge_deleted=expunge_deleted)

        else:
            logger.info("Optimize and merge are off, doing nothing")

    def _find_merges(self, expunge_deleted: bool = False
                     ) -> Sequence[merging.Merge]:
        # Use the MergeStrategy object to decide which segments, if any, we
        # should merge

        strategy = self.merge_strategy
        merging_segment_ids = self.seglist.merging_ids()
        merges = strategy.get_merges(self.seglist.segments,
                                     merging_segment_ids,
                                     expunge_deleted=expunge_deleted)
        return merges

    def _try_merging(self, expunge_deleted: bool=False):
        # If there are any merges to do, do them. This gets the list of merges
        # to perform from _find_merges and calls start_merge on each one.

        logger.info("Trying to merge")
        merges = self._find_merges(expunge_deleted=expunge_deleted)
        logger.info("Found merges %r", merges)
        for merge in merges:
            self._start_merge(merge)

    def _start_merge(self, merge: merging.Merge):
        logger.info("Starting merge %r", merge)

        # Sanity check
        ids_to_merge = set(seg.segment_id() for seg in merge.segments)
        conflicted = ids_to_merge & self.seglist.merging_ids()
        if conflicted:
            raise Exception("Segments %r are already merging" % conflicted)

        # Make a new segment for the merge
        newsegment = self.codec.new_segment(self.session)

        # Tell the SegmentList about the merge
        self.seglist.add_merge(merge)

        # If we have an executor, schedule the merge on it; otherwise, just
        # perform the merge serially right now
        if self.executor:
            logger.info("Submitting merge %r to %r", merge, self.executor)

            store = self.session.store
            if store.supports_multiproc_writing():
                # The storage supports recursive locks, so use a version of the
                # merge function that takes a recursive lock
                args = (
                    merging.perform_r_merge, self.codec, self.session.store,
                    self.schema, merge, newsegment, self.session.read_key(),
                    self.session.indexname
                )
            else:
                # We don't support multi-processing, but if this is a threading
                # executor we will try to pass the session between threads
                args = (
                    merging.perform_merge, self.codec, self.session,
                    self.schema, merge, newsegment
                )
            # Submit the job to the executor
            future = self.executor.submit(*args)

            # Add a callback to complete the merge when the future finishes
            def merge_callback(f):
                newsegment, merge_id = f.results()
                self.seglist.integrate(newsegment, merge_id)
            future.add_done_callback(merge_callback)

        else:
            # Do the merge serially now
            logger.info("Performing serial merge of %r", merge)
            newsegment, merge_id = merging.perform_merge(
                self.codec, self.session, self.schema, merge, newsegment
            )
            self.seglist.integrate(newsegment, merge_id)
            logger.info("Finished serial merge of %r", merge)

    # Commit/cancel/close methods

    @unclosed
    def commit(self, merge: bool=None, optimize: bool=None):
        """
        Finishes writing and unlocks the index.

        :param merge: Try to merge segments after flushing. Skipping merging
            is faster but eventually will fill up the index with small segments.
        :param optimize: Merge more aggressively.
        """

        logger.info("Finished indexing, starting commit")
        merge = merge if merge is not None else self.merge
        optimize = optimize if optimize is not None else self.optimize

        # If there are any documents sitting in the SegmentWriter, flush them
        # out into a new segment
        if self.segwriter.doc_count:
            self.flush_segment(merge=merge, optimize=optimize, restart=False)
        elif optimize:
            self._maybe_merge(merge=merge, optimize=optimize,
                              expunge_deleted=False)

        # Wait for background jobs to complete
        if self.executor:
            logger.info("Waiting for background jobs to complete")
            self.executor.shutdown(wait=True)

        # Sync the TOC to storage
        self._sync_toc()
        self._close()

    @unclosed
    def cancel(self):
        """
        Cancels any documents/deletions added by this object and unlocks the
        index.
        """

        # Close the codec writers
        logger.info("Cancelling writer")
        self.segwriter.cancel()
        # self.store.cleanup(self.session)
        self._close()

    def _sync_toc(self):
        from whoosh.index import Toc

        logger.info("Syncing TOC to storage")
        self.seglist.save_all_buffered_deletes()
        toc = Toc(
            schema=self.schema, segments=self.seglist.segments,
            generation=self.generation
        )
        self.session.store.save_toc(self.session, toc)

    def _close(self):
        # Release the lock if we have one
        if self.session:
            self.session.close()
        self.closed = True

    # Convenience methods

    # Context manager protocol
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not exc_type:
            self.commit()

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

        self._group_depth += 1

    def end_group(self):
        """
        Finish indexing a group of hierarchical documents. See
        :meth:`~SegmentWriter.start_group`.
        """

        self._group_depth -= 1

    # Schema modification methods

    @unclosed
    def add_field(self, fieldname: str, field: 'fields.FieldType'):
        """
        Adds a field to the index's schema.

        :param fieldname: the name of the field to add.
        :param field: an instantiated :class:`whoosh.fields.FieldType`
            object.
        """

        self.schema.add(fieldname, field)

    @unclosed
    def remove_field(self, fieldname):
        """
        Removes the named field from the index's schema. Depending on the
        backend implementation, this may or may not actually remove existing
        data for the field from the index. Optimizing the index should always
        clear out existing data for a removed field.

        :param fieldname: the name of the field to remove.
        """

        self.schema.remove(fieldname)

    # Reading methods

    @unclosed
    def reader(self, **kwargs):
        """
        Returns a reader for the existing index.

        :param kwargs: keyword arguments passed to the index's reader() method.
        """

        return self.seglist.multireader()

    @unclosed
    def searcher(self, **kwargs) -> 'searchers.Searcher':
        """
        Returns a searcher for the existing index.

        :param kwargs: keyword arguments passed to the index's reader() method.
        """

        from whoosh import searching

        return searching.ConcreteSearcher(self.reader(), **kwargs)


