import logging
import os
import pickle
import typing
from collections import defaultdict
from concurrent import futures
from contextlib import contextmanager
from datetime import datetime, timedelta
from tempfile import mkstemp
from typing import Any, List, Sequence

from whoosh import columns, fields, storage
from whoosh.codec import codecs
from whoosh.writing import merging, reporting, segmentlist
from whoosh.postings.ptuples import PostTuple, TERMBYTES, DOCID
from whoosh.util import now, times, unclosed

# Typing imports
if typing.TYPE_CHECKING:
    from whoosh import index, query, reading, searching


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
        self._perdoc.close()
        self._terms.close()

        segment = self.segment
        self.codec.finish_segment(self.session, segment)
        return segment

    def cancel(self):
        self._perdoc.close()
        self._terms.close()


class SpoolingWriter(SegmentWriter):
    """
    This implements the SegmentWriter interface but all it does is record the
    calls to `index_field()` so they can be played back in a separate process
    for parallel indexing.
    """

    def __init__(self, segment):
        self.segment = segment
        self.doc_count = 0
        self.post_count = 0

        fd, self.filepath = mkstemp(suffix=".pickle", prefix="multi")
        self._tempfile = os.fdopen(fd, "wb")

        self._arglist = []

    def start_document(self):
        self._arglist = []

    def index_field(self, fieldname: str, value: Any, stored_val: Any,
                    boost=1.0):
        self._arglist.append((fieldname, value, stored_val, boost))

    def finish_document(self):
        pickle.dump(self._arglist, self._tempfile, -1)
        self.doc_count += 1

    def finish_segment(self):
        self._tempfile.close()
        return self.filepath, self.segment

    def cancel(self):
        self._tempfile.close()
        os.remove(self.filepath)


# High-level writer object manages writing and merging multiple segments in an
# indexing session

class IndexWriter:
    def __init__(self, for_index: 'index.Index',
                 executor: futures.Executor=None, multiproc: bool=False,
                 multithreaded: bool=False, procs: int=None, threads: int=None,
                 codec: 'codecs.Codec'=None, schema: 'fields.Schema'=None,
                 doc_limit: int=10000, post_limit: int=2000000,
                 merge_strategy: merging.MergeStrategy=None,
                 reporter: reporting.Reporter=None, merge: bool=True,
                 optimize: bool=False):
        """
        :param for_index: the `index.Index` object representing the index to
            write into.
        :param executor: a `futures.Executor` object to use for multi-writing.
            If you don't supply one, the object will create one if necessary
            based on the other arguments. Note that even if you supply a
            pre-configured Executor, you still need to turn on `multiproc` or
            `multithreaded` to enable multi-writing.
        :param multiproc: whether this object should use multiprocessing.
        :param multithreaded: whether this object should use multithreading. If
            you set both `multiproc` and `multithreaded` to `True`, the object
            will use multiprocessing.
        :param procs: when `multiproc` is `True`, the number of processors to
            use in the pool. The default is None, which uses the executor's
            default, the number of CPUs.
        :param threads: when `multithreaded` is `True`, the number of threads to
            use in the pool. The default is None, which uses the executor's
            default, the number of CPUs.
        :param codec: write to the index using this codec instead of the
            default.
        :param schema: index using this schema instead of the default.
        :param doc_limit: the maximum number of documents to spool before
            flushing a segment to storage.
        :param post_limit: the maximum number of postings to spool before
            flushing a segment to storage.
        :param merge_strategy: use this merge strategy to decide how to merge
            segments, overriding the default.
        :param reporter: supply a Reporter object to get feedback on writing
            progress.
        """

        from whoosh.codec import default_codec

        toc = for_index.toc
        self.codec = codec or default_codec()
        self.store = for_index.storage()
        self.session = self.store.open(for_index.indexname, writable=True)
        self.schema = schema or for_index.schema
        self.generation = toc.generation + 1

        self.original_doc_limit = self.doc_limit = doc_limit
        self.original_post_limit = self.post_limit = post_limit
        self.merge_strategy = merge_strategy or merging.default_strategy()
        self.executor = executor
        self.reporter = reporter or reporting.null_reporter()

        # This object keeps track of the current segments, buffered deletions,
        # and ongoing merges
        self.seglist = segmentlist.SegmentList(self.session, self.schema,
                                               list(toc.segments))
        self._futures = []

        self.closed = False
        self._cancelled = False
        self._group_depth = 0
        self._optimized_segment_count = 1

        # The user can set these flags while writing to tell the writer what to
        # do when it commits
        self.merge = merge
        self.optimize = optimize

        self._is_multi = False
        self.set_execution(executor, multiproc, multithreaded, procs, threads)

        # Low-level object that knows how to write a single segment
        self.segwriter = None  # type: SegmentWriter
        # self._start_new_segment()

    def _start_new_segment(self):
        segment = self.codec.new_segment(self.session)
        self.reporter._start_new_segment(list(self.seglist.segments), segment)
        if self._is_multi:
            w = SpoolingWriter(segment)
        else:
            w = SegmentWriter(self.codec, self.session, segment, self.schema)
        self.segwriter = w

    def _get_segwriter(self):
        if self.segwriter is None:
            self._start_new_segment()
        return self.segwriter

    # User API

    def set_execution(self, executor: futures.Executor=None,
                      multiproc: bool=False, multithreaded: bool=False,
                      procs: int=None, threads: int=None):
        """
        Sets up this writer for multiprocessing/multithreading (or not). This is
        the same as the arguments to the initializer, however this method allows
        you to change the set up after creating the writer object.

        :param executor: a `futures.Executor` object to use. If you don't supply
            one, the object will create one if necessary based on the other
            arguments.
        :param multiproc: whether this object should use multiprocessing.
        :param multithreaded: whether this object should use multithreading. If
            you set both `multiproc` and `multithreaded` to `True`, the object
            will use multiprocessing.
        :param procs: when `multiproc` is `True`, the number of processors to
            use in the pool. The default is None, which uses the executor's
            default, the number of CPUs.
        :param threads: when `multithreaded` is `True`, the number of threads to
            use in the pool. The default is None, which uses the executor's
            default, the number of CPUs.
        """

        if multiproc and not self.store.supports_multiproc_writing():
            raise Exception("%r does not support multiproc writing")

        if multiproc or multithreaded:
            self._is_multi = True

            if multiproc:
                workers = procs or os.cpu_count() or 1
            elif multithreaded:
                workers = threads or os.cpu_count() or 1
            self.doc_limit = max(self.original_doc_limit // workers, 10)
            self.post_limit = max(self.original_post_limit // workers, 1000)

            if not executor:
                if multiproc:
                    executor = futures.ProcessPoolExecutor(procs)
                elif multithreaded:
                    executor = futures.ThreadPoolExecutor(threads)

        self.executor = executor

    def is_mulitwriter(self):
        return self._is_multi and self.executor

    @unclosed
    def clear(self):
        self.seglist.clear()
        self.reporter._cleared_segments()

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
    def delete_by_query(self, q: 'query.Query'):
        """
        Deletes any documents matching a query object.

        :param q: delete documents which match this query.
        """

        # Tell the SegmentList to buffer the deletions on existing segments and
        # remember to apply the deletion to in-progress merges
        self.reporter._delete_by_query(q)
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
        self.reporter._start_document(kwargs)
        segwriter = self._get_segwriter()

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

        schema = self.schema

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
                field = schema[fieldname]
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
        self.reporter._finish_document()
        segwriter.finish_document()

        if (
            segwriter.doc_count >= self.doc_limit or
            segwriter.post_count >= self.post_limit
        ):
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
    def add_reader(self, reader: 'reading.IndexReader'):
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
    def flush_segment(self, merge: bool=None, expunge_deleted: bool=False,
                      restart: bool=True):
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

        segwriter = self.segwriter
        if segwriter is None or not segwriter.doc_count:
            return

        logger.info("Flushing current segment")
        # Should we try to merge after integrating the new segment?
        merge = merge if merge is not None else self.merge

        if self._is_multi:
            self._flush_multi(merge, False, expunge_deleted)
        else:
            newsegment = segwriter.finish_segment()
            # Add the new segment to the segment list
            self.seglist.add_segment(newsegment)
            self.reporter._finish_segment(list(self.seglist.segments),
                                          newsegment)
            if merge:
                self._try_merging(False, expunge_deleted)

        self.segwriter = None
        # if restart:
        #     self._start_new_segment()

    def _flush_multi(self, merge, optimize, expunge_deleted):
        count = self.segwriter.doc_count
        filepath, newsegment = self.segwriter.finish_segment()

        logger.info("Submitting parallel segment flush of %r to %r",
                    newsegment, self.executor)
        store = self.session.store
        if store.supports_multiproc_writing():
            # The storage supports recursive locks, so use a version of the
            # merge function that takes a recursive lock
            sessionkey = self.session.read_key()
            assert sessionkey is not None
            args = (
                batch_r_index,
                filepath, count, self.codec, store, self.schema, newsegment,
                sessionkey, self.session.indexname
            )
        else:
            # We don't support multi-processing, but if this is a threading
            # executor we will try to pass the session between threads
            args = (
                batch_index,
                filepath, count, self.codec, self.session, self.schema,
                newsegment
            )

        future = self.executor.submit(*args)
        self._futures = [f for f in self._futures if not f.done()]
        self._futures.append(future)

        # Add a callback to complete adding the segment when the future finishes
        def multi_flush_callback(f):
            newsegment = f.result()
            self.seglist.add_segment(newsegment)
            self.reporter._finish_segment(list(self.seglist.segments),
                                          newsegment)
            if merge:
                self._try_merging(optimize, expunge_deleted)
        future.add_done_callback(multi_flush_callback)

    # Merge methods

    def _find_merges(self, optimize: bool=False, expunge_deleted: bool=False
                     ) -> Sequence[merging.Merge]:
        # Use the MergeStrategy object to decide which segments, if any, we
        # should merge

        strategy = self.merge_strategy
        merging_segment_ids = self.seglist.merging_ids()
        if optimize:
            merges = strategy.get_forced_merges(self.seglist.segments,
                                                self._optimized_segment_count,
                                                merging_segment_ids)
        else:
            merges = strategy.get_merges(self.seglist.segments,
                                         merging_segment_ids,
                                         expunge_deleted=expunge_deleted)
        return merges

    def _try_merging(self, optimize: bool=False, expunge_deleted: bool=False):
        # If there are any merges to do, do them. This gets the list of merges
        # to perform from _find_merges and calls start_merge on each one.

        logger.info("Trying to merge (optimize=%s)", optimize)
        merges = self._find_merges(optimize=optimize,
                                   expunge_deleted=expunge_deleted)
        logger.info("Found merges %r", merges)
        for merge in merges:
            self._start_merge(merge)

    def _start_merge(self, merge: merging.Merge):
        from whoosh.filedb.filestore import BaseFileStorage

        logger.info("Starting merge %r", merge)

        # Sanity check
        ids_to_merge = set(seg.segment_id() for seg in merge.segments)
        conflicted = ids_to_merge & self.seglist.merging_ids()
        if conflicted:
            raise Exception("Segments %r are already merging" % conflicted)

        # Make a new segment for the merge
        newsegment = self.codec.new_segment(self.session)
        self.reporter._start_merge(merge.merge_id, list(merge.segments),
                                   newsegment.segment_id())

        # Tell the SegmentList about the merge
        self.seglist.add_merge(merge)

        # If we have an executor, schedule the merge on it; otherwise, just
        # perform the merge serially right now
        if self.executor:
            store = self.session.store
            if isinstance(store, BaseFileStorage):
                logger.info("Starting multi-merge of %r", merge)
                merging.perform_multi_merge(self.executor, self.codec, store,
                                            self.schema, merge, newsegment,
                                            self.session.read_key(),
                                            self.session.indexname)
                self.seglist.integrate(newsegment, merge.merge_id)
            else:
                logger.info("Submitting merge %r to %r", merge, self.executor)
                if store.supports_multiproc_writing():
                    # The storage supports recursive locks, so use a version of
                    # the merge function that takes a recursive lock
                    args = (
                        merging.perform_r_merge, self.codec, self.session.store,
                        self.schema, merge, newsegment, self.session.read_key(),
                        self.session.indexname
                    )
                else:
                    # The storage doesn't support multi-processing, but if this
                    # is a threading executor we will try to pass the session
                    # between threads
                    args = (
                        merging.perform_merge, self.codec, self.session,
                        self.schema, merge, newsegment
                    )
                # Submit the job to the executor
                future = self.executor.submit(*args)

                # Add a callback to complete the merge when the future finishes
                def merge_callback(f):
                    newsegment, mergeid = f.result()
                    self.reporter._finish_merge(
                        mergeid, self.seglist.merging_segments(mergeid),
                        newsegment
                    )
                    self.seglist.integrate(newsegment, mergeid)
                future.add_done_callback(merge_callback)

        else:
            # Do the merge serially now
            logger.info("Performing serial merge of %r", merge)
            newsegment, mergeid = merging.perform_merge(
                self.codec, self.session, self.schema, merge, newsegment
            )
            self.reporter._finish_merge(
                mergeid, self.seglist.merging_segments(mergeid), newsegment
            )
            self.seglist.integrate(newsegment, mergeid)
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
        self.reporter._committing(optimize)

        # If there are any documents sitting in the SegmentWriter, flush them
        # out into a new segment
        if self.segwriter and self.segwriter.doc_count:
            self.flush_segment(merge=merge, restart=False)

        # Wait for background jobs to complete. We do this separately from
        # shutting down the executor below, because finishing segments may still
        # add more work (merges) to the executor, and trying to submit work to
        # a shutting down executor is an error.
        futures.wait(self._futures)

        # Shut down segment list
        self.seglist.close()

        # Perform final merges/optimizations
        if merge or optimize:
            self._try_merging(optimize, False)

        # Shut down the executor
        if self.executor:
            logger.info("Waiting for background jobs to complete")
            self.executor.shutdown(wait=True)

        # Sync the TOC to storage
        logger.info("Syncing the new TOC to disk")
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
        if self.segwriter:
            self.segwriter.cancel()
        self.session.store.cleanup(self.session, all_tocs=False)
        self._close()
        self._cancelled = True

    def _sync_toc(self):
        from whoosh.index import Toc

        logger.info("Syncing TOC to storage")
        segments = [seg for seg in self.seglist.segments if not seg.is_empty()]
        toc = Toc(
            schema=self.schema, segments=segments,
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
            if not self._cancelled:
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

        return self.seglist.full_reader()

    @unclosed
    def searcher(self, **kwargs) -> 'searching.SearcherType':
        """
        Returns a searcher for the existing index.

        :param kwargs: keyword arguments passed to the index's reader() method.
        """

        from whoosh import searching

        return searching.Searcher(self.reader(), **kwargs)


# Helper functions for indexing from a batch file in a different thread/process

def batch_r_index(batch_filename: str,
                  count: int,
                  codec: 'codecs.Codec',
                  store: 'storage.Storage',
                  schema: 'fields.Schema',
                  newsegment: 'codecs.Segment',
                  key: int,
                  indexname: str
                  ) -> 'codecs.Segment':
    session = store.recursive_write_open(key, indexname)
    return batch_index(batch_filename, count, codec, session, schema,
                       newsegment)


def batch_index(batch_filename: str,
                count: int,
                codec: 'codecs.Codec',
                session: 'storage.Session',
                schema: 'fields.Schema',
                newsegment: 'codecs.Segment',
                ) -> 'codecs.Segment':
    logger.info("Batching indexing file %s to %r", batch_filename, newsegment)
    t = now()

    segwriter = SegmentWriter(codec, session, newsegment, schema)

    # The batch file contains a series of pickled lists of arguments to
    # SegmentWriter.index_field
    with open(batch_filename, "rb") as f:
        for _ in range(count):
            arg_list = pickle.load(f)
            segwriter.start_document()
            for fieldname, value, stored_val, boost in arg_list:
                segwriter.index_field(fieldname, value, stored_val, boost)
            segwriter.finish_document()

    # Get the finished segment from the segment writer
    newsegment = segwriter.finish_segment()

    # Delete the used up batch file
    os.remove(batch_filename)

    logger.info("Batch indexed %s to %r in %0.06f s",
                batch_filename, newsegment, now() - t)

    return newsegment
