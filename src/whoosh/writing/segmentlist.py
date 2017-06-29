import logging
from functools import wraps
from threading import RLock
from typing import Dict, Iterable, List, Sequence, Set

from whoosh import fields
from whoosh.ifaces import codecs, queries, readers, storage
from whoosh.writing import merging
from whoosh.writing.reporting import Reporter


logger = logging.getLogger(__name__)


def synchronized(orig):
    @wraps(orig)
    def inner(self, *args, **kwargs):
        with self._lock:
            return orig(self, *args, **kwargs)
    return inner


class SegmentList:
    """
    This object keeps track of the list of segments in the index during writing,
    as newly indexed segments are added and existing segments are merged into
    new segments.

    It also buffers deletions on segments; the deletions are applied to the
    segment when it is written back to disk at the end of the write session.
    While the default codec's segment deletion tracking is fast (it's just a
    set() object), another codec might have a slower/less volatile
    implementation that would benefit from "batching up" deletions.
    """

    def __init__(self, session: 'storage.Session', schema: 'fields.Schema',
                 segments: 'List[codecs.Segment]'):
        from whoosh.reading import SegmentReader

        # Store these on the SegmentList so we can make readers for deletion
        # queries... otherwise we'd have to pass them with every method
        # related to deletion, which is a bit tedious
        self.session = session
        self.schema = schema

        # Current segments
        self.segments = segments

        # Ongoing merges, keyed by the merge ID
        self._current_merges = {}  # type: Dict[str, merging.Merge]
        # Cache readers for the segments for computing deletions
        self._cached_readers = {}  # type: Dict[str, readers.IndexReader]

        # Buffer deletes in memory before applying them to the segment, to
        # "batch up" changes to the segment instead of doing them one document
        # at a time (just in case the codec's implementation of saving deletions
        # is slow)
        self._buffered_deletes = {}  # type: Dict[str, Set[int]]

        # Lock around modifying operations to prevent threaded multiwriter from
        # messing up internal book-keeping
        self._lock = RLock()

        self.readerclass = SegmentReader

    def __len__(self):
        return len(self.segments)

    @synchronized
    def merging_ids(self) -> Set[str]:
        ids = set()
        for merge in self._current_merges.values():
            ids.update(merge.segment_ids())
        return ids

    @synchronized
    def add_segment(self, segment: 'codecs.Segment',
                    buffered_deletes: Set[int]=None):
        logger.info("Adding %r to segments", segment)
        self.segments.append(segment)

        buffered_deletes = buffered_deletes or set()
        segid = segment.segment_id()
        self._buffered_deletes[segid] = buffered_deletes

    @synchronized
    def remove_segment(self, segment: 'codecs.Segment'):
        segid = segment.segment_id()
        logger.info("Removing %r from segments", segment)
        # Close and remove the cached reader if it exists
        if segid in self._cached_readers:
            self._cached_readers.pop(segid).close()

        # Remove the buffered deletes set. It would be nice if we could
        # detect errors by making sure it's empty, but it might legit have
        # leftover deletions if they were buffered while the segment was
        # merging
        self._buffered_deletes.pop(segid, None)

        # Remove segment from segments list
        for i in range(len(self.segments)):
            if self.segments[i].segment_id() == segment.segment_id():
                del self.segments[i]
                break
        else:
            raise KeyError("Segment %s not in list" % segid)

    @synchronized
    def save_buffered_deletes(self, segment: 'codecs.Segment'):
        # Apply any buffered deletions for the given segment to the segment
        segid = segment.segment_id()
        buffered = self._buffered_deletes.pop(segid, None)
        if buffered:
            segment.delete_documents(buffered)

    @synchronized
    def save_all_buffered_deletes(self):
        for segment in self.segments:
            self.save_buffered_deletes(segment)

    @synchronized
    def add_merge(self, mergeobj: merging.Merge):
        # A merge object represents a promise that eventually the SegmentList
        # will get a follow-up call to integrate() with the segment resulting
        # from the merge and the merge ID so the SegmentList can remove the
        # merge from its list of ongoing merges

        logger.info("Adding merge %r" % mergeobj)
        for segment in mergeobj.segments:
            self.save_buffered_deletes(segment)
        self._current_merges[mergeobj.merge_id] = mergeobj

    @synchronized
    def integrate(self, newsegment: 'codecs.Segment', merge_id: str):
        # This method is called when the job represented by merge_id has
        # completed and it's time to add the newly finished segment to the list
        # of current segments

        logger.info("Integrating %r (merge ID %r) into segments",
                    newsegment, merge_id)
        # Get the previously added merge object by its ID
        mergeobj = self._current_merges.pop(merge_id)

        # Remove the merged segments from the list of current segments
        segids = set(mergeobj.segment_ids())
        dropped = [s for s in self.segments if s.segment_id() in segids]
        self.segments = [s for s in self.segments
                         if s.segment_id() not in segids]

        # Add the new segment
        self.add_segment(newsegment)

        # Apply queued query deletes to the new segment
        if mergeobj.delete_queries:
            self.buffer_query_deletions(newsegment, mergeobj.delete_queries)

        # Try to delete the merged-out segments from storage
        store = self.session.store
        for segment in dropped:
            store.clean_segment(segment)

    @synchronized
    def reader(self, segment: 'codecs.Segment') -> 'readers.IndexReader':
        # Apply any pending deletions to the segment before we open a reader
        # for it, so the reader reflects the changes
        self.save_buffered_deletes(segment)

        segid = segment.segment_id()
        try:
            return self._cached_readers[segid]
        except KeyError:
            r = self.readerclass(self.session.store, self.schema, segment)
            self._cached_readers[segid] = r
        return r

    @synchronized
    def multireader(self, segments: 'Sequence[codecs.Segment]'=None,
                    ) -> 'readers.IndexReader':
        from whoosh import reading

        segments = segments or self.segments  # type: Sequence[codecs.Segment]
        rs = [self.reader(seg) for seg in segments]
        assert rs
        if len(rs) == 1:
            return rs[0]
        else:
            return reading.MultiReader(rs)

    @synchronized
    def buffer_query_deletions(self, segment: 'codecs.Segment',
                               qs: 'Iterable[queries.Query]'):
        # Create a searcher around the given segment
        from whoosh.searching import ConcreteSearcher
        r = self.reader(segment)
        s = ConcreteSearcher(r)

        # Iterate through the given queries, find the corresponding documents,
        # and add them to the buffered deletions
        delbuf = self._buffered_deletes.setdefault(segment.segment_id(), set())
        for q in qs:
            delbuf.update(q.docs(s, deleting=True))

    @synchronized
    def delete_by_query(self, q: 'queries.Query'):
        # For current segments, run the query and buffer the deletions
        for segment in self.segments:
            self.buffer_query_deletions(segment, (q,))

        # For ongoing merges, remember to perform this deletion when they're
        # finished
        for mergeobj in self._current_merges.values():
            mergeobj.delete_queries.append(q)

    @synchronized
    def close(self):
        # Close any cached readers
        for reader in self._cached_readers.values():
            reader.close()

        # Save any buffered deletions to the segments
        self.save_all_buffered_deletes()

    # Testing methods

    def test_is_deleted(self, segment: 'codecs.Segment', docnum: int):
        # This is useful for external testing... returns True if the given
        # docnum is deleted in the given segment

        segid = segment.segment_id()
        if docnum in self._buffered_deletes[segid]:
            return True

        for seg in self.segments:
            if seg.segment_id() == segid:
                return seg.is_deleted(docnum)
        raise ValueError("Segment %r is not in this list" % segment)

