import logging
import typing
from functools import wraps
from threading import RLock
from typing import Dict, Iterable, List, Sequence, Set

from whoosh import fields, storage
from whoosh.query import queries
from whoosh.codec import codecs
from whoosh.writing import merging

# Typing imports
if typing.TYPE_CHECKING:
    from whoosh import reading


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
        # Cache readers for the segments for computing deletions, keyed by the
        # segment ID
        self._cached_readers = {}  # type: Dict[str, reading.IndexReader]

        # Lock around modifying operations to prevent threaded multiwriter from
        # messing up internal book-keeping
        self._lock = RLock()

        self.readerclass = SegmentReader

    def __len__(self):
        return len(self.segments)

    @synchronized
    def clear(self):
        self.segments = []
        # self._current_merges = {}
        self._cached_readers = {}

    @synchronized
    def merging_ids(self) -> Set[str]:
        ids = set()
        for merge in self._current_merges.values():
            ids.update(merge.segment_ids())
        return ids

    @synchronized
    def add_segment(self, segment: 'codecs.Segment'):
        logger.info("Adding %r to segments", segment)
        if segment.is_empty():
            logger.info("Not added because the segment is empty")
        else:
            self.segments.append(segment)

    @synchronized
    def remove_segment(self, segment: 'codecs.Segment'):
        segid = segment.segment_id()
        logger.info("Removing %r from segments", segment)
        # Close and remove the cached reader if it exists
        if segid in self._cached_readers:
            self._cached_readers.pop(segid).close()

        # Remove segment from segments list
        for i in range(len(self.segments)):
            if self.segments[i].segment_id() == segment.segment_id():
                del self.segments[i]
                break
        else:
            raise KeyError("Segment %s not in list" % segid)

    @synchronized
    def add_merge(self, mergeobj: merging.Merge):
        # A merge object represents a promise that eventually the SegmentList
        # will get a follow-up call to integrate() with the segment resulting
        # from the merge and the merge ID so the SegmentList can remove the
        # merge from its list of ongoing merges

        logger.info("Adding merge %r" % mergeobj)
        assert mergeobj.merge_id not in self._current_merges
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
            self.apply_query_deletions(newsegment, mergeobj.delete_queries)

        # Try to delete the merged-out segments from storage
        store = self.session.store
        for segment in dropped:
            logger.info("Deleting merged segment %s", segment)
            store.clean_segment(self.session, segment)

    @synchronized
    def segment_reader(self, segment: 'codecs.Segment'
                       ) -> 'reading.IndexReader':
        segid = segment.segment_id()
        try:
            return self._cached_readers[segid]
        except KeyError:
            r = self.readerclass(self.session.store, self.schema, segment)
            self._cached_readers[segid] = r
        return r

    @synchronized
    def full_reader(self, segments: 'Sequence[codecs.Segment]'=None
                    ) -> 'reading.IndexReader':
        from whoosh import reading

        segments = segments or self.segments  # type: Sequence[codecs.Segment]
        rs = [self.segment_reader(seg) for seg in segments]

        if not rs:
            return reading.EmptyReader()

        if len(rs) == 1:
            return rs[0]
        else:
            return reading.MultiReader(rs)

    @synchronized
    def apply_query_deletions(self, segment: 'codecs.Segment',
                              qs: 'Sequence[queries.Query]'):
        # Create a searcher around the given segment
        from whoosh.searching import Searcher

        logger.info("Applying deletion queries %r to segment %r", qs, segment)
        docids = set()
        r = self.segment_reader(segment)
        with Searcher(r, closereader=False) as s:
            # Iterate through the given queries, find the corresponding
            # documents, and add them to the buffered deletions
            for q in qs:
                docids.update(q.docs(s, deleting=True))

        if docids:
            logger.debug("Deleting docset %s from segment %r", docids, segment)
            segment.delete_documents(docids)
            if segment.is_empty():
                self.remove_segment(segment)
                logger.debug("Removed empty segment %r", segment)

    @synchronized
    def delete_by_query(self, q: 'queries.Query'):
        # For current segments, run the query and apply the deletions
        for segment in self.segments:
            self.apply_query_deletions(segment, (q,))

        # For ongoing merges, remember to perform this deletion when they're
        # finished
        for mergeobj in self._current_merges.values():
            mergeobj.delete_queries.append(q)

    @synchronized
    def close(self):
        # Close any cached readers
        for reader in self._cached_readers.values():
            reader.close()

    # Testing methods

    def test_is_deleted(self, segment: 'codecs.Segment', docnum: int):
        # This is useful for external testing... returns True if the given
        # docnum is deleted in the given segment

        segid = segment.segment_id()
        for seg in self.segments:
            if seg.segment_id() == segid:
                return seg.is_deleted(docnum)
        raise ValueError("Segment %r is not in this list" % segment)


