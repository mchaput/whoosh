# Copyright 2015 Matt Chaput. All rights reserved.
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

from __future__ import division
import logging
import shutil
import typing
from abc import abstractmethod
from concurrent import futures
from math import ceil
from operator import itemgetter
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union

from whoosh import fields
from whoosh.postings.ptuples import post_docid, update_post
from whoosh.util import now, random_name

# Typing imports
if typing.TYPE_CHECKING:
    from whoosh import reading, storage
    from whoosh.codec import codecs
    from whoosh.filedb.filestore import BaseFileStorage


logger = logging.getLogger(__name__)


class Merge:
    def __init__(self, segments: 'Sequence[codecs.Segment]'):
        assert segments
        self.merge_id = random_name(12)
        self.segments = list(segments)
        self.delete_queries = []

    def __repr__(self):
        return "<%s %s %r>" % (
            type(self).__name__, self.merge_id, self.segments
        )

    def __eq__(self, other: 'Merge'):
        return isinstance(other, Merge) and self.segments == other.segments

    def __ne__(self, other: 'Merge'):
        return not self == other

    def __len__(self):
        return len(self.segments)

    def __bool__(self):
        return bool(self.segments)

    def __contains__(self, seg: 'codecs.Segment') -> bool:
        segid = seg.segment_id()
        return any(s.segment_id() == segid for s in self.segments)

    def add_segment(self, segment: 'codecs.Segment'):
        self.segments.append(segment)

    def segment_ids(self) -> Iterable[str]:
        for segment in self.segments:
            yield segment.segment_id()

    def before_size(self) -> int:
        """
        Returns the current (pre-merge) total size of the segments in this merge
        set.
        """

        return sum(s.size() for s in self.segments)

    def after_size(self) -> int:
        """
        Returns a rough estimate of the post-merge size by prorating the current
        sizes by the number of deleted documents.
        """

        undel_ratio = self.doc_count() / self.doc_count_all()
        return int(self.before_size() * undel_ratio)

    def doc_count(self) -> int:
        """
        Returns the total number of UNDELETED documents in the segments in this
        merge set.
        """

        return sum(s.doc_count() for s in self.segments)

    def doc_count_all(self) -> int:
        """
        Returns the total number of documents (deleted and undeleted) in the
        segments in this merge set.
        """

        return sum(s.doc_count_all() for s in self.segments)


class MergeStrategy:
    @abstractmethod
    def get_merges(self, segments: 'Sequence[codecs.Segment]',
                   merging: Union[Sequence[str], Set[str]],
                   expunge_deleted: bool=False) -> Iterable[Merge]:
        """
        Returns a sequence of 0 or more ``Merge`` objects representing merges
        to do.

        :param segments: the full list of existing segments.
        :param merging: the IDs of the segments that are already being merged.
        :param expunge_deleted: if True, try extra hard to remove deletions
            from the index.
        """

        raise NotImplementedError


class TieredMergeStrategy(MergeStrategy):
    """
    A simple logarithmic merge strategy, which allows a certain number of
    segments at each "level", and initiates a merge when there are "too many"
    segments at some level.
    """

    def __init__(self, max_at_once: int=10, per_tier: int=10,
                 deletion_boost: float=2.0, deleted_max_percent: float=10.0,
                 segment_size_floor: int=2 * 1024 * 1024,
                 max_merged_size: int=5 * 1024 * 1024 * 1024):
        """
        :param max_at_once: the maximum number of segments to merge at once.
        :param per_tier: the number of segments allowed at each tier. Smaller
            numbers mean more merging but fewer segments.
        :param deletion_boost: the higher this number, the more weight is given
            to merges that would clean deletions.
        :param deleted_max_percent: the maximum deletion percentage allowed when
            explicitly merging with expunge_deleted=True.
        :param segment_size_floor: segments smaller than this are rounded up to
            this size. This prevents many small segments from accumulating.
        :param max_merged_size: the maximum merged size for a segment.
        """

        self.max_at_once = max_at_once
        self.per_tier = per_tier
        self.deletion_boost = deletion_boost
        self.deleted_max_percent = deleted_max_percent
        self.segment_size_floor = segment_size_floor
        self.max_merged_size = max_merged_size

    def get_tidy_merges(self, segments: 'Sequence[codecs.Segment]',
                        merging: Union[Sequence[str], Set[str]],):
        segments = sorted(segments, reverse=True, key=lambda s: s.size())
        merges = []

        merge = None
        merge_size = 0
        for seg in segments:
            if seg.segment_id() in merging:
                continue
            if seg.size() < self.segment_size_floor:
                if merge is None:
                    merge = Merge([seg])
                    merge_size = seg.size()
                else:
                    merge.add_segment(seg)
                    merge_size += seg.size()
                    if len(merge) == self.max_at_once or \
                            merge_size >= self.segment_size_floor:
                        merges.append(merge)
                        merge = None
        if merge and len(merge) > 1:
            merges.append(merge)

        return merges

    def get_merges(self, segments: 'Sequence[codecs.Segment]',
                   merging: Union[Sequence[str], Set[str]],
                   expunge_deleted: bool=False) -> Sequence[Merge]:
        logger.info("Looking for merges in %r", segments)

        # Eliminate candidates if they're too large to merge
        segs = []
        for seg in segments:
            size = seg.size()
            logger.debug("Segment %r size=%d", seg, size)
            if size > self.max_merged_size / 2.0:
                logger.debug("Segment %r too big to merge", seg)
                pass
            else:
                segs.append((size, seg))

        # If there's nothing left, we can't merge
        if len(segs) <= 1:
            logger.debug("No candidates for merging")
            return ()

        # Order the segments by decreasing size
        segs.sort(reverse=True, key=itemgetter(0))
        # The total bytes in all segments
        totalsize = sum(s[0] for s in segs)
        logger.debug("Total size of all mergeable segments=%d", totalsize)
        # Find the minimum size
        minsize = max(self.segment_size_floor, segs[-1][0])
        logger.debug("Smallest floored size=%d", minsize)

        # Calculate the number of allowed segments
        levelsize = minsize
        remaining = totalsize
        allowed = 0
        logger.debug("Calculating allowed segments")
        while True:
            logger.debug("%d remaining to distribute", remaining)
            count = remaining / levelsize
            logger.debug("Levelsize %d can take %f segments", levelsize, count)

            if count < self.per_tier:
                logger.debug("All remaining fit at this level")
                allowed += ceil(count)
                break

            logger.debug("Add %d at this level", self.per_tier)
            allowed += self.per_tier

            logger.debug("Take away %d", self.per_tier * levelsize)
            remaining -= self.per_tier * levelsize

            logger.debug("New levelsize %d", levelsize * self.max_at_once)
            levelsize *= self.max_at_once

        logger.info("%d segments allowed", allowed)

        merges = []
        merging = frozenset(merging)
        to_merge = set()

        # Loop to find as many merges as needed
        while True:
            mergingsize = 0
            # Find available segments (segments that are not already merging)
            eligible = []
            for size, seg in segs:
                segid = seg.segment_id()
                if segid in merging:
                    # Segment is already merging
                    mergingsize += size
                elif segid in to_merge:
                    # Segment is already part of a previously generated merge
                    continue
                else:
                    eligible.append((size, seg))
            logger.info("%d eligible segments: %r", len(eligible), eligible)

            # If no segments are eligible, or if this number of segments is
            # allowed, we're done
            if not eligible or len(eligible) <= allowed:
                logger.info("No more merging necessary")
                return merges

            # Is there already a max merge running?
            already_maxed = mergingsize >= self.max_merged_size
            if already_maxed:
                logger.info("Already merging max bytes")

            # Find the best range to merge
            best_range = []  # type: List[codecs.Segment]
            best_score = -1
            # best_size = 0
            # best_too_large = False
            i = 0
            while i <= len(eligible) - 1:
                logger.debug("Try range starting at %d", i)
                thisrange = []  # type: List[Tuple[int, codecs.Segment]]
                range_size = 0
                range_too_large = False

                # Add segments to the range until we reach max_at_once
                j = i
                while j < len(eligible) and len(thisrange) < self.max_at_once:
                    size, seg = eligible[j]
                    logger.debug("Try adding seg at %d, size %d", j, size)
                    if range_size + size > self.max_merged_size:
                        logger.debug("Too large, can't add to this range")
                        # Adding this segment would make the merge too big
                        range_too_large = True
                        # Continue so we can try packing smaller segments
                        # into the leftover space
                        j += 1
                        continue

                    thisrange.append((size, seg))
                    range_size += size
                    logger.debug("Added, range=%r. size=%d",
                                 thisrange, range_size)
                    j += 1
                i += 1

                # The list should never be empty
                assert thisrange

                # A singleton merge with no deletions is not valid
                if len(thisrange) == 1:
                    single = thisrange[0][1]
                    if not single.has_deletions():
                        continue

                # Smaller scores are better
                score = self._score(thisrange, range_too_large)
                logger.debug("-- Range=%r score=%f", thisrange, score)

                if ((best_score < 0 or score < best_score) and
                        (not range_too_large or not already_maxed)):
                    # This is the best range yet
                    logger.debug("* New best range")
                    best_range = thisrange
                    best_score = score
                    # best_size = range_size
                    # best_too_large = range_too_large

            if best_range:
                best_segs = [s for size, s in best_range]
                to_merge.update(s.segment_id() for s in best_segs)

                m = Merge(best_segs)
                logger.info("Adding merge %r", m)
                merges.append(m)
            else:
                logger.info("No more merges found")
                return merges

    def get_forced_merges(self, segments: 'Sequence[codecs.Segment]',
                          max_segment_count: int,
                          merging: Union[Sequence[str], Set[str]],
                          ) -> Sequence[Merge]:
        logger.info("Looking for FORCED merges in %r", segments)

        # Eliminate candidates if they're too large to merge
        segs = []  # type: List[Tuple[int, codecs.Segment]]
        merging = frozenset(merging)
        for seg in segments:
            size = seg.size()
            logger.debug("Segment %r size=%d", seg, size)
            if size >= self.max_merged_size:
                logger.debug("Segment %r too big to merge", seg)
                pass
            elif seg.segment_id() in merging:
                continue
            else:
                segs.append((size, seg))
        # print("segs=", segs)

        # If there's nothing left, we can't merge
        if (not segs) or (len(segs) == 1 and not segs[0][1].has_deletions()):
            logger.debug("No candidates for merging")
            return ()

        # The total bytes in all segments
        segs.sort(reverse=True, key=itemgetter(0))
        totalsize = sum(s[0] for s in segs)
        # print("totalsize=", totalsize)

        if max_segment_count == 1:
            # No limit to the number of bytes in a segment
            max_bytes = totalsize * 10
        else:
            max_bytes = int(max(self.max_merged_size,
                                int(totalsize / max_segment_count)) * 1.25)
        # print("max_bytes=", max_bytes)
        # print("max_segment_count=", max_segment_count)

        # Special case when we can merge all segments at once
        if (
            len(segs) <= self.max_at_once and
            max_segment_count == 1 and
            totalsize < self.max_merged_size
        ):
            # print("special case")
            merge = Merge([s for _, s in segs])
            return [merge]

        i = len(segs) - 1
        seg_count = len(segs)
        merges = []
        while True:
            eligible = []
            eligible_size = 0
            allowed = self.max_at_once
            # print("i=", i)

            while i >= 0 and seg_count > max_segment_count and allowed > 0:
                # print("eligible=", eligible, "size=", eligible_size)
                # print("allowed=", allowed)

                size, seg = segs[i]
                # print("seg=", seg, "size=", size)
                # print("fits=", eligible_size + size <= max_bytes)

                if eligible_size + size <= max_bytes or len(eligible) < 2:
                    if len(eligible) > 0:
                        seg_count -= 1

                    eligible.append(seg)
                    eligible_size += size
                    allowed -= 1
                    i -= 1

                    # print("*seg_count=", seg_count, max_segment_count)
                    # print("*eligible=", eligible)
                    # print("*allowed=", allowed)
                    # print("i=", i)
                else:
                    break

            if eligible:
                merges.append(Merge(eligible))
            else:
                return merges

    @staticmethod
    def _prorated_size(seg, size):
        # Estimates the size of the segment after removing deletions
        undeleted_pct = seg.doc_count() / seg.doc_count_all()
        return int(size * undeleted_pct)

    def _score(self, segments: 'List[Tuple[int, codecs.Segment]]',
               was_too_large: bool) -> float:
        # NOTE: lower scores are better

        # Sum the current size of each segment in the set
        before = sum(size for size, seg in segments)
        # Calculate the pro-rated size of each segment
        pro_sizes = [self._prorated_size(seg, size) for size, seg in segments]
        # Sum the estimated size after each segment is merged
        after = sum(pro_sizes)
        # Calculate the "after" size but with each size rounded up to the floor
        after_floored = sum(max(self.segment_size_floor, pro_size) for pro_size
                            in pro_sizes)

        # Roughly calculate the "skew" of the merge (low skew means the segments
        # are of roughly the same size)
        if was_too_large:
            skew = 1.0 / self.max_at_once
        else:
            # Get the floored size of the largest segment
            biggest_floored = max(self.segment_size_floor, pro_sizes[0])
            skew = biggest_floored / after_floored

        # Use the skew as the basis for the score
        score = skew * 100.0

        # Favor merges that reclaim deletions
        undeleted_pct = after / before
        score *= pow(undeleted_pct, self.deletion_boost)

        return score


default_strategy = TieredMergeStrategy


# Merging helper functions

def perform_r_merge(codec: 'codecs.Codec',
                    store: 'storage.Storage',
                    schema: 'fields.Schema',
                    merge_obj: Merge,
                    newsegment: 'codecs.Segment',
                    key: int,
                    indexname: str
                    ) -> 'Tuple[codecs.Segment, str]':
    session = store.recursive_write_open(key, indexname)
    return perform_merge(codec, session, schema, merge_obj, newsegment)


def perform_merge(codec: 'codecs.Codec',
                  session: 'storage.Session',
                  schema: 'fields.Schema',
                  merge_obj: Merge,
                  newsegment: 'codecs.Segment'
                  ) -> 'Tuple[codecs.Segment, str]':
    logger.info("Merging %r into %r", merge_obj, newsegment)
    t = now()

    # Make a reader for the segments to merge
    reader = get_reader(session, schema, merge_obj.segments)

    # Copy the reader into the new segment
    newsegment = copy_reader(reader, codec, session, schema, newsegment)

    logger.info("Merged new segment %r in %0.06f s", newsegment, now() - t)
    return newsegment, merge_obj.merge_id


def get_reader(session, schema, segments):
    from whoosh.reading import SegmentReader, MultiReader

    rs = [SegmentReader(session.store, schema, segment) for segment
          in segments]
    assert rs
    if len(rs) == 1:
        reader = rs[0]
    else:
        reader = MultiReader(rs)

    reader.set_merging_hint()
    return reader


def perform_multi_merge(executor: futures.Executor, codec: 'codecs.Codec',
                        store: 'BaseFileStorage', schema: 'fields.Schema',
                        merge_obj: Merge, newsegment: 'codecs.Segment',
                        key: int, indexname: str
                        ) -> 'Tuple[codecs.Segment, str]':
    logger.info("Multi-merging %r into %r", merge_obj, newsegment)

    session = store.recursive_write_open(key, indexname)
    reader = get_reader(session, schema, merge_obj.segments)
    indexednames = [fname for fname in reader.indexed_field_names()
                    if fname in schema]

    perdoc = codec.per_document_writer(session, newsegment)
    docmap = _copy_perdoc(schema, reader, perdoc)
    perdoc.close()
    reader.close()

    t = now()
    fs = []
    for fieldname in indexednames:
        field_f = executor.submit(mm_field, fieldname, codec, store, schema,
                                  merge_obj, newsegment, key, indexname, docmap)
        fs.append(field_f)

    fwriter = codec.field_writer(session, newsegment)
    for fieldname, future in zip(indexednames, fs):
        future.result()

        fwriter.start_field(fieldname, schema[fieldname])
        treader = codec.terms_reader(session, newsegment, subname=fieldname)
        fwriter.copy_from(schema, treader)
        treader.close()

        store.delete_file(treader._terms_filename)
        store.delete_file(treader._posts_filename)

    fwriter.close()
    codec.finish_segment(session, newsegment)

    logger.info("Multi-merged terms in %0.04f seconds", now() - t)
    return newsegment, merge_obj.merge_id


def mm_field(fieldname: str, codec: 'codecs.Codec', store: 'storage.Storage',
             schema: 'fields.Schema', merge_obj: Merge,
             newsegment: 'codec.Segment', key: int, indexname: str, docmap):
    session = store.recursive_write_open(key, indexname)
    reader = get_reader(session, schema, merge_obj.segments)
    fieldobj = schema[fieldname]

    fwriter = codec.field_writer(session, newsegment, subname=fieldname)
    fwriter.start_field(fieldname, fieldobj)
    for termbytes in reader.lexicon(fieldname):
        _copy_1term(reader, fieldname, fieldobj, termbytes, fwriter, docmap)
    fwriter.finish_field()
    fwriter.close()


# Helper function to copy the information from a reader into a new segment

def copy_reader(reader: 'reading.IndexReader',
                codec: 'codecs.Codec',
                session: 'storage.Session',
                schema: 'fields.Schema',
                newsegment: 'codecs.Segment'
                ) -> 'codecs.Segment':
    # Enable any optimizations to make linear, batch reading faster
    reader.set_merging_hint()

    # Create writers for the new segment
    perdoc = codec.per_document_writer(session, newsegment)
    fwriter = codec.field_writer(session, newsegment)

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

    # Give the codec a chance to perform perform work on the new segment
    # (eg assemble a compound segment)
    codec.finish_segment(session, newsegment)

    return newsegment


def _copy_perdoc(schema: 'fields.Schema', reader: 'reading.IndexReader',
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

    logger.info("Copying per-doc data from %r to %r", reader, perdoc)
    t = now()

    # If the incoming reading has deletions, we need to return a dictionary
    # to map old document numbers to new document numbers
    has_del = reader.has_deletions()
    docmap = {}  # type: Dict[int, int]

    fieldnames = sorted(set(schema.names()) | set(reader.indexed_field_names()))

    # Open all column readers
    cols = {}
    for fieldname in fieldnames:
        fieldobj = schema[fieldname]
        colobj = fieldobj.column
        if colobj and reader.has_column(fieldname):
            creader = reader.column_reader(fieldname, colobj, translate=False)
            cols[fieldname] = creader

    to_io = perdoc.postings_io()

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
            to_vectorfmt = fieldobj.vector
            if to_vectorfmt and reader.has_vector(docnum, fieldname):
                vreader = reader.vector(docnum, fieldname)
                if vreader.can_copy_raw_to(to_io, to_vectorfmt):
                    rawbytes = vreader.raw_bytes()
                    perdoc.add_raw_vector(fieldname, rawbytes)
                else:
                    posts = tuple(vreader.postings())
                    perdoc.add_vector_postings(fieldname, fieldobj, posts)

            # Copy any column value
            if fieldname in cols:
                colobj = fieldobj.column
                cval = cols[fieldname][docnum]
                perdoc.add_column_value(fieldname, colobj, cval)

        perdoc.finish_doc()
        newdoc += 1

    logger.info("Copied perdoc data in %0.06f", now() - t)
    if has_del:
        return docmap


def _copy_terms(schema: 'fields.Schema', reader: 'reading.IndexReader',
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

    logger.info("Merging term data from %r to %r", reader, fwriter)
    termcount = 0

    last_fieldname = None
    fieldobj = None  # type: fields.FieldType
    for fieldname, termbytes in reader.all_terms():
        if fieldname not in fieldnames:
            continue

        if fieldname != last_fieldname:
            if last_fieldname is not None:
                fwriter.finish_field()
                logger.debug("Merged %s in %s", last_fieldname, now() - tt)
            logger.debug("Merging %s field", fieldname)
            fieldobj = schema[fieldname]
            fwriter.start_field(fieldname, fieldobj)
            last_fieldname = fieldname
            tt = now()

        # logger.debug("Copying term %s:%s", fieldname, termbytes)
        _copy_1term(reader, fieldname, fieldobj, termbytes, fwriter, docmap)
        termcount += 1


def _copy_1term(reader, fieldname, fieldobj, termbytes, fwriter, docmap):
    fwriter.start_term(termbytes)
    m = reader.matcher(fieldname, termbytes)
    can_copy_raw = m.can_copy_raw_to(fwriter.postings_io(), fieldobj.format)

    logger.debug("Copying term %r raw=%s", termbytes, can_copy_raw)
    if can_copy_raw:
        for rp in m.all_raw_postings():
            docid = post_docid(rp)
            length = reader.doc_field_length(docid, fieldname)
            if docmap:
                docid = docmap[docid]
            rp = update_post(rp, docid=docid, length=length)
            fwriter.add_raw_post(rp)
    else:
        for p in m.all_postings():
            docid = post_docid(p)
            length = reader.doc_field_length(docid, fieldname)
            if docmap:
                docid = docmap[docid]
            p = update_post(p, docid=docid, length=length)
            fwriter.add_posting(p)

    m.close()
    # logger.debug("Copied term %s:%s in %0.06f s",
    #              fieldname, termbytes, now() - tt)
    fwriter.finish_term()


def _copy_terms2(schema: 'fields.Schema', reader: 'reading.IndexReader',
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

    logger.info("Merging term data from %r to %r", reader, fwriter)
    t = now()
    tt = now()
    termcount = 0
    docmap_get = docmap.get if docmap else None

    last_fieldname = None
    fieldobj = None  # type: fields.FieldType
    can_copy_raw = None

    for (fieldname, termbytes), subr in reader.all_terms_with_reader():
        if fieldname not in fieldnames:
            continue

        if fieldname != last_fieldname:
            if last_fieldname is not None:
                fwriter.finish_field()
                logger.debug("Merged %s in %s", last_fieldname, now() - tt)
            logger.debug("Merging %s field", fieldname)
            fieldobj = schema[fieldname]
            fwriter.start_field(fieldname, fieldobj)
            last_fieldname = fieldname
            tt = now()
            to_io = fwriter.postings_io()
            can_copy_raw = None

        # logger.debug("Copying term %s:%s", fieldname, termbytes)
        # tt = now()
        termcount += 1

        fwriter.start_term(termbytes)
        m = reader.matcher(fieldname, termbytes)
        if can_copy_raw is None:
            can_copy_raw = m.can_copy_raw_to(to_io, fieldobj.format)

        logger.debug("Copying term %r raw=%s", termbytes, can_copy_raw)
        if can_copy_raw:
            if subr is not None and m.supports_raw_blocks():
                terminfo = reader.term_info(fieldname, termbytes)
                for blockbytes in m.rewrite_raw_blocks(docmap_get):
                    fwriter.write_raw_blockbytes(blockbytes)
                fwriter.copy_term_info(terminfo, docmap_get)
            else:
                for rp in m.all_raw_postings():
                    docid = post_docid(rp)
                    length = reader.doc_field_length(docid, fieldname)
                    if docmap:
                        docid = docmap.get(docid, docid)
                    rp = update_post(rp, docid=docid, length=length)
                    fwriter.add_raw_post(rp)

        else:
            for p in m.all_postings():
                docid = post_docid(p)
                length = reader.doc_field_length(docid, fieldname)
                if docmap:
                    docid = docmap.get(docid, docid)
                p = update_post(p, docid=docid, length=length)
                fwriter.add_posting(p)

        m.close()
        # logger.debug("Copied term %s:%s in %0.06f s",
        #              fieldname, termbytes, now() - tt)
        fwriter.finish_term()

    if last_fieldname is not None:
        fwriter.finish_field()

    logger.info("Copied %d terms in %0.06f s", termcount, now() - t)




