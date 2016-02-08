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
from abc import abstractmethod
from math import ceil
from operator import itemgetter
from typing import Sequence, Iterable, List, Set, Tuple, Union

from whoosh.ifaces import codecs
from whoosh.util import random_name


logger = logging.getLogger(__name__)


class Merge(object):
    def __init__(self, segments: 'Sequence[codecs.Segment]'):
        assert segments
        self.merge_id = random_name(24)
        self.segments = segments
        self.delete_queries = []

    def __repr__(self):
        return "<%s %r>" % (type(self).__name__, self.segments)

    def __eq__(self, other: 'Merge'):
        return isinstance(other, Merge) and self.segments == other.segments

    def __ne__(self, other: 'Merge'):
        return not self == other

    def __len__(self):
        return len(self.segments)

    def __contains__(self, seg: 'codecs.Segment') -> bool:
        segid = seg.segment_id()
        return any(s.segment_id() == segid for s in self.segments)

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
        return self.before_size() * undel_ratio

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


class MergeStrategy(object):
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

    def get_merges(self, segments: 'Sequence[codecs.Segment]',
                   merging: Union[Sequence[str], Set[str]],
                   expunge_deleted: bool=False) -> Sequence[Merge]:
        logger.info("Starting merge")

        # Associate each segment with its size
        sized = [(s.size(), s) for s in segments]
        # Eliminate candidates if they're too large to merge
        segs = []
        for size, seg in sized:
            logger.debug("Segment %r size=%d", seg, size)
            if size >= self.max_merged_size / 2.0:
                logger.debug("Segment %r too big to merge", seg)
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
            while i <= len(eligible) - self.max_at_once:
                thisrange = []  # type: List[Tuple[int, codecs.Segment]]
                range_size = 0
                range_too_large = False

                # Add segments to the range until we reach max_at_once
                j = i
                while j < len(eligible) and len(thisrange) < self.max_at_once:
                    size, seg = eligible[j]
                    if range_size + size > self.max_merged_size:
                        # Adding this segment would make the merge too big
                        range_too_large = True
                        # Continue so we can try packing smaller segments
                        # into the leftover space
                        j += 1
                        continue

                    thisrange.append((size, seg))
                    range_size += size
                    j += 1

                # The list should never be empty
                assert thisrange
                # Smaller scores are better
                score = self._score(thisrange, range_too_large)
                logger.debug("Range=%r score=%f", thisrange, score)

                if ((best_score < 0 or score < best_score) and
                        (not range_too_large or not already_maxed)):
                    # This is the best range yet
                    logger.debug("New best range")
                    best_range = thisrange
                    best_score = score
                    # best_size = range_size
                    # best_too_large = range_too_large

                i += 1

            if best_range:
                best_segs = [s for size, s in best_range]
                to_merge.update(s.segment_id() for s in best_segs)

                m = Merge(best_segs)
                logger.info("Adding merge %r", m)
                merges.append(m)
            else:
                logger.info("No more merges found")
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
