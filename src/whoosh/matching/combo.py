# Copyright 2010 Matt Chaput. All rights reserved.
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
from array import array
from typing import Any, Sequence

from whoosh.ifaces import matchers, queries
from whoosh.compat import xrange
from whoosh.matching import binary


__all__ = ("ArrayUnionMatcher", )


class ArrayUnionMatcher(matchers.Matcher):
    """
    Instead of marching the sub-matchers along in parallel, this matcher
    pre-reads the scores for a large block of documents at a time from each
    matcher, accumulating the scores in an array.

    This is faster than the implementation using a binary tree of
    :class:`~whoosh.matching.binary.UnionMatcher` objects (possibly just
    because of less overhead).
    """

    def __init__(self, submatchers: 'Sequence[matchers.Matcher]',
                 union: 'binary.UnionMatcher', doccount: int,
                 boost: float=1.0, scored: bool=True, partsize: int=2048):
        """
        :param submatchers: the matchers to union together.
        :param union: a ``UnionMatcher`` version of the submatchers. This must
            use independent copies of the ``submatchers``.
        :param doccount: The total number of documents.
        :param boost: a boost factor on scores from sub-matchers.
        :param scored: whether the documents need to be scored.
        :param partsize: the number of documents to pre-read at a time.
        """

        self._submatchers = submatchers
        self._union = union
        self._boost = boost
        self._scored = scored
        self._doccount = doccount
        self._partsize = partsize or doccount
        self._maxquality = self._union.max_quality()

        # Array to hold the scores of each document in the read part
        typecode = "d" if scored else "B"
        self._scores = array(typecode, (0 for _ in xrange(self._partsize)))
        # Docnum corresponding to first item in the score array
        self._offset = 0
        # Docnum after last item in the score array
        self._limit = 0
        # Current ID of this matcher
        self._id = self._min_id()

        self._read_part()

    def __repr__(self):
        return "%s(%r, boost=%f, scored=%s, partsize=%d)" % (
            type(self).__name__, self._submatchers, self._boost, self._scored,
            self._partsize
        )

    def _min_id(self):
        active = [subm for subm in self._submatchers if subm.is_active()]
        if active:
            return min(subm.id() for subm in active)
        else:
            return self._doccount

    def _read_part(self):
        scored = self._scored
        boost = self._boost
        limit = min(self._id + self._partsize, self._doccount)
        offset = self._id
        a = self._scores

        # Clear the array
        for i in xrange(self._partsize):
            a[i] = 0

        # Add the scores from the submatchers into the array
        for m in self._submatchers:
            while m.is_active() and m.id() < limit:
                i = m.id() - offset
                if scored:
                    a[i] += m.score() * boost
                else:
                    a[i] = 1
                m.next()

        self._offset = offset
        self._limit = limit

    def _find_next(self):
        # Move to the next document with a non-zero score in the array

        a = self._scores
        while self._id < self._doccount:
            # If we're at the end of the array, we need to read more
            if self._id == self._limit and self._id < self._doccount:
                self._id = self._min_id()
                self._read_part()

            # If this place in the array is non-zero, we're done
            if a[self._id - self._offset] > 0:
                return

            # Otherwise go to the next doc and loop
            self._id += 1

    # Interface

    def is_active(self):
        return self._id < self._doccount

    def id(self) -> int:
        return self._id

    def next(self):
        self._id += 1
        return self._find_next()

    def skip_to(self, docnum: int):
        if docnum < self._offset:
            # We've already passed it
            return
        elif docnum < self._limit:
            # It's in the current part
            self._id = docnum
            self._find_next()
            return

        # Advance all active submatchers
        for m in self._submatchers:
            if m.is_active():
                m.skip_to(docnum)

        if any(m.is_active() for m in self._submatchers):
            # Rebuffer
            self._id = self._min_id()
            self._read_part()
        else:
            # Nothing is active, move past the end to indicate we're inactive
            self._id = self._doccount

    def save(self):
        state = (self._id, self._offset, self._limit, self._scores)
        mstates = tuple(m.save() for m in self._submatchers)
        ustate = self._union.save()
        return state, mstates, ustate

    def restore(self, place: Any):
        state, mstates, ustate = place
        self._id, self._offset, self._limit, self._scores = state
        for i, m in enumerate(self._submatchers):
            m.restore(mstates[i])
        self._union.restore(ustate)

    def weight(self) -> float:
        if self._union.id() < self._id:
            self._union.skip_to(self._id)
        return self._union.weight()

    def score(self) -> float:
        return self._scores[self._id - self._offset]

    def children(self) -> 'Sequence[matchers.Matcher]':
        if self._union.id() < self._id:
            self._union.skip_to(self._id)
        return self._union.children()

    def spans(self):
        if self._union.id() < self._id:
            self._union.skip_to(self._id)
        return self._union.spans()

    def copy(self) -> 'matchers.Matcher':
        from copy import deepcopy

        return deepcopy(self)

    def supports(self, name: str) -> bool:
        return self._union.supports(name)

    def supports_block_quality(self):
        return True

    def max_quality(self) -> float:
        return self._maxquality

    def block_quality(self) -> float:
        return max(self._scores)

    def skip_to_quality(self, minquality: float) -> int:
        skipped = 0
        while self.is_active() and self.block_quality() <= minquality:
            skipped += 1
            self._id = self._limit
            self._read_part()

        if self.is_active():
            self._find_next()

        return skipped

    def all_ids(self):
        return self._union.all_ids()

