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

from whoosh.compat import xrange
from whoosh.matching import mcore


class CombinationMatcher(mcore.Matcher):
    def __init__(self, submatchers, boost=1.0):
        assert submatchers, submatchers
        self._submatchers = submatchers
        self._boost = boost

    def supports_block_quality(self):
        return all(m.supports_block_quality() for m in self._submatchers)

    def max_quality(self):
        return max(m.max_quality() for m in self._submatchers
                   if m.is_active()) * self._boost

    def supports(self, astype):
        return all(m.supports(astype) for m in self._submatchers)

    def children(self):
        return iter(self._submatchers)

    def score(self):
        return sum(m.score() for m in self._submatchers) * self._boost


class ArrayUnionMatcher(CombinationMatcher):
    """Instead of marching the sub-matchers along in parallel, pre-reads the
    scores for a large block of documents at a time from each matcher,
    accumulating the scores in an array.
    
    This is faster than the implementation using a binary tree of
    :class:`~whoosh.matching.binary.UnionMatcher` objects (possibly just
    because of less overhead), but it doesn't allow getting information about
    the "current" document other than the score, because there isn't really a
    current document, just an array of scores.
    """

    def __init__(self, submatchers, doccount, boost=1.0, partsize=1024):
        CombinationMatcher.__init__(self, submatchers, boost=boost)
        self._doccount = doccount
        self._partsize = partsize

        self._a = array("f", (0.0 for _ in xrange(self._partsize)))
        self._docnum = 0
        self._offset = 0
        self._limit = 0
        self._read_part()
        self._find_next()

    def _read_part(self):
        boost = self._boost
        limit = min(self._limit + self._partsize, self._doccount)
        offset = self._limit
        a = self._a
        for i in xrange(self._partsize):
            a[i] = 0.0

        for m in self._submatchers:
            while m.is_active() and m.id() < limit:
                a[m.id() - offset] = m.score() * boost
                m.next()

        self._offset = offset
        self._docnum = offset
        self._limit = limit

    def _find_next(self):
        a = self._a
        doccount = self._doccount
        offset = self._offset
        limit = self._limit

        while self._docnum < doccount:
            dn = self._docnum
            if dn == limit:
                self._read_part()
                limit = self._limit
                offset = self._offset
            elif a[dn - offset] <= 0.0:
                self._docnum += 1
            else:
                break

    def is_active(self):
        return self._docnum < self._doccount

    def max_quality(self):
        return max(m.max_quality() for m in self._submatchers)

    def block_quality(self):
        return max(self._a)

    def skip_to(self, docnum):
        if docnum < self._offset:
            return

        while docnum >= self._limit:
            self._read_part()
        self._docnum = docnum
        self._find_next()

    def skip_to_quality(self, minquality):
        skipped = 0
        while self.block_quality() <= minquality:
            skipped += 1
            self._read_part()
        self._find_next()
        return skipped

    def id(self):
        return self._docnum

    def all_ids(self):
        doccount = self._doccount
        docnum = self._docnum
        offset = self._offset
        limit = self._limit

        a = self._a
        while docnum < doccount:
            if a[docnum - offset] > 0:
                yield docnum

            docnum += 1
            if docnum == limit:
                self._read_part()
                offset = self._offset
                limit = self._limit

    def next(self):
        self._docnum += 1
        return self._find_next()

    def score(self):
        return self._a[self._docnum - self._offset]


# Failed experiment -- an intersection matcher that keeps a list of submatchers
# is slower than using a binary tree

#class ComboIntersectionMatcher(CombinationMatcher):
#    def __init__(self, submatchers, boost=1.0):
#        CombinationMatcher.__init__(self, submatchers, boost=boost)
#        self._find_next()
#
#    def _find_next(self):
#        ms = self._submatchers
#        if len(ms) < 2:
#            return
#
#        while self.is_active():
#            ms.sort(key=lambda m: m.id())
#            lastid = ms[-1].id()
#            if ms[0].id() == lastid:
#                break
#            for i in xrange(len(ms) - 1):
#                ms[i].skip_to(lastid)
#
#    def is_active(self):
#        return all(m.is_active() for m in self._submatchers)
#
#    def id(self):
#        return self._submatchers[0].id()
#
#    def skip_to(self, id_):
#        if not self.is_active():
#            raise mcore.ReadTooFar
#        for m in self._submatchers:
#            m.skip_to(id_)
#        self._find_next()
#
#    def block_quality(self):
#        return sum(m.block_quality() for m in self._submatchers) * self._boost
#
#    def replace(self, minquality=0):
#        if not self.is_active():
#            return mcore.NullMatcher()
#        if minquality > self.max_quality():
#            # If the combined quality of the sub-matchers can't contribute,
#            # return an inactive matcher
#            return mcore.NullMatcher()
#
#        ms = [m.replace() for m in self._submatchers]
#        if not all(m.is_active() for m in ms):
#            return mcore.NullMatcher()
#
#        return self.__class__(ms, boost=self._boost)
#
#    def skip_to_quality(self, minquality):
#        if not self.is_active():
#            raise mcore.ReadTooFar
#
#        ms = self._submatchers
#        if len(ms) == 1:
#            return ms[0].skip_to_quality(minquality)
#
#        skipped = 0
#        while self.is_active() and self.block_quality() <= minquality:
#            ms.sort(key=lambda m: m.score())
#            restq = sum(m.block_quality() for m in ms[1:])
#            sk = ms[0].skip_to_quality(minquality - restq)
#            skipped += sk
#            if not sk:
#                ms[0].next()
#            self._find_next()
#        return skipped
#
#    def next(self):
#        if not self.is_active():
#            raise mcore.ReadTooFar
#
#        for m in self._submatchers:
#            m.next()
#        self._find_next()
#
#    def spans(self):
#        spans = set()
#        for m in self._submatchers:
#            spans |= set(m.spans())
#        return sorted(spans)












