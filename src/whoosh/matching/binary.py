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

from typing import Any, Sequence, Tuple

from whoosh.ifaces import matchers


__all__ = ("BiMatcher", "AdditiveBiMatcher", "UnionMatcher",
           "DisjunctionMaxMatcher", "IntersectionMatcher", "RequireMatcher")


class BiMatcher(matchers.Matcher):
    """
    Base class for matchers that combine the results of two sub-matchers in
    some way.
    """

    def __init__(self, a: matchers.Matcher, b: matchers.Matcher):
        super(BiMatcher, self).__init__()
        self.a = a
        self.b = b

    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__, self.a, self.b)

    # Subclasses should override _rewrap to return a new instance when one or
    # both of the sub-queries are copied or replaced

    def _rewrap(self, new_a: matchers.Matcher, new_b: matchers.Matcher
                ) -> matchers.Matcher:
        return self.__class__(new_a, new_b)

    # Override some interface methods that are common to all binary matchers

    def save(self) -> Tuple[Any, Any]:
        return self.a.save(), self.b.save()

    def restore(self, place: Tuple[Any, Any]):
        self.a.restore(place[0])
        self.b.restore(place[1])

    def children(self) -> Sequence[matchers.Matcher]:
        return self.a, self.b

    def copy(self):
        return self._rewrap(self.a.copy(), self.b.copy())

    @matchers.check_active
    def skip_to(self, docid: int) -> bool:
        ra = self.a.skip_to(docid)
        rb = self.b.skip_to(docid)
        return ra or rb

    def supports(self, name: str) -> bool:
        return self.a.supports(name) and self.b.supports(name)

    def supports_block_quality(self):
        return (self.a.supports_block_quality() and
                self.b.supports_block_quality())


class AdditiveBiMatcher(BiMatcher):
    """
    Base class for binary matchers where the scores of the sub-matchers are
    added together.
    """

    # Override methods dealing with scoring to add the scores together

    def weight(self) -> float:
        return self.a.weight() + self.b.weight()

    def score(self) -> float:
        return self.a.score() + self.b.score()

    def max_quality(self) -> float:
        q = 0.0
        if self.a.is_active():
            q += self.a.max_quality()
        if self.b.is_active():
            q += self.b.max_quality()
        return q

    def block_quality(self) -> float:
        bq = 0.0
        if self.a.is_active():
            bq += self.a.block_quality()
        if self.b.is_active():
            bq += self.b.block_quality()
        return bq


class UnionMatcher(AdditiveBiMatcher):
    """
    Matches the union (OR) of the postings in the two sub-matchers.
    """

    def __init__(self, a: matchers.Matcher, b: matchers.Matcher):
        super(UnionMatcher, self).__init__(a, b)

    @matchers.check_active
    def _combine(self, a_meth, b_meth, combine_fn):
        # If one of the subqueries is inactive, use the other
        if not self.b.is_active():
            return a_meth()
        elif not self.a.is_active():
            return b_meth()

        # Use the spans from the earlier matcher, or if they're on the same
        # document, combine them
        id_a = self.a.id()
        id_b = self.b.id()
        if id_a < id_b:
            return a_meth()
        elif id_b < id_a:
            return b_meth()
        else:
            return combine_fn(a_meth(), b_meth())

    # Override interface methods

    def is_active(self) -> bool:
        return self.a.is_active() or self.b.is_active()

    @matchers.check_active
    def id(self) -> int:
        if not self.a.is_active():
            return self.b.id()
        elif not self.b.is_active():
            return self.a.id()

        return min(self.a.id(), self.b.id())

    @matchers.check_active
    def next(self) -> bool:
        # Shortcut when one matcher is inactive
        if not self.a.is_active():
            return self.b.next()
        elif not self.b.is_active():
            return self.a.next()

        a_id = self.a.id()
        b_id = self.b.id()
        # If A is before B, advance A, and vice-versa. If they're on the same
        # document, advance them both.
        if a_id < b_id:
            return self.a.next()
        elif b_id < a_id:
            return self.b.next()
        else:
            ar = self.a.next()
            br = self.b.next()
            return ar or br

    @matchers.check_active
    def skip_to(self, id: int) -> bool:
        ra = rb = False

        if self.a.is_active():
            ra = self.a.skip_to(id)
        if self.b.is_active():
            rb = self.b.skip_to(id)

        return ra or rb

    def replace(self, minquality: float=0) -> matchers.Matcher:
        from .wrappers import AndMaybeMatcher

        a = self.a
        b = self.b
        a_active = a.is_active()
        b_active = b.is_active()

        # If one or both of the sub-matchers are inactive, convert
        if not (a_active or b_active):
            return matchers.NullMatcher()
        elif not a_active:
            return b.replace(minquality)
        elif not b_active:
            return a.replace(minquality)

        # If neither sub-matcher on its own has a high enough max quality to
        # contribute, convert to an intersection matcher
        if minquality and a_active and b_active:
            a_max = a.max_quality()
            b_max = b.max_quality()
            if a_max < minquality and b_max < minquality:
                return IntersectionMatcher(a, b).replace(minquality)
            elif a_max < minquality:
                return AndMaybeMatcher(b, a)
            elif b_max < minquality:
                return AndMaybeMatcher(a, b)

        # Otherwise, try to replace the subqueries to see if they can become
        # more efficient
        a = a.replace(minquality - b.max_quality() if minquality else 0)
        b = b.replace(minquality - a.max_quality() if minquality else 0)
        # If one of the sub-matchers changed, return a new union
        if a is not self.a or b is not self.b:
            return self._rewrap(a, b)
        else:
            return self

    def spans(self) -> 'Sequence[spans.Span]':
        return self._combine(self.a.spans, self.b.spans,
                             lambda x, y: sorted(set(x) | set(y)))

    def weight(self) -> float:
        return self._combine(self.a.weight, self.b.weight, lambda x, y: x + y)

    def score(self) -> float:
        return self._combine(self.a.score, self.b.score, lambda x, y: x + y)

    @matchers.check_active
    def skip_to_quality(self, minquality: float) -> int:
        a = self.a
        b = self.b

        # Short circuit if one matcher is inactive
        if not a.is_active():
            return b.skip_to_quality(minquality)
        elif not b.is_active():
            return a.skip_to_quality(minquality)

        skipped = 0
        aq = a.block_quality()
        bq = b.block_quality()
        # If we're below the required quality, try skipping the lowest quality
        # sub-matcher until we get the required quality or reach the end
        while a.is_active() and b.is_active() and aq + bq <= minquality:
            if aq < bq:
                skipped += a.skip_to_quality(minquality - bq)
                aq = a.block_quality()
            else:
                skipped += b.skip_to_quality(minquality - aq)
                bq = b.block_quality()

        return skipped

    # Using sets is faster in most cases, but could potentially use a lot of
    # memory.
    #
    # def all_ids(self):
    #     return iter(sorted(set(self.a.all_ids()) | set(self.b.all_ids())))


class DisjunctionMaxMatcher(UnionMatcher):
    """
    Matches the union (OR) of two sub-matchers. Where both sub-matchers
    match the same posting, returns the weight/score of the higher-scoring
    posting.
    """

    # TODO: this class inherits from AdditiveBiMatcher (through UnionMatcher)
    # but it does not add the scores of the sub-matchers together (it
    # overrides all methods that perform addition). Need to clean up the
    # inheritance.

    def __init__(self, a, b, tiebreak=0.0):
        super(DisjunctionMaxMatcher, self).__init__(a, b)
        self._tiebreak = tiebreak

    def _rewrap(self, new_a: matchers.Matcher, new_b: matchers.Matcher
                ) -> 'DisjunctionMaxMatcher':
        return self.__class__(new_a, new_b, self._tiebreak)

    def replace(self, minquality: float=0):
        a = self.a
        b = self.b
        a_active = a.is_active()
        b_active = b.is_active()

        if not a_active and not b_active:
            return matchers.NullMatcher()
        elif not a_active:
            return b.replace(minquality)
        elif not b_active:
            return a.replace(minquality)

        # DisMax takes the max of the sub-matcher qualities instead of adding
        # them, so we need special logic here
        if minquality and a_active and b_active:
            a_max = a.max_quality()
            b_max = b.max_quality()

            if a_max < minquality and b_max < minquality:
                # If neither sub-matcher has a high enough max quality to
                # contribute, return an inactive matcher
                return matchers.NullMatcher()
            elif b_max < minquality:
                # If the b matcher can't contribute, return a
                return a.replace(minquality)
            elif a_max < minquality:
                # If the a matcher can't contribute, return b
                return b.replace(minquality)

        # We can pass the minquality down here, since we don't add the two
        # scores together
        a = a.replace(minquality)
        b = b.replace(minquality)
        a_active = a.is_active()
        b_active = b.is_active()
        # It's kind of tedious to check for inactive sub-matchers all over
        # again here after we replace them, but it's probably better than
        # returning a replacement with an inactive sub-matcher
        if not (a_active and b_active):
            return matchers.NullMatcher()
        elif not a_active:
            return b
        elif not b_active:
            return a
        elif a is not self.a or b is not self.b:
            # If one of the sub-matchers changed, return a new DisMax
            return self.__class__(a, b)
        else:
            return self

    # Need to replace score/quality measures from AdditiveBiMatcher

    def weight(self) -> float:
        return self._combine(self.a.weight, self.b.weight, max)

    def score(self):
        return self._combine(self.a.score, self.b.score, max)

    def max_quality(self):
        return self._combine(self.a.max_quality, self.b.max_quality, max)

    def block_quality(self):
        return self._combine(self.a.block_quality, self.b.block_quality, max)

    @matchers.check_active
    def skip_to_quality(self, minquality: float) -> int:
        a = self.a
        b = self.b

        # Short circuit if one matcher is inactive
        if not a.is_active():
            return b.skip_to_quality(minquality)
        elif not b.is_active():
            return a.skip_to_quality(minquality)

        # Skip whichever sub-matcher is lowest quality, until one of them
        # has high-enough quality, or we reach the end
        skipped = 0
        aq = a.block_quality()
        bq = b.block_quality()
        while a.is_active() and b.is_active() and max(aq, bq) <= minquality:
            if aq <= minquality:
                skipped += a.skip_to_quality(minquality)
                aq = a.block_quality()
            if bq <= minquality:
                skipped += b.skip_to_quality(minquality)
                bq = b.block_quality()
        return skipped


class IntersectionMatcher(AdditiveBiMatcher):
    """
    Matches the intersection (AND) of the postings in the two sub-matchers.
    """

    def __init__(self, a, b):
        super(IntersectionMatcher, self).__init__(a, b)

        if (
            self.a.is_active() and
            self.b.is_active() and
            self.a.id() != self.b.id()
        ):
            self._find_next()

    def _find_next(self) -> bool:
        a_id = self.a.id()
        b_id = self.b.id()
        assert a_id != b_id

        # Skip the lowest to the highest until they land on the same document
        # or we reach the end
        r = False
        while self.a.is_active() and self.b.is_active() and a_id != b_id:
            if a_id < b_id:
                ra = self.a.skip_to(b_id)
                if not self.a.is_active():
                    return True
                r = r or ra
                a_id = self.a.id()
            else:
                rb = self.b.skip_to(a_id)
                if not self.b.is_active():
                    return True
                r = r or rb
                b_id = self.b.id()
        return r

    # Override interface

    def is_active(self) -> bool:
        return self.a.is_active() and self.b.is_active()

    def id(self) -> int:
        # The sub-matchers should always be on the same document, so it's OK
        # to just pick one and return its document ID
        return self.a.id()

    @matchers.check_active
    def next(self) -> bool:
        # We must assume that the ids are equal whenever next() is called (they
        # should have been made equal by _find_next), so advance them both
        ar = self.a.next()
        if self.is_active():
            nr = self._find_next()
            return ar or nr

    @matchers.check_active
    def skip_to(self, docid: int) -> bool:
        # Skip both sub-matchers
        ra = self.a.skip_to(docid)
        rb = self.b.skip_to(docid)

        if self.is_active():
            rn = False
            if self.a.id() != self.b.id():
                # Find the next matching document
                rn = self._find_next()
            return ra or rb or rn
        else:
            return True

    def replace(self, minquality=0):
        if not self.is_active():
            return matchers.NullMatcher()

        if minquality:
            a_max = self.a.max_quality()
            b_max = self.b.max_quality()

            # If the combined quality of the sub-matchers can't contribute,
            # return an inactive matcher
            if a_max + b_max < minquality:
                return matchers.NullMatcher()

            # Require that the replacements be able to contribute results
            # higher than the minquality
            a_min = minquality - b_max
            b_min = minquality - a_max
        else:
            a_min = b_min = 0

        # Replace the sub-matchers
        a = self.a.replace(a_min)
        b = self.b.replace(b_min)
        if not (a.is_active() or b.is_active()):
            # Both went inactive, return an inactive matcher
            return matchers.NullMatcher()
        elif not a.is_active():
            return b
        elif not b.is_active():
            return a
        elif a is not self.a or b is not self.b:
            return self._rewrap(a, b)
        else:
            return self

    @matchers.check_active
    def skip_to_quality(self, minquality: float) -> int:
        a = self.a
        b = self.b

        skipped = 0
        aq = a.block_quality()
        bq = b.block_quality()

        while a.is_active() and b.is_active() and aq + bq <= minquality:
            if aq < bq:
                # If the block quality of A is less than B, skip A ahead until
                # it can contribute at least the balance of the required min
                # quality when added to B
                sk = a.skip_to_quality(minquality - bq)
                skipped += sk
                if not sk and a.is_active():
                    # The matcher couldn't skip ahead for some reason, so just
                    # advance and try again
                    a.next()
            else:
                # And vice-versa
                sk = b.skip_to_quality(minquality - aq)
                skipped += sk
                if not sk and b.is_active():
                    b.next()

            # Stop if one of the matchers is exhausted
            if not a.is_active() or not b.is_active():
                break

            # We need to always the matcher leave in a state where the
            # sub-matchers are at the same document, so call _find_next() to
            # sync them
            if a.id() != b.id():
                self._find_next()

            # Get the block qualities at the new matcher positions
            if self.is_active():
                aq = a.block_quality()
                bq = b.block_quality()

        return skipped

    # Override derived

    @matchers.check_active
    def spans(self):
        return sorted(set(self.a.spans()) | set(self.b.spans()))

    # Using sets is faster in some cases, but could potentially use a lot of
    # memory
    def all_ids(self):
        return iter(sorted(set(self.a.all_ids()) & set(self.b.all_ids())))


class RequireMatcher(IntersectionMatcher):
    """
    Matches postings that are in both sub-matchers, but only uses scores
    from the first.
    """

    # Override score/quality methods to only use the first matcher

    def weight(self) -> float:
        return self.a.weight()

    def score(self) -> float:
        return self.a.score()

    def replace(self, minquality=0):
        # If one of the sub-matchers is inactive, or the first matcher can't
        # possibly contribute, return a null matcher
        if not self.is_active() or self.a.max_quality() <= minquality:
            return matchers.NullMatcher()

        a = self.a.replace(minquality)
        b = self.b.replace()
        if not a.is_active() or not b.is_active():
            return matchers.NullMatcher()

        if a is not self.a or b is not self.b:
            return self.__class__(a, b)
        else:
            return self

    def supports_block_quality(self):
        return self.a.supports_block_quality()

    @matchers.check_active
    def max_quality(self) -> float:
        return self.a.max_quality()

    @matchers.check_active
    def block_quality(self) -> float:
        return self.a.block_quality()

    def skip_to_quality(self, minquality: float):
        a = self.a
        b = self.b

        skipped = 0
        while (a.is_active() and b.is_active() and
               a.block_quality() <= minquality):
            # Skip the first matcher
            skipped += a.skip_to_quality(minquality)

            # Stop if one of the matchers is exhausted
            if not a.is_active() or not b.is_active():
                break

            # We need to always the matcher leave in a state where the
            # sub-matchers are at the same document, so call _find_next() to
            # sync them
            if a.id() != b.id():
                self._find_next()

        return skipped


