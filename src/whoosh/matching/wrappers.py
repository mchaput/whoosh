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
from typing import (
    Any, Callable, Iterable, Optional, Sequence, Set, Tuple, Union,
)

from whoosh import idsets, postings
from whoosh.ifaces import matchers, weights


__all__ = ("WrappingMatcher", "ConstantScoreMatcher", "MultiMatcher",
           "FilterMatcher", "InverseMatcher", "SingleTermMatcher",
           "CoordMatcher", "AndNotMatcher", "AndMaybeMatcher")


class WrappingMatcher(matchers.Matcher):
    """
    Base class for matchers that wrap sub-matchers.
    """

    def __init__(self, child: matchers.Matcher, boost: float=1.0):
        self.child = child
        self._boost = boost

    def __repr__(self):
        return "%s(%r, boost=%s)" % (self.__class__.__name__, self.child,
                                     self._boost)

    # Subclasses should override _rewrap to wrap a replaced or copied child

    def _rewrap(self, newchild: matchers.Matcher):
        return self.__class__(newchild, self._boost)

    def copy(self) -> matchers.Matcher:
        return self._rewrap(self.child.copy())

    def children(self) -> Sequence[matchers.Matcher]:
        return self.child.children()

    def replace(self, minquality=0) -> matchers.Matcher:
        # Replace the child matcher
        r = self.child.replace(minquality / self._boost)
        if r is not self.child:
            # If the child changed, return a new wrapper on the new child
            return self._rewrap(r)
        else:
            return self

    # Implement defaults for Matcher interface

    def is_active(self) -> bool:
        return self.child.is_active()

    def id(self) -> int:
        return self.child.id()

    def next(self) -> bool:
        self.child.next()

    def skip_to(self, docid: int) -> bool:
        return self.child.skip_to(docid)

    def save(self) -> Any:
        return self.child.save()

    def restore(self, place: Any):
        self.child.restore(place)

    def weight(self):
        return self.child.weight() * self._boost

    def score(self):
        return self.child.score() * self._boost

    def supports(self, name: str) -> bool:
        return self.child.supports(name)

    def supports_block_quality(self) -> bool:
        return self.child.supports_block_quality()

    def max_quality(self) -> float:
        return self.child.max_quality()

    def block_quality(self) -> float:
        return self.child.block_quality()

    def skip_to_quality(self, minquality: float) -> int:
        return self.child.skip_to_quality(minquality)

    # Use super's all_ids() and all_postings() because we want to use our
    # is_active(), next(), id(), etc., not the child's

    # Leaf methods

    def term(self) -> Optional[Tuple[str, bytes]]:
        return self.child.term()

    def posting(self) -> 'Optional[postings.PostTuple]':
        return self.child.posting()

    def can_copy_raw_to(self, fmt: 'postings.Format'):
        return self.child.can_copy_raw_to(fmt)

    def raw_posting(self) -> 'postings.RawPost':
        return self.child.raw_posting()

    # def all_postings(self) -> Iterable[postings.PostTuple]:
    #     return self.child.all_postings()

    # def all_raw_postings(self) -> 'Iterable[postings.RawPost]':
    #     return self.child.all_raw_postings()

    def has_weights(self) -> bool:
        return self.child.has_weights()

    def has_lengths(self) -> bool:
        return self.child.has_lengths()

    def has_positions(self) -> bool:
        return self.child.has_positions()

    def has_chars(self) -> bool:
        return self.child.has_chars()

    def has_payloads(self) -> bool:
        return self.child.has_payloads()

    def length(self) -> int:
        return self.child.length()

    def positions(self) -> Sequence[int]:
        return self.child.positions()

    def chars(self) -> Sequence[Tuple[int]]:
        return self.child.chars()

    def payloads(self) -> Sequence[bytes]:
        return self.child.payloads()


class ConstantScoreMatcher(WrappingMatcher):
    def __init__(self, child, score=1.0):
        self.child = child
        self._boost = score
        self._active = True

    def _rewrap(self, newchild: matchers.Matcher) -> matchers.Matcher:
        return self.__class__(newchild, self._boost)

    def is_active(self) -> bool:
        return self._active and self.child.is_active()

    def supports_block_quality(self):
        return True

    def max_quality(self) -> float:
        return self._boost

    def replace(self, minquality: float=0) -> matchers.Matcher:
        if minquality and self._boost <= minquality:
            return matchers.NullMatcher()
        else:
            return self

    def block_quality(self) -> float:
        return self._boost

    def skip_to_quality(self, minquality):
        if minquality >= self._boost:
            self._active = False

    def score(self) -> float:
        return self._boost


class DocOffsetMatcher(WrappingMatcher):
    def __init__(self, child: 'matchers.Matcher', doc_offset: int):
        super(DocOffsetMatcher, self).__init__(child)
        self._doc_offset = doc_offset

    def _rewrap(self, newchild: matchers.Matcher) -> 'DocOffsetMatcher':
        return self.__class__(newchild, self._doc_offset)

    def id(self):
        return self.child.id() + self._doc_offset

    def skip_to(self, docid: int) -> bool:
        return self.child.skip_to(docid - self._doc_offset)

    def all_ids(self) -> Iterable[int]:
        docoffset = self._doc_offset
        for docid in self.child.all_ids():
            yield docoffset + docid

    def posting(self):
        post = self.child.posting()
        docid = postings.post_docid(post)
        return postings.change_docid(post, docid + self._doc_offset)


class MultiMatcher(matchers.Matcher):
    """
    Serializes the results of a list of sub-matchers.
    """

    def __init__(self, matchers: Sequence[matchers.Matcher],
                 idoffsets: Sequence[int], scorer: 'weights.Scorer'=None,
                 current: int=0):
        """
        :param matchers: a list of Matcher objects.
        :param idoffsets: a list of offsets corresponding to items in the
            ``matchers`` list.
        :param scorer: a Scorer to use to score the matched documents.
        :param current: the index of the current matcher.
        """

        self._matchers = matchers
        self._offsets = idoffsets
        self._scorer = scorer
        self._current = current
        self._next_matcher()

    def __repr__(self):
        return "%s(%r, %r, current=%s)" % (self.__class__.__name__,
                                           self._matchers, self._offsets,
                                           self._current)

    def _next_matcher(self):
        # Moves to the next active sub-matcher
        matchers = self._matchers
        while (self._current < len(matchers) and
               not matchers[self._current].is_active()):
            self._current += 1

    # Override interface

    def can_copy_raw_to(self, fmt: 'postings.Format') -> bool:
        return all(m.can_copy_raw_to(fmt) for m in self._matchers)

    def is_active(self) -> bool:
        return self._current < len(self._matchers)

    @matchers.check_active
    def id(self) -> int:
        current = self._current
        return self._matchers[current].id() + self._offsets[current]

    @matchers.check_active
    def next(self) -> bool:
        self._matchers[self._current].next()
        if not self._matchers[self._current].is_active():
            self._next_matcher()

    @matchers.check_active
    def skip_to(self, docid: int) -> bool:
        if docid <= self.id():
            return

        matchers = self._matchers
        offsets = self._offsets
        r = False

        while self._current < len(matchers) and docid > self.id():
            mr = matchers[self._current]
            sr = mr.skip_to(docid - offsets[self._current])
            r = sr or r
            if mr.is_active():
                break

            self._next_matcher()

        return r

    def save(self) -> Any:
        return tuple(m.save() for m in self._matchers)

    def restore(self, place: Any):
        for i, m in self._matchers:
            m.restore(place[i])

    @matchers.check_active
    def weight(self) -> float:
        return self._matchers[self._current].weight()

    @matchers.check_active
    def score(self) -> float:
        current = self._matchers[self._current]
        return self._scorer.score(current)

    @matchers.check_active
    def posting(self) -> 'Optional[postings.PostTuple]':
        offset = self._offsets[self._current]
        p = self._matchers[self._current].posting()
        return postings.update_post(p, docid=offset + p[postings.DOCID])

    def _filter_postings(self, ps: 'Iterable[postings.PostTuple]', offset: int
                         ) -> 'Iterable[postings.PostTuple]':
        update_post = postings.update_post
        DOCID = postings.DOCID
        for p in ps:
            yield update_post(p, docid=p[DOCID] + offset)

    def all_postings(self) -> 'Iterable[postings.PostTuple]':
        if not self.is_active():
            return

        offsets = self._offsets
        for i, m in enumerate(self._matchers):
            for p in self._filter_postings(m.all_postings(), offsets[i]):
                yield p

    def all_raw_postings(self) -> 'Iterable[postings.RawPost]':
        if not self.is_active():
            return

        offsets = self._offsets
        for i, m in enumerate(self._matchers):
            for p in self._filter_postings(m.all_raw_postings(), offsets[i]):
                yield p

    def children(self) -> Sequence[matchers.Matcher]:
        # Not sure what the right thing to do is here... for now, I'm returning
        # the current matcher as the only "child"
        return [self._matchers[self._current]]

    def copy(self) -> 'MultiMatcher':
        return self.__class__([mr.copy() for mr in self._matchers],
                              self._offsets, self._scorer, self._current)

    def replace(self, minquality=0):
        m = self.__class__(self._matchers, self._offsets, self._scorer,
                           self._current)
        if minquality:
            # Skip sub-matchers that don't have a high enough max quality to
            # contribute
            while (m.is_active() and
                   m._matchers[m._current].max_quality() < minquality):
                m._current += 1
                m._next_matcher()

        if not m.is_active():
            return matchers.NullMatcher()

        # TODO: Possible optimization: if the last matcher is current, replace
        # this with the last matcher, but wrap it with a matcher that adds the
        # offset. Have to check whether that's actually faster, though.
        return m

    def supports(self, name: str) -> bool:
        return all(mr.supports(name) for mr in self._matchers)

    def supports_block_quality(self) -> bool:
        return all(mr.supports_block_quality() for mr in self._matchers)

    @matchers.check_active
    def max_quality(self):
        return max(m.max_quality() for m in self._matchers[self._current:])

    @matchers.check_active
    def block_quality(self):
        return self._matchers[self._current].block_quality()

    # Override derived

    def all_ids(self):
        if not self.is_active():
            return

        offsets = self._offsets
        for i, mr in enumerate(self._matchers):
            offset = offsets[i]
            if mr.is_active():
                for id in mr.all_ids():
                    yield id + offset

    # Since this matcher serializes the sub-matchers, rather than combines them,
    # it can forward the leaf methods to the current sub-matcher

    def has_weights(self) -> bool:
        return self._matchers[self._current].has_weights()

    def has_lengths(self) -> bool:
        return self._matchers[self._current].has_lengths()

    def has_positions(self) -> bool:
        return self._matchers[self._current].has_positions()

    def has_chars(self) -> bool:
        return self._matchers[self._current].has_chars()

    def has_payloads(self) -> bool:
        return self._matchers[self._current].has_payloads()

    def length(self) -> int:
        return self._matchers[self._current].length()

    def positions(self) -> Sequence[int]:
        return self._matchers[self._current].positions()

    def chars(self) -> Sequence[Tuple[int]]:
        return self._matchers[self._current].chars()

    def payloads(self) -> Sequence[bytes]:
        return self._matchers[self._current].payloads()


class FilterMatcher(WrappingMatcher):
    """
    Filters the postings from the wrapped based on whether the IDs are
    present in or absent from a set.
    """

    def __init__(self, child: matchers.Matcher,
                 ids: 'Union[idsets.DocIdSet, Set]', exclude: bool=False,
                 boost: float=1.0):
        """
        :param child: the child matcher.
        :param ids: a set of IDs to filter by.
        :param exclude: by default, only IDs from the wrapped matcher that are
            **in** the set are used. If this argument is True, only IDs from
            the wrapped matcher that are **not in** the set are used.
        :param boost: Multiply scores in the wrapped matcher by this factor.
        """

        super(FilterMatcher, self).__init__(child, boost)
        self._ids = ids
        self._exclude = exclude
        self._find_next()

    def __repr__(self):
        key = "exclude" if self._exclude else "include"
        return "%s(%r, %s=%r, boost=%s)" % (
            type(self).__name__, self.child, key, self._ids, self._boost
        )

    def _rewrap(self, newchild: matchers.Matcher):
        return self.__class__(newchild, self._ids, self._exclude, self._boost)

    def _find_next(self) -> bool:
        child = self.child
        ids = self._ids
        r = False

        if self._exclude:
            while child.is_active() and child.id() in ids:
                r = child.next() or r
        else:
            while child.is_active() and child.id() not in ids:
                r = child.next() or r
        return r

    # Override interface

    def is_active(self) -> bool:
        return self.child.is_active()

    def next(self):
        self.child.next()
        self._find_next()

    def skip_to(self, id):
        self.child.skip_to(id)
        self._find_next()

    def replace(self, minquality=0) -> 'matchers.Matcher':
        if not self.child.is_active():
            return matchers.NullMatcher()
        else:
            return self

    # Override derived

    def all_ids(self) -> Iterable[int]:
        if not self.is_active():
            return iter(())

        ids = self._ids
        if self._exclude:
            return (id for id in self.child.all_ids() if id not in ids)
        else:
            return (id for id in self.child.all_ids() if id in ids)

    def _filter_postings(self, ps: 'Iterable[postings.PostTuple]'):
        ids = self._ids
        DOCID = postings.DOCID

        if self._exclude:
            return (p for p in ps if p[DOCID] not in ids)
        else:
            return (p for p in ps if p[DOCID] in ids)

    def all_postings(self) -> 'Iterable[postings.PostTuple]':
        return self._filter_postings(self.child.all_postings())

    def all_raw_postings(self) -> 'Iterable[postings.RawPost]':
        return self._filter_postings(self.child.all_raw_postings())


class InverseMatcher(WrappingMatcher):
    """
    Generates matches that are NOT present in the wrapped matcher.
    """

    def __init__(self, child: matchers.Matcher, limit: int,
                 missing: Callable[[int], bool]=None, weight: float=1.0,
                 start: int=0):
        """

        :param child: the matcher to invert.
        :param limit: the document count.
        :param missing: an optional function that returns true if a given
            document number is missing (for example, deleted), and shouldn't
            be generated by this matcher.
        :param weight: the weight to return for generated matches.
        :param start: the document number to start at.
        """

        super(InverseMatcher, self).__init__(child)
        self._limit = limit
        self._weight = weight
        self._is_missing = missing or (lambda id: False)
        self._id = start
        self._find_next()

    def _rewrap(self, newchild: matchers.Matcher):
        return self.__class__(newchild, self._limit, self._is_missing,
                              self._weight, self._id)

    def _find_next(self):
        child = self.child
        missing = self._is_missing

        # If the current docnum isn't missing and the child matcher is
        # exhausted (so we don't have to worry about skipping its matches), we
        # don't have to do anything
        if not child.is_active() and not missing(self._id):
            return

        # Skip missing documents
        while self._id < self._limit and missing(self._id):
            self._id += 1

        # Catch the child matcher up to where this matcher is
        if child.is_active() and child.id() < self._id:
            child.skip_to(self._id)

        # While self._id is missing or is in the child matcher, increase it
        while child.is_active() and self._id < self._limit:
            if missing(self._id):
                self._id += 1
                continue

            if self._id == child.id():
                self._id += 1
                child.next()
                continue

            break

    # Override interface

    def is_active(self) -> bool:
        return self._id < self._limit

    @matchers.check_active
    def id(self) -> int:
        return self._id

    @matchers.check_active
    def next(self):
        self._id += 1
        self._find_next()

    @matchers.check_active
    def skip_to(self, docid):
        if self._id >= self._limit:
            raise matchers.ReadTooFar
        if docid < self._id:
            return
        self._id = docid
        self._find_next()

    def save(self) -> Tuple[int, Any]:
        return self._id, self.child.save()

    def restore(self, place: Tuple[int, Any]):
        self._id = place[0]
        self.child.restore(place[1])

    @matchers.check_active
    def weight(self) -> float:
        return self._weight

    @matchers.check_active
    def score(self):
        return self._weight

    @matchers.check_active
    def posting(self) -> 'Optional[postings.PostTuple]':
        return None

    def term(self) -> Optional[Tuple[str, bytes]]:
        return None

    def children(self) -> 'Sequence[Matcher]':
        # Not sure what to do here, but it doesn't seem like this matcher should
        # report having children
        return ()

    def supports(self, name: str):
        return False

    def supports_block_quality(self):
        return False


class SingleTermMatcher(WrappingMatcher):
    """
    Makes a tree of matchers act as if they were a matcher for a single
    term for the purposes of "what terms are matching?" questions.
    """

    def __init__(self, child: matchers.Matcher, term: Tuple[str, bytes]):
        super(SingleTermMatcher, self).__init__(child)
        self._term = term

    def _rewrap(self, newchild: matchers.Matcher) -> matchers.Matcher:
        return self.__class__(newchild, self._term)

    def term(self) -> Tuple[str, bytes]:
        return self._term


class CoordMatcher(WrappingMatcher):
    """
    Modifies the computed score to penalize documents that don't match all
    terms in the matcher tree.

    Because this matcher modifies the score, it may give unexpected results
    when compared to another matcher returning the unmodified score.
    """

    def __init__(self, child: matchers.Matcher, scale: float=1.0,
                 termcount: int=0):
        """
        :param child: the matcher to boost scores on.
        :param scale: a scaling factor on the score boost.
        :param termcount: used when the matcher is re-wrapped.
        """

        super(CoordMatcher, self).__init__(child)
        self._termcount = termcount or len(list(child.term_matchers()))
        self._maxqual = child.max_quality()
        self._scale = scale

    def _rewrap(self, newchild):
        return self.__class__(newchild, self._scale, self._termcount)

    def _sqr(self, score: float, matching):
        # This is the "SQR" (Short Query Ranking) function used by Apple's old
        # V-twin search library, described in the paper "V-Twin: A Lightweight
        # Engine for Interactive Use".
        #
        # http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.56.1916

        # score - document score using the current weighting function
        # matching - number of matching terms in the current document
        termcount = self._termcount  # Number of terms in this tree
        scale = self._scale  # Scaling factor

        sqr = ((score + ((matching - 1) / (termcount - scale) ** 2)) *
               ((termcount - 1) / termcount))
        return sqr

    # Override interface

    def max_quality(self):
        return self._sqr(self.child.max_quality(), self._termcount)

    def block_quality(self):
        return self._sqr(self.child.block_quality(), self._termcount)

    def score(self):
        child = self.child

        score = child.score()
        matching = 0
        for _ in child.matching_terms(child.id()):
            matching += 1

        sqr = self._sqr(score, matching)
        return sqr


class AndNotMatcher(WrappingMatcher):
    """
    Matches the postings in the first sub-matcher that are NOT present in
    the second sub-matcher.
    """

    def __init__(self, child: matchers.Matcher, nots: matchers.Matcher):
        super(AndNotMatcher, self).__init__(child)
        self.neg = nots

        if (
            self.child.is_active() and
            self.neg.is_active() and
            self.child.id() == self.neg.id()
        ):
            self.next()

    def _rewrap(self, newchild: matchers.Matcher) -> 'AndNotMatcher':
        return self.__class__(newchild, self.neg)

    def _find_next(self):
        child = self.child
        neg = self.neg

        # If the second ("negative") matcher is inactive, we don't need to skip
        # at all
        if not self.neg.is_active():
            return

        if self.neg.id() < child.id():
            # The negative matcher is behind, so skip it to at least the
            # positive matcher's document
            neg.skip_to(child.id())
            if not neg.is_active():
                return

        # As long as the positive and negative matchers are on the same document
        # we need to advance
        r = False
        while child.is_active() and child.id() == neg.id():
            # Advance the positive matcher
            nr = self.child.next()
            if not self.child.is_active():
                return True
            r = r or nr

            # Skip the negative matcher to where the positive matcher is now
            self.neg.skip_to(self.child.id())
            if not self.neg.is_active():
                return r

        return r

    # Override methods that advance the matcher to avoid negative matches

    def next(self) -> bool:
        ar = self.child.next()
        nr = self._find_next()
        return ar or nr

    def skip_to(self, docid: int) -> bool:
        ar = self.child.skip_to(docid)
        nr = self._find_next()
        return ar or nr

    def save(self) -> Tuple[Any, Any]:
        return self.child.save(), self.neg.save()

    def restore(self, place: Tuple[Any, Any]):
        self.child.restore(place[0])
        self.neg.restore(place[1])

    def copy(self) -> matchers.Matcher:
        return self.__class__(self.child.copy(), self.neg.copy())

    def replace(self, minquality: float=0) -> matchers.Matcher:
        # If the child matcher is  inactive, or can't contribute, return a null
        # matcher
        if not self.child.is_active() or self.max_quality() <= minquality:
            return matchers.NullMatcher()

        child = self.child.replace(minquality)

        # If the second matcher is inactive, we don't need to worry about it
        # anymore, so just replace with the first matcher
        if not self.neg.is_active():
            return child

        # Replace the second matcher, in case it can become more efficient
        neg = self.neg.replace()
        if child is not self.child or neg is not self.neg:
            return self.__class__(child, neg)
        else:
            return self

    def skip_to_quality(self, minquality: float) -> int:
        skipped = self.child.skip_to_quality(minquality)
        self._find_next()
        return skipped

    def all_ids(self) -> Iterable[int]:
        return matchers.Matcher.all_ids(self)


class AndMaybeMatcher(WrappingMatcher):
    """
    Matches postings in the first sub-matcher, and if the same posting is
    in the second sub-matcher, adds their scores.
    """

    def __init__(self, child: matchers.Matcher, maybe: matchers.Matcher):
        super(AndMaybeMatcher, self).__init__(child)
        self.maybe = maybe
        self._keep_up()

    def _keep_up(self) -> bool:
        # As long as "maybe" is active it must be on or ahead of child
        if (
            self.child.is_active() and self.maybe.is_active() and
            self.maybe.id() < self.child.id()
        ):
            return self.maybe.skip_to(self.child.id())

    # Override advancing methods to also advance the maybe matcher

    def next(self):
        ar = self.child.next()
        kr = self._keep_up()
        return ar or kr

    def skip_to(self, id):
        ar = self.child.skip_to(id)
        kr = self._keep_up()
        return ar or kr

    def save(self) -> Tuple[Any, Any]:
        return self.child.save(), self.maybe.save()

    def restore(self, place: Tuple[Any, Any]):
        self.child.restore(place[0])
        self.maybe.restore(place[1])

    def copy(self) -> matchers.Matcher:
        return self.__class__(self.child.copy(), self.maybe.copy())

    def replace(self, minquality: float=0) -> matchers.Matcher:
        # If the child matcher is  inactive, return a null matcher
        if not self.child.is_active():
            return matchers.NullMatcher()

        # If the maybe matcher isn't active, we don't have to worry about it
        # anymore, just replace the child
        if not self.maybe.is_active():
            return self.child.replace(minquality)

        if minquality:
            if self.child.max_quality() + self.maybe.max_quality() < minquality:
                # The combined max quality of the sub-matchers isn't high
                # enough to possibly contribute, return an inactive matcher
                return matchers.NullMatcher()

            elif self.child.max_quality() < minquality:
                # If the max quality of the main sub-matcher isn't high enough
                # to ever contribute without the optional sub-matcher, convert
                # into an IntersectionMatcher
                from whoosh.matching.binary import IntersectionMatcher

                return IntersectionMatcher(self.child, self.maybe)

        # Try to replace the sub-matchers
        child = self.child.replace(minquality - self.maybe.max_quality())
        maybe = self.maybe.replace(minquality - self.child.max_quality())
        if child is not self.child or maybe is not self.maybe:
            # If one of the sub-matchers changed, return a new object
            return self.__class__(child, maybe)
        else:
            return self

    # Override score/quality methods to add the scores when the sub-matchers
    # are on the same document

    def max_quality(self) -> float:
        q = self.child.max_quality()
        if self.maybe.is_active():
            q += self.maybe.max_quality()
        return q

    def skip_to_quality(self, minquality: float) -> int:
        if not self.child.is_active():
            raise matchers.ReadTooFar

        # If the maybe matcher isn't active, ignore it
        if not self.maybe.is_active():
            return self.child.skip_to_quality(minquality)

        # As long as the two sub-matchers together can't contribute, skip the
        # lower quality one
        skipped = 0
        cq = self.child.block_quality()
        mq = self.maybe.block_quality()
        while (self.child.is_active() and self.maybe.is_active() and
               cq + mq <= minquality):
            if cq < mq:
                skipped += self.child.skip_to_quality(minquality - mq)
                cq = self.child.block_quality()
            else:
                skipped += self.maybe.skip_to_quality(minquality - cq)
                mq = self.maybe.block_quality()

        return skipped

    def weight(self) -> float:
        v = self.child.weight()
        if self.maybe.is_active() and self.child.id() == self.maybe.id():
            v += self.maybe.weight()
        return v

    def score(self) -> float:
        v = self.child.score()
        if self.maybe.is_active() and self.child.id() == self.maybe.id():
            v += self.maybe.score()
        return v


