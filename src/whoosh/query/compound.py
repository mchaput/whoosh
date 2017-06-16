# Copyright 2007 Matt Chaput. All rights reserved.
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
import copy
from typing import Callable, Iterable, Optional, Sequence

from whoosh import collectors
from whoosh.compat import text_type
from whoosh.ifaces import matchers, queries, readers, searchers
from whoosh.query import ranges


__all__ = ("CompoundQuery", "And", "Or", "DisjunctionMax", "BinaryQuery",
           "AndNot", "AndMaybe", "Otherwise", "Require")


class CompoundQuery(queries.Query):
    """
    Base class for queries that combine or manipulate the results
    of multiple sub-queries .
    """

    joint = "X"
    intersect_merge = True

    def __init__(self, subqueries: 'Iterable[queries.Query]',
                 startchar: int=None, endchar: int=None, error: str=None,
                 boost: float=1.0):
        super(CompoundQuery, self).__init__(
            startchar=startchar, endchar=endchar, error=error, boost=boost
        )
        self.subqueries = []
        self.set_children(list(subqueries))

    def __repr__(self):
        r = "%s(%r" % (self.__class__.__name__, self.subqueries)
        if hasattr(self, "boost") and self.boost != 1:
            r += ", boost=%s" % self.boost
        r += ")"
        return r

    def __unicode__(self):
        r = u"("
        r += self.joint.join([text_type(s) for s in self.subqueries])
        r += u")"
        return r

    __str__ = __unicode__

    def __eq__(self, other):
        return (other and
                self.__class__ is other.__class__ and
                self.subqueries == other.subqueries and
                self.boost == other.boost)

    def __getitem__(self, i) -> 'queries.Query':
        return self.subqueries[i]

    def __len__(self):
        return len(self.subqueries)

    def __iter__(self):
        return iter(self.subqueries)

    def __hash__(self):
        h = hash(self.__class__.__name__) ^ hash(self.boost)
        for q in self.subqueries:
            h ^= hash(q)
        return h

    @classmethod
    def combine_collector(cls, collector: 'collectors.Collector',
                          args, kwargs) -> 'collectors.Collector':
        qs = [collector.query()]
        for arg in args:
            if isinstance(arg, (list, tuple)):
                qs.extend(arg)
            elif isinstance(arg, queries.Query):
                qs.append(arg)
            elif isinstance(arg, collectors.Collector):
                qs.append(arg.query())
            else:
                raise ValueError("%s: don't know how to add %r" %
                                 (cls.__name__, arg))

        return collector.with_query(cls(qs, **kwargs))

    def is_leaf(self):
        return False

    def children(self):
        return self.subqueries

    def set_children(self, children: 'Sequence[queries.Query]'):
        for c in children:
            if not isinstance(c, queries.Query):
                raise queries.QueryError("%r is not a query" % c)
        self.subqueries = children

    def field(self):
        if self.subqueries:
            f = self.subqueries[0].field()
            if all(q.field() == f for q in self.subqueries[1:]):
                return f

    def estimate_size(self, ixreader):
        est = sum(q.estimate_size(ixreader) for q in self.subqueries)
        return min(est, ixreader.doc_count())

    def can_merge_with(self, other: 'queries.Query'):
        return self.__class__ is other.__class__

    def merge_subqueries(self) -> 'queries.Query':
        if not self.analyzed:
            return self

        newq = self.copy()
        subqs = []
        for s in self.subqueries:
            if self.can_merge_with(s):
                for ss in s.children():
                    ss.set_boost(ss.boost * s.boost)
                    subqs.append(ss)
            else:
                subqs.append(s)

        # if len(subqs) == 1:
        #     return subqs[0]
        # else:
        newq.set_children(subqs)
        return newq

    def normalize(self) -> 'queries.Query':
        from whoosh.query.ranges import Every

        # Normalize children
        self.set_children([s.normalize() for s in self.children()])
        subqueries = [q for q in self.merge_subqueries().children()
                      if not isinstance(q, queries.IgnoreQuery)]

        # If every subquery is Null, this query is Null
        if all(isinstance(q, queries.NullQuery) for q in subqueries):
            return queries.NullQuery()

        # If there's an unfielded Every inside, then this query is Every
        if any((isinstance(q, Every) and q.is_total())
               for q in subqueries):
            return Every()

        # Merge ranges and Everys
        everyfields = set()
        i = 0
        while i < len(subqueries):
            q = subqueries[i]
            f = q.field()
            if f and f in everyfields:
                subqueries.pop(i)
                continue

            if isinstance(q, (ranges.TermRange, ranges.NumericRange)):
                j = i + 1
                while j < len(subqueries):
                    if q.overlaps(subqueries[j]):
                        qq = subqueries.pop(j)
                        q = q.merge(qq, intersect=self.intersect_merge)
                    else:
                        j += 1
                q = subqueries[i] = q.normalize()

            if isinstance(q, Every):
                everyfields.add(q.field())
            i += 1

        # Eliminate duplicate queries
        subqs = []
        seenqs = set()
        for s in subqueries:
            if not isinstance(s, Every) and s.field() in everyfields:
                continue
            if s in seenqs:
                continue
            seenqs.add(s)
            subqs.append(s)

        # If no children are left, this query is Null
        if not subqs:
            return queries.NullQuery()

        # If there's only one child left, just return that
        if len(subqs) == 1:
            sub = subqs[0]
            sub_boost = sub.boost
            sub.set_boost(sub_boost * self.boost)
            return sub

        newq = self.copy()
        newq.set_children(subqs)
        return newq

    def simplify(self, ixreader: 'readers.IndexReader'):
        subs = [s.simplify(ixreader) for s in self.subqueries]
        if all(isinstance(s, queries.NullQuery) for s in subs):
            return queries.NullQuery()

        c = copy.copy(self)
        c.set_children(subs)
        return c

    def _tree_matcher(self, subs: 'Sequence[queries.Query]',
                      mcls: 'type(matchers.Matcher)',
                      searcher: 'searchers.Searcher',
                      context: 'searchers.SearchContext',
                      q_weight_fn: 'Optional[Callable[[queries.Query], float]]',
                      **kwargs):
        # Builds a tree of binary matchers from a linear sequence of queries.
        #
        # subs - the queries
        # mcls - the Matcher class to build the tree from
        # q_weight_fn - called on each query to build a huffman-like weighted
        #   tree. This can be None if the tree doesn't need weighting
        # kwargs - passed to matcher initializer

        if not subs:
            from whoosh.matching import NullMatcher
            return NullMatcher()

        # Get matchers for each query
        subms = [q.matcher(searcher, context) for q in subs]

        if len(subms) == 1:
            # Only one matcher, just return it
            m = subms[0]
        elif q_weight_fn is None:
            # No weighting function, just make a binary tree
            m = queries.make_binary_tree(mcls, subms, kwargs)
        else:
            # Use weighting function to make a huffman-like weighted tree
            w_subms = [(q_weight_fn(q), m) for q, m in zip(subs, subms)]
            m = queries.make_weighted_tree(mcls, w_subms, kwargs)

        # If this query had a boost, add a wrapping matcher to apply the boost
        if self.boost != 1.0:
            from whoosh.matching.wrappers import WrappingMatcher

            m = WrappingMatcher(m, self.boost)

        return m


@collectors.register("and_", compound=True)
class And(CompoundQuery):
    """
    Matches documents that match ALL of the subqueries.
    """

    # This is used by the superclass's __unicode__ method.
    joint = " AND "
    # When merging ranges inside ANDs, take the intersection
    intersect_merge = True

    def estimate_size(self, ixreader: 'readers.IndexReader') -> int:
        return min(q.estimate_size(ixreader) for q in self.subqueries)

    def matcher(self, searcher: 'searchers.Searcher',
                context: 'searchers.SearchContext'=None) -> 'matchers.Matcher':
        from whoosh.matching.binary import IntersectionMatcher

        r = searcher.reader()
        return self._tree_matcher(self.subqueries, IntersectionMatcher,
                                  searcher, context,
                                  lambda q: 0 - q.estimate_size(r))


@collectors.register("or_", compound=True)
class Or(CompoundQuery):
    """
    Matches documents that match ANY of the subqueries.
    """

    # This is used by the superclass's __unicode__ method.
    joint = " OR "
    # When merging ranges inside ORs, take the union
    intersect_merge = False
    # Use pre-loaded matcher for small indexes
    preload = True
    # Can't have more than this many clauses in one tree
    TOO_MANY_CLAUSES = 1024

    def __init__(self, subqueries: 'Iterable[queries.Query]', startchar: int=None,
                 endchar: int=None, error: str=None, boost: float=1.0,
                 minmatch: float=0.0, scale: float=None):
        """
        :param subqueries: a list of :class:`Query` objects to search for.
        :param boost: a boost factor to apply to the scores of all matching
            documents.
        :param minmatch: not yet implemented.
        :param scale: a scaling factor for a "coordination bonus". If this
            value is not None, it should be a floating point number between 0
            and 1. The scores of the matching documents are boosted/penalized
            based on the number of query terms that matched in the document.
            This number scales the effect of the bonuses.
        """

        super(Or, self).__init__(
            subqueries, startchar=startchar, endchar=endchar, error=error,
            boost=boost,
        )
        self.minmatch = minmatch
        self.scale = scale

    def __unicode__(self):
        r = u"("
        r += (self.joint).join([text_type(s) for s in self.subqueries])
        r += u")"
        if self.minmatch:
            r += u">%s" % self.minmatch
        return r

    __str__ = __unicode__

    def normalize(self):
        norm = CompoundQuery.normalize(self)
        if norm.__class__ is self.__class__:
            norm.minmatch = self.minmatch
            norm.scale = self.scale
        return norm

    def matcher(self, searcher: 'searchers.Searcher',
                context: 'searchers.SearchContext'=None) -> 'matchers.Matcher':
        from whoosh.matching.binary import UnionMatcher
        from whoosh.matching.wrappers import CoordMatcher

        reader = searcher.reader()
        scored = context.scored if context else True
        subs = self.subqueries

        # Make a tree of UnionMatchers
        m = self._tree_matcher(subs, UnionMatcher, searcher, context,
                               lambda q: q.estimate_size(reader))

        if self.scale and any(m.term_matchers()):
            # If a scaling factor was given, wrap the matcher in a CoordMatcher
            # to alter scores based on term coordination
            return CoordMatcher(m, scale=self.scale)

        preload = False
        dc = searcher.doc_count_all()
        if self.preload and len(subs) > 2:
            if dc <= 5000 or len(subs) >= self.TOO_MANY_CLAUSES:
                preload = True

        if preload:
            from whoosh.matching.combo import ArrayUnionMatcher

            ms = [sub.matcher(searcher, context) for sub in subs]
            m = ArrayUnionMatcher(ms, m, dc, boost=self.boost, scored=scored)

        return m


@collectors.register("dismax_", compound=True)
class DisjunctionMax(CompoundQuery):
    """
    Matches all documents that match any of the subqueries, but scores each
    document using the maximum score from the subqueries.
    """

    def __init__(self, subqueries, boost=1.0, tiebreak=0.0):
        super(DisjunctionMax, self).__init__(subqueries, boost=boost)
        self.tiebreak = tiebreak

    def __unicode__(self):
        r = u"DisMax("
        r += " ".join(sorted(text_type(s) for s in self.subqueries))
        r += u")"
        if self.tiebreak:
            r += u"~" + text_type(self.tiebreak)
        return r

    __str__ = __unicode__

    def normalize(self):
        norm = CompoundQuery.normalize(self)
        if norm.__class__ is self.__class__:
            norm.tiebreak = self.tiebreak
        return norm

    def matcher(self, searcher: 'searchers.Searcher',
                context: 'searchers.SearchContext'=None) -> 'matchers.Matcher':
        from whoosh.matching.binary import DisjunctionMaxMatcher

        r = searcher.reader()
        return self._tree_matcher(self.subqueries, DisjunctionMaxMatcher,
                                  searcher, context,
                                  lambda q: q.estimate_size(r),
                                  tiebreak=self.tiebreak)


# Boolean queries

class BinaryQuery(CompoundQuery):
    """
    Base class for binary queries (queries which are composed of two
    sub-queries). Subclasses should set the ``matcherclass`` attribute or
    override ``matcher()``, and may also need to override ``normalize()``,
    ``estimate_size()``, and/or ``estimate_min_size()``.
    """

    boost = 1.0

    def __init__(self, a, b):
        super(BinaryQuery, self).__init__((a, b))
        self.a = a
        self.b = b

    def __eq__(self, other):
        return (other and self.__class__ is other.__class__
                and self.a == other.a and self.b == other.b)

    def __hash__(self):
        return hash(self.__class__.__name__) ^ hash(self.a) ^ hash(self.b)

    def set_children(self, children: 'Sequence[queries.Query]'):
        assert len(children) == 2
        self.a = children[0]
        self.b = children[1]
        self.subqueries = [self.a, self.b]

    @classmethod
    def combine_collector(cls, collector: 'collectors.Collector',
                          args, kwargs) -> 'collectors.Collector':
        q1 = collector.query()
        q = cls(q1, args[0], *args[1:], **kwargs)
        return collector.with_query(q)

    def needs_spans(self):
        return self.a.needs_spans() or self.b.needs_spans()

    def field(self):
        f = self.a.field()
        if self.b.field() == f:
            return f

    def with_boost(self, boost):
        return self.__class__(self.a.with_boost(boost),
                              self.b.with_boost(boost))

    def normalize(self):
        a = self.a.normalize()
        b = self.b.normalize()

        if isinstance(a, queries.NullQuery) and isinstance(b, queries.NullQuery):
            return queries.NullQuery()
        elif isinstance(a, queries.NullQuery):
            return b
        elif isinstance(b, queries.NullQuery):
            return a

        return self.__class__(a, b)


@collectors.register("and_not")
class AndNot(BinaryQuery):
    """
    Binary boolean query of the form 'a ANDNOT b', where documents that
    match b are removed from the matches for a.
    """

    joint = " ANDNOT "

    def normalize(self):
        a = self.a.normalize()
        b = self.b.normalize()

        if isinstance(a, queries.NullQuery):
            return queries.NullQuery()
        elif isinstance(b, queries.NullQuery):
            return a

        return self.__class__(a, b)

    def requires(self):
        return self.a.requires()

    def matcher(self, searcher, context=None):
        from whoosh.matching.wrappers import AndNotMatcher

        scoredm = self.a.matcher(searcher, context)
        notm = self.b.matcher(searcher, searcher.boolean_context())
        return AndNotMatcher(scoredm, notm)


@collectors.register("otherwise")
class Otherwise(BinaryQuery):
    """
    A binary query that only matches the second clause if the first clause
    doesn't match any documents.
    """

    joint = " OTHERWISE "

    def matcher(self, searcher, context=None):
        m = self.a.matcher(searcher, context)
        if not m.is_active():
            m = self.b.matcher(searcher, context)
        return m


@collectors.register("require")
class Require(BinaryQuery):
    """
    Binary query returns results from the first query that also appear in
    the second query, but only uses the scores from the first query. This lets
    you filter results without affecting scores.
    """

    joint = " REQUIRE "

    def requires(self):
        return self.a.requires() | self.b.requires()

    def estimate_size(self, ixreader):
        return self.b.estimate_size(ixreader)

    def estimate_min_size(self, ixreader):
        return self.b.estimate_min_size(ixreader)

    def with_boost(self, boost):
        return self.__class__(self.a.with_boost(boost), self.b)

    def normalize(self):
        a = self.a.normalize()
        b = self.b.normalize()
        if isinstance(b, queries.IgnoreQuery):
            return a
        if isinstance(a, queries.NullQuery) or isinstance(b, queries.NullQuery):
            return queries.NullQuery()
        return self.__class__(a, b)

    def docs(self, searcher: 'searchers.Searcher',
             deleting: bool=False) -> Iterable[int]:
        return And(self.subqueries).docs(searcher, deleting)

    def matcher(self, searcher, context=None):
        from whoosh.matching.binary import RequireMatcher

        scoredm = self.a.matcher(searcher, context)
        requiredm = self.b.matcher(searcher, searcher.boolean_context())
        return RequireMatcher(scoredm, requiredm)


@collectors.register("and_maybe")
class AndMaybe(BinaryQuery):
    """
    Binary query takes results from the first query. If and only if the
    same document also appears in the results from the second query, the score
    from the second query will be added to the score from the first query.
    """

    joint = " ANDMAYBE "

    def normalize(self):
        a = self.a.normalize()
        b = self.b.normalize()
        if isinstance(a, queries.NullQuery):
            return queries.NullQuery()
        if isinstance(b, queries.NullQuery):
            return a
        return self.__class__(a, b)

    def estimate_min_size(self, ixreader):
        return self.subqueries[0].estimate_min_size(ixreader)

    def docs(self, searcher: 'searchers.Searcher',
             deleting: bool=False) -> Iterable[int]:
        return self.subqueries[0].docs(searcher, deleting)

    def matcher(self, searcher, context=None):
        from whoosh.matching.wrappers import AndMaybeMatcher

        return AndMaybeMatcher(self.a.matcher(searcher, context),
                               self.b.matcher(searcher, context))


# def BooleanQuery(required, should, prohibited):
#     return AndNot(AndMaybe(And(required), Or(should)),
#                   Or(prohibited)).normalize()
