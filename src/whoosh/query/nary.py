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

from whoosh import matching
from whoosh.compat import text_type, u
from whoosh.query import qcore
from whoosh.util import make_binary_tree, make_weighted_tree


class CompoundQuery(qcore.Query):
    """Abstract base class for queries that combine or manipulate the results
    of multiple sub-queries .
    """

    def __init__(self, subqueries, boost=1.0):
        self.subqueries = subqueries
        self.boost = boost

    def __repr__(self):
        r = "%s(%r" % (self.__class__.__name__, self.subqueries)
        if hasattr(self, "boost") and self.boost != 1:
            r += ", boost=%s" % self.boost
        r += ")"
        return r

    def __unicode__(self):
        r = u("(")
        r += (self.JOINT).join([text_type(s) for s in self.subqueries])
        r += u(")")
        return r

    __str__ = __unicode__

    def __eq__(self, other):
        return other and self.__class__ is other.__class__ and\
        self.subqueries == other.subqueries and\
        self.boost == other.boost

    def __getitem__(self, i):
        return self.subqueries.__getitem__(i)

    def __len__(self):
        return len(self.subqueries)

    def __iter__(self):
        return iter(self.subqueries)

    def __hash__(self):
        h = hash(self.__class__.__name__) ^ hash(self.boost)
        for q in self.subqueries:
            h ^= hash(q)
        return h

    def is_leaf(self):
        return False

    def children(self):
        return iter(self.subqueries)

    def apply(self, fn):
        return self.__class__([fn(q) for q in self.subqueries],
                              boost=self.boost)

    def field(self):
        if self.subqueries:
            f = self.subqueries[0].field()
            if all(q.field() == f for q in self.subqueries[1:]):
                return f

    def estimate_size(self, ixreader):
        return sum(q.estimate_size(ixreader) for q in self.subqueries)

    def estimate_min_size(self, ixreader):
        subs, nots = self._split_queries()
        subs_min = min(q.estimate_min_size(ixreader) for q in subs)
        if nots:
            nots_sum = sum(q.estimate_size(ixreader) for q in nots)
            subs_min = max(0, subs_min - nots_sum)
        return subs_min

    def normalize(self):
        from whoosh.query import Every, TermRange, NumericRange

        # Normalize subqueries and merge nested instances of this class
        subqueries = []
        for s in self.subqueries:
            s = s.normalize()
            if isinstance(s, self.__class__):
                subqueries += [ss.with_boost(ss.boost * s.boost) for ss in s]
            else:
                subqueries.append(s)

        # If every subquery is Null, this query is Null
        if all(q is qcore.NullQuery for q in subqueries):
            return qcore.NullQuery

        # If there's an unfielded Every inside, then this query is Every
        if any((isinstance(q, Every) and q.fieldname is None)
               for q in subqueries):
            return Every()

        # Merge ranges and Everys
        everyfields = set()
        i = 0
        while i < len(subqueries):
            q = subqueries[i]
            f = q.field()
            if f in everyfields:
                subqueries.pop(i)
                continue

            if isinstance(q, (TermRange, NumericRange)):
                j = i + 1
                while j < len(subqueries):
                    if q.overlaps(subqueries[j]):
                        qq = subqueries.pop(j)
                        q = q.merge(qq, intersect=self.intersect_merge)
                    else:
                        j += 1
                q = subqueries[i] = q.normalize()

            if isinstance(q, Every):
                everyfields.add(q.fieldname)
            i += 1

        # Eliminate duplicate queries
        subqs = []
        seenqs = set()
        for s in subqueries:
            if (not isinstance(s, Every) and s.field() in everyfields):
                continue
            if s in seenqs:
                continue
            seenqs.add(s)
            subqs.append(s)

        # Remove NullQuerys
        subqs = [q for q in subqs if q is not qcore.NullQuery]

        if not subqs:
            return qcore.NullQuery

        if len(subqs) == 1:
            sub = subqs[0]
            if not (self.boost == 1.0 and sub.boost == 1.0):
                sub = sub.with_boost(sub.boost * self.boost)
            return sub

        return self.__class__(subqs, boost=self.boost)

    def _split_queries(self):
        from whoosh.query import Not

        subs = [q for q in self.subqueries if not isinstance(q, Not)]
        nots = [q.query for q in self.subqueries if isinstance(q, Not)]
        return (subs, nots)

    def simplify(self, ixreader):
        subs, nots = self._split_queries()

        if subs:
            subs = self.__class__([subq.simplify(ixreader) for subq in subs],
                                  boost=self.boost).normalize()
            if nots:
                nots = Or(nots).simplify().normalize()
                return AndNot(subs, nots)
            else:
                return subs
        else:
            return qcore.NullQuery

    def _matcher(self, matchercls, q_weight_fn, searcher, weighting=None,
                 **kwargs):
        # q_weight_fn is a function which is called on each query and returns a
        # "weight" value which is used to build a huffman-like matcher tree. If
        # q_weight_fn is None, an order-preserving binary tree is used instead.

        # Pull any queries inside a Not() out into their own list
        subs, nots = self._split_queries()

        if not subs:
            return matching.NullMatcher()

        # Create a matcher from the list of subqueries
        subms = [q.matcher(searcher, weighting=weighting) for q in subs]
        if len(subms) == 1:
            m = subms[0]
        elif q_weight_fn is None:
            m = make_binary_tree(matchercls, subms)
        else:
            w_subms = [(q_weight_fn(q), m) for q, m in zip(subs, subms)]
            m = make_weighted_tree(matchercls, w_subms)

        # If there were queries inside Not(), make a matcher for them and
        # wrap the matchers in an AndNotMatcher
        if nots:
            if len(nots) == 1:
                notm = nots[0].matcher(searcher)
            else:
                r = searcher.reader()
                notms = [(q.estimate_size(r), q.matcher(searcher))
                         for q in nots]
                notm = make_weighted_tree(matching.UnionMatcher, notms)

            if notm.is_active():
                m = matching.AndNotMatcher(m, notm)

        # If this query had a boost, add a wrapping matcher to apply the boost
        if self.boost != 1.0:
            m = matching.WrappingMatcher(m, self.boost)

        return m


class And(CompoundQuery):
    """Matches documents that match ALL of the subqueries.

    >>> And([Term("content", u"render"),
    ...      Term("content", u"shade"),
    ...      Not(Term("content", u"texture"))])
    >>> # You can also do this
    >>> Term("content", u"render") & Term("content", u"shade")
    """

    # This is used by the superclass's __unicode__ method.
    JOINT = " AND "
    intersect_merge = True

    def requires(self):
        s = set()
        for q in self.subqueries:
            s |= q.requires()
        return s

    def estimate_size(self, ixreader):
        return min(q.estimate_size(ixreader) for q in self.subqueries)

    def matcher(self, searcher, weighting=None):
        r = searcher.reader()
        return self._matcher(matching.IntersectionMatcher,
                             lambda q: 0 - q.estimate_size(r), searcher,
                             weighting=weighting)


class Or(CompoundQuery):
    """Matches documents that match ANY of the subqueries.

    >>> Or([Term("content", u"render"),
    ...     And([Term("content", u"shade"), Term("content", u"texture")]),
    ...     Not(Term("content", u"network"))])
    >>> # You can also do this
    >>> Term("content", u"render") | Term("content", u"shade")
    """

    # This is used by the superclass's __unicode__ method.
    JOINT = " OR "
    intersect_merge = False
    matcher_class = matching.UnionMatcher

    def __init__(self, subqueries, boost=1.0, minmatch=0):
        CompoundQuery.__init__(self, subqueries, boost=boost)
        self.minmatch = minmatch

    def __unicode__(self):
        r = u("(")
        r += (self.JOINT).join([text_type(s) for s in self.subqueries])
        r += u(")")
        if self.minmatch:
            r += u(">%s") % self.minmatch
        return r

    __str__ = __unicode__

    def normalize(self):
        norm = CompoundQuery.normalize(self)
        if norm.__class__ is self.__class__:
            norm.minmatch = self.minmatch
        return norm

    def requires(self):
        if len(self.subqueries) == 1:
            return self.subqueries[0].requires()
        else:
            return set()

    def matcher(self, searcher, weighting=None):
        r = searcher.reader()
        return self._matcher(self.matcher_class, lambda q: q.estimate_size(r),
                             searcher, weighting=weighting)


class DisjunctionMax(CompoundQuery):
    """Matches all documents that match any of the subqueries, but scores each
    document using the maximum score from the subqueries.
    """

    def __init__(self, subqueries, boost=1.0, tiebreak=0.0):
        CompoundQuery.__init__(self, subqueries, boost=boost)
        self.tiebreak = tiebreak

    def __unicode__(self):
        r = u("DisMax(")
        r += " ".join([text_type(s) for s in self.subqueries])
        r += u(")")
        if self.tiebreak:
            s += u("~") + text_type(self.tiebreak)
        return r

    __str__ = __unicode__

    def normalize(self):
        norm = CompoundQuery.normalize(self)
        if norm.__class__ is self.__class__:
            norm.tiebreak = self.tiebreak
        return norm

    def requires(self):
        if len(self.subqueries) == 1:
            return self.subqueries[0].requires()
        else:
            return set()

    def matcher(self, searcher, weighting=None):
        r = searcher.reader()
        return self._matcher(matching.DisjunctionMaxMatcher,
                             lambda q: q.estimate_size(r), searcher,
                             weighting=weighting, tiebreak=self.tiebreak)


# Boolean queries

class BinaryQuery(CompoundQuery):
    """Base class for binary queries (queries which are composed of two
    sub-queries). Subclasses should set the ``matcherclass`` attribute or
    override ``matcher()``, and may also need to override ``normalize()``,
    ``estimate_size()``, and/or ``estimate_min_size()``.
    """

    boost = 1.0

    def __init__(self, a, b):
        self.a = a
        self.b = b
        self.subqueries = (a, b)

    def __eq__(self, other):
        return (other and self.__class__ is other.__class__
                and self.a == other.a and self.b == other.b)

    def __hash__(self):
        return (hash(self.__class__.__name__) ^ hash(self.a) ^ hash(self.b))

    def apply(self, fn):
        return self.__class__(fn(self.a), fn(self.b))

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
        if a is qcore.NullQuery and b is qcore.NullQuery:
            return qcore.NullQuery
        elif a is qcore.NullQuery:
            return b
        elif b is qcore.NullQuery:
            return a

        return self.__class__(a, b)

    def matcher(self, searcher, weighting=None):
        return self.matcherclass(self.a.matcher(searcher, weighting=weighting),
                                 self.b.matcher(searcher, weighting=weighting))


class AndNot(BinaryQuery):
    """Binary boolean query of the form 'a ANDNOT b', where documents that
    match b are removed from the matches for a.
    """

    JOINT = " ANDNOT "
    matcherclass = matching.AndNotMatcher

    def with_boost(self, boost):
        return self.__class__(self.a.with_boost(boost), self.b)

    def normalize(self):
        a = self.a.normalize()
        b = self.b.normalize()

        if a is qcore.NullQuery:
            return qcore.NullQuery
        elif b is qcore.NullQuery:
            return a

        return self.__class__(a, b)

    def requires(self):
        return self.a.requires()


class Otherwise(BinaryQuery):
    """A binary query that only matches the second clause if the first clause
    doesn't match any documents.
    """

    JOINT = " OTHERWISE "

    def matcher(self, searcher):
        m = self.a.matcher(searcher)
        if not m.is_active():
            m = self.b.matcher(searcher)
        return m


class Require(BinaryQuery):
    """Binary query returns results from the first query that also appear in
    the second query, but only uses the scores from the first query. This lets
    you filter results without affecting scores.
    """

    JOINT = " REQUIRE "
    matcherclass = matching.RequireMatcher

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
        if a is qcore.NullQuery or b is qcore.NullQuery:
            return qcore.NullQuery
        return self.__class__(a, b)

    def docs(self, searcher):
        return And(self.subqueries).docs(searcher)


class AndMaybe(BinaryQuery):
    """Binary query takes results from the first query. If and only if the
    same document also appears in the results from the second query, the score
    from the second query will be added to the score from the first query.
    """

    JOINT = " ANDMAYBE "
    matcherclass = matching.AndMaybeMatcher

    def normalize(self):
        a = self.a.normalize()
        b = self.b.normalize()
        if a is qcore.NullQuery:
            return qcore.NullQuery
        if b is qcore.NullQuery:
            return a
        return self.__class__(a, b)

    def requires(self):
        return self.a.requires()

    def estimate_min_size(self, ixreader):
        return self.subqueries[0].estimate_min_size(ixreader)

    def docs(self, searcher):
        return self.subqueries[0].docs(searcher)


def BooleanQuery(required, should, prohibited):
    return AndNot(AndMaybe(And(required), Or(should)),
                  Or(prohibited)).normalize()


