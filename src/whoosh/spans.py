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

"""
This module contains Query objects that deal with "spans".

Span queries allow for positional constraints on matching documents. For
example, the :class:`whoosh.spans.SpanNear` query matches documents where one
term occurs near another. Because you can nest span queries, and wrap them
around almost any non-span query, you can create very complex constraints.

For example, to find documents containing "whoosh" at most 5 positions before
"library" in the "text" field::

    from whoosh import query, spans
    t1 = query.Term("text", "whoosh")
    t2 = query.Term("text", "library")
    q = spans.SpanNear(t1, t2, slop=5)

"""

from whoosh.matching import (WrappingMatcher, AndMaybeMatcher, UnionMatcher,
                             IntersectionMatcher, NullMatcher)
from whoosh.query import Query, And, AndMaybe, Or, Term
from whoosh.util import make_binary_tree


# Span class

class Span(object):
    __slots__ = ("start", "end", "startchar", "endchar")
    
    def __init__(self, start, end=None, startchar=None, endchar=None):
        if end is None:
            end = start
        assert start <= end
        self.start = start
        self.end = end
        self.startchar = startchar
        self.endchar = endchar

    def __repr__(self):
        if self.startchar or self.endchar:
            return "<%d-%d %d:%d>" % (self.start, self.end, self.startchar, self.endchar)
        else:
            return "<%d-%d>" % (self.start, self.end)

    def __eq__(self, span):
        return (self.start == span.start
                and self.end == span.end
                and self.startchar == span.startchar
                and self.endchar == span.endchar)
    
    def __ne__(self, span):
        return self.start != span.start or self.end != span.end
    
    def __lt__(self, span):
        return self.start < span.start
    
    def __gt__(self, span):
        return self.start > span.start
    
    def __hash__(self):
        return hash((self.start, self.end))
    
    @classmethod
    def merge(cls, spans):
        """Merges overlapping and touches spans in the given list of spans.
        
        Note that this modifies the original list.
        
        >>> spans = [Span(1,2), Span(3)]
        >>> Span.merge(spans)
        >>> spans
        [<1-3>]
        """
        
        i = 0
        while i < len(spans) - 1:
            here = spans[i]
            j = i + 1
            while j < len(spans):
                there = spans[j]
                if there.start > here.end + 1:
                    break
                if here.touches(there) or here.overlaps(there):
                    here = here.to(there)
                    spans[i] = here
                    del spans[j]
                else:
                    j += 1
            i += 1
        return spans
    
    def to(self, span):
        return self.__class__(min(self.start, span.start), max(self.end, span.end),
                              min(self.startchar, span.startchar), max(self.endchar, span.endchar))
    
    def overlaps(self, span):
        return ((self.start >= span.start and self.start <= span.end)
                or (self.end >= span.start and self.end <= span.end)
                or (span.start >= self.start and span.start <= self.end)
                or (span.end >= self.start and span.end <= self.end))
    
    def surrounds(self, span):
        return self.start < span.start and self.end > span.end
    
    def is_within(self, span):
        return self.start >= span.start and self.end <= span.end
    
    def is_before(self, span):
        return self.end < span.start
    
    def is_after(self, span):
        return self.start > span.end
    
    def touches(self, span):
        return self.start == span.end + 1 or self.end == span.start - 1
    
    def distance_to(self, span):
        if self.overlaps(span):
            return 0
        elif self.is_before(span):
            return span.start - self.end
        elif self.is_after(span):
            return self.start - span.end


# Base matchers

class SpanWrappingMatcher(WrappingMatcher):
    """An abstract matcher class that wraps a "regular" matcher. This matcher
    uses the sub-matcher's matching logic, but only matches documents that have
    matching spans, i.e. where ``_get_spans()`` returns a non-empty list.
    
    Subclasses must implement the ``_get_spans()`` method, which returns a list
    of valid spans for the current document.
    """
    
    def __init__(self, child):
        super(SpanWrappingMatcher, self).__init__(child)
        self._spans = None
        if self.is_active():
            self._find_next()
    
    def copy(self):
        m = self.__class__(self.child.copy())
        m._spans = self._spans
        return m
    
    def _replacement(self, newchild):
        return self.__class__(newchild)
    
    def _find_next(self):
        if not self.is_active():
            return
        
        child = self.child
        r = False
        
        spans = self._get_spans()
        while child.is_active() and not spans:
            r = child.next() or r
            if not child.is_active():
                return True
            spans = self._get_spans()
        self._spans = spans
        
        return r
    
    def spans(self):
        return self._spans
    
    def next(self):
        self.child.next()
        self._find_next()
        
    def skip_to(self, id):
        self.child.skip_to(id)
        self._find_next()
        
    def all_ids(self):
        while self.is_active():
            if self.spans():
                yield self.id()
            self.next()


class SpanBiMatcher(SpanWrappingMatcher):
    def copy(self):
        return self.__class__(self.a.copy(), self.b.copy())
    
    def replace(self):
        if not self.is_active():
            return NullMatcher()
        return self


# Queries

class SpanQuery(Query):
    """Abstract base class for span-based queries. Each span query type wraps
    a "regular" query that implements the basic document-matching functionality
    (for example, SpanNear wraps an And query, because SpanNear requires that
    the two sub-queries occur in the same documents. The wrapped query is
    stored in the ``q`` attribute.
    
    Subclasses usually only need to implement the initializer to set the
    wrapped query, and ``matcher()`` to return a span-aware matcher object.
    """
    
    def _subm(self, s):
        return self.q.matcher(s)
    
    def __getattr__(self, name):
        return getattr(self.q, name)
    
    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.q)
    
    def __eq__(self, other):
        return (other and self.__class__ is other.__class__
                and self.q == other.q)
        
    def __hash__(self):
        return hash(self.__class__.__name__) ^ hash(self.q)
    

class SpanFirst(SpanQuery):
    """Matches spans that end within the first N positions. This lets you
    for example only match terms near the beginning of the document.
    """
    
    def __init__(self, q, limit=0):
        """
        :param q: the query to match.
        :param limit: the query must match within this position at the start
            of a document. The default is ``0``, which means the query must
            match at the first position.
        """
        
        self.q = q
        self.limit = limit
    
    def __eq__(self, other):
        return (other and self.__class__ is other.__class__
                and self.q == other.q and self.limit == other.limit)
    
    def __hash__(self):
        return hash(self.q) ^ hash(self.limit)
    
    def is_leaf(self):
        return False
    
    def apply(self, fn):
        return self.__class__(fn(self.q), limit=self.limit)
    
    def matcher(self, searcher):
        return SpanFirst.SpanFirstMatcher(self._subm(searcher),
                                          limit=self.limit)
        
    class SpanFirstMatcher(SpanWrappingMatcher):
        def __init__(self, child, limit=0):
            self.limit = limit
            super(SpanFirst.SpanFirstMatcher, self).__init__(child)
        
        def copy(self):
            return self.__class__(self.child.copy(), limit=self.limit)
        
        def _replacement(self, newchild):
            return self.__class__(newchild, limit=self.limit)
        
        def _get_spans(self):
            return [span for span in self.child.spans()
                    if span.end <= self.limit]


class SpanNear(SpanQuery):
    """Matches queries that occur near each other. By default, only matches
    queries that occur right next to each other (slop=1) and in order
    (ordered=True).
    
    For example, to find documents where "whoosh" occurs next to "library"
    in the "text" field::
    
        from whoosh import query, spans
        t1 = query.Term("text", "whoosh")
        t2 = query.Term("text", "library")
        q = spans.SpanNear(t1, t2)
        
    To find documents where "whoosh" occurs at most 5 positions before
    "library"::
    
        q = spans.SpanNear(t1, t2, slop=5)
        
    To find documents where "whoosh" occurs at most 5 positions before or after
    "library"::
    
        q = spans.SpanNear(t1, t2, slop=5, ordered=False)
        
    You can use the ``phrase()`` class method to create a tree of SpanNear
    queries to match a list of terms::
    
        q = spans.SpanNear.phrase("text", [u"whoosh", u"search", u"library"], slop=2)
    """
    
    def __init__(self, a, b, slop=1, ordered=True, mindist=1):
        """
        :param a: the first query to match.
        :param b: the second query that must occur within "slop" positions of
            the first query.
        :param slop: the number of positions within which the queries must
            occur. Default is 1, meaning the queries must occur right next
            to each other.
        :param ordered: whether a must occur before b. Default is True.
        :pram mindist: the minimum distance allowed between the queries.
        """
        
        self.q = And([a, b])
        self.a = a
        self.b = b
        self.slop = slop
        self.ordered = ordered
        self.mindist = mindist
    
    def __repr__(self):
        return "%s(%r, slop=%d, ordered=%s, mindist=%d)" % (self.__class__.__name__,
                                                            self.q, self.slop,
                                                            self.ordered,
                                                            self.mindist)
    
    def __eq__(self, other):
        return (other and self.__class__ == other.__class__
                and self.q == other.q and self.slop == other.slop
                and self.ordered == other.ordered
                and self.mindist == other.mindist)
    
    def __hash__(self):
        return (hash(self.a) ^ hash(self.b) ^ hash(self.slop)
                ^ hash(self.ordered) ^ hash(self.mindist))
    
    def is_leaf(self):
        return False
    
    def apply(self, fn):
        return self.__class__(fn(self.a), fn(self.b), slop=self.slop,
                              ordered=self.ordered, mindist=self.mindist)
    
    def matcher(self, searcher):
        ma = self.a.matcher(searcher)
        mb = self.b.matcher(searcher)
        return SpanNear.SpanNearMatcher(ma, mb, slop=self.slop,
                                        ordered=self.ordered,
                                        mindist=self.mindist)
    
    @classmethod
    def phrase(cls, fieldname, words, slop=1, ordered=True):
        """Returns a tree of SpanNear queries to match a list of terms.
        
        This class method is a convenience for constructing a phrase query
        using a binary tree of SpanNear queries.
        
        >>> SpanNear.phrase("f", [u"a", u"b", u"c", u"d"])
        SpanNear(SpanNear(Term("f", u"a"), Term("f", u"b")), SpanNear(Term("f", u"c"), Term("f", u"d")))
        
        :param fieldname: the name of the field to search in.
        :param words: a sequence of token texts to search for.
        :param slop: the number of positions within which the terms must
            occur. Default is 1, meaning the terms must occur right next
            to each other.
        :param ordered: whether the terms must occur in order. Default is True.
        """
        
        terms = [Term(fieldname, word) for word in words]
        return make_binary_tree(cls, terms, slop=slop, ordered=ordered)
    
    class SpanNearMatcher(SpanWrappingMatcher):
        def __init__(self, a, b, slop=1, ordered=True, mindist=1):
            self.a = a
            self.b = b
            self.slop = slop
            self.ordered = ordered
            self.mindist = mindist
            isect = IntersectionMatcher(a, b)
            super(SpanNear.SpanNearMatcher, self).__init__(isect)
        
        def copy(self):
            return self.__class__(self.a.copy(), self.b.copy(), slop=self.slop,
                                  ordered=self.ordered, mindist=self.mindist)
        
        def replace(self):
            if not self.is_active():
                return NullMatcher()
            return self
        
        def _get_spans(self):
            slop = self.slop
            mindist = self.mindist
            ordered = self.ordered
            spans = set()
            
            bspans = self.b.spans()
            for aspan in self.a.spans():
                for bspan in bspans:
                    if (bspan.end < aspan.start - slop
                        or (ordered and aspan.start > bspan.start)):
                        # B is too far in front of A, or B is in front of A
                        # *at all* when ordered is True
                        continue
                    if bspan.start > aspan.end + slop:
                        # B is too far from A. Since spans are listed in
                        # start position order, we know that all spans after
                        # this one will also be too far.
                        break
                    
                    # Check the distance between the spans
                    dist = aspan.distance_to(bspan)
                    if dist >= mindist and dist <= slop:
                        spans.add(aspan.to(bspan))
            
            return sorted(spans)


class SpanOr(SpanQuery):
    """Matches documents that match any of a list of sub-queries. Unlike
    query.Or, this class merges together matching spans from the different
    sub-queries when they overlap.
    """
    
    def __init__(self, subqs):
        """
        :param subqs: a list of queries to match.
        """
        
        self.q = Or(subqs)
        self.subqs = subqs
    
    def is_leaf(self):
        return False
    
    def apply(self, fn):
        return self.__class__([fn(sq) for sq in self.subqs])
    
    def matcher(self, searcher):
        matchers = [q.matcher(searcher) for q in self.subqs]
        return make_binary_tree(SpanOr.SpanOrMatcher, matchers)
    
    class SpanOrMatcher(SpanBiMatcher):
        def __init__(self, a, b):
            self.a = a
            self.b = b
            super(SpanOr.SpanOrMatcher, self).__init__(UnionMatcher(a, b))
    
        def _get_spans(self):
            if self.a.is_active() and self.b.is_active() and self.a.id() == self.b.id():
                spans = sorted(set(self.a.spans()) | set(self.b.spans()))
            elif not self.b.is_active() or self.a.id() < self.b.id():
                spans = self.a.spans()
            else:
                spans = self.b.spans()
            
            Span.merge(spans)
            return spans


class SpanBiQuery(SpanQuery):
    # Intermediate base class for methods common to "a/b" span query types

    def is_leaf(self):
        return False

    def apply(self, fn):
        return self.__class__(fn(self.a), fn(self.b))
    
    def matcher(self, searcher):
        ma = self.a.matcher(searcher)
        mb = self.b.matcher(searcher)
        return self._Matcher(ma, mb)


class SpanNot(SpanBiQuery):
    """Matches spans from the first query only if they don't overlap with
    spans from the second query. If there are no non-overlapping spans, the
    document does not match.
    
    For example, to match documents that contain "bear" at most 2 places after
    "apple" in the "text" field but don't have "cute" between them::
    
        from whoosh import query, spans
        t1 = query.Term("text", "apple")
        t2 = query.Term("text", "bear")
        near = spans.SpanNear(t1, t2, slop=2)
        q = spans.SpanNot(near, query.Term("text", "cute"))
    """
    
    def __init__(self, a, b):
        """
        :param a: the query to match.
        :param b: do not match any spans that overlap with spans from this
            query.
        """
        
        self.q = AndMaybe(a, b)
        self.a = a
        self.b = b
    
    class _Matcher(SpanBiMatcher):
        def __init__(self, a, b):
            self.a = a
            self.b = b
            super(SpanNot._Matcher, self).__init__(AndMaybeMatcher(a, b))
        
        def _get_spans(self):
            if self.a.id() == self.b.id():
                spans = []
                bspans = self.b.spans()
                for aspan in self.a.spans():
                    overlapped = False
                    for bspan in bspans:
                        if aspan.overlaps(bspan):
                            overlapped = True
                            break
                    if not overlapped:
                        spans.append(aspan)
                return spans
            else:
                return self.a.spans()


class SpanContains(SpanBiQuery):
    """Matches documents where the spans of the first query contain any spans
    of the second query.
    
    For example, to match documents where "apple" occurs at most 10 places
    before "bear" in the "text" field and "cute" is between them::
    
        from whoosh import query, spans
        t1 = query.Term("text", "apple")
        t2 = query.Term("text", "bear")
        near = spans.SpanNear(t1, t2, slop=10)
        q = spans.SpanContains(near, query.Term("text", "cute"))
    """
    
    def __init__(self, a, b):
        """
        :param a: the query to match.
        :param b: the query whose spans must occur within the matching spans
            of the first query.
        """
        
        self.q = And([a, b])
        self.a = a
        self.b = b
    
    class _Matcher(SpanBiMatcher):
        def __init__(self, a, b):
            self.a = a
            self.b = b
            isect = IntersectionMatcher(a, b)
            super(SpanContains._Matcher, self).__init__(isect)
            
        def _get_spans(self):
            spans = []
            bspans = self.b.spans()
            for aspan in self.a.spans():
                for bspan in bspans:
                    if aspan.start > bspan.end:
                        continue
                    if aspan.end < bspan.start:
                        break
                    
                    if bspan.is_within(aspan):
                        spans.append(aspan)
                        break
            return spans


class SpanBefore(SpanBiQuery):
    """Matches documents where the spans of the first query occur before any
    spans of the second query.
    
    For example, to match documents where "apple" occurs anywhere before
    "bear"::
    
        from whoosh import query, spans
        t1 = query.Term("text", "apple")
        t2 = query.Term("text", "bear")
        q = spans.SpanBefore(t1, t2)
    """
    
    def __init__(self, a, b):
        """
        :param a: the query that must occur before the second.
        :param b: the query that must occur after the first.
        """
        
        self.a = a
        self.b = b
        self.q = And([a, b])
    
    class _Matcher(SpanBiMatcher):
        def __init__(self, a, b):
            self.a = a
            self.b = b
            isect = IntersectionMatcher(a, b)
            super(SpanBefore._Matcher, self).__init__(isect)

        def _get_spans(self):
            bminstart = min(bspan.start for bspan in self.b.spans())
            return [aspan for aspan in self.a.spans() if aspan.end < bminstart]


class SpanCondition(SpanBiQuery):
    """Matches documents that satisfy both subqueries, but only uses the spans
    from the first subquery.
    
    This is useful when you want to place conditions on matches but not have
    those conditions affect the spans returned.
    
    For example, to get spans for the term ``alfa`` in documents that also
    must contain the term ``bravo``::
    
        SpanCondition(Term("text", u"alfa"), Term("text", u"bravo"))
    
    """
    
    def __init__(self, a, b):
        self.a = a
        self.b = b
        self.q = And([a, b])
    
    class _Matcher(SpanBiMatcher):
        def __init__(self, a, b):
            self.a = a
            isect = IntersectionMatcher(a, b)
            super(SpanCondition._Matcher, self).__init__(isect)

        def _get_spans(self):
            return self.a.spans()





