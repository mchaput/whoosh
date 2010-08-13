#===============================================================================
# Copyright 2010 Matt Chaput
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#===============================================================================

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
                             IntersectionMatcher, Span)
from whoosh.query import And, AndMaybe, Or, Query, Term
from whoosh.util import make_binary_tree


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
        m = self.__class__(self.child.copy(), self.fn)
        m._spans = self._spans
        return m
    
    def _find_next(self):
        child = self.child
        r = False
        
        spans = self._get_spans()
        while child.is_active() and not spans:
            r = child.next() or r
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
    
    def _subm(self, s, excl):
        return self.q.matcher(s, exclude_docs=excl)
    
    def __getattr__(self, name):
        return getattr(self.q, name)
    
    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.q)


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
        
    def matcher(self, searcher, exclude_docs=None):
        return SpanFirst.SpanFirstMatcher(self._subm(searcher, exclude_docs),
                                          limit=self.limit)
        
    class SpanFirstMatcher(SpanWrappingMatcher):
        def __init__(self, child, limit=0):
            self.limit = limit
            super(SpanFirst.SpanFirstMatcher, self).__init__(child)
        
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
    
        q = spans.SpanNear.phrase("text", ["whoosh", "search", "library"], slop=2)
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
    
    def matcher(self, searcher, exclude_docs=None):
        ma = self.a.matcher(searcher, exclude_docs=exclude_docs)
        mb = self.b.matcher(searcher, exclude_docs=exclude_docs)
        return SpanNear.SpanNearMatcher(ma, mb, slop=self.slop,
                                        ordered=self.ordered,
                                        mindist=self.mindist)
    
    @classmethod
    def phrase(cls, fieldname, words, slop=1, ordered=True):
        """Returns a tree of SpanNear queries to match a list of terms.
        
        :param fieldname: the name of the field to search in.
        :param words: a sequence of tokens to search for.
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
        
        def _get_spans(self):
            slop = self.slop
            mindist = self.mindist
            ordered = self.ordered
            spans = set()
            
            bspans = self.b.spans()
            for aspan in self.a.spans():
                for bspan in bspans:
                    if ordered and aspan.start > bspan.start:
                        break
                    if bspan.start > aspan.end + slop:
                        break
                    if bspan.end < aspan.start - slop:
                        continue
                    
                    dist = aspan.distance_to(bspan)
                    if dist >= mindist and dist <= slop:
                        spans.add(aspan.to(bspan))
            
            return sorted(spans)
    

class SpanNot(SpanQuery):
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
        
    def matcher(self, searcher, exclude_docs=None):
        ma = self.a.matcher(searcher, exclude_docs=exclude_docs)
        mb = self.b.matcher(searcher, exclude_docs=exclude_docs)
        return SpanNot.SpanNotMatcher(ma, mb)
    
    class SpanNotMatcher(SpanWrappingMatcher):
        def __init__(self, a, b):
            self.a = a
            self.b = b
            super(SpanNot.SpanNotMatcher, self).__init__(AndMaybeMatcher(a, b))
        
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


class SpanOr(SpanQuery):
    """Matches documents that match any a list of sub-queries. Unlike
    query.Or, this class is span-aware, and merges together matching spans
    from the different sub-queries when they overlap.
    """
    
    def __init__(self, subqs):
        """
        :param subqs: a list of queries to match.
        """
        
        self.q = Or(subqs)
        self.subqs = subqs
        
    def matcher(self, searcher, exclude_docs=None):
        matchers = [q.matcher(searcher, exclude_docs=exclude_docs)
                    for q in self.subqs]
        return make_binary_tree(SpanOr.SpanOrMatcher, matchers)
    
    class SpanOrMatcher(SpanWrappingMatcher):
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


class SpanContains(SpanQuery):
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
    
    def matcher(self, searcher, exclude_docs=None):
        ma = self.a.matcher(searcher, exclude_docs=exclude_docs)
        mb = self.b.matcher(searcher, exclude_docs=exclude_docs)
        return SpanContains.SpanContainsMatcher(ma, mb)
    
    class SpanContainsMatcher(SpanWrappingMatcher):
        def __init__(self, a, b):
            self.a = a
            self.b = b
            isect = IntersectionMatcher(a, b)
            super(SpanContains.SpanContainsMatcher, self).__init__(isect)
            
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


class SpanBefore(SpanQuery):
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
        
    def matcher(self, searcher, exclude_docs=None):
        ma = self.a.matcher(searcher, exclude_docs=exclude_docs)
        mb = self.b.matcher(searcher, exclude_docs=exclude_docs)
        return SpanBefore.SpanBeforeMatcher(ma, mb)
        
    class SpanBeforeMatcher(SpanWrappingMatcher):
        def __init__(self, a, b):
            self.a = a
            self.b = b
            isect = IntersectionMatcher(a, b)
            super(SpanBefore.SpanBeforeMatcher, self).__init__(isect)

        def _get_spans(self):
            bminstart = min(bspan.start for bspan in self.b.spans())
            return [aspan for aspan in self.a.spans() if aspan.end < bminstart]





