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

from whoosh.matching import (WrappingMatcher, AndMaybeMatcher, UnionMatcher,
                             IntersectionMatcher, Span)
from whoosh.query import And, AndMaybe, Or, Query, Term
from whoosh.util import make_binary_tree


class SpanWrappingMatcher(WrappingMatcher):
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
    def __init__(self, q):
        self.q = q
    
    def _subm(self, s, excl):
        return self.q.matcher(s, exclude_docs=excl)
    
    def __getattr__(self, name):
        return getattr(self.q, name)
    
    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.q)


class SpanFirst(SpanQuery):
    def __init__(self, q, limit=0):
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
    def __init__(self, a, b, slop=1, ordered=True, mindist=1):
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
    def phrase(cls, fieldname, words, slop=1):
        terms = [Term(fieldname, word) for word in words]
        return make_binary_tree(SpanNear, terms, slop=slop)
    
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
    def __init__(self, a, b):
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
    def __init__(self, a, b):
        self.q = Or([a, b])
        self.a = a
        self.b = b
        
    def matcher(self, searcher, exclude_docs=None):
        ma = self.a.matcher(searcher, exclude_docs=exclude_docs)
        mb = self.b.matcher(searcher, exclude_docs=exclude_docs)
        return SpanOr.SpanOrMatcher(ma, mb)
    
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
    def __init__(self, a, b):
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
    def __init__(self, a, b):
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





