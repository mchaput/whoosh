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

from whoosh.matching import (WrappingMatcher, AndMaybeMatcher, UnionMatcher, IntersectionMatcher)
from whoosh.query import Query, And, Or, AndMaybe


# Queries

class SpanQuery(Query):
    def __init__(self, q):
        self.q = q
    
    def _subm(self, s, excl):
        return self.q.matcher(s, exclude_docs=excl)
    
    def __getattr__(self, name):
        return getattr(self.q, name)


class SpanFirst(SpanQuery):
    def __init__(self, q, limit=0):
        self.q = q
        self.limit = limit
        
    def matcher(self, searcher, exclude_docs=None):
        return SpanFirstMatcher(self._subm(searcher, exclude_docs),
                                limit=self.limit)


class SpanNear(SpanQuery):
    def __init__(self, a, b, slop=1, ordered=True):
        self.q = And([a, b])
        self.a = a
        self.b = b
        self.slop = slop
        self.ordered = ordered
        
    def matcher(self, searcher, exclude_docs=None):
        return SpanNearMatcher(self.a.matcher(searcher, exclude_docs=exclude_docs),
                               self.b.matcher(searcher, exclude_docs=exclude_docs),
                               slop=self.slop, ordered=self.ordered)
    

class SpanNot(SpanQuery):
    def __init__(self, a, b):
        self.q = AndMaybe([a, b])
        self.a = a
        self.b = b
        
    def matcher(self, searcher, exclude_docs=None):
        return SpanNotMatcher(self.a.matcher(searcher, exclude_docs=exclude_docs),
                               self.b.matcher(searcher, exclude_docs=exclude_docs))


class SpanOr(SpanQuery):
    def __init__(self, a, b):
        self.q = Or([a, b])
        self.a = a
        self.b = b
        
    def matcher(self, searcher, exclude_docs=None):
        return SpanOrMatcher(self.a.matcher(searcher, exclude_docs=exclude_docs),
                             self.b.matcher(searcher, exclude_docs=exclude_docs))


# Matchers

class SpanWrappingMatcher(WrappingMatcher):
    def __init__(self, child):
        super(SpanWrappingMatcher, self).__init__(child)
        self._spans = None
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


class SpanFirstMatcher(SpanWrappingMatcher):
    def __init__(self, child, limit=0):
        self.limit = limit
        super(SpanFirstMatcher, self).__init__(child)
    
    def _get_spans(self):
        return [span for span in self.child.spans()
                if span.start <= self.limit]


class SpanNearMatcher(SpanWrappingMatcher):
    def __init__(self, a, b, slop=1, ordered=True):
        self.a = a
        self.b = b
        self.slop = slop
        self.ordered = ordered
        super(SpanNearMatcher, self).__init__(IntersectionMatcher(a, b))
    
    def _get_spans(self):
        slop = self.slop
        ordered = self.ordered
        spans = set()
        
        for i, aspan in enumerate(self.a.spans()):
            for j, bspan in enumerate(self.b.spans()):
                if aspan != bspan and aspan.distance_to(bspan) <= slop:
                    if ordered and aspan.start > bspan.start: continue
                    spans.add(aspan.to(bspan))
                
        return sorted(spans)


class SpanNotMatcher(SpanWrappingMatcher):
    def __init__(self, a, b):
        self.a = a
        self.b = b
        super(SpanNotMatcher, self).__init__(AndMaybeMatcher(a, b))
    
    def _get_spans(self):
        if self.a.id() == self.b.id():
            spans = []
            for aspan in self.a.spans():
                overlapped = False
                for bspan in self.b.spans():
                    if aspan.overlaps(bspan):
                        overlapped = True
                        break
                if not overlapped:
                    spans.append(aspan)
            return spans
        else:
            return self.a.spans()


class SpanOrMatcher(SpanWrappingMatcher):
    def __init__(self, a, b):
        self.a = a
        self.b = b
        super(SpanOrMatcher, self).__init__(UnionMatcher(a, b))

    def _get_spans(self):
        if self.a.is_active() and self.b.is_active() and self.a.id() == self.b.id():
            spans = []
            for slist in (self.a.spans(), self.b.spans()):
                for aspan in slist:
                    overlapped = False
                    for i, span in enumerate(spans):
                        if span.overlaps(aspan):
                            spans[i] = span.to(aspan)
                            overlapped = True
                            break
                    if not overlapped:
                        spans.append(aspan)
            return spans
        elif not self.b.is_active() or self.a.id() < self.b.id():
            return self.a.spans()
        else:
            return self.b.spans()
        
        







