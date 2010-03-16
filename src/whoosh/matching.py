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

from bisect import bisect_left, bisect_right


class ReadTooFar(Exception):
    """Raised when next() or skip_to() is called on an inactive matchers.
    """


class NoQualityAvailable(Exception):
    """Raised when quality methods are called on a matcher that does not
    support quality-based optimizations.
    """


class Matcher(object):
    """Base class for all matchers.
    """
    
    def is_active(self):
        raise NotImplementedError
    
    def replace(self):
        return self
    
    def depth(self):
        return 0
    
    def supports_quality(self):
        return False
    
    def quality(self):
        raise NoQualityAvailable
    
    def block_quality(self):
        raise NoQualityAvailable
    
    def reset(self):
        raise NotImplementedError
    
    def id(self):
        raise NotImplementedError
    
    def value(self):
        raise NotImplementedError
    
    def skip_to(self, id):
        raise NotImplementedError
    
    def next(self):
        raise NotImplementedError
    
    def weight(self):
        raise NotImplementedError
    
    def score(self):
        raise NotImplementedError
    

class NullMatcher(object):
    def is_active(self):
        return False


class WrappingMatcher(Matcher):
    """Base class for matchers that wrap sub-matchers.
    """
    
    def __init__(self, child):
        self.child = child
    
    def depth(self):
        return 1 + self.child.depth()
    
    def replace(self):
        r = self.child.replace()
        if not r.is_active(): return NullMatcher
        if r is not self.child: return self.__class__(r)
        return self
    
    def supports_quality(self):
        return self.child.supports_quality()
    
    def quality(self):
        return self.child.quality()
    
    def block_quality(self):
        return self.child.block_quality()
    
    def reset(self):
        self.child.reset()
        
    def id(self):
        return self.child.id()
    
    def is_active(self):
        return self.child.is_active()
    
    def value(self):
        return self.child.value()
    
    def skip_to(self, id):
        return self.child.skip_to(id)
    
    def next(self):
        self.child.next()
    
    def weight(self):
        return self.child.weight()
    
    def score(self):
        return self.child.score()
    

class BiMatcher(Matcher):
    """Base class for matchers that combine the results of two sub-matchers in
    some way.
    """
    
    def __init__(self, a, b):
        super(BiMatcher, self).__init__()
        self.a = a
        self.b = b

    def depth(self):
        return 1 + max(self.a.depth(), self.b.depth())

    def reset(self):
        self.a.reset()
        self.b.reset()
        
    def skip_to(self, id):
        if not self.active(): raise ReadTooFar
        ra = self.a.skip_to(id)
        rb = self.b.skip_to(id)
        return ra or rb
        
    def supports_quality(self):
        return self.a.supports_quality() and self.b.supports_quality()


class AdditiveBiMatcher(BiMatcher):
    """Base class for binary matchers where the scores of the sub-matchers are
    added together in some way.
    """
    
    def quality(self):
        return self.a.quality() + self.b.quality()
    
    def block_quality(self):
        return self.a.block_quality() + self.b.block_quality()
    
    def weight(self):
        return self.a.weight() + self.b.weight()
    
    def score(self):
        return (self.a.score() + self.b.score())
    

class UnionMatcher(AdditiveBiMatcher):
    """Matches the union (OR) of the postings in the two sub-matchers.
    """
    
    def replace(self):
        a = self.a.replace()
        b = self.b.replace()
        
        a_active = a.is_active()
        b_active = b.is_active()
        if not (a_active or b_active): return NullMatcher
        if not a_active:
            return b
        if not b_active:
            return a
        
        if a is not self.a or b is not self.b:
            return self.__class__(a, b)
        return self
    
    def is_active(self):
        return self.a.is_active() or self.b.is_active()
    
    def id(self):
        a = self.a
        b = self.b
        if not a.is_active(): return b.id()
        if not b.is_active(): return a.id()
        return min(a.id(), b.id())
    
    def next(self):
        a = self.a
        b = self.b
        a_active = a.is_active()
        b_active = b.is_active()
        
        # Shortcut when one matcher is inactive
        if not (a_active or b_active):
            raise ReadTooFar
        elif not a_active:
            return b.next()
        elif not b_active:
            return a.next()
        
        a_id = a.id()
        b_id = b.id()
        ar = br = None
        if a_id <= b_id: ar = a.next()
        if b_id <= a_id: br = b.next()
        return ar or br
    
    def score(self):
        a = self.a
        b = self.b
        
        if not a.is_active(): return b.score()
        if not b.is_active(): return a.score()
        
        id_a = a.id()
        id_b = b.id()
        if id_a < id_b:
            return a.score()
        elif id_b < id_a:
            return b.score()
        else:
            return (a.score() + b.score())
        
    def skip_to_quality(self, minquality):
        a = self.a
        b = self.b
        
        # Short circuit if one matcher is inactive
        if not a.is_active():
            sk = b.skip_to_quality(minquality)
            return sk
        elif not b.is_active():
            return a.skip_to_quality(minquality)
        
        skipped = 0
        aq = a.block_quality()
        bq = b.block_quality()
        while a.is_active() and b.is_active() and aq + bq <= minquality:
            if aq < bq:
                skipped += a.skip_to_quality(minquality - bq)
                aq = a.block_quality()
            else:
                skipped += b.skip_to_quality(minquality - aq)
                bq = b.block_quality()
                
        return skipped
        

class DisjunctionMaxMatcher(UnionMatcher):
    def __init__(self, a, b, tiebreak=0.0):
        super(DisjunctionMaxMatcher, self).__init__(a, b)
        self.tiebreak = tiebreak
        
    def score(self):
        return max(self.a.score(), self.b.score())
    
    def quality(self):
        return max(self.a.quality(), self.b.quality())
    
    def block_quality(self):
        return max(self.a.block_quality(), self.b.block_quality())
    
    def skip_to_quality(self, minquality):
        a = self.a
        b = self.b
        
        skipped = 0
        aq = a.block_quality()
        bq = b.block_quality()
        while (a.is_active() or b.is_active()) and max(aq, bq) <= minquality:
            if a.is_active() and aq <= minquality:
                skipped += a.skip_to_quality(minquality)
                aq = a.block_quality()
            if b.is_active() and bq <= minquality:
                skipped += b.skip_to_quality(minquality)
                bq = b.block_quality()
        return skipped
        

class IntersectionMatcher(AdditiveBiMatcher):
    """Matches the intersection (AND) of the postings in the two sub-matchers.
    """
    
    def __init__(self, a, b):
        super(IntersectionMatcher, self).__init__(a, b)
        if self.a.id() != self.b.id():
            self._find_next()
    
    def replace(self):
        a = self.a.replace()
        b = self.b.replace()
        
        a_active = a
        b_active = b.is_active()
        if not (a_active and b_active): return NullMatcher
        
        if a is not self.a or b is not self.b:
            return self.__class__(a, b)
        return self
    
    def is_active(self):
        return self.a.is_active() and self.b.is_active()
    
    def _find_next(self):
        a = self.a
        b = self.b
        a_id = a.id()
        b_id = b.id()
        assert a_id != b_id
        r = False
        
        while a.is_active() and b.is_active() and a_id != b_id:
            if a_id < b_id:
                ra = a.skip_to(b_id)
                r = r or ra
                a_id = a.id()
            else:
                rb = b.skip_to(a_id)
                r = r or rb
                b_id = b.id()
        return r
    
    def id(self):
        return self.a.id()
    
    def skip_to(self, id):
        if not self.is_active(): raise ReadTooFar
        ra = self.a.skip_to(id)
        rb = self.b.skip_to(id)
        rn = self._find_next()
        return ra or rb or rn
    
    def skip_to_quality(self, minquality):
        a = self.a
        b = self.b
        
        skipped = 0
        aq = a.block_quality()
        bq = b.block_quality()
        while a.is_active() and b.is_active() and aq + bq <= minquality:
            if aq < bq:
                skipped += a.skip_to_quality(minquality - bq)
            else:
                skipped += b.skip_to_quality(minquality - aq)
            if a.id() != b.id():
                self._find_next()
            aq = a.block_quality()
            bq = b.block_quality()
        return skipped
    
    def reset(self):
        super(IntersectionMatcher, self).reset()
        self._find_next()
        
    def next(self):
        if not self.is_active(): raise ReadTooFar
        
        # We must assume that the ids are equal whenever next() is called (they
        # should have been made equal by _find_next), so advance them both
        ar = self.a.next()
        if self.is_active():
            nr = self._find_next()
            return ar or nr


class AndNotMatcher(BiMatcher):
    """Matches the postings in the first sub-matcher that are NOT present in
    the second sub-matcher.
    """

    def _find_next(self):
        pos = self.a
        neg = self.b
        if not neg.is_active(): return
        pos_id = pos.id()
        r = False
        
        if neg.id() < pos_id:
            neg.skip_to(pos_id)
        
        while pos_id == neg.id():
            nr = pos.next()
            r = r or nr
            pos_id = pos.id()
            neg.skip_to(pos_id)
        
        return r
    
    def replace(self):
        if not self.a.is_active(): return NullMatcher
        if not self.b.is_active(): return self.a
        return self
    
    def quality(self):
        return self.a.quality()
    
    def block_quality(self):
        return self.a.block_quality()
    
    def skip_to_quality(self, minquality):
        skipped = self.a.skip_to_quality(minquality)
        self._find_next()
        return skipped
    
    def id(self):
        return self.a.id()
    
    def reset(self):
        self.a.reset()
        self.b.reset()
        self._find_next()

    def next(self):
        if not self.a.is_active(): raise ReadTooFar
        ar = self.a.next()
        nr = self._find_next()
        return ar or nr
        
    def skip_to(self, id):
        if not self.a.is_active(): raise ReadTooFar
        if id < self.a.id(): return
        
        self.a.skip_to(id)
        if self.b.is_active():
            self.b.skip_to(id)
            self._find_next()
    
    def weight(self):
        return self.a.weight()
    
    def score(self):
        return self.a.score()


class InverseMatcher(WrappingMatcher):
    """Synthetic matcher, generates postings that are NOT present in the
    wrapped matcher.
    """
    
    def __init__(self, child, maxid, weight=1.0, missing=frozenset()):
        super(InverseMatcher, self).__init__(child)
        self.maxid = maxid
        self._weight = weight
        self.missing = missing
        self._id = 0
        self._find_next()
    
    def is_active(self):
        return self._id is not None
    
    def supports_quality(self):
        return False
    
    def _find_next(self):
        child = self.child
        missing = self.missing
        while self._id == child.id() and id not in missing:
            self._id += 1
            if child.is_active():
                child.next()
        if self._id >= self.maxid:
            self._id = None
    
    def reset(self):
        self.child.reset()
        self._id = 0
        self._find_next()
        
    def next(self):
        if self._id is None: raise ReadTooFar
        self._id += 1
        return self._find_next()
        
    def skip_to(self, id):
        _id = self._id
        if _id is None: raise ReadTooFar
        if id < _id: return
        self._id = id
        self._find_next()
    
    def weight(self):
        return self.weight
    
    def score(self):
        return self._weight


class RequireMatcher(WrappingMatcher):
    """Matches postings that are in both sub-matchers, but only uses scores
    from the first.
    """
    
    def __init__(self, a, b):
        self.a = a
        self.b = b
        self.child = IntersectionMatcher(a, b)
    
    def replace(self):
        if not self.child.is_active(): return NullMatcher
        return self
    
    def quality(self):
        return self.a.quality()
    
    def block_quality(self):
        return self.a.block_quality()
    
    def skip_to_quality(self, minquality):
        skipped = self.a.skip_to_quality(minquality)
        self.child._find_next()
        return skipped
    
    def weight(self):
        return self.a.weight()
    
    def score(self):
        return self.a.score()


class AndMaybeMatcher(AdditiveBiMatcher):
    """Matches postings in the first sub-matcher, and if the same posting is
    in the second sub-matcher, adds their scores.
    """
    
    def next(self):
        ar = self.a.next()
        br = self.b.skip_to(self.a.id())
        return ar or br
    
    def replace(self):
        ar = self.a.replace()
        br = self.b.replace()
        if not ar.is_active(): return NullMatcher
        if not br.is_active(): return ar
        if ar is not self.a or br is not self.b:
            return self.__class__(ar, br)
        return self
    
    def weight(self):
        if self.a.id() == self.b.id():
            return self.a.weight() + self.b.weight()
        else:
            return self.a.weight()
    
    def score(self):
        if self.a.id() == self.b.id():
            return self.a.score() + self.b.score()
        else:
            return self.a.score()
    

class BasePhraseMatcher(WrappingMatcher):
    def __init__(self, word_matchers, isect, slop=1):
        self.word_matchers = word_matchers
        self.child = isect
        self.slop = slop
        self._find_next()
    
    def reset(self):
        self.child.reset()
        self._find_next()
    
    def next(self):
        ri = self.isect.next()
        rn = self._find_next()
        return ri or rn
    
    def skip_to(self, id):
        rs = self.child.skip_to(id)
        rn = self._find_next()
        return rs or rn
    
    def _find_next(self):
        isect = self.child
        slop = self.slop
        current = []
        while not current and isect.is_active():
            poses = self._poses()
            current = poses[0]
            for poslist in poses[1:]:
                newposes = []
                for newpos in poslist:
                    start = bisect_left(current, newpos - slop)
                    end = bisect_right(current, newpos)
                    for curpos in current[start:end]:
                        delta = newpos - curpos
                        # Note that the delta can be less than 1. This is
                        # useful sometimes where multiple tokens are generated
                        # with the same position. However it means the query
                        # phrase "linda linda linda" will match a single
                        # "linda" because it will match three times with a
                        # delta of 0.
                        
                        # TODO: Fix this somehow?

                        if delta <= slop:
                            newposes.append(newpos)
                    
                current = newposes
                if not current: break
            
            if not current:
                isect.next()
        
        self._count = len(current)


class PostingPhraseMatcher(BasePhraseMatcher):
    def _poses(self):
        return [m.value_as("positions") for m in self.word_matchers]


class VectorPhraseMatcher(BasePhraseMatcher):
    def __init__(self, reader, word_matchers, isect, slop=1):
        super(VectorPhraseMatcher, self).__init__(word_matchers, isect, slop=slop)
        self.reader = reader
        self.fieldnum = word_matchers[0].fieldnum
        self.words = [m.text for m in word_matchers]
        self.sortedwords = sorted(self.words)
    
    def _poses(self):
        docnum = self.child.id()
        fieldnum = self.fieldnum
        vreader = self.reader.vector(docnum, fieldnum)
        poses = {}
        for word in self.sortedwords:
            vreader.skip_to(word)
            if vreader.id != word:
                raise Exception("Phrase query: %r in term index but not in"
                                "vector (possible analyzer mismatch" % word)
            poses[word] = vreader.value_as("positions")
        # Now put the position lists in phrase order
        return [poses[word] for word in self.words]
            




