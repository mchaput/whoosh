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


def make_tree(cls, matchers):
    if len(matchers) > 1:
        return cls(matchers[0], make_tree(cls, matchers[1:]))
    else:
        return matchers[0]


class Matcher(object):
    """Base class for all matchers.
    """
    
    def is_active(self):
        raise NotImplementedError
    
    def replace(self):
        return self
    
    def copy(self):
        raise NotImplementedError
    
    def depth(self):
        return 0
    
    def supports_quality(self):
        return False
    
    def quality(self):
        raise NoQualityAvailable
    
    def block_quality(self):
        raise NoQualityAvailable
    
    def id(self):
        raise NotImplementedError
    
    def all_ids(self):
        i = 0
        while self.is_active():
            yield self.id()
            self.next()
            i += 1
            if i == 10:
                self = self.replace()
                i = 0
                
    def all_items(self):
        i = 0
        while self.is_active():
            yield (self.id(), self.value())
            self.next()
            i += 1
            if i == 10:
                self = self.replace()
                i = 0
    
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
    
    def __init__(self, child, boost=1.0):
        self.child = child
        self.boost = boost
    
    def copy(self):
        return self.__class__(self.child.copy(), boost=self.boost)
    
    def depth(self):
        return 1 + self.child.depth()
    
    def replace(self):
        r = self.child.replace()
        if not r.is_active(): return NullMatcher
        if r is not self.child: return self.__class__(r)
        return self
    
    def id(self):
        return self.child.id()
    
    def all_ids(self):
        return self.child.all_ids()
    
    def is_active(self):
        return self.child.is_active()
    
    def value(self):
        return self.child.value()
    
    def skip_to(self, id):
        return self.child.skip_to(id)
    
    def next(self):
        self.child.next()
    
    def supports_quality(self):
        return self.child.supports_quality()
    
    def skip_to_quality(self, minquality):
        return self.child.skip_to_quality(minquality/self.boost)
    
    def quality(self):
        return self.child.quality() * self.boost
    
    def block_quality(self):
        return self.child.block_quality() * self.boost
    
    def weight(self):
        return self.child.weight() * self.boost
    
    def score(self):
        return self.child.score() * self.boost


class MultiMatcher(Matcher):
    def __init__(self, matchers, idoffsets):
        self.matchers = matchers
        self.offsets = idoffsets
        self.current = 0
        self._active = bool(matchers)
        self._next_matcher()
        
    def _next_matcher(self):
        if not self._active: return
        matchers = self.matchers
        current = self.current
        
        while not matchers[current].is_active():
            current += 1
            if current >= len(matchers):
                self._active = False
                return
        
        self.current = current
    
    def copy(self):
        return self.__class__([mr.copy() for mr in self.matchers[self.current:]],
                              self.offsets[self.current:])
        
    def depth(self):
        return 1 + max(mr.depth() for mr in self.matchers[self.current:])
    
    def is_active(self):
        return self._active
    
    def replace(self):
        if not self._active: return NullMatcher()
        if self.current == len(self.matchers) - 1:
            return self.matchers[-1]
    
    def id(self):
        current = self.current
        return self.matchers[current].id() + self.offsets[current]
    
    def all_ids(self):
        offsets = self.offsets
        for i, mr in enumerate(self.matchers):
            for id in mr.all_ids():
                yield id + offsets[i]
    
    def value(self):
        return self.matchers[self.current].value()
    
    def next(self):
        if not self.matchers[self.current].is_active():
            self._next_matcher()
        if not self._active: raise ReadTooFar
        
        return self.matchers[self.current].next()
    
    def skip_to(self, id):
        if not self._active: raise ReadTooFar
        if id <= self.id(): return
        
        current = self.current
        matchers = self.matchers
        offsets = self.offsets
        r = False
        
        while current < len(matchers):
            mr = matchers[current]
            if not mr.is_active():
                current += 1
                continue
            
            if id < mr.id():
                break
            
            sr = mr.skip_to(id - offsets[current])
            r = sr or r
            if mr.is_active():
                break
            
            current += 1
            
        self.current = current
        return r
    
    def supports_quality(self):
        return all(mr.supports_quality() for mr in self.matchers[self.current:])
    
    def quality(self):
        return self.matchers[self.current].quality()
    
    def block_quality(self):
        return self.matchers[self.current].block_quality()
    
    def weight(self):
        return self.matchers[self.current].weight()
    
    def score(self):
        return self.matchers[self.current].score()


class ExcludeMatcher(WrappingMatcher):
    def __init__(self, child, excluded):
        super(ExcludeMatcher, self).__init__(child)
        self.excluded = excluded
        self.find_next()
    
    def _find_next(self):
        child = self.child
        excluded = self.excluded
        r = False
        while child.is_active() and child.id() in excluded:
            nr = child.next()
            r = r or nr
        return r
    
    def next(self):
        self.child.next()
        self._find_next()
        
    def skip_to(self, id):
        self.child.skip_to(id)
        self._find_next()
        
    def all_ids(self):
        excluded = self.excluded
        return (id for id in self.child.all_ids() if id not in excluded)
    
    def all_items(self):
        excluded = self.excluded
        return (item for item in self.child.all_items()
                if item[0] not in excluded)


class BiMatcher(Matcher):
    """Base class for matchers that combine the results of two sub-matchers in
    some way.
    """
    
    def __init__(self, a, b):
        super(BiMatcher, self).__init__()
        self.a = a
        self.b = b

    def copy(self):
        return self.__class__(self.a.copy(), self.b.copy())

    def depth(self):
        return 1 + max(self.a.depth(), self.b.depth())

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
        return (self.a.weight() + self.b.weight())
    
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
    
    def all_ids(self):
        return iter(set(self.a.all_ids()) | set(self.b.all_ids()))
    
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
        minquality = minquality
        
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
    def __init__(self, a, b, tiebreak=0.0, boost=1.0):
        super(DisjunctionMaxMatcher, self).__init__(a, b, boost=boost)
        self.tiebreak = tiebreak
    
    def copy(self):
        return self.__class__(self.a.copy(), self.b.copy(),
                              tiebreak=self.tiebreak)
    
    def score(self):
        return max(self.a.score(), self.b.score())
    
    def quality(self):
        return max(self.a.quality(), self.b.quality())
    
    def block_quality(self):
        return max(self.a.block_quality(), self.b.block_quality())
    
    def skip_to_quality(self, minquality):
        a = self.a
        b = self.b
        minquality = minquality
        
        # Short circuit if one matcher is inactive
        if not a.is_active():
            sk = b.skip_to_quality(minquality)
            return sk
        elif not b.is_active():
            return a.skip_to_quality(minquality)
        
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
    """Matches the intersection (AND) of the postings in the two sub-matchers.
    """
    
    def __init__(self, a, b, boost=1.0):
        super(IntersectionMatcher, self).__init__(a, b, boost=boost)
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
    
    def all_ids(self):
        return iter(set(self.a.all_ids()) & set(self.b.all_ids()))
    
    def skip_to(self, id):
        if not self.is_active(): raise ReadTooFar
        ra = self.a.skip_to(id)
        rb = self.b.skip_to(id)
        rn = self._find_next()
        return ra or rb or rn
    
    def skip_to_quality(self, minquality):
        a = self.a
        b = self.b
        minquality = minquality
        
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
    
    def all_ids(self):
        return iter(set(self.a.all_ids()) - set(self.b.all_ids()))
    
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
    
    def __init__(self, child, maxid, missing=None, weight=1.0):
        super(InverseMatcher, self).__init__(child)
        self.maxid = maxid
        self._weight = weight
        self.missing = missing or (lambda id: False)
        self._id = 0
        self._find_next()
    
    def copy(self):
        return self.__class__(self.child.copy(), self.maxid,
                              weight=self._weight, missing=self.missing)
    
    def is_active(self):
        return self._id is not None
    
    def supports_quality(self):
        return False
    
    def _find_next(self):
        child = self.child
        missing = self.missing
        while self._id == child.id() and not missing(id):
            self._id += 1
            if child.is_active():
                child.next()
        if self._id >= self.maxid:
            self._id = None
    
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
    
    def copy(self):
        return self.__class__(self.a.copy(), self.b.copy())
    
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
    def __init__(self, isect, decodefn, slop=1):
        self.child = isect
        self.decode_positions = decodefn
        self.slop = slop
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
    def __init__(self, wordmatchers, isect, decodefn, slop=1):
        super(PostingPhraseMatcher, self).__init__(isect, decodefn, slop=slop)
        self.wordmatchers = wordmatchers
    
    def _poses(self):
        decode_positions = self.decode_positions
        return [decode_positions(m.value()) for m in self.wordmatchers]


class VectorPhraseMatcher(BasePhraseMatcher):
    def __init__(self, searcher, fieldnum, words, isect, slop=1):
        """
        :param reader: an IndexReader.
        :param words: a sequence of token texts representing the words in the
            phrase.
        :param isect: an intersection matcher for the words in the phrase.
        :param slop: 
        """
        
        decodefn = searcher.field(fieldnum).format.decoder("positions")
        super(VectorPhraseMatcher, self).__init__(isect, decodefn, slop=slop)
        self.reader = searcher.reader()
        self.fieldnum = fieldnum
        self.words = words
        self.sortedwords = sorted(self.words)
    
    def _poses(self):
        vreader = self.reader.vector(self.child.id(), self.fieldnum)
        poses = {}
        decode_positions = self.decode_positions
        for word in self.sortedwords:
            vreader.skip_to(word)
            if vreader.id != word:
                raise Exception("Phrase query: %r in term index but not in"
                                "vector (possible analyzer mismatch" % word)
            poses[word] = decode_positions(vreader.value())
        # Now put the position lists in phrase order
        return [poses[word] for word in self.words]


class EveryMatcher(Matcher):
    def __init__(self, limit, exclude, weight=1.0):
        self.limit = limit
        self.exclude = exclude
        self._id = 0
        self._find_next()
        self._weight = weight
    
    def _find_next(self):
        limit = self.limit
        exclude = self.exclude
        _id = self._id
        while _id < limit and _id in exclude:
            _id += 1
        self._id = _id
    
    def is_active(self):
        return self._id < self.limit
    
    def copy(self):
        c = self.__class__(self.limit, self.exclude)
        c._id = self._id
        return c
    
    def id(self):
        return self._id
    
    def all_ids(self):
        exclude = self.exclude
        return (id for id in xrange(self.limit) if id not in exclude)
    
    def skip_to(self, id):
        self._id = id
        self._find_next()
    
    def next(self):
        self._id += 1
        self._find_next()
    
    def weight(self):
        return self._weight
    
    def score(self):
        return self._weight









