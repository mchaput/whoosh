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

from itertools import izip, repeat


"""
This module contains "matcher" classes. Matchers deal with posting lists. The
most basic matcher, which reads the list of postings for a term, will be
provided by the backend implementation (for example,
``whoosh.filedb.filepostings.FilePostingReader``). The classes in this module
provide additional functionality, such as combining the results of two
matchers, or modifying the results of a matcher.

You do not need to deal with the classes in this module unless you need to
write your own Matcher implementation to provide some new functionality. These
classes are not instantiated by the user. They are usually created by a
:class:`~whoosh.query.Query` object's ``matcher()`` method, which returns the
appropriate matcher to implement the query (for example, the ``Or`` query's
``matcher()`` method returns a ``UnionMatcher`` object).

Certain backends support "quality" optimizations. These backends have the
ability to skip ahead if it knows the current block of postings can't
contribute to the top N documents. If the matcher tree and backend support
these optimizations, the matcher's ``supports_quality()`` method will return
``True``.
"""


class ReadTooFar(Exception):
    """Raised when next() or skip_to() is called on an inactive matchers.
    """


class NoQualityAvailable(Exception):
    """Raised when quality methods are called on a matcher that does not
    support quality-based optimizations.
    """


# Matchers

class Matcher(object):
    """Base class for all matchers.
    """
    
    def is_active(self):
        """Returns True if this matcher is still "active", that is, it has not
        yet reached the end of the posting list.
        """
        
        raise NotImplementedError
    
    def replace(self):
        """Returns a possibly-simplified version of this matcher. For example,
        if one of the children of a UnionMatcher is no longer active, calling
        this method on the UnionMatcher will return the other child.
        """
        
        return self
    
    def copy(self):
        """Returns a copy of this matcher.
        """
        
        raise NotImplementedError
    
    def depth(self):
        """Returns the depth of the tree under this matcher, or 0 if this
        matcher does not have any children.
        """
        
        return 0
    
    def supports_quality(self):
        """Returns True if this matcher supports the use of ``quality`` and
        ``block_quality``.
        """
        
        return False
    
    def quality(self):
        """Returns a quality measurement of the current posting, according to
        the current weighting algorithm. Raises ``NoQualityAvailable`` if the
        matcher or weighting do not support quality measurements.
        """
        
        raise NoQualityAvailable
    
    def block_quality(self):
        """Returns a quality measurement of the current block of postings,
        according to the current weighting algorithm. Raises
        ``NoQualityAvailable`` if the matcher or weighting do not support
        quality measurements.
        """
        
        raise NoQualityAvailable(self.__class__)
    
    def id(self):
        """Returns the ID of the current posting.
        """
        
        raise NotImplementedError
    
    def all_ids(self):
        """Returns a generator of all IDs in the matcher.
        
        What this method returns for a matcher that has already read some
        postings (whether it only yields the remaining postings or all postings
        from the beginning) is undefined, so it's best to only use this method
        on fresh matchers.
        """
        
        i = 0
        while self.is_active():
            yield self.id()
            self.next()
            i += 1
            if i == 10:
                self = self.replace()
                i = 0
                
    def all_items(self):
        """Returns a generator of all (ID, encoded value) pairs in the matcher.
        
        What this method returns for a matcher that has already read some
        postings (whether it only yields the remaining postings or all postings
        from the beginning) is undefined, so it's best to only use this method
        on fresh matchers.
        """
        
        i = 0
        while self.is_active():
            yield (self.id(), self.value())
            self.next()
            i += 1
            if i == 10:
                self = self.replace()
                i = 0
    
    def items_as(self, astype):
        """Returns a generator of all (ID, decoded value) pairs in the matcher.
        
        What this method returns for a matcher that has already read some
        postings (whether it only yields the remaining postings or all postings
        from the beginning) is undefined, so it's best to only use this method
        on fresh matchers.
        """
        
        while self.is_active():
            yield (self.id(), self.value_as(astype))
    
    def value(self):
        """Returns the encoded value of the current posting.
        """
        
        raise NotImplementedError
    
    def supports(self, astype):
        """Returns True if the field's format supports the named data type,
        for example 'frequency' or 'characters'.
        """
        
        raise NotImplementedError("supports not implemented in %s" % self.__class__)
    
    def value_as(self, astype):
        """Returns the value(s) of the current posting as the given type.
        """
        
        raise NotImplementedError("value_as not implemented in %s" % self.__class__)
    
    def spans(self):
        """Returns a list of :class:`whoosh.spans.Span` objects for the matches
        in this document. Raises an exception if the field being searched does
        not store positions.
        """
        
        from whoosh.spans import Span
        if self.supports("characters"):
            return [Span(pos, startchar=startchar, endchar=endchar)
                    for pos, startchar, endchar in self.value_as("characters")]
        elif self.supports("positions"):
            return [Span(pos) for pos in self.value_as("positions")]
        else:
            raise Exception("Field does not support spans")
        
    def skip_to(self, id):
        """Moves this matcher to the first posting with an ID equal to or
        greater than the given ID.
        """
        
        while self.is_active() and self.id() < id:
            self.next()
    
    def skip_to_quality(self, minquality):
        """Moves this matcher to the next block with greater than the given
        minimum quality value.
        """
        
        raise NotImplementedError(self.__class__.__name__)
    
    def next(self):
        """Moves this matcher to the next posting.
        """
        
        raise NotImplementedError(self.__class__.__name__)
    
    def weight(self):
        """Returns the weight of the current posting.
        """
        
        return self.value_as("weight")
    
    def score(self):
        """Returns the score of the current posting.
        """
        
        raise NotImplementedError(self.__class__.__name__)
    

class NullMatcher(Matcher):
    """Matcher with no postings which is never active.
    """
    
    def is_active(self):
        return False
    
    def all_ids(self):
        return []
    
    def copy(self):
        return self
    

class ListMatcher(Matcher):
    """Synthetic matcher backed by a list of IDs.
    """
    
    def __init__(self, ids, weights=None, values=None, format=None,
                 scorer=None, position=0, all_weights=None,
                 maxwol=0.0, minlength=0):
        """
        :param ids: a list of doc IDs.
        :param weights: a list of weights corresponding to the list of IDs.
            If this argument is not supplied, a list of 1.0 values is used.
        :param values: a list of encoded values corresponding to the list of
            IDs.
        :param format: a :class:`whoosh.formats.Format` object representing the
            format of the field.
        :param scorer: a :class:`whoosh.scoring.BaseScorer` object for scoring
            the postings.
        """
        
        self._ids = ids
        self._weights = weights
        self._all_weights = all_weights
        self._values = values
        self._i = position
        self._format = format
        self._scorer = scorer
        self._maxwol = maxwol
        self._minlength = minlength
    
    def __repr__(self):
        return "<%s>" % self.__class__.__name__
    
    def is_active(self):
        return self._i < len(self._ids)
    
    def copy(self):
        return self.__class__(self._ids, self._weights, self._values,
                              self._format, self._scorer, self._i,
                              self._all_weights, self._maxwol, self._minlength)
    
    def supports_quality(self):
        return self._scorer is not None and self._scorer.supports_quality()
    
    def quality(self):
        return self._scorer.quality(self)
    
    def block_quality(self):
        return self._scorer.block_quality(self)
    
    def skip_to_quality(self, minquality):
        self._i += 1
        while self._i < len(self._ids) and self.quality() <= minquality:
            self._i += 1
        return 0
    
    def id(self):
        return self._ids[self._i]
    
    def all_ids(self):
        return iter(self._ids)
    
    def all_items(self):
        values = self._values
        if values is None:
            values = repeat('')
        
        return izip(self._ids, values)
    
    def value(self):
        if self._values:
            return self._values[self._i]
        else:
            return ''
    
    def value_as(self, astype):
        decoder = self._format.decoder(astype)
        return decoder(self.value())
    
    def supports(self, astype):
        return self._format.supports(astype)
    
    def next(self):
        self._i += 1
    
    def weight(self):
        if self._all_weights:
            return self._all_weights
        elif self._weights:
            return self._weights[self._i]
        else:
            return 1.0
    
    def block_maxweight(self):
        if self._all_weights:
            return self._all_weights
        elif self._weights:
            return max(self._weights)
        else:
            return 1.0
    
    def block_maxwol(self):
        return self._maxwol
    
    def block_maxid(self):
        return max(self._ids)
    
    def block_minlength(self):
        return self._minlength
    
    def score(self):
        if self._scorer:
            return self._scorer.score(self)
        else:
            return self.weight()


class WrappingMatcher(Matcher):
    """Base class for matchers that wrap sub-matchers.
    """
    
    def __init__(self, child, boost=1.0):
        self.child = child
        self.boost = boost
    
    def __repr__(self):
        return "%s(%r, boost=%s)" % (self.__class__.__name__, self.child, self.boost)
    
    def copy(self):
        kwargs = {}
        if hasattr(self, "boost"):
            kwargs["boost"] = self.boost
        return self.__class__(self.child.copy(), **kwargs)
    
    def depth(self):
        return 1 + self.child.depth()
    
    def _replacement(self, newchild):
        return self.__class__(newchild, boost=self.boost)
    
    def replace(self):
        r = self.child.replace()
        if not r.is_active():
            return NullMatcher()
        if r is not self.child:
            try:
                return self._replacement(r)
            except TypeError, e:
                raise TypeError("Class %s got exception %s trying "
                                "to replace itself" % (self.__class__, e))
        else:
            return self
    
    def id(self):
        return self.child.id()
    
    def all_ids(self):
        return self.child.all_ids()
    
    def is_active(self):
        return self.child.is_active()
    
    def supports(self, astype):
        return self.child.supports(astype)
    
    def value(self):
        return self.child.value()
    
    def value_as(self, astype):
        return self.child.value_as(astype)
    
    def spans(self):
        return self.child.spans()
    
    def skip_to(self, id):
        return self.child.skip_to(id)
    
    def next(self):
        self.child.next()
    
    def supports_quality(self):
        return self.child.supports_quality()
    
    def skip_to_quality(self, minquality):
        return self.child.skip_to_quality(minquality / self.boost)
    
    def quality(self):
        return self.child.quality() * self.boost
    
    def block_quality(self):
        return self.child.block_quality() * self.boost
    
    def weight(self):
        return self.child.weight() * self.boost
    
    def score(self):
        return self.child.score() * self.boost


class MultiMatcher(Matcher):
    """Serializes the results of a list of sub-matchers.
    """
    
    def __init__(self, matchers, idoffsets, current=0):
        """
        :param matchers: a list of Matcher objects.
        :param idoffsets: a list of offsets corresponding to items in the
            ``matchers`` list.
        """
        
        self.matchers = matchers
        self.offsets = idoffsets
        self.current = current
        self._next_matcher()
    
    def __repr__(self):
        return "%s(%r, %r, current=%s)" % (self.__class__.__name__,
                                           self.matchers, self.offsets,
                                           self.current)
    
    def is_active(self):
        return self.current < len(self.matchers)
    
    def _next_matcher(self):
        matchers = self.matchers
        while self.current < len(matchers) and not matchers[self.current].is_active():
            self.current += 1
        
    def copy(self):
        return self.__class__([mr.copy() for mr in self.matchers],
                              self.offsets, current=self.current)
    
    def depth(self):
        if self.is_active():
            return 1 + max(mr.depth() for mr in self.matchers[self.current:])
        else:
            return 0
    
    def replace(self):
        if not self.is_active():
            return NullMatcher()
        # TODO: Possible optimization: if the last matcher is current, replace
        # this with the last matcher, but wrap it with a matcher that adds the
        # offset. Have to check whether that's actually faster, though.
        return self
    
    def id(self):
        current = self.current
        return self.matchers[current].id() + self.offsets[current]
    
    def all_ids(self):
        offsets = self.offsets
        for i, mr in enumerate(self.matchers):
            for id in mr.all_ids():
                yield id + offsets[i]
    
    def spans(self):
        return self.matchers[self.current].spans()
    
    def supports(self, astype):
        return self.matchers[self.current].supports(astype)
    
    def value(self):
        return self.matchers[self.current].value()
    
    def value_as(self, astype):
        return self.matchers[self.current].value_as(astype)
    
    def next(self):
        if not self.is_active():
            raise ReadTooFar
        
        self.matchers[self.current].next()
        if not self.matchers[self.current].is_active():
            self._next_matcher()
        
    def skip_to(self, id):
        if not self.is_active():
            raise ReadTooFar
        if id <= self.id():
            return
        
        matchers = self.matchers
        offsets = self.offsets
        r = False
        
        while self.current < len(matchers) and id > self.id():
            mr = matchers[self.current]
            sr = mr.skip_to(id - offsets[self.current])
            r = sr or r
            if mr.is_active():
                break
            
            self._next_matcher()
            
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


def ExcludeMatcher(child, excluded, boost=1.0):
    return FilterMatcher(child, excluded, exclude=True, boost=boost)


class FilterMatcher(WrappingMatcher):
    """Filters the postings from the wrapped based on whether the IDs are
    present in or absent from a set.
    """
    
    def __init__(self, child, ids, exclude=False, boost=1.0):
        """
        :param child: the child matcher.
        :param ids: a set of IDs to filter by.
        :param exclude: by default, only IDs from the wrapped matcher that are
            IN the set are used. If this argument is True, only IDs from the
            wrapped matcher that are NOT IN the set are used.
        """
        
        super(FilterMatcher, self).__init__(child)
        self._ids = ids
        self._exclude = exclude
        self.boost = boost
        self._find_next()
    
    def __repr__(self):
        return "%s(%r, %r, %r, boost=%s)" % (self.__class__.__name__,
                                             self.child, self._ids,
                                             self._exclude, self.boost)
    
    def copy(self):
        return self.__class__(self.child.copy(), self._ids, self._exclude,
                              boost=self.boost)
    
    def _replacement(self, newchild):
        return self.__class__(newchild, self._ids, exclude=self._exclude, boost=self.boost)
    
    def _find_next(self):
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
    
    def next(self):
        self.child.next()
        self._find_next()
        
    def skip_to(self, id):
        self.child.skip_to(id)
        self._find_next()
        
    def all_ids(self):
        ids = self._ids
        if self._exclude:
            return (id for id in self.child.all_ids() if id not in ids)
        else:
            return (id for id in self.child.all_ids() if id in ids)
    
    def all_items(self):
        ids = self._ids
        if self._exclude:
            return (item for item in self.child.all_items() if item[0] not in ids)
        else:
            return (item for item in self.child.all_items() if item[0] in ids)
        

class BiMatcher(Matcher):
    """Base class for matchers that combine the results of two sub-matchers in
    some way.
    """
    
    def __init__(self, a, b):
        super(BiMatcher, self).__init__()
        self.a = a
        self.b = b

    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__, self.a, self.b)

    def copy(self):
        return self.__class__(self.a.copy(), self.b.copy())

    def depth(self):
        return 1 + max(self.a.depth(), self.b.depth())
    
    def skip_to(self, id):
        if not self.is_active():
            raise ReadTooFar
        ra = self.a.skip_to(id)
        rb = self.b.skip_to(id)
        return ra or rb
        
    def supports_quality(self):
        return self.a.supports_quality() and self.b.supports_quality()
    
    def supports(self, astype):
        return self.a.supports(astype) and self.b.supports(astype)
    

class AdditiveBiMatcher(BiMatcher):
    """Base class for binary matchers where the scores of the sub-matchers are
    added together.
    """
    
    def quality(self):
        q = 0.0
        if self.a.is_active():
            q += self.a.quality()
        if self.b.is_active():
            q += self.b.quality()
        return q
    
    def block_quality(self):
        bq = 0.0
        if self.a.is_active():
            bq += self.a.block_quality()
        if self.b.is_active():
            bq += self.b.block_quality()
        return bq
    
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
        if not (a_active or b_active):
            return NullMatcher()
        if not a_active:
            return b
        if not b_active:
            return a
        
        if a is not self.a or b is not self.b:
            return self.__class__(a, b)
        return self
    
    def is_active(self):
        return self.a.is_active() or self.b.is_active()
    
    def skip_to(self, id):
        ra = rb = False
        
        if self.a.is_active():
            ra = self.a.skip_to(id)
        if self.b.is_active():
            rb = self.b.skip_to(id)
        
        return ra or rb
    
    def id(self):
        a = self.a
        b = self.b
        if not a.is_active():
            return b.id()
        if not b.is_active():
            return a.id()
        return min(a.id(), b.id())
    
    # Using sets is faster in most cases, but could potentially use a lot of
    # memory. Comment out this method override to not use sets.
    def all_ids(self):
        return iter(sorted(set(self.a.all_ids()) | set(self.b.all_ids())))
    
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
        
        # After all that, here's the actual implementation
        if a_id <= b_id:
            ar = a.next()
        if b_id <= a_id:
            br = b.next()
        return ar or br
    
    def spans(self):
        if not self.a.is_active():
            return self.b.spans()
        if not self.b.is_active():
            return self.a.spans()
        
        id_a = self.a.id()
        id_b = self.b.id()
        if id_a < id_b:
            return self.a.spans()
        elif id_b < id_a:
            return self.b.spans()
        else:
            return sorted(set(self.a.spans()) | set(self.b.spans()))
    
    def weight(self):
        a = self.a
        b = self.b
        
        if not a.is_active():
            return b.weight()
        if not b.is_active():
            return a.weight()
        
        id_a = a.id()
        id_b = b.id()
        if id_a < id_b:
            return a.weight()
        elif id_b < id_a:
            return b.weight()
        else:
            return (a.weight() + b.weight())
    
    def score(self):
        a = self.a
        b = self.b
        
        if not a.is_active():
            return b.score()
        if not b.is_active():
            return a.score()
        
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
        if not (a.is_active() or b.is_active()):
            raise ReadTooFar
        
        # Short circuit if one matcher is inactive
        if not a.is_active():
            return b.skip_to_quality(minquality)
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
    """Matches the union (OR) of two sub-matchers. Where both sub-matchers
    match the same posting, returns the weight/score of the higher-scoring
    posting.
    """
    
    # TODO: this class inherits from AdditiveBiMatcher (through UnionMatcher)
    # but it does not add the scores of the sub-matchers together (it
    # overrides all methods that perform addition). Need to clean up the
    # inheritance. 
    
    def __init__(self, a, b, tiebreak=0.0):
        super(DisjunctionMaxMatcher, self).__init__(a, b)
        self.tiebreak = tiebreak
    
    def copy(self):
        return self.__class__(self.a.copy(), self.b.copy(),
                              tiebreak=self.tiebreak)
    
    def score(self):
        if not self.a.is_active():
            return self.b.score()
        elif not self.b.is_active():
            return self.a.score()
        else:
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
    
    def __init__(self, a, b):
        super(IntersectionMatcher, self).__init__(a, b)
        if (self.a.is_active()
            and self.b.is_active()
            and self.a.id() != self.b.id()):
            self._find_next()
    
    def replace(self):
        a = self.a.replace()
        b = self.b.replace()
        
        a_active = a
        b_active = b.is_active()
        if not (a_active and b_active):
            return NullMatcher()
        
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
                if not a.is_active():
                    return
                r = r or ra
                a_id = a.id()
            else:
                rb = b.skip_to(a_id)
                if not b.is_active():
                    return
                r = r or rb
                b_id = b.id()
        return r
    
    def id(self):
        return self.a.id()
    
    # Using sets is faster in some cases, but could potentially use a lot of
    # memory
    def all_ids(self):
        return iter(sorted(set(self.a.all_ids()) & set(self.b.all_ids())))
    
    def skip_to(self, id):
        if not self.is_active():
            raise ReadTooFar
        ra = self.a.skip_to(id)
        rb = self.b.skip_to(id)
        if self.is_active():
            rn = False
            if self.a.id() != self.b.id():
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
            if not a.is_active() or not b.is_active():
                break
            if a.id() != b.id():
                self._find_next()
            aq = a.block_quality()
            bq = b.block_quality()
        return skipped
    
    def next(self):
        if not self.is_active():
            raise ReadTooFar
        
        # We must assume that the ids are equal whenever next() is called (they
        # should have been made equal by _find_next), so advance them both
        ar = self.a.next()
        if self.is_active():
            nr = self._find_next()
            return ar or nr
    
    def spans(self):
        return sorted(set(self.a.spans()) | set(self.b.spans()))
    

class AndNotMatcher(BiMatcher):
    """Matches the postings in the first sub-matcher that are NOT present in
    the second sub-matcher.
    """

    def __init__(self, a, b):
        super(AndNotMatcher, self).__init__(a, b)
        if (self.a.is_active()
            and self.b.is_active()
            and self.a.id() == self.b.id()):
            self._find_next()

    def is_active(self):
        return self.a.is_active()

    def _find_next(self):
        pos = self.a
        neg = self.b
        if not neg.is_active():
            return
        pos_id = pos.id()
        r = False
        
        if neg.id() < pos_id:
            neg.skip_to(pos_id)
        
        while pos.is_active() and neg.is_active() and pos_id == neg.id():
            nr = pos.next()
            if not pos.is_active():
                break
            
            r = r or nr
            pos_id = pos.id()
            neg.skip_to(pos_id)
        
        return r
    
    def replace(self):
        if not self.a.is_active():
            return NullMatcher()
        if not self.b.is_active():
            return self.a.replace()
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
        return iter(sorted(set(self.a.all_ids()) - set(self.b.all_ids())))
    
    def next(self):
        if not self.a.is_active():
            raise ReadTooFar
        ar = self.a.next()
        nr = False
        if self.b.is_active():
            nr = self._find_next()
        return ar or nr
        
    def skip_to(self, id):
        if not self.a.is_active():
            raise ReadTooFar
        if id < self.a.id():
            return
        
        self.a.skip_to(id)
        if self.b.is_active():
            self.b.skip_to(id)
            self._find_next()
    
    def weight(self):
        return self.a.weight()
    
    def score(self):
        return self.a.score()
    
    def supports(self, astype):
        return self.a.supports(astype)
    
    def value(self):
        return self.a.value()
    
    def value_as(self, astype):
        return self.a.value_as(astype)
    

class InverseMatcher(WrappingMatcher):
    """Synthetic matcher, generates postings that are NOT present in the
    wrapped matcher.
    """
    
    def __init__(self, child, limit, missing=None, weight=1.0):
        super(InverseMatcher, self).__init__(child)
        self.limit = limit
        self._weight = weight
        self.missing = missing or (lambda id: False)
        self._id = 0
        self._find_next()
    
    def copy(self):
        return self.__class__(self.child.copy(), self.limit,
                              weight=self._weight, missing=self.missing)
    
    def _replacement(self, newchild):
        return self.__class__(newchild, self.limit, missing=self.missing,
                              weight=self.weight)
    
    def is_active(self):
        return self._id < self.limit
    
    def supports_quality(self):
        return False
    
    def _find_next(self):
        child = self.child
        missing = self.missing
        
        if not child.is_active() and not missing(self._id):
            return
        
        if child.is_active() and child.id() < self._id:
            child.skip_to(self._id)
        
        # While self._id is missing or is in the child matcher, increase it
        while child.is_active() and self._id < self.limit:
            if missing(self._id):
                self._id += 1
                continue

            if self._id == child.id():
                self._id += 1
                child.next()
                continue
            
            break
        
    def id(self):
        return self._id
    
    def all_ids(self):
        missing = self.missing
        negs = set(self.child.all_ids())
        return (id for id in xrange(self.limit)
                if id not in negs and not missing(id))
    
    def next(self):
        if self._id >= self.limit:
            raise ReadTooFar
        self._id += 1
        self._find_next()
        
    def skip_to(self, id):
        if self._id >= self.limit:
            raise ReadTooFar
        if id < self._id:
            return
        self._id = id
        self._find_next()
    
    def weight(self):
        return self._weight
    
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
        if not self.child.is_active():
            return NullMatcher()
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
    
    def supports(self, astype):
        return self.a.supports(astype)
    
    def value(self):
        return self.a.value()
        
    def value_as(self, astype):
        return self.a.value_as(astype)
    

class AndMaybeMatcher(AdditiveBiMatcher):
    """Matches postings in the first sub-matcher, and if the same posting is
    in the second sub-matcher, adds their scores.
    """
    
    def __init__(self, a, b):
        self.a = a
        self.b = b
        
        if a.is_active() and b.is_active() and a.id() != b.id():
            b.skip_to(a.id())
    
    def is_active(self):
        return self.a.is_active()
    
    def id(self):
        return self.a.id()
    
    def next(self):
        if not self.a.is_active():
            raise ReadTooFar
        
        ar = self.a.next()
        br = False
        if self.a.is_active() and self.b.is_active():
            br = self.b.skip_to(self.a.id())
        return ar or br
    
    def skip_to(self, id):
        if not self.a.is_active():
            raise ReadTooFar
        
        ra = self.a.skip_to(id)
        rb = False
        if self.a.is_active() and self.b.is_active():
            rb = self.b.skip_to(id)
        return ra or rb
    
    def replace(self):
        ar = self.a.replace()
        br = self.b.replace()
        if not ar.is_active():
            return NullMatcher()
        if not br.is_active():
            return ar
        if ar is not self.a or br is not self.b:
            return self.__class__(ar, br)
        return self
    
    def skip_to_quality(self, minquality):
        a = self.a
        b = self.b
        minquality = minquality
        
        if not a.is_active():
            raise ReadTooFar
        if not b.is_active():
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
    
    def quality(self):
        q = 0.0
        if self.a.is_active():
            q += self.a.quality()
            if self.b.is_active() and self.a.id() == self.b.id():
                q += self.b.quality()
        return q
    
    def weight(self):
        if self.a.id() == self.b.id():
            return self.a.weight() + self.b.weight()
        else:
            return self.a.weight()
    
    def score(self):
        if self.b.is_active() and self.a.id() == self.b.id():
            return self.a.score() + self.b.score()
        else:
            return self.a.score()
        
    def supports(self, astype):
        return self.a.supports(astype)
    
    def value(self):
        return self.a.value()
        
    def value_as(self, astype):
        return self.a.value_as(astype)


class ConstantScoreMatcher(WrappingMatcher):
    def __init__(self, child, score=1.0):
        super(ConstantScoreMatcher, self).__init__(child)
        self._score = score
    
    def copy(self):
        return self.__class__(self.child.copy(), score=self._score)
    
    def _replacement(self, newchild):
        return self.__class__(newchild, score=self._score)
    
    def quality(self):
        return self._score
    
    def block_quality(self):
        return self._score
    
    def score(self):
        return self._score
    

#class PhraseMatcher(WrappingMatcher):
#    """Matches postings where a list of sub-matchers occur next to each other
#    in order.
#    """
#    
#    def __init__(self, wordmatchers, slop=1, boost=1.0):
#        self.wordmatchers = wordmatchers
#        self.child = make_binary_tree(IntersectionMatcher, wordmatchers)
#        self.slop = slop
#        self.boost = boost
#        self._spans = None
#        self._find_next()
#    
#    def copy(self):
#        return self.__class__(self.wordmatchers[:], slop=self.slop, boost=self.boost)
#    
#    def replace(self):
#        if not self.is_active():
#            return NullMatcher()
#        return self
#    
#    def all_ids(self):
#        # Need to redefine this because the WrappingMatcher parent class
#        # forwards to the submatcher, which in this case is just the
#        # IntersectionMatcher.
#        while self.is_active():
#            yield self.id()
#            self.next()
#    
#    def next(self):
#        ri = self.child.next()
#        rn = self._find_next()
#        return ri or rn
#    
#    def skip_to(self, id):
#        rs = self.child.skip_to(id)
#        rn = self._find_next()
#        return rs or rn
#    
#    def skip_to_quality(self, minquality):
#        skipped = 0
#        while self.is_active() and self.quality() <= minquality:
#            # TODO: doesn't count the documents matching the phrase yet
#            skipped += self.child.skip_to_quality(minquality/self.boost)
#            self._find_next()
#        return skipped
#    
#    def positions(self):
#        if not self.is_active():
#            raise ReadTooFar
#        if not self.wordmatchers:
#            return []
#        return self.wordmatchers[0].positions()
#    
#    def _find_next(self):
#        isect = self.child
#        slop = self.slop
#        
#        # List of "active" positions
#        current = []
#        
#        while not current and isect.is_active():
#            # [[list of positions for word 1],
#            #  [list of positions for word 2], ...]
#            poses = [m.positions() for m in self.wordmatchers]
#            
#            # Set the "active" position list to the list of positions of the
#            # first word. We well then iteratively update this list with the
#            # positions of subsequent words if they are within the "slop"
#            # distance of the positions in the active list.
#            current = poses[0]
#            
#            # For each list of positions for the subsequent words...
#            for poslist in poses[1:]:
#                # A list to hold the new list of active positions
#                newposes = []
#                
#                # For each position in the list of positions in this next word
#                for newpos in poslist:
#                    # Use bisect to only check the part of the current list
#                    # that could contain positions within the "slop" distance
#                    # of the new position
#                    start = bisect_left(current, newpos - slop)
#                    end = bisect_right(current, newpos)
#                    
#                    # 
#                    for curpos in current[start:end]:
#                        delta = newpos - curpos
#                        if delta > 0 and delta <= slop:
#                            newposes.append(newpos)
#                    
#                current = newposes
#                if not current: break
#            
#            if not current:
#                isect.next()
#        
#        self._count = len(current)
#
#
#class VectorPhraseMatcher(BasePhraseMatcher):
#    """Phrase matcher for fields with a vector with positions (i.e. Positions
#    or CharacterPositions format).
#    """
#    
#    def __init__(self, searcher, fieldname, words, isect, slop=1, boost=1.0):
#        """
#        :param searcher: a Searcher object.
#        :param fieldname: the field in which to search.
#        :param words: a sequence of token texts representing the words in the
#            phrase.
#        :param isect: an intersection matcher for the words in the phrase.
#        :param slop: 
#        """
#        
#        decodefn = searcher.field(fieldname).vector.decoder("positions")
#        self.reader = searcher.reader()
#        self.fieldname = fieldname
#        self.words = words
#        self.sortedwords = sorted(self.words)
#        super(VectorPhraseMatcher, self).__init__(isect, decodefn, slop=slop,
#                                                  boost=boost)
#    
#    def _poses(self):
#        vreader = self.reader.vector(self.child.id(), self.fieldname)
#        poses = {}
#        decode_positions = self.decode_positions
#        for word in self.sortedwords:
#            vreader.skip_to(word)
#            if vreader.id() != word:
#                raise Exception("Phrase query: %r in term index but not in"
#                                " vector (possible analyzer mismatch)" % word)
#            poses[word] = decode_positions(vreader.value())
#        # Now put the position lists in phrase order
#        return [poses[word] for word in self.words]












