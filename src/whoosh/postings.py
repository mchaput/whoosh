#===============================================================================
# Copyright 2009 Matt Chaput
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
This module contains classes for writing and reading postings.

The PostIterator interface is the base interface for the two "cursor"
interfaces (PostingReader and QueryScorer). It defines the basic methods for
moving through the posting list (e.g. ``reset()``, ``next()``, ``skip_to()``).

The PostingReader interface allows reading raw posting information. Individual
backends must provide a PostingReader implementation that will be returned by
the backend's :meth:`whoosh.reading.IndexReader.postings` method. PostingReader
subclasses in this module provide synthetic readers or readers that wrap other
readers and modify their behavior.

The QueryScorer interface allows retrieving and scoring search results.
QueryScorer objects will be returned by the :meth:`~whoosh.query.Query.scorer`
method on :class:`whoosh.query.Query` objects. QueryScorer subclasses in this
module provide synthetic scorers or scorers that wrap other scorers and modify
their behavior.
"""


from heapq import heapify, heappop, heapreplace


# Exceptions

class ReadTooFar(Exception):
    """Raised if a user calls next() or skip_to() on a reader that has reached
    the end of its items.
    """
    pass


# Base classes

class PostingWriter(object):
    def write(self, id, value):
        """Write the given id and value to the posting store.
        
        :param id: The identifier for this posting.
        :param value: The encoded value string for this posting.
        """
        raise NotImplementedError
    
    def finish(self):
        "Called when the current set of postings is finished."
        pass
    
    def close(self):
        raise NotImplementedError


class PostIterator(object):
    """Base class for PostingReader and QueryScorer. This interface provides
    methods for moving the "cursor" along posting lists.
    """
    
    def __cmp__(self, other):
        return cmp(self.id, other.id)
    
    def __repr__(self):
        return "<%s : %s>" % (self.__class__.__name__, self.id)
    
    def reset(self):
        "Resets the reader to the beginning of the postings"
        raise NotImplementedError(self)
    
    def next(self):
        "Moves to the next posting."
        raise NotImplementedError(self)
    
    def skip_to(self, id):
        """Skips ahead to the given id. The default implementation simply calls
        next() repeatedly until it gets to the id, but subclasses will often be
        more clever.
        """
        
        if id <= self.id:
            return
        if self.id is None:
            raise ReadTooFar
        
        next = self.next
        while self.id < id:
            next()
    
    # Iterator convenience functions
    
    def all_ids(self):
        """Yields all posting IDs. This may or may not change the cursor
        position, depending on the subclass and backend implementations.
        """
        self.reset()
        return self.ids()
    
    def ids(self):
        """Yields the remaining IDs in the reader. This may or may not change
        the cursor position, depending on the subclass and backend
        implementations.
        """
        
        next = self.next
        while self.id is not None:
            yield self.id
            next()


class PostingReader(PostIterator):
    """Base class for posting readers.
    
    "Postings" are used for two purposes in Whoosh.
    
    For each term in the index, the postings are the list of documents the term
    appears in and any associated value for each document. For example, if the
    field format is Frequency, the postings for the field might look like::
      
        [(0, 1), (10, 3), (12, 5)]
        
    ...where 0, 10, and 12 are document numbers, and 1, 3, and 5 are the
    frequencies of the term in those documents.
      
    To get a PostingReader object for a term, use the
    :meth:`~whoosh.reading.IndexReader.postings` method on an IndexReader or
    Searcher object.
    
    >>> # Get a PostingReader for the term "render" in the "content" field.
    >>> r = myindex.reader()
    >>> preader = r.postings("content", u"render")
      
    For fields with term vectors, the vector postings are the list of terms
    that appear in the field and any associated value for each term. For
    example, if the term vector format is Frequency, the postings for the term
    vector might look like::
    
        [(u"apple", 1), (u"bear", 5), (u"cab", 2)]
        
    ...where "apple", "bear", and "cab" are the terms in the document field,
    and 1, 5, 2 are the frequencies of those terms in the document field.
    
    To get a PostingReader object for a vector, use the
    :meth:`~whoosh.reading.IndexReader.vector` method on an IndexReader or
    Searcher object.
    
    >>> # Get a PostingReader for the vector of the "content" field
    >>> # of document 100 
    >>> r = myindex.reader()
    >>> vreader = r.vector(100, "content")
    
    PostingReader defines a fairly simple interface.
    
    * The current posting ID is in the reader.id attribute.
    * Reader.value() to get the posting payload.
    * Reader.value_as(astype) to get the interpreted posting payload.
    * Reader.next() to move the reader to the next posting.
    * Reader.skip_to(id) to move the reader to that id in the list.
    * Reader.reset() to reset the reader to the beginning.
    
    In addition, PostingReader supports a few convenience methods:
    
    * ids() returns an iterator of the remaining IDs.
    * items() returns an iterator of the remaining (id, encoded_value) pairs.
    * items_as(astype) returns an interator of the remaining
      (id, decoded_value) pairs.
    
    all_ids(), all_items(), and all_as() are similar, but return iterators of
    *all* IDs/items in the reader, regardless of the current position of the
    reader.
      
    Different implementations may leave the reader in different positions
    during and after use of the iteration methods; that is, the effect of the
    iterators on the reader's position is undefined and may be different in
    different PostingReader subclasses and different backend implementations.
    """
    
    def value(self):
        "Returns the encoded value string for the current id."
        raise NotImplementedError

    def value_as(self, astype):
        """Returns the value for the current id as the given type.
        
        :param astype: a string, such as "weight" or "positions". The
            Format object associated with this reader must have a
            corresponding "as_*" method, e.g. as_weight(), for decoding
            the value.
        """
        return self.format.decoder(astype)(self.value())
    
    # Iterator convenience functions
    
    def all_items(self):
        """Yields all (id, encoded_value) pairs in the reader.
        Use all_as() to get decoded values. This may or may not change the
        cursor position, depending on the subclass and backend implementations.
        """
        self.reset()
        return self.items()
    
    def all_as(self, astype):
        """Yield a series of (id, decoded_value) pairs for each posting.
        This may or may not change the cursor position, depending on the
        subclass and backend implementations.
        """
        self.reset()
        return self.items_as(astype)
    
    def items(self):
        """Yields the remaining (id, encoded_value) pairs in the reader.
        Use items_as() to get decoded values. This may or may not change the
        cursor position, depending on the subclass and backend implementations.
        """
        
        next = self.next
        while self.id is not None:
            yield self.id, self.value()
            next()
            
    def items_as(self, astype):
        """Yields the remaining (id, decoded_value) pairs in the reader.
        This may or may not change the cursor position, depending on the
        subclass and backend implementations.
        """
        decoder = self.format.decoder(astype)
        for id, valuestring in self.items():
            yield (id, decoder(valuestring))


class QueryScorer(PostIterator):
    """QueryScorer extends the PostIterator interface with two methods:
    
    * score() return the score for the current item.
    * __iter__() returns an iterator of (id, score) pairs.
    """
    
    def __iter__(self):
        next = self.next
        while self.id is not None:
            yield self.id, self.score()
            next()
    
    def score(self):
        """Returns the score for the current document.
        """
        raise NotImplementedError


class FakeIterator(object):
    """A mix-in that provides methods for a fake PostingReader or
    QueryScorer.
    """
    
    def __init__(self, *ids):
        self.ids = ids
        self.reset()
        
    def reset(self):
        self.i = 0
        if self.ids:
            self.id = self.ids[0]
        else:
            self.id = None
    
    def next(self):
        if self.id is None:
            raise ReadTooFar
        
        if self.i == len(self.ids) - 1:
            self.id = None
        else:
            self.i += 1
            self.id = self.ids[self.i]
    
    def skip_to(self, target):
        if target <= self.id:
            return
        if self.id is None:
            raise ReadTooFar
        
        i, ids = self.i, self.ids
        
        while ids[i] < target:
            i += 1
            if i == len(ids):
                self.id = None
                return
        
        self.i = i
        self.id = ids[i]


# Posting readers

class MultiPostingReader(PostingReader):
    """This posting reader concatenates the results from serial sub-readers.
    This is useful for backends that use a segmented index.
    """
    
    def __init__(self, format, readers, idoffsets):
        """
        :param format: the :class:`~whoosh.formats.Format` object for the field
            being read.
        :param readers: a list of :class:`~whoosh.postings.PostingReader`
            objects.
        :param idoffsets: a list of integers, where each item in the list
            represents the ID offset of the corresponding reader in the
            'readers' list.
        """
        
        self.format = format
        self.readers = readers
        self.offsets = idoffsets
        self.current = 0
        self._prep()
    
    def _prep(self):
        readers = self.readers
        current = self.current
        if not readers:
            self.id = None
            
        while readers[current].id is None:
            current += 1
            if current >= len(readers):
                self.id = None
                return
        
        self.current = current
        self.id = readers[current].id + self.offsets[current]

    def reset(self):
        if not self.readers:
            return
        
        for r in self.readers:
            r.reset()
        self.current = 0
        self._prep()

    def all_items(self):
        offsets = self.offsets
        for i, r in enumerate(self.readers):
            for id, valuestring in r.all_items():
                yield id + offsets[i], valuestring

    def all_ids(self):
        offsets = self.offsets
        for i, r in enumerate(self.readers):
            for id in r.all_ids():
                yield id + offsets[i]

    def next(self):
        if self.id is None:
            raise ReadTooFar
        
        readers = self.readers
        current = self.current
        readers[current].next()
        while current < len(readers) - 1 and self.readers[current].id is None:
            current += 1
            
        if self.readers[current].id is None:
            self.id = None
        else:
            self.id = readers[current].id + self.offsets[current]
            self.current = current

    def skip_to(self, target):
        if target <= self.id:
            return
        if self.id is None:
            raise ReadTooFar
        
        current = self.current
        readers = self.readers
        offsets = self.offsets
        
        while current < len(readers):
            r = readers[current]
            if r.id is None:
                current += 1
                continue
            
            if target < r.id:
                self.current = current
                self.id = r.id + offsets[current]
                return
            
            r.skip_to(target - offsets[current])
            if r.id is not None:
                self.current = current
                self.id = r.id + offsets[current]
                return
            
            current += 1
            
        self.id = None

    def value(self):
        return self.readers[self.current].value()


class Exclude(PostingReader):
    """PostingReader that removes certain IDs from a sub-reader.
    """
    
    def __init__(self, postreader, excludes):
        """
        :param postreader: the PostingReader object to read from.
        :param excludes: a collection of ids to exclude (may be any object,
            such as a BitVector or set, that implements __contains__).
        """
        
        self.postreader = postreader
        if hasattr(postreader, "format"):
            self.format = postreader.format
        
        self.excludes = excludes
        self._find_nonexcluded()
        self.value = postreader.value
    
    def reset(self):
        self.postreader.reset()
        self._find_nonexcluded()
    
    def _find_nonexcluded(self):
        pr, excl = self.postreader, self.excludes
        next = pr.next
        while pr.id is not None and pr.id in excl:
            next()
        self.id = pr.id
    
    def next(self):
        self.postreader.next()
        self._find_nonexcluded()
    
    def skip_to(self, target):
        if target <= self.id:
            return
        self.postreader.skip_to(target)
        self._find_nonexcluded()


class CachedPostingReader(PostingReader):
    """Reads postings from a list in memory instead of from storage.
    
    >>> preader = ixreader.postings("content", "render")
    >>> creader = CachedPostingReader(preader.all_items())
    """
    
    def __init__(self, items):
        """
        :param items: a sequence of (id, encodedvalue) pairs. If this is
            not a list or tuple, it is converted using tuple().
        """
        
        if not isinstance(items, (list, tuple)):
            items = tuple(items)
        
        self._items = items
        self.reset()
    
    def reset(self):
        self.p = 0
        self.id = self._items[0][0]
    
    def all_ids(self):
        return (item[0] for item in self._items)
    
    def all_items(self):
        return iter(self._items)
    
    def next(self):
        if self.id is None:
            raise ReadTooFar
        
        self.p += 1
        if self.p >= len(self._items):
            self.id = None
        else:
            self.id = self._items[self.p][0]
            
    def skip_to(self, target):
        if self.id is None:
            raise ReadTooFar
        if target < self.id:
            return
        
        items = self._items
        p = self.p + 1
        while p < len(items):
            id = items[p][0]
            if id >= target:
                self.p = p
                self.id = id
                return
        
        self.id = None
        
    def value(self):
        return self._items[self.p][1]


class FakeReader(FakeIterator, PostingReader):
    """This is a fake posting reader for testing purposes. You create the
    object with the posting IDs as arguments, and then returns them as you call
    next() or skip_to().
    
    >>> fpr = FakeReader(1, 5, 10, 80)
    >>> fpr.id
    1
    >>> fpr.next()
    >>> fpr.id
    5
    """
    
    _value = 100
    
    def value(self):
        return self._value
    

# QueryScorers

class FakeScorer(FakeIterator, QueryScorer):
    """This is a fake query scorer for testing purposes. You create the
    object with the posting IDs as arguments, and then returns them as you call
    next() or skip_to().
    
    >>> fpr = FakeScorer(1, 5, 10, 80)
    >>> fpr.id
    1
    >>> fpr.next()
    >>> fpr.id
    5
    """
    
    _score = 10
    
    def score(self):
        return self._score


class EmptyScorer(QueryScorer):
    """A QueryScorer representing a query that doesn't match any documents.
    """
    
    def __init__(self):
        self.id = None
    def reset(self):
        pass
    def next(self):
        pass
    def skip_to(self, id):
        pass
    def ids(self):
        return []
    def items(self):
        return []
    def items_as(self, astype):
        return []
    def score(self):
        return 0
    

class ListScorer(QueryScorer):
    """A Scorer implementation that gets document postings and scores
    from a sequence of (id, score) pairs.
    """
    
    def __init__(self, postings):
        self.postings = postings
        self.reset()
    
    def reset(self):
        self.i = 0
        self.id = self.postings[0]
    
    def next(self):
        self.i += 1
        if self.i < len(self.postings):
            self.id = self.postings[self.i]
        else:
            self.id = None
    
    def skip_to(self, id):
        postings = self.postings
        i = self.i
        while i < len(postings) and postings[i][0] < id:
            i += 1
        if i < len(postings):
            self.i = i
            self.id = postings[i]
        else:
            self.id = None
    
    def ids(self):
        return [id for id, _ in self.postings]
    
    def items(self):
        return self.postings[:]
    
    def score(self):
        if self.id is None:
            return 0
        return self.postings[self.i][1]


class IntersectionScorer(QueryScorer):
    """Acts like the intersection of items in a set of QueryScorers
    """
    
    def __init__(self, scorers, boost=1.0):
        self.scorers = scorers
        self.state = list(scorers)
        self.boost = boost
        self.id = -1
        self._prep()

    def __repr__(self):
        return "<%s %r: %r>" % (self.__class__.__name__, self.scorers, self.id)

    def _prep(self):
        state = self.state
        state.sort()
        id = state[0].id
        if all(r.id == id for r in state[1:]):
            self.id = id
        else:
            self.next()

    def reset(self):
        for r in self.state:
            r.reset()
        self.id = -1
        self._prep()

    def skip_to(self, target):
        if self.id is None:
            raise ReadTooFar
        
        state = self.state
        for r in state:
            r.skip_to(target)
        self._prep()
    
    def next(self):
        if self.id is None:
            raise ReadTooFar
        
        id = self.id
        state = self.state
        for r in state:
            if r.id == id: r.next()
        state.sort()
        
        while True:
            oldstate = tuple(s.id for s in state)
            lowid = state[0].id
            if lowid is None:
                self.id = None
                return
            
            if all(r.id == lowid for r in state[1:]):
                self.id = lowid
                return
            else:
                highid = state[-1].id
                for r in state[:-1]:
                    r.skip_to(highid)
                if state[0].id is not None:
                    state.sort()
            if tuple(s.id for s in state) == oldstate:
                raise Exception("Infinite loop")
                    
    def score(self):
        if self.id is None:
            return 0
        return sum(r.score() for r in self.state) * self.boost
                

class UnionScorer(QueryScorer):
    """Acts like the union of a set of QueryScorers
    """
    
    def __init__(self, scorers, boost=1.0, minmatch=0):
        self.scorers = scorers
        self.boost = boost
        self.minmatch = minmatch
        self.state = [s for s in scorers if s.id is not None]
        
        if self.state:
            heapify(self.state)
            self.id = self.state[0].id
        else:
            self.id = None
    
    def reset(self):
        for s in self.scorers:
            s.reset()
        self.state = [s for s in self.scorers if s.id is not None]
        
        if self.state:
            heapify(self.state)
            self.id = self.state[0].id
        else:
            self.id = None
    
    def skip_to(self, target):
        if self.id is None:
            raise ReadTooFar
        
        state = self.state
        for r in state:
            r.skip_to(target)
        
        heapify(state)
        while state and state[0].id is None:
            heappop(state)
        
        if state:
            self.id = self.state[0].id
        else:
            self.id = None
        
    def next(self):
        if self.id is None:
            raise ReadTooFar
        
        state = self.state
        
        if len(state) < self.minmatch:
            # Can't match the minimum if there aren't enough readers left
            self.id = None
        elif len(state) == 1:
            # Short circuit if there's only one reader
            r = state[0]
            r.next()
            self.id = r.id
        else:
            # Advance all the readers that match the current id
            lowid = state[0].id
            while state and state[0].id == lowid:
                r = state[0]
                r.next()
                if r.id is None:
                    heappop(state)
                else:
                    heapreplace(state, r)
            
            if state:
                self.id = state[0].id
            else:
                self.id = None
            
    def score(self):
        id = self.id
        if id is None:
            return 0
        
        minmatch = self.minmatch
        if minmatch:
            count = 0
            # Count the number of sub-scorers matching the current ID
            for r in self.state:
                if r.id == id:
                    count += 1
                    if count >= minmatch: break
                else:
                    break
            if count < minmatch: return 0
        
        score = sum(r.score() for r in self.state if r.id == id)
        return score * self.boost


class AndNotScorer(QueryScorer):
    """Takes two QueryScorers and pulls items from the first, skipping items
    that also appear in the second.
    
    THIS SCORER IS NOT ACTUALLY USED, since it turns out to be slightly faster
    to simply create an "excluded_docs" filter from the "not" query and pass
    that into the "positive" query.
    """
    
    def __init__(self, positive, negative):
        """
        :param positive: a QueryScorer from which to take items.
        :param negative: a QueryScorer, the IDs of which will be
            removed from the 'positive' scorer.
        """
        
        self.positive = positive
        self.negative = negative
        self.id = None
        self._find_next()
    
    def reset(self):
        self.positive.reset()
        self.negative.reset()
        self._find_next()
    
    def _find_next(self):
        pos, neg = self.positive, self.negative
        if pos.id is None:
            self.id = None
            return
        elif neg.id is None:
            self.id = pos.id
            return
        
        if neg.id < pos.id:
            neg.skip_to(pos.id)
        while pos.id == neg.id:
            pos.next()
            neg.skip_to(pos.id)
        self.id = pos.id
    
    def next(self):
        if self.id is None:
            raise ReadTooFar
        
        self.positive.next()
        self._find_next()
        
    def skip_to(self, target):
        if self.id is None:
            raise ReadTooFar
        if target <= self.id:
            return
        
        self.positive.skip_to(target)
        if self.negative.id is not None:
            self.negative.skip_to(target)
            self._find_next()
        else:
            self.id = self.positive.id
        
    def score(self):
        if self.id is None:
            return 0
        return self.positive.score()


class InverseScorer(QueryScorer):
    """Takes a sub-scorer, and returns all documents *not* found in the
    sub-scorer. Assigns a static score to the "found" documents.
    """
    
    def __init__(self, scorer, maxid, is_deleted, docscore=1.0):
        self.scorer = scorer
        self.maxid = maxid
        self.is_deleted = is_deleted
        self.docscore = docscore
        self.id = 0
        self._find_next()
    
    def reset(self):
        self.scorer.reset()
        self.id = 0
        self._find_next()
    
    def _find_next(self):
        while self.id == self.scorer.id and not self.is_deleted(self.id):
            self.id += 1
            if self.scorer.id is not None:
                self.scorer.next()
        if self.id >= self.maxid:
            self.id = None
    
    def next(self):
        if self.id is None:
            raise ReadTooFar
        self.id += 1
        self._find_next()
    
    def skip_to(self, target):
        if self.id is None:
            raise ReadTooFar
        if target <= self.id:
            return
        
        self.scorer.skip_to(target)
        self.id = target
        self._find_next()
    
    def score(self):
        if self.id is None:
            return 0
        return self.docscore


class RequireScorer(QueryScorer):
    """Takes the intersection of two sub-scorers, but only takes scores from
    the first.
    """
    
    def __init__(self, scorer, required):
        self.scorer = scorer
        self.intersection = IntersectionScorer([scorer, required])
        
        self.reset = self.intersection.reset
        self.next = self.intersection.next
        self.skip_to = self.intersection.skip_to
    
    @property
    def id(self):
        return self.intersection.id
    
    def score(self):
        if self.id is None:
            return 0
        return self.scorer.score()


class AndMaybeScorer(QueryScorer):
    """Takes two sub-scorers, and returns documents that appear in the first,
    but if the document also appears in the second, adds their scores together.
    """
    
    def __init__(self, required, optional):
        self.required = required
        self.optional = optional
        
    def reset(self):
        self.required.reset()
        self.optional.reset()
        
    def next(self):
        self.required.next()
        self.optional.skip_to(self.required.id)

    def skip_to(self, target):
        self.required.skip_to(target)
        self.optional.skip_to(target)

    def score(self):
        if self.required.id == self.optional.id:
            return self.required.score() + self.optional.score()
        else:
            return self.required.score()











