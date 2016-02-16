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
This module contains "matcher" classes. Matchers deal with posting lists. The
most basic matcher, which reads the list of postings for a term, will be
provided by the backend implementation (for example,
:class:`whoosh.filedb.filepostings.FilePostingReader`). The classes in this
module provide additional functionality, such as combining the results of two
matchers, or modifying the results of a matcher.

You do not need to deal with the classes in this module unless you need to
write your own Matcher implementation to provide some new functionality. These
classes are not instantiated by the user. They are usually created by a
:class:`~whoosh.query.Query` object's :meth:`~whoosh.query.Query.matcher()`
method, which returns the appropriate matcher to implement the query (for
example, the :class:`~whoosh.query.Or` query's
:meth:`~whoosh.query.Or.matcher()` method returns a
:py:class:`~whoosh.matching.UnionMatcher` object).

Certain backends support "quality" optimizations. These backends have the
ability to skip ahead if it knows the current block of postings can't
contribute to the top N documents. If the matcher tree and backend support
these optimizations, the matcher's :meth:`Matcher.supports_block_quality()`
method will return ``True``.
"""

import copy
from abc import abstractmethod
from functools import wraps
from typing import Any, Iterable, Optional, Sequence, Set, Tuple, Union

from whoosh import idsets, postings
from whoosh.compat import next
from whoosh.ifaces import codecs, readers, weights


# Typing aliases

RawPost = Tuple[int, Tuple[bytes, bytes, bytes, bytes, bytes]]


# Exceptions

class ReadTooFar(Exception):
    """
    Raised when :meth:`~whoosh.matching.Matcher.next()` or
    :meth:`~whoosh.matching.Matcher.skip_to()` are called on an inactive
    matcher.
    """


class NoQualityAvailable(Exception):
    """
    Raised when quality methods are called on a matcher that does not
    support block quality optimizations.
    """


class NotLeafMatcher(Exception):
    """
    Raised when you call a method that's only valid on leaf matchers on a
    non-leaf matcher.
    """


# Decorator for matchers that checks whether the matcher is active
def check_active(method):
    """
    Decorator to check if the object is closed.
    """

    @wraps(method)
    def check_active_wrapper(self, *args, **kwargs):
        if not self.is_active():
            raise ReadTooFar(self)
        return method(self, *args, **kwargs)
    return check_active_wrapper


# Classes

class Matcher(object):
    """
    Base class for all matchers.
    """

    def __eq__(self, other: 'Matcher'):
        return type(self) is type(other) and self.__dict__ == other.__dict__

    def __ne__(self, other: 'Matcher'):
        return not self.__eq__(other)

    def __lt__(self, other: 'Matcher'):
        return False

    def __gt__(self, other: 'Matcher'):
        return False

    def close(self):
        pass

    def is_leaf(self) -> bool:
        return False

    # Interface

    @abstractmethod
    def is_active(self) -> bool:
        """
        Returns True if this matcher is still "active", that is, it has not
        yet reached the end of the posting list.
        """

        raise NotImplementedError

    @abstractmethod
    def id(self) -> int:
        """
        Returns the doc ID of the current posting.
        """

        raise NotImplementedError

    @abstractmethod
    def next(self) -> bool:
        """
        Moves this matcher to the next posting.
        """

        raise NotImplementedError(self.__class__.__name__)

    def skip_to(self, docid: int) -> bool:
        """
        Moves this matcher to the first posting with an ID equal to or
        greater than the given ID.

        :param docid: the document number to skip to.
        """

        # The default implementation just uses next(), a subclass might be able
        # to do something more efficient
        while self.is_active() and self.id() < docid:
            self.next()

    @abstractmethod
    def save(self) -> Any:
        """
        Returns a "bookmark" object representing the matcher's current position,
        which can be restored by calling ``Matcher.restore(bookmark)``. Be VERY
        careful with bookmark objects, since they can become invalid any time
        the matcher tree is replaced. This method supports the ``read_ahead()``
        method, which saves and restores almost immediately.
        """

        raise NotImplementedError

    @abstractmethod
    def restore(self, place: Any):
        """
        Restores the matcher to a position saved using the ``save()`` method.

        :param place: the "bookmark" object returned by this matcher's
            ``save()`` method. NEVER use a bookmark returned by a differnt
            matcher (including a matcher from before replacement).
        """

        raise NotImplementedError

    def read_ahead(self, count: int) -> Iterable(Tuple[int, float]):
        """
        Reads the next (at most) ``count`` number of ``(docid, score)`` tuples,
        and then returns to the current position.

        :param count: the maximum number of document IDs to read.
        """

        place = self.save()
        while count > 0 and self.is_active():
            yield self.id(), self.score()
            count -= 1
            self.next()
        self.restore(place)

    @abstractmethod
    def weight(self):
        """
        Returns the weight of the current posting.
        """

        raise NotImplementedError(self.__class__.__name__)

    @abstractmethod
    def score(self):
        """
        Returns the score of the current posting.
        """

        raise NotImplementedError(self.__class__.__name__)

    def children(self) -> 'Sequence[Matcher]':
        """
        Returns an (possibly empty) list of the submatchers of this
        matcher.
        """

        return ()

    def copy(self) -> 'Matcher':
        """
        Returns a copy of this matcher.
        """

        return copy.copy(self)

    def replace(self, minquality: float=0.0) -> 'Matcher':
        """
        Returns a possibly-simplified version of this matcher. For example,
        if one of the children of a UnionMatcher is no longer active, calling
        this method on the UnionMatcher will return the other child.

        :param minquality: The minimum quality required for the matcher to be
            valid.
        """

        if minquality and minquality > self.max_quality():
            return NullMatcher()
        else:
            return self

    def supports(self, name: str) -> bool:
        """
        Returns True if the field's format supports the given feature,
        for example 'weight' or 'chars'.

        :param name: a string containing the name of the feature to check for.
        """

        return False

    def supports_block_quality(self) -> bool:
        """
        Returns True if this matcher supports the use of ``quality`` and
        ``block_quality``.
        """

        return False

    def max_quality(self) -> float:
        """
        Returns the maximum possible quality measurement for this matcher,
        according to the current weighting algorithm. Raises
        ``NoQualityAvailable`` if the matcher or weighting do not support
        quality measurements.
        """

        raise NoQualityAvailable(self.__class__)

    def block_quality(self) -> float:
        """
        Returns a quality measurement of the current block of postings,
        according to the current weighting algorithm. Raises
        ``NoQualityAvailable`` if the matcher or weighting do not support
        quality measurements.
        """

        raise NoQualityAvailable(self.__class__)

    def skip_to_quality(self, minquality: float) -> int:
        """
        Moves this matcher to the next block with greater than the given
        minimum quality value.

        :param minquality: skip to the next block that has greater than this
            quality level.
        """

        raise NoQualityAvailable(self.__class__.__name__)

    # Derived methods

    def term_matchers(self) -> 'Iterable[Matcher]':
        """
        Returns an iterator of term matchers in this tree.
        """

        if self.is_leaf():
            yield self

        for cm in self.children():
            for m in cm.term_matchers():
                yield m

    def matching_terms(self, docid: int=None) -> 'Iterable[Tuple[str, bytes]]':
        """
        Returns an iterator of ``("fieldname", "termtext")`` tuples for the
        **currently matching** term matchers in this tree.

        :param docid: only yield terms on term matchers currently on this
            document.
        """

        if not self.is_active():
            return

        if docid is None:
            docid = self.id()
        elif docid != self.id():
            return

        if self.is_leaf():
            yield self.term()

        for cm in self.children():
            for t in cm.matching_terms(docid):
                yield t

    def _run_out(self) -> 'Iterable[Matcher]':
        i = 0
        m = self
        while m.is_active():
            yield m
            m.next()

            # Every 10 documents, call replace() to try to make this matcher
            # more efficient
            i += 1
            if i == 10:
                m = m.replace()
                i = 0

    def all_ids(self) -> Iterable[int]:
        """
        Returns a generator of all IDs in the matcher.

        What this method returns for a matcher that has already read some
        postings (whether it only yields the remaining postings or all postings
        from the beginning) is undefined, so you should only use this method
        on fresh matchers.
        """

        # The default implementation just uses id() and next(), subclasses might
        # be able to do something more efficient
        for m in self._run_out():
            yield m.id()

    def spans(self) -> 'Sequence[spans.Span]':
        """
        Returns a list of :class:`~whoosh.query.spans.Span` objects for the
        matches in this document. Raises an exception if the field being
        searched does not store positions.
        """
        from whoosh.query.spans import posting_to_spans

        post = self.posting()
        if post is not None:
            return posting_to_spans(post)
        else:
            return []

    def can_copy_raw_to(self, fmt: 'postings.Format') -> bool:
        return False

    # Leaf methods

    def term(self) -> Optional[Tuple[str, bytes]]:
        """
        Returns a ``("fieldname", b"termbytes")`` tuple for the term this
        matcher matches, if this is a term matcher.
        """

        raise NotLeafMatcher(self.__class__.__name__)

    def posting(self) -> 'Optional[postings.PostTuple]':
        """
        Returns a posting tuple corresponding to the current document, if this
        is a term matcher.
        """

        raise NotLeafMatcher(self.__class__.__name__)

    def raw_posting(self) -> 'postings.RawPost':
        raise NotLeafMatcher(self.__class__.__name__)

    def all_postings(self) -> 'Iterable[postings.PostTuple]':
        """
        Returns a generator of posting tuples for each document in the matcher.
        What happens if you call this on a matcher that's already advanced is
        undefined.
        """

        raise NotLeafMatcher(self.__class__.__name__)

    def all_raw_postings(self) -> 'Iterable[postings.PostTuple]':
        """
        Returns a generator of "raw" (partially encoded) posting tuples for each
        document in the matcher. What happens if you call this on a matcher
        that's already advanced is undefined.
        """

        raise NotLeafMatcher(self.__class__.__name__)

    def has_weights(self) -> bool:
        """
        Returns True if this matcher has per-posting weights. This is only
        valid on leaf matchers.
        """

        raise NotLeafMatcher(self.__class__.__name__)

    def has_lengths(self) -> bool:
        """
        Returns True if this matcher has per-posting lengths. You can get the
        field length for the current document using ``length()``. This is only
        valid on leaf matchers.
        """

        raise NotLeafMatcher(self.__class__.__name__)

    def has_positions(self) -> bool:
        """
        Returns True if this matcher has term positions. You can get the
        positions for the current document using ``positions()``. This is only
        valid on leaf matchers.
        """

        raise NotLeafMatcher(self.__class__.__name__)

    def has_chars(self) -> bool:
        """
        Returns True if this matcher has character ranges. You can get the
        character ranges for the current document using ``chars()``. This is
        only valid on leaf matchers.
        """

        raise NotLeafMatcher(self.__class__.__name__)

    def has_payloads(self) -> bool:
        """
        Returns True if this matcher has payloads. You can get the
        payloads for the current document using ``payloads()``. This is only
        valid on leaf matchers.
        """

        raise NotLeafMatcher(self.__class__.__name__)

    def length(self) -> int:
        """
        Returns the stored field length in the current document.
        Raises ``whoosh.postings.UnsupportedFeature`` if the field did not
        store per-posting lengths.
        """

        raise NotLeafMatcher(self.__class__.__name__)

    def positions(self) -> Sequence[int]:
        """
        Returns a list of 0-based occurrence positions in the current document.
        Raises ``whoosh.postings.UnsupportedFeature`` if the field did not
        store positions.
        """

        raise NotLeafMatcher(self.__class__.__name__)

    def chars(self) -> Sequence[Tuple[int]]:
        """
        Returns a list of 0-based ``(startchar, endchar)`` indices for term
        occurrences in the current document.
        Raises ``whoosh.postings.UnsupportedFeature`` if the field did not
        store character indices.
        """

        raise NotLeafMatcher(self.__class__.__name__)

    def payloads(self) -> Sequence[bytes]:
        """
        Returns a list of bytestring payloads for term occurrences in the
        current document.
        Raises ``whoosh.postings.UnsupportedFeature`` if the field did not
        store payloads.
        """

        raise NotLeafMatcher(self.__class__.__name__)


# Null matcher

class NullMatcherClass(Matcher):
    """
    Matcher with no postings which is never active.
    """

    def __call__(self):
        return self

    def __repr__(self):
        return "<NullMatcher>"

    def is_active(self):
        return False

    def id(self):
        raise ReadTooFar

    def next(self):
        raise ReadTooFar

    def skip_to(self, docid: int) -> bool:
        return False

    def save(self) -> None:
        pass

    def restore(self, place: None):
        pass

    def weight(self):
        raise ReadTooFar

    def score(self):
        raise ReadTooFar

    def posting(self):
        raise ReadTooFar

    def copy(self) -> Matcher:
        return self

    def supports_block_quality(self) -> bool:
        return True

    def max_quality(self) -> float:
        return 0

    def block_quality(self) -> float:
        return 0

    def skip_to_quality(self, minquality: float) -> int:
        return False

    def all_ids(self) -> Iterable[int]:
        return iter(())


# Singleton instance
NullMatcher = NullMatcherClass()


# Term/vector leaf posting matcher middleware

class LeafMatcher(Matcher):
    # Subclasses need to set
    #   self.scorer -- a Scorer object or None
    #   self.format -- Format object for the posting values

    def __init__(self, fieldname: str, tbytes: bytes, fmt: 'postings.Format',
                 terminfo: 'codecs.TermInfo', scorer: 'weights.Scorer'=None):
        self._fieldname = fieldname
        self._tbytes = tbytes
        self._format = fmt
        self._terminfo = terminfo
        self._scorer = scorer

    def __repr__(self):
        return "%s(%r, %s)" % (self.__class__.__name__, self.term(),
                               self.is_active())

    def term(self) -> Tuple[str, bytes]:
        return self._fieldname, self._tbytes

    def format(self) -> 'postings.Format':
        return self._format

    def can_copy_raw_to(self, fmt: 'postings.Format') -> bool:
        return self.format().can_copy_raw_to(fmt)

    def replace(self, minquality: float=0.0) -> Matcher:
        if not self.is_active():
            return NullMatcher()
        elif minquality and self.max_quality() <= minquality:
            return NullMatcher()
        else:
            return self

    def supports(self, name: str) -> bool:
        return self._format.supports(name)

    def supports_block_quality(self) -> bool:
        return self._scorer and self._scorer.supports_block_quality()

    def max_quality(self) -> float:
        if self._scorer:
            return self._scorer.max_quality()
        else:
            return self._terminfo.max_weight()

    def block_quality(self) -> float:
        if self.supports_block_quality():
            return self._scorer.block_quality(self)
        else:
            raise NoQualityAvailable

    def score(self) -> float:
        return self._scorer.score(self)

    def term_matchers(self):
        yield self

    def is_leaf(self) -> bool:
        return True

    def close(self):
        if self._scorer:
            self._scorer.close()

    # Forward "has_" methods to format object

    def has_weights(self):
        return self._format.has_weights

    def has_lengths(self):
        return self._format.has_lengths

    def has_positions(self):
        return self._format.has_positions

    def has_chars(self):
        return self._format.has_chars

    def has_payloads(self):
        return self._format.has_payloads

    # Subclasses need to implement these!

    def posting(self):
        raise NotImplementedError(self.__class__.__name__)

    def raw_posting(self) -> 'postings.RawPost':
        raise NotImplementedError(self.__class__.__name__)

    def length(self) -> int:
        raise NotImplementedError(self.__class__.__name__)

    def positions(self) -> Sequence[int]:
        raise NotImplementedError(self.__class__.__name__)

    def chars(self) -> Sequence[Tuple[int, int]]:
        raise NotImplementedError(self.__class__.__name__)

    def payloads(self) -> Sequence[bytes]:
        raise NotImplementedError(self.__class__.__name__)

    # Block stats methods - override if subclass uses blocks

    def block_min_length(self) -> int:
        raise NoQualityAvailable

    def block_max_length(self) -> int:
        raise NoQualityAvailable

    def block_max_weight(self):
        raise NoQualityAvailable

    # Derived

    def all_postings(self) -> 'Iterable[postings.PostTuple]':
        for m in self._run_out():
            yield m.posting()

    def all_raw_postings(self) -> 'Iterable[postings.RawPost]':
        for m in self._run_out():
            yield m.raw_posting()


# Provide a Matcher interface for a postings.DocPostReader object

class PostReaderMatcher(LeafMatcher):
    def __init__(self, dpreader: 'postings.DocListReader',
                 format_: 'postings.Format',
                 fieldname: str, tbytes: bytes,
                 terminfo: 'readers.TermInfo', scorer: 'weights.Scorer'=None):
        self._posts = dpreader
        self._format = format_
        self._fieldname = fieldname
        self._tbytes = tbytes
        self._terminfo = terminfo
        self._scorer = scorer

        self._i = 0

    def __repr__(self):
        return "<%s %d %r>" % (type(self).__name__, self._i, self._posts)

    def is_active(self) -> bool:
        return self._i < len(self._posts)

    def id(self) -> int:
        if self._i >= len(self._posts):
            raise ReadTooFar
        return self._posts.id(self._i)

    def next(self):
        if self._i >= len(self._posts):
            raise ReadTooFar

        self._i += 1

    def skip_to(self, docid: int):
        posts = self._posts

        while self._i < len(posts) and posts.id(self._i) < docid:
            self._i += 1

    def save(self) -> int:
        return self._i

    def restore(self, place: int):
        self._i = place

    def read_ahead(self, count: int) -> Sequence[int]:
        return self._posts.id_slice(self._i, self._i + count)

    def weight(self) -> float:
        if self.has_weights():
            return self._posts.weight(self._i)
        else:
            return 1.0

    def posting(self) -> 'postings.PostTuple':
        return self._posts.posting_at(self._i, termbytes=self._tbytes)

    def raw_posting(self) -> 'postings.RawPost':
        return self._posts.raw_posting_at(self._i)

    def skip_to_quality(self, minquality: float):
        # This whole reader acts as one "block", so if the max quality isn't
        # good enough, we can just give up
        if self.block_quality() <= minquality:
            self._i = len(self._posts)

    def all_ids(self):
        posts = self._posts
        i = self._i
        while i < len(posts):
            yield posts.id(i)
            i += 1

    # Raw copy methods

    def raw_postings(self) -> Iterable[RawPost]:
        return self._posts.raw_postings()

    # Format methods

    def length(self) -> int:
        return self._posts.length(self._i)

    def positions(self) -> Sequence[int]:
        return self._posts.positions(self._i)

    def chars(self) -> Sequence[Tuple[int, int]]:
        return self._posts.chars(self._i)

    def payloads(self) -> Sequence[bytes]:
        return self._posts.payloads(self._i)

    # Block stats

    def block_min_length(self) -> int:
        return self._terminfo.min_length()

    def block_max_length(self) -> int:
        return self._terminfo.max_length()

    def block_max_weight(self) -> float:
        return self._posts.max_weight()


class ListMatcher(Matcher):
    """
    Provides a Matcher interface for a list of doc IDs.
    """

    def __init__(self, docids: Sequence[int], all_weights: float=1.0):
        self._docids = docids
        self._weight = all_weights

        self._i = 0

    def is_active(self) -> bool:
        return self._i < len(self._docids)

    def id(self) -> int:
        if self._i >= len(self._docids):
            raise ReadTooFar
        return self._docids[self._i]

    def next(self):
        if self._i >= len(self._docids):
            raise ReadTooFar
        self._i += 1

    def save(self) -> int:
        return self._i

    def restore(self, place: int):
        self._i = place

    def supports_block_quality(self) -> bool:
        return True

    def skip_to_quality(self, minquality: float):
        # This whole reader acts as one "block", so if the max quality isn't
        # good enough, we can just give up
        if self.block_quality() <= minquality:
            self._i = len(self._docids)
            return True

    def all_ids(self):
        docids = self._docids
        i = self._i
        while i < len(docids):
            yield docids[i]
            i += 1

    def weight(self) -> float:
        return self._weight

    def score(self) -> float:
        return self._weight

    def max_quality(self) -> float:
        return self._weight

    def block_quality(self) -> float:
        return self._weight


class IteratorMatcher(Matcher):
    """
    Provides a matcher interface for an iterator of doc IDs.
    """

    def __init__(self, docids: Iterable[int], all_weights: float=1.0,
                 include: 'Union[idsets.DocIdSet, Set]'=None,
                 exclude: 'Union[idsets.DocIdSet, Set]'=None):
        self._docids = docids
        self._weight = all_weights
        self._include = include
        self._exclude = exclude

        self._active = True
        self._id = -1
        self.next()

    def __repr__(self):
        return "%s(%r, all_weights=%s, include=%r, exclude=%r)" % (
            type(self).__name__, self._docids, self._weight, self._include,
            self._exclude,
        )

    def is_active(self) -> bool:
        return self._active

    def id(self) -> int:
        if not self._active:
            raise ReadTooFar
        return self._id

    def next(self):
        docids = self._docids
        include = self._include
        exclude = self._exclude
        try:
            while True:
                newid = next(docids)
                if include is not None and newid not in include:
                    continue
                if exclude is not None and newid in exclude:
                    continue
                self._id = newid
                break
        except StopIteration:
            self._active = False

    def save(self) -> Tuple[int, Sequence[int]]:
        if not self._active:
            raise ReadTooFar
        return self._id, list(self._docids)

    def restore(self, place: Tuple[int, Sequence[int]]):
        self._id = place[0]
        self._docids = iter(place[1])
        self._active = True

    def supports_block_quality(self) -> bool:
        return True

    def skip_to_quality(self, minquality: float):
        # This whole reader acts as one "block", so if the max quality isn't
        # good enough, we can just give up
        if self.block_quality() <= minquality:
            self._active = False
            return True

    def all_ids(self) -> Iterable[int]:
        include = self._include
        exclude = self._exclude

        yield self._id
        for docid in self._docids:
            if include and docid not in include:
                continue
            if exclude and docid not in exclude:
                continue
            yield docid
        self._active = False

    def weight(self) -> float:
        return self._weight

    def score(self) -> float:
        return self._weight

    def max_quality(self) -> float:
        return self._weight

    def block_quality(self) -> float:
        return self._weight
