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

from abc import abstractmethod
from typing import Iterable, List, Sequence, Tuple


# Exceptions

class CompositionError(Exception):
    pass


# Utility functions

def unstopped(tokenstream: 'Iterable[Token]') -> 'Iterable[Token]':
    """
    Removes tokens from a token stream where token.stopped = True.

    :param tokenstream: an iterable of Token objects.
    """
    return (t for t in tokenstream if not t.stopped)


def entoken(textstream: Iterable[str], positions: bool=False,
            ranges: bool=False, start_pos: int=0, range_start: int=0,
            **kwargs) -> 'Iterable[Token]':
    """
    Takes a sequence of unicode strings and yields a series of Token objects
    (actually the same Token object over and over, for performance reasons),
    with the attributes filled in with reasonable values (for example, if
    ``positions`` or ``ranges`` is True, the function assumes each token was
    separated by one space).

    :param textstream: an iterable of unicode strings.
    :param positions: whether to store positions on the tokens.
    :param ranges: whether to store character extents on the tokens.
    :param start_pos: the position to start with when numbering tokens.
    :param range_start: the index to start with when recording chars.
    :param kwargs: passed as keyword arguments to the Token object.
    """

    pos = start_pos
    range_index = range_start
    t = Token(positions=positions, ranges=ranges, **kwargs)

    for text in textstream:
        t.text = text

        if positions:
            t.pos = pos
            pos += 1

        if ranges:
            t.range_start = range_index
            range_index += len(text)
            t.range_end = range_index

        yield t


# Token object

class Token:
    """
    Represents a "token" (usually a word) extracted from the source text being
    indexed.

    See "Advanced analysis" in the user guide for more information.

    Because object instantiation in Python is slow, tokenizers should create
    ONE SINGLE Token object and YIELD IT OVER AND OVER, changing the attributes
    each time.

    This trick means that consumers of tokens (i.e. filters) must never try to
    hold onto the token object between loop iterations, or convert the token
    generator into a list. Instead, save the attributes between iterations,
    not the object::

        def RemoveDuplicatesFilter(self, stream):
            # Removes duplicate words.
            lasttext = None
            for token in stream:
                # Only yield the token if its text doesn't
                # match the previous token.
                if lasttext != token.text:
                    yield token
                lasttext = token.text

    ...or, call token.copy() to get a copy of the token object.
    """

    __slots__ = ("fieldname", "positions", "ranges", "payloads", "removestops",
                 "mode", "no_morph", "field_boost", "stopped", "boost",
                 "source", "text", "pos", "range_start", "range_end", "payload",
                 "matched", "original")

    def __init__(self,
                 fieldname: str=None,
                 positions: bool=False,
                 ranges: bool=False,
                 payloads: bool=False,
                 removestops: bool=True,
                 mode: str='',
                 no_morph: bool=False,
                 field_boost: float=1.0,
                 source: str=None,
                 text: str=u'',
                 original: str=None,
                 boost=1.0,
                 stopped=False,
                 pos=-1,
                 range_start=-1,
                 range_end=-1,
                 payload=b'',
                 matched=False):
        """
        :param fieldname: The name of the field this token is for. Usually this
            is implied and equal to None, however in some applications (for
            example spelling) it is filled out.
        :param positions: Whether tokens should have the token position in the
            'pos' attribute.
        :param ranges: Whether tokens should have range offsets in the
            'range_start' and 'range_end' attributes.
        :param payloads: Whether tokens should have payloads filled in.
        :param removestops: whether to remove stop words from the stream (if
            the tokens pass through a stop filter).
        :param mode: contains a string describing the purpose for which the
            analyzer is being called, i.e. 'index' or 'query'.
        :param no_morph: whether to skip filters that morphologically change
            tokens (e.g. stemming).
        :param field_boost: the default boost value of the field. This is used
            as the initial value for `.boost` when the token resets.
        :param source: the original string being tokenized.
        :param text: the text of the token.
        :param original: contains the original (pre-morophologized) word string.
        :param boost: the weight of this individual occurance.
        :param stopped: whether this is a stop-word. Usually stopped words are
            removed from the token stream, but sometimes they are left in (for
            example, for highlighting), in which case they can be excluded from
            certain operations by checking this attribute.
        :param pos: the position of this token in the stream.
        :param range_start: the start of this token's "range". For words, this
            is the character index of the start of the word. For annotation
            ranges, this is the start position.
        :param range_end: the end of this token's "range". For words, this is
            the character index of the end of the word. For annoation ranges,
            this is the end position.
        :param payload: an arbitrary bytestring associated with this occurance
            of the word.
        :param matched: used in highlighting to mark a token that should be
            highlighted.
        """

        self.fieldname = fieldname
        self.positions = positions
        self.ranges = ranges
        self.payloads = payloads
        self.removestops = removestops
        self.mode = mode
        self.no_morph = no_morph
        self.field_boost = field_boost
        self.source = source

        self.text = text
        self.boost = boost
        self.stopped = stopped
        self.pos = pos
        self.range_start = range_start
        self.range_end = range_end
        self.payload = payload

        self.matched = matched
        self.original = original

    def __repr__(self):
        parms = ", ".join("%s=%r" % (name, getattr(self, name))
                          for name in self.__slots__)
        return "%s(%s)" % (self.__class__.__name__, parms)

    def copy(self) -> 'Token':
        return Token(fieldname=self.fieldname,
                     positions=self.positions,
                     ranges=self.ranges,
                     payloads=self.payloads,
                     removestops=self.removestops,
                     mode=self.mode,
                     no_morph=self.no_morph,
                     field_boost=self.field_boost,
                     source=self.source,
                     text=self.text,
                     boost=self.boost,
                     stopped=self.stopped,
                     pos=self.pos,
                     range_start=self.range_start,
                     range_end=self.range_end,
                     payload=self.payload,
                     matched=self.matched,
                     original=self.original)


# Base class

class Analyzer:
    # True if this analyzer morphs term texts (e.g. stemming)
    is_morph = False

    def __repr__(self):
        comps = [c for c in self.components() if c is not self]
        return "%s(%r)" % (type(self).__name__, comps)

    def __eq__(self, other: 'Analyzer'):
        return (other
                and self.__class__ is other.__class__
                and self.__dict__ == other.__dict__)

    def __ne__(self, other: 'Analyzer'):
        return not self == other

    @abstractmethod
    def __call__(self, value: str, tokenize=True, **kwargs):
        raise NotImplementedError

    def components(self) -> 'Iterable[Analyzer]':
        yield self

    def has_morph(self) -> bool:
        return self.is_morph

    def clean(self):
        pass


class Tokenizer(Analyzer):
    @abstractmethod
    def __call__(self, value: str, **kwargs):
        raise NotImplementedError

    def __or__(self, other: 'Filter') -> 'CompositeAnalyzer':
        if isinstance(other, FilterChain):
            return CompositeAnalyzer(self, *other.components())

        elif isinstance(other, Filter):
            return CompositeAnalyzer(self, other)

        else:
            raise CompositionError("Cannot compose %r and %r" % (self, other))

    def is_token_start(self, s: str, at: int) -> bool:
        raise NotImplementedError


class Filter(Analyzer):
    def __call__(self, value: str, **kwargs):
        raise TypeError("Can't call a Filter, use the filter method")

    def __or__(self, other: 'Filter') -> 'FilterChain':
        if not isinstance(other, Filter):
            raise CompositionError("Cannot compose %r and %r" % (self, other))
        return FilterChain(self, other)

    @abstractmethod
    def filter(self, tokens: Iterable[Token]) -> Iterable[Token]:
        raise NotImplementedError(self.__class__)

    def set_options(self, kwargs):
        pass


class FilterChain(Filter):
    def __init__(self, *filters):
        self._filters = filters  # type: Tuple[Filter]

    def __or__(self, other: 'Filter') -> 'FilterChain':
        if not isinstance(other, Filter):
            raise CompositionError("Cannot compose %r and %r" % (self, other))
        fs = list(self._filters) + [other]
        return FilterChain(self, *fs)

    def filter(self, gen: Iterable[Token]) -> Iterable[Token]:
        for f in self._filters:
            gen = f.filter(gen)
        return gen

    def components(self):
        return self._filters


class CompositeAnalyzer(Tokenizer):
    def __init__(self, tokenizer: Tokenizer, *filters):
        self._tokenizer = tokenizer
        self._filters = []  # type: List[Filter]
        self.extend(filters)

    def __eq__(self, other: 'CompositeAnalyzer'):
        return (
            type(self) is type(other) and
            self._tokenizer == other._tokenizer and
            self._filters == other._filters
        )

    def __or__(self, other: 'Analyzer') -> 'CompositeAnalyzer':
        if isinstance(other, FilterChain):
            comps = list(self.components()) + list(other.components())
            return CompositeAnalyzer(*comps)

        elif isinstance(other, Filter):
            comps = list(self.components()) + [other]
            return CompositeAnalyzer(*comps)

        else:
            raise CompositionError("Cannot compose %r and %r" % (self, other))

    def __getitem__(self, i: int):
        if i == 0:
            return self._tokenizer
        else:
            return self._filters[i - 1]

    def __len__(self):
        return len(self._filters) + 1

    def is_token_start(self, s: str, at: int) -> bool:
        return self._tokenizer.is_token_start(s, at)

    def __call__(self, value: str, tokenize=True, no_morph: bool=False, **kwargs
                 ) -> Iterable[Token]:
        # Allow filters to change options
        for f in self._filters:
            f.set_options(kwargs)

        # Start with tokenizer
        gen = self._tokenizer(value, tokenize=tokenize, **kwargs)

        # Add the filters
        for f in self._filters:
            # Ignore this filter if we were called with no_morph=True and the
            # filter has is_morph=True
            if no_morph and getattr(f, "is_morph"):
                continue

            gen = f.filter(gen)

        return gen

    def add(self, f: Filter):
        if isinstance(f, CompositeAnalyzer):
            self.extend(f.components())
        self._filters.append(f)

    def extend(self, filters: Iterable[Filter]):
        for c in filters:
            for cc in c.components():
                self.add(cc)

    def components(self) -> Iterable[Analyzer]:
        yield self._tokenizer
        for f in self._filters:
            yield f

    def has_morph(self) -> bool:
        return any(getattr(c, "is_morph") for c in self._filters)

    def clean(self):
        self._tokenizer.clean()
        for f in self._filters:
            if hasattr(f, "clean"):
                f.clean()

