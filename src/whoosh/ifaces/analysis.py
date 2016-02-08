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
from typing import Iterable, List, Sequence

from whoosh.compat import iteritems, text_type


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


def entoken(textstream: Iterable[text_type], positions: bool=False,
            chars: bool=False, start_pos: int=0, start_char: int=0,
            **kwargs) -> 'Iterable[Token]':
    """
    Takes a sequence of unicode strings and yields a series of Token objects
    (actually the same Token object over and over, for performance reasons),
    with the attributes filled in with reasonable values (for example, if
    ``positions`` or ``chars`` is True, the function assumes each token was
    separated by one space).

    :param textstream: an iterable of unicode strings.
    :param positions: whether to store positions on the tokens.
    :param chars: whether to store character extents on the tokens.
    :param start_pos: the position to start with when numbering tokens.
    :param start_char: the index to start with when recording chars.
    :param kwargs: passed as keyword arguments to the Token object.
    """

    pos = start_pos
    char = start_char
    t = Token(positions=positions, chars=chars, **kwargs)

    for text in textstream:
        t.text = text

        if positions:
            t.pos = pos
            pos += 1

        if chars:
            t.startchar = char
            char += len(text)
            t.endchar = char

        yield t


# Token object

class Token(object):
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

    def __init__(self, positions: bool=False, chars: bool=False,
                 payloads: bool=False, removestops: bool=True, mode: str='',
                 no_morph: bool=False, field_boost: float=1.0, **kwargs):
        """
        :param positions: Whether tokens should have the token position in the
            'pos' attribute.
        :param chars: Whether tokens should have character offsets in the
            'startchar' and 'endchar' attributes.
        :param removestops: whether to remove stop words from the stream (if
            the tokens pass through a stop filter).
        :param mode: contains a string describing the purpose for which the
            analyzer is being called, i.e. 'index' or 'query'.
        :param no_morph: whether to skip filters that morphologically change
            tokens (e.g. stemming).
        """

        self.positions = positions
        self.chars = chars
        self.payloads = payloads
        self.stopped = False
        self.field_boost = field_boost
        self.boost = 1.0
        self.removestops = removestops
        self.mode = mode
        self.no_morph = no_morph

        self.text = u''
        self.pos = -1
        self.startchar = -1
        self.endchar = -1
        self.payload = b''

        self.__dict__.update(kwargs)

    def __repr__(self):
        parms = ", ".join("%s=%r" % (name, value)
                          for name, value in iteritems(self.__dict__))
        return "%s(%s)" % (self.__class__.__name__, parms)

    def copy(self) -> 'Token':
        # This is faster than using the copy module
        return Token(**self.__dict__)


# Base class

class Analyzer(object):
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
    def __call__(self, value: text_type, **kwargs):
        raise NotImplementedError

    def components(self) -> 'Iterable[Analyzer]':
        yield self

    def has_morph(self) -> bool:
        return self.is_morph

    def clean(self):
        pass


class Tokenizer(Analyzer):
    @abstractmethod
    def __call__(self, value: text_type, **kwargs):
        raise NotImplementedError

    def __or__(self, other: 'Filter') -> 'CompositeAnalyzer':
        if isinstance(other, FilterChain):
            return CompositeAnalyzer(self, *other.components())

        elif isinstance(other, Filter):
            return CompositeAnalyzer(self, other)

        else:
            raise CompositionError("Cannot compose %r and %r" % (self, other))


class Filter(Analyzer):
    def __call__(self, value: text_type, **kwargs):
        raise TypeError("Can't call a Filter, use the filter method")

    def __or__(self, other: 'Filter') -> 'CompositeAnalyzer':
        if not isinstance(other, Filter):
            raise CompositionError("Cannot compose %r and %r" % (self, other))
        return FilterChain(self, other)

    @abstractmethod
    def filter(self, tokens: Iterable[Token]) -> Iterable[Token]:
        raise NotImplementedError(self.__class__)


class FilterChain(Filter):
    def __init__(self, *filters: Sequence[Filter]):
        self._filters = filters

    def __or__(self, other: 'Filter') -> 'CompositeAnalyzer':
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
    def __init__(self, tokenizer: Tokenizer, *filters: Sequence[Filter]):
        self._tokenizer = tokenizer
        self._filters = []  # type: List[Filter]
        self.extend(filters)

    def __eq__(self, other):
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

    def __call__(self, value: text_type, no_morph: bool=False,
                 **kwargs) -> Iterable[Token]:
        # Start with tokenizer
        gen = self._tokenizer(value, **kwargs)

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

