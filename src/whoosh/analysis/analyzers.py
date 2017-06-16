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

from typing import Callable, Sequence, Set, Union

from whoosh.ifaces import analysis
from whoosh.analysis import tokenizers, filters, morph, intraword
from whoosh.lang.porter import stem


# Type aliases

StrSet = Union[Sequence[str], Set[str]]


# Analyzers

class IDAnalyzer(analysis.CompositeAnalyzer):
    """
    Returns the input whole as a single token (with optional lowercasing).
    """

    def __init__(self, lowercase: bool=False):
        super(IDAnalyzer, self).__init__(tokenizers.IDTokenizer())
        if lowercase:
            self.add(filters.LowercaseFilter())


class KeywordAnalyzer(analysis.CompositeAnalyzer):
    """
    Parses whitespace- or comma-separated tokens.

    >>> ana = KeywordAnalyzer()
    >>> [token.text for token in ana("Hello there, this is a TEST")]
    ["Hello", "there,", "this", "is", "a", "TEST"]

    """

    def __init__(self, lowercase: bool=False, commas: bool=False):
        """
        :param lowercase: whether to lowercase the tokens.
        :param commas: if True, items are separated by commas rather than
            whitespace.
        """

        tk = (tokenizers.CommaSeparatedTokenizer() if commas else
              tokenizers.SpaceSeparatedTokenizer())
        super(KeywordAnalyzer, self).__init__(tk)
        if lowercase:
            self.add(filters.LowercaseFilter())


class RegexAnalyzer(analysis.CompositeAnalyzer):
    """
    Deprecated, just use a RegexTokenizer directly.
    """

    def __init__(self, expression: str=r"\w+(\.?\w+)*", gaps: bool=False):
        super(RegexAnalyzer, self).__init__(
            tokenizers.RegexTokenizer(expression=expression, gaps=gaps),
        )


class SimpleAnalyzer(analysis.CompositeAnalyzer):
    """
    Composes a RegexTokenizer with a LowercaseFilter.

    >>> ana = SimpleAnalyzer()
    >>> [token.text for token in ana("Hello there, this is a TEST")]
    ["hello", "there", "this", "is", "a", "test"]
    """

    def __init__(self, expression: str=tokenizers.default_pattern,
                 gaps: bool=False):
        """
        :param expression: The regular expression pattern to use to extract
            tokens.
        :param gaps: If True, the tokenizer *splits* on the expression, rather
            than matching on the expression.
        """

        super(SimpleAnalyzer, self).__init__(
            tokenizers.RegexTokenizer(expression=expression, gaps=gaps),
            filters.LowercaseFilter()
        )


class StandardAnalyzer(analysis.CompositeAnalyzer):
    """
    Composes a RegexTokenizer with a LowercaseFilter and optional
    StopFilter.

    >>> ana = StandardAnalyzer()
    >>> [token.text for token in ana("Testing is testing and testing")]
    ["testing", "testing", "testing"]
    """

    def __init__(self, expression: str=tokenizers.default_pattern,
                 stoplist: StrSet=filters.STOP_WORDS,
                 minsize: int=2, maxsize: int=None, gaps: bool=False):
        """
        :param expression: The regular expression pattern to use to extract tokens.
        :param stoplist: A list of stop words. Set this to None to disable
            the stop word filter.
        :param minsize: Words smaller than this are removed from the stream.
        :param maxsize: Words longer that this are removed from the stream.
        :param gaps: If True, the tokenizer *splits* on the expression, rather
            than matching on the expression.
        """

        super(StandardAnalyzer, self).__init__(
            tokenizers.RegexTokenizer(expression=expression, gaps=gaps),
            filters.LowercaseFilter(),
        )
        if stoplist is not None:
            self.add(filters.StopFilter(stoplist=stoplist, minsize=minsize,
                                        maxsize=maxsize))


class StemmingAnalyzer(analysis.CompositeAnalyzer):
    """
    Composes a RegexTokenizer with a lower case filter, an optional stop
    filter, and a stemming filter.

    >>> ana = StemmingAnalyzer()
    >>> [token.text for token in ana("Testing is testing and testing")]
    ["test", "test", "test"]
    """

    def __init__(self, expression: str=tokenizers.default_pattern,
                 stoplist: StrSet=filters.STOP_WORDS,
                 minsize: int=2, maxsize: int=None, gaps: bool=False,
                 stemfn: Callable[[str], str]=stem,
                 ignore: StrSet=None, cachesize: int=50000):
        """
        :param expression: The regular expression pattern to use to extract
            tokens.
        :param stoplist: A list of stop words. Set this to None to disable
            the stop word filter.
        :param minsize: Words smaller than this are removed from the stream.
        :param maxsize: Words longer that this are removed from the stream.
        :param gaps: If True, the tokenizer *splits* on the expression, rather
            than matching on the expression.
        :param ignore: a set of words to not stem.
        :param cachesize: the maximum number of stemmed words to cache. The
            larger this number, the faster stemming will be but the more memory
            it will use. Use None for no cache, or -1 for an unbounded cache.
        """

        super(StemmingAnalyzer, self).__init__(
            tokenizers.RegexTokenizer(expression=expression, gaps=gaps),
            filters.LowercaseFilter()
        )
        if stoplist is not None:
            self.add(filters.StopFilter(stoplist=stoplist, minsize=minsize,
                                        maxsize=maxsize))
        self.add(morph.StemFilter(stemfn=stemfn, ignore=ignore,
                                  cachesize=cachesize))


class FancyAnalyzer(analysis.CompositeAnalyzer):
    """
    Composes a RegexTokenizer with an IntraWordFilter, LowercaseFilter, and
    StopFilter.

    >>> ana = FancyAnalyzer()
    >>> [token.text for token in ana("Should I call getInt or get_real?")]
    ["should", "call", "getInt", "get", "int", "get_real", "get", "real"]
    """

    def __init__(self, expression: str=r"\s+",
                 stoplist: StrSet=filters.STOP_WORDS,
                 minsize: int=2, maxsize: int=None, gaps: bool=False,
                 splitwords: bool=True, splitnums: bool=True,
                 mergewords: bool=False, mergenums: bool=False):
        """
        :param expression: The regular expression pattern to use to extract
            tokens.
        :param stoplist: A list of stop words. Set this to None to disable
            the stop word filter.
        :param minsize: Words smaller than this are removed from the stream.
        :param maxsize: Words longer that this are removed from the stream.
        :param gaps: If True, the tokenizer *splits* on the expression, rather
            than matching on the expression.
        """

        super(FancyAnalyzer, self).__init__(
            tokenizers.RegexTokenizer(expression=expression, gaps=gaps),
            intraword.IntraWordFilter(splitwords=splitwords,
                                      splitnums=splitnums,
                                      mergewords=mergewords,
                                      mergenums=mergenums),
            filters.LowercaseFilter(),
            filters.StopFilter(stoplist=stoplist, minsize=minsize)
        )


class LanguageAnalyzer(analysis.CompositeAnalyzer):
    """
    Configures a simple analyzer for the given language, with a
    LowercaseFilter, StopFilter, and StemFilter.

    >>> ana = LanguageAnalyzer("es")
    >>> [token.text for token in ana("Por el mar corren las liebres")]
    ['mar', 'corr', 'liebr']

    The list of available languages is in `whoosh.lang.languages`.
    You can use :func:`whoosh.lang.has_stemmer` and
    :func:`whoosh.lang.has_stopwords` to check if a given language has a
    stemming function and/or stop word list available.
    """

    def __init__(self, lang: str, expression: str=tokenizers.default_pattern,
                 gaps: bool=False, cachesize: int=50000):
        """
        :param expression: The regular expression pattern to use to extract
            tokens.
        :param gaps: If True, the tokenizer *splits* on the expression, rather
            than matching on the expression.
        :param cachesize: the maximum number of stemmed words to cache. The
            larger this number, the faster stemming will be but the more memory
            it will use.
        """

        from whoosh.lang import NoStemmer, NoStopWords

        super(LanguageAnalyzer, self).__init__(
            tokenizers.RegexTokenizer(expression=expression, gaps=gaps),
            filters.LowercaseFilter(),
        )

        # Add a stop word filter
        try:
            self.add(filters.StopFilter(lang=lang))
        except NoStopWords:
            pass

        # Add a stemming filter
        try:
            self.add(morph.StemFilter(lang=lang, cachesize=cachesize))
        except NoStemmer:
            pass

