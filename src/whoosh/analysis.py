#===============================================================================
# Copyright 2007 Matt Chaput
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
TFunctions and classes for turning a piece of text into an
indexable stream of words.

This module defines three types of functions/classes:

Tokenizers: callables that take a string and yield tokenized "words".

Filters: callables that take a "word" generator and filter it.

Analyzers: a callable that combines a tokenizer and filters for
convenience.
"""

import re

from whoosh.lang.porter import stem

# Default list of stop words (words so common it's usually
# wasteful to index them). This list is used by the StopFilter
# class, which allows you to supply an optional list to override
# this one.

STOP_WORDS = ["the", "to", "of", "a", "and", "is", "in", "this",
              "you", "for", "be", "on", "or", "will", "if", "can", "are",
              "that", "by", "with", "it", "as", "from", "an", "when",
              "not", "may", "tbd", "yet"]

# Token object

class Token(object):
    __slots__ = ("positions", "chars",
                 "orig", "text", "pos", "startchar", "endchar",
                 "stopped")
    
    def __init__(self, positions, chars):
        self.positions = positions
        self.chars = chars
        self.stopped = False

# Support functions

def gram(text, min, max):
    """
    Breaks a text into N-grams. min is the minimum size of the N-grams,
    max is the maximum size. For example, gram("hello", 3, 4) will yield
    ["hel", "ell", "llo", "hell", "ello"]
    """
    
    inLen = len(text)
    for g in xrange(min, max + 1):
        pos = 0
        limit = inLen - g + 1
        for pos in xrange(0, limit):
            yield text[pos:pos + g]

# Tokenizers

def IDTokenizer(value, positions = False, chars = False, start_pos = 0, start_char = 0):
    """
    Returns the entire input string as a single token. For use
    in indexed but untokenized fields, such as a document's path.
    """
    
    t = Token(positions, chars)
    t.orig = t.text = value
    if positions:
        t.pos = start_pos + 1
    if chars:
        t.startchar = start_char
        t.endchar = start_char + len(value)
    yield t
    

class RegexTokenizer(object):
    """
    Uses a regular expression to extract tokens from text.
    """
    
    default_expression = re.compile("\w+", re.UNICODE)
    
    def __init__(self, expression = None):
        self.expression = expression or self.default_expression
        
    def __call__(self, value, positions = False, chars = False,
                 start_pos = 0, start_char = 0):
        t = Token(positions, chars)
        
        for pos, match in enumerate(self.expression.finditer(value)):
            t.orig = t.text = match.group(0)
            t.stopped = False
            if positions:
                t.pos = start_pos + pos
            if chars:
                t.startchar = start_char + match.start()
                t.endchar = start_char + match.end()
            yield t


class SpaceSeparatedTokenizer(RegexTokenizer):
    """
    Splits tokens by whitespace.
    """
    
    default_expression = re.compile("[^ \t\r\n]+")


class CommaSeparatedTokenizer(RegexTokenizer):
    """
    Splits tokens by commas with optional whitespace.
    """
    
    default_expression = re.compile("[^,]+")
    
    def __call__(self, value, positions = False, chars = False,
                 start_pos = 0, start_char = 0):
        t = Token(positions, chars)
        
        for pos, match in enumerate(self.expression.finditer(value)):
            t.orig = t.text = match.group(0).strip()
            t.stopped = False
            if positions:
                t.pos = start_pos + pos
            if chars:
                t.startchar = start_char + match.start()
                t.endchar = start_char + match.end()
            yield t


class NgramTokenizer(object):
    """
    Splits input text into Ngrams instead of words.
    """
    
    def __init__(self, minsize, maxsize = None):
        """
        min is the minimum length of the Ngrams to output, max is the
        maximum length to output. normalize is a regular expression that
        is globally replaced by spaces (used to eliminate punctuation).
        """
        
        self.min = minsize
        self.max = maxsize or minsize
        
    def __call__(self, value, positions = False, chars = False,
                 start_pos = 0, start_char = 0):
        inLen = len(value)
        t = Token(positions, chars)
        
        pos = start_pos
        for size in xrange(self.min, self.max + 1):
            limit = inLen - size + 1
            for start in xrange(0, limit):
                end = start + size
                t.orig = t.text = value[start : end]
                t.stopped = False
                if positions:
                    t.pos = pos
                if chars:
                    t.startchar = start_char + start
                    t.endchar = start_char + end
                yield t
                pos += 1

# Filters

class StemFilter(object):
    """
    Stems (removes suffixes from) words using the Porter stemming algorithm.
    Stemming attempts to reduce multiple forms of the same root word (for
    example, "rendering", "renders", "rendered", etc.) to a single word in
    the index.
    """
    
    def __init__(self, ignore = None):
        """
        ignore is a sequence of words to avoid stemming; the default
        is to stem all words.
        """
        
        self.cache = {}
        if ignore is None:
            self.ignores = frozenset()
        else:
            self.ignores = frozenset(ignore)
    
    def clear(self):
        self.cache.clear()
    
    def __call__(self, tokens):
        cache = self.cache
        ignores = self.ignores
        
        for t in tokens:
            if t.stopped:
                yield t
                continue
            
            text = t.text
            if text in ignores:
                yield t
            elif text in cache:
                t.text = cache[text]
                yield t
            else:
                t.text = s = stem(text)
                cache[text] = s
                yield s


_camel_exp = re.compile("[A-Z][a-z]*|[a-z]+|[0-9]+")
def CamelFilter(tokens):
    """
    Splits CamelCased words into multiple words. For example,
    the string "getProcessedToken" yields tokens
    "getProcessedToken", "get", "Processed", and "Token".
    
    Obviously this filter needs to precede LowerCaseFilter in a filter
    chain.
    """
    
    for t in tokens:
        yield t
        text = t.text
        
        if text and not text.islower() and not text.isupper() and not text.isdigit():
            chars = t.chars
            if chars:
                oldstart = t.startchar
            
            for match in _camel_exp.finditer(text):
                sub = match.group(0)
                if sub != text:
                    t.text = sub
                    if chars:
                        t.startchar = oldstart + match.start()
                        t.endchar = oldstart + match.end()
                    yield t


class StopFilter(object):
    """
    Removes "stop" words (words too common to index) from
    the stream.
    """

    def __init__(self, stoplist = None, minsize = 2):
        """
        Stoplist is a sequence of words to remove from the stream (this
        is converted to a frozenset); the default is a list of common
        stop words (analysis.STOP_WORDS). minsize is a minimum length
        requirement for any word; the default is 2. Words smaller than
        minsize are removed from the stream.
        """
        
        if stoplist is None:
            stoplist = STOP_WORDS
        self.stops = frozenset(stoplist)
        self.min = minsize
    
    def __call__(self, tokens):
        stoplist = self.stops
        minsize = self.min
        
        for t in tokens:
            text = t.text
            if len(text) < minsize or text in stoplist:
                t.stopped = True
            yield t


def LowerCaseFilter(tokens):
    """
    Lowercases (using .lower()) words in the stream.
    """
    
    for t in tokens:
        t.text = t.text.lower()
        yield t

# Analyzers

class Analyzer(object):
    def __repr__(self):
        return "%s()" % self.__class__.__name__


class IDAnalyzer(Analyzer):
    def __init__(self, strip = True):
        self.strip = strip
    
    def __call__(self, value, **kwargs):
        if self.strip: value = value.strip()
        return IDTokenizer(value, **kwargs)


class SpaceSeparatedAnalyzer(Analyzer):
    def __init__(self):
        self.tokenizer = SpaceSeparatedTokenizer()
    
    def __call__(self, value, **kwargs):
        return self.tokenizer(value, **kwargs)


class CommaSeparatedAnalyzer(Analyzer):
    def __init__(self):
        self.tokenizer = CommaSeparatedTokenizer()
        
    def __call__(self, value, **kwargs):
        return self.tokenizer(value, **kwargs)


class SimpleAnalyzer(Analyzer):
    """
    Uses a RegexTokenizer and applies a LowerCaseFilter.
    """
    
    def __init__(self):
        self.tokenizer = RegexTokenizer()
        
    def __call__(self, value, **kwargs):
        return LowerCaseFilter(self.tokenizer(value, **kwargs))


class StandardAnalyzer(Analyzer):
    """
    Uses a RegexTokenizer (by default) and applies a LowerCaseFilter
    and StopFilter.
    """
    
    def __init__(self, stoplist = None, minsize = 2):
        self.tokenizer = RegexTokenizer()
        self.stopper = StopFilter(stoplist = stoplist, minsize = minsize)
        
    def __call__(self, value, **kwargs):
        return self.stopper(LowerCaseFilter(
                            self.tokenizer(value, **kwargs)))


class FancyAnalyzer(Analyzer):
    """
    Uses a RegexTokenizer (by default) and applies a CamelFilter,
    LowerCaseFilter, and StopFilter.
    """
    
    def __init__(self, stoplist = None, minsize = 2):
        self.tokenizer = RegexTokenizer()
        self.stopper = StopFilter(stoplist = stoplist, minsize = minsize)
        
    def __call__(self, value, **kwargs):
        return self.stopper(LowerCaseFilter(
                            CamelFilter(
                            self.tokenizer(value, **kwargs))))


class NgramAnalyzer(Analyzer):
    """
    Uses an NgramTokenizer and applies a LowerCaseFilter.
    """
    
    def __init__(self, minsize, maxsize = None):
        self.tokenizer = NgramTokenizer(minsize, maxsize = maxsize)
        
    def __call__(self, value, positions = False, chars = False):
        return LowerCaseFilter(self.tokenizer(value,
                                              positions = positions, chars = chars))


if __name__ == '__main__':
    import timeit
    
    fix = """
from whoosh.analysis import CamelFilter, FancyAnalyzer, StandardAnalyzer
d = open("/Volumes/Storage/Development/help/documents/nodes/sop/copy.txt").read()
sa = StandardAnalyzer()
fa = FancyAnalyzer()
"""
    
    t = timeit.Timer("l = [t.text for t in sa(d)]", fix)
    print t.timeit(100)
    
    t = timeit.Timer("l = [t.text for t in fa(d)]", fix)
    print t.timeit(100)







