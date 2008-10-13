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
This module contains functions and classes for turning a piece of
text into an indexable stream of words.

This module defines three types of functions/classes:

Tokenizers: callables that take a string and yield tokenized "words".

Filters: callables that take a "word" generator and filter it.

Analyzers: classes that implement Analyzer.words() and
Analyzer.positioned_words(). Analyzers package up a tokenizer and
zero or more filters into a high-level interface used by other code.
When you create an index, you specify an Analyzer for each field.
"""

import re

from lang.porter import stem

# Default list of stop words (words so common it's usually
# wasteful to index them). This list is used by the StopFilter
# class, which allows you to supply an optional list to override
# this one.

STOP_WORDS = ["the", "to", "of", "a", "and", "is", "in", "this",
              "you", "for", "be", "on", "or", "will", "if", "can", "are",
              "that", "by", "with", "it", "as", "from", "an", "when",
              "not", "may", "tbd", "yet"]

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

def SimpleTokenizer(text):
    """
    Uses a regular expression to pull words out of text.
    """
    
    exp = re.compile(r"\W", re.UNICODE)
    for w in exp.split(text):
        if w and len(w) > 0:
            yield w

_space_split_exp = re.compile(r"(\s|,)+")
def ListTokenizer(text):
    """
    Instead of splitting words by ALL punctuation and whitespace, this
    tokenizer only splits by whitespace and commas. This is useful for
    lists of IDs.
    """
    
    for w in _space_split_exp.split(text):
        if w and len(w) > 0:
            yield w
            
_comma_split_exp = re.compile("\s*,\s*")
def CommaTokenizer(text):
    """
    Instead of splitting words by ALL punctuation and whitespace, this
    tokenizer only splits by commas. This is useful for lists of tokens
    that might contain spaces.
    """
    
    for w in _comma_split_exp.split(text):
        if w and len(w) > 0:
            yield w

class NgramTokenizer(object):
    """
    Splits input text into Ngrams instead of words.
    """
    
    def __init__(self, min, max, normalize = r"\W+"):
        """
        min is the minimum length of the Ngrams to output, max is the
        maximum length to output. normalize is a regular expression that
        is globally replaced by spaces (used to eliminate punctuation).
        """
        
        self.min = min
        self.max = max
        
        self.normalize = normalize
        if normalize:
            self.normalize_exp = re.compile(normalize)
    
    def __call__(self, text):
        if self.normalize:
            text = self.normalize_exp.sub(" ", " %s " % text).strip()
        return gram(text, self.min, self.max)

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
    
    def __call__(self, ws):
        cache = self.cache
        ignores = self.ignores
        
        for w in ws:
            if w in ignores:
                yield w
            elif w in cache:
                yield cache[w]
            else:
                s = stem(w)
                cache[w] = s
                yield s

_camel_exp = re.compile("[A-Z][a-z]*|[a-z]+|[0-9]+")
def CamelFilter(ws):
    """
    Splits CamelCased words into multiple words. For example,
    splits "getProcessedToken" into "get", "Processed", and "Token".
    """
    
    for w in ws:
        yield w
        for match in _camel_exp.finditer(w):
            sw = match.group(0)
            if sw != w:
                yield sw

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
    
    def __call__(self, ws):
        stoplist = self.stops
        minsize = self.min
        
        for w in ws:
            if len(w) > minsize and not w in stoplist:
                yield w

def LowerCaseFilter(ws):
    """
    Lowercases (using str.lower()) words in the stream.
    """
    
    for w in ws:
        yield w.lower()

# Analyzers

class Analyzer(object):
    """
    Base class for "analyzers" -- classes that package up
    a tokenizer and zero or more filters to provide higher-level
    functionality.
    """
    
    def filter(self, ws):
        """
        If a derived class accepts the default tokenizer
        (SimpleTokenizer) used by the base class, it only needs
        to override this method. Otherwise they can override
        Analyzer.words() instead for complete control.
        """
        
        return ws
    
    def words(self, text):
        """
        Takes the text to index and yields a series of terms.
        """
        
        return self.filter(SimpleTokenizer(text))
    
    def position_words(self, text, start_pos = 0):
        """
        Takes the text to index and yields a series of (position, term)
        tuples. The base method simply enumerates the terms from the
        words() method, but if you want something more complex you can
        override this method.
        start_pos is the base position to start numbering at.
        """
        
        for i, w in enumerate(self.words(text)):
            yield (start_pos + i, w)

class SimpleAnalyzer(Analyzer):
    """
    Simple analyzer: does nothing but return the result of the
    SimpleTokenizer.
    """
    
    def words(self, text):
        return SimpleTokenizer(text)

class IDAnalyzer(Analyzer):
    """
    Does no tokenization or analysis of the text at all: simply passes it
    through as a single term.
    """
    
    def __init__(self):
        self.tokenizer = None
        self.filters = []
    
    def words(self, text):
        yield text

class KeywordAnalyzer(Analyzer):
    """
    Simple analyzer: does nothing but return the result of the
    ListTokenizer.
    """
    
    def words(self, text):
        return ListTokenizer(text)

class CommaAnalyzer(Analyzer):
    """
    Simple analyzer: does nothing but return the result of the
    CommaTokenizer.
    """
    
    def words(self, text):
        return CommaTokenizer(text)

class LCAnalyzer(Analyzer):
    """
    Filters SimpleTokenizer through the LowerCaseFilter.
    """
    
    def filter(self, ws):
        return LowerCaseFilter(ws)

class StopAnalyzer(Analyzer):
    """
    Filters SimpleTokenizer through LowerCaseFilter and StopFilter.
    """
    
    def __init__(self, stopwords = None):
        """
        stopwords is a sequence of words not to index; the default
        is a list of common words.
        """
        
        self.stopwords = stopwords
        self.stopper = StopFilter(stopwords)
    
    def filter(self, ws):
        return self.stopper(LowerCaseFilter(ws))
    
class StemmingAnalyzer(Analyzer):
    """
    Filters SimpleTokenizer through LowerCaseFilter, StopFilter,
    and StemFilter.
    """
    
    def __init__(self, stopwords = None):
        """
        stopwords is a sequence of words not to index; the default
        is a list of common words.
        """
        
        self.stemmer = StemFilter()
        self.stopper = StopFilter(stopwords)
    
    def clear(self):
        """
        Releases memory used by the stem cache.
        """
        
        self.stemmer.clear()
    
    def filter(self, ws):
        return self.stemmer(self.stopper(LowerCaseFilter(CamelFilter(ws))))
    
class NgramAnalyzer(Analyzer):
    """
    Converts a string into a stream of (lower-case) N-grams
    instead of words.
    """
    
    def __init__(self, min = 3, max = None, normalize = r"\W+"):
        """
        min is the minimum length of the Ngrams to output, max is the
        maximum length to output. normalize is a regular expression that
        is globally replaced by spaces (used to eliminate punctuation).
        """
        
        if max is None: max = min
        assert type(min) == type(max) == int
        self.min = min
        self.max = max
        
        self.tokenizer = NgramTokenizer(min, max, normalize = normalize)
    
    def words(self, text):
        for w in self.filter(self.tokenizer(text)):
            yield w
    
    def filter(self, ws):
        return LowerCaseFilter(ws)


if __name__ == '__main__':
    import time
    import index
    from collections import defaultdict
    
    st = time.time()
    map = defaultdict(list)
    ix = index.open_dir("../index")
    tr = ix.term_reader()
    
    c = 0
    for t in tr.field_words("content"):
        map[stem(t)].append(t)
        c += 1
    
    print time.time() - st
    print "\n".join("%r %r" % (stm, lst) for stm, lst in map.iteritems())







