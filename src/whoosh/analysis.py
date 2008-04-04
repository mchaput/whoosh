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

import re

from whoosh.support.porter import stem

try:
    # Use C extensions for splitting and stemming from zopyx,
    # if available
    from zopyx.txng3 import stemmer #, splitter
    StemFilter = stemmer.Stemmer("english").stem
    #SimpleTokenizer = splitter.Splitter().split
except ImportError:
    pass

def SimpleTokenizer(text):
    exp = re.compile(r"\W|_", re.UNICODE)
    for w in exp.split(text):
        if w and len(w) > 0:
            yield w

_STOP_WORDS = ["the", "to", "of", "a", "and", "is", "in", "this",
               "you", "for", "be", "on", "or", "will", "if", "can", "are",
               "that", "by", "with", "it", "as", "from", "an", "when",
               "not", "may", "tbd", "yet"]

_split_exp = re.compile(r"(\s|,)+")
def ListTokenizer(text):
    for w in _split_exp.split(text):
        if w and len(w) > 0:
            yield w

class NgramTokenizer(object):
    def __init__(self, min, max):
        self.min = min
        self.max = max
        
        self.normalizeExp = re.compile(r"(\W|_)+")
    
    def __call__(self, text):
        text = "".join([" ", self.normalizeExp.sub(" ", text).strip(), " "])
        inLen = len(text)
        
        for gram in xrange(self.min, self.max + 1):
            pos = 0
            limit = inLen - gram + 1
            for pos in xrange(0, limit):
                yield text[pos:pos + gram]

class StemFilter(object):
    def __init__(self):
        self.cache = {}
    
    def clear(self):
        self.cache.clear()
    
    def __call__(self, ws):
        cache = self.cache
        for w in ws:
            if w in cache:
                yield cache[w]
            else:
                s = stem(w)
                cache[w] = s
                yield s

_camel_exp = re.compile("[A-Z][a-z]*|[a-z]+|[0-9]+")
def CamelFilter(ws):
    for w in ws:
        for match in _camel_exp.finditer(w):
            yield match.group(0)

class StopFilter(object):
    def __init__(self, stoplist):
        self.stops = frozenset(stoplist)
    
    def __call__(self, ws):
        for w in ws:
            if len(w) > 2 and not w in self.stops:
                yield w

def LowerCaseFilter(ws):
    for w in ws:
        yield w.lower()


class Analyzer(object):
    def filter(self, ws):
        return ws
    
    def words(self, text):
        return self.filter(SimpleTokenizer(text))
    
    def position_words(self, text, start_pos = 0):
        for i, w in enumerate(self.words(text)):
            yield (start_pos + i, w)

class SimpleAnalyzer(Analyzer):
    def words(self, text):
        return SimpleTokenizer(text)

class KeywordAnalyzer(Analyzer):
    def words(self, text):
        return ListTokenizer(text)

class LCAnalyzer(Analyzer):
    def filter(self, ws):
        return LowerCaseFilter(ws)

class StopAnalyzer(Analyzer):
    def __init__(self, stopwords = _STOP_WORDS):
        self.stopwords = stopwords
        self.stopper = StopFilter(stopwords)
    
    def filter(self, ws):
        return self.stopper(LowerCaseFilter(ws))
    
class StemmingAnalyzer(Analyzer):
    def __init__(self, stop_words = _STOP_WORDS):
        self.stemmer = StemFilter()
        self.stopper = StopFilter(stop_words)
    
    def clear(self):
        self.stemmer.clear()
    
    def filter(self, ws):
        return self.stemmer(list(LowerCaseFilter(CamelFilter(self.stopper(ws)))))
    
class IDAnalyzer(Analyzer):
    def __init__(self):
        self.tokenizer = None
        self.filters = []
    
    def words(self, text):
        yield text

class NgramAnalyzer(Analyzer):
    def __init__(self, min = 3, max = None):
        if max is None: max = min
        self.min = min
        self.max = max
        
        self.tokenizer = NgramTokenizer(min, max)
    
    def words(self, text):
        for w in self.filter(self.tokenizer(text)):
            yield w
    
    def filter(self, ws):
        return LowerCaseFilter(ws)










