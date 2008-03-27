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
def StemFilter(ws):
    for w in ws:
        yield stem(w)

def SimpleTokenizer(text):
    exp = re.compile(r"\W|_", re.UNICODE)
    for w in exp.split(text):
        if w and len(w) > 0:
            yield w

try:
    # Use C extensions for splitting and stemming from zopyx,
    # if available
    from zopyx.txng3 import stemmer #, splitter
    StemFilter = stemmer.Stemmer("english").stem
    #SimpleTokenizer = splitter.Splitter().split
except ImportError:
    pass
    

_defaultStopWords = ["the", "to", "of", "a", "and", "is", "in", "this",
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


_camel_exp = re.compile("[A-Z][a-z]*|[a-z]+|[0-9]+")
def CamelFilter(ws):
    for w in ws:
        for match in _camel_exp.finditer(w):
            yield match.group(0)

class StopFilter(object):
    def __init__(self, stop_words):
        self.stops = frozenset(stop_words)
    
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
    def __init__(self, stop_words = _defaultStopWords):
        self.stopper = StopFilter(stop_words)
    
    def filter(self, ws):
        return self.stopper(ws)
    
class StemmingAnalyzer(Analyzer):
    def __init__(self, stop_words = _defaultStopWords):
        self.stopper = StopFilter(stop_words)
    
    def filter(self, ws):
        return StemFilter(list(LowerCaseFilter(CamelFilter(self.stopper(ws)))))
    
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

    



