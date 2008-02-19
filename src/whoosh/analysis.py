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

try:
    # Use C extensions for splitting and stemming from zopyx,
    # if available
    from zopyx.txng3 import stemmer, splitter
    StemFilter = stemmer.Stemmer("english").stem
    SimpleTokenizer = splitter.Splitter().split
    
except ImportError:
    # Use pure-Python versions.
    from whoosh.support.porter import stem
    def StemFilter(self, ws):
        for w in ws:
            yield stem(w)
    
    def SimpleTokenizer(text):
        exp = re.compile(r"\W|_")
        for w in exp.split(text):
            if w and len(w) > 0:
                yield w.lower()

_defaultStopWords = ["the", "to", "of", "a", "and", "is", "in", "this",
                     "you", "for", "be", "on", "or", "will", "if", "can", "are",
                     "that", "by", "with", "it", "as", "from", "an", "when",
                     "not", "may", "use", "tbd"]

class StopFilter(object):
    def __init__(self, stop_words):
        self.stops = set(stop_words)
    
    def __call__(self, ws):
        for w in ws:
            if len(w) > 2 and not w in self.stops:
                yield w


class NgramTokenizer(object):
    def __init__(self, min, max):
        self.min = min
        self.max = max
        
        self.normalizeExp = re.compile(r"\W+")
    
    def __call__(self, text):
        text = "".join([" ", self.normalizeExp.sub(" ", text).strip(), " "])
        inLen = len(text)
        
        for gramSize in xrange(self.min, self.max + 1):
            pos = 0
            limit = inLen - gramSize + 1
            
            for pos in xrange(0, limit):
                yield text[pos:pos + gramSize]


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
        
class StopAnalyzer(Analyzer):
    def __init__(self, stop_words = _defaultStopWords):
        self.stopper = StopFilter(stop_words)
    
    def filter(self, ws):
        return self.stopper(ws)
    
class StemmingAnalyzer(Analyzer):
    def __init__(self, stop_words = _defaultStopWords):
        self.stopper = StopFilter(stop_words)
    
    def filter(self, ws):
        return StemFilter(list(self.stopper(ws)))
    
class IDAnalyzer(Analyzer):
    def __init__(self):
        self.tokenizer = None
        self.filters = []
    
    def words(self, text):
        yield text

class NgramAnalyzer(Analyzer):
    def __init__(self, min = 3, max = 4):
        self.tokenizer = NgramTokenizer(min, max)
    
    def words(self, text):
        for w in self.filter(self.tokenizer(text)):
            yield w
    
    def filter(self, ws):
        return LowerCaseFilter(ws)

    



