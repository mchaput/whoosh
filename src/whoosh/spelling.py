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
This module contains functions/classes using a Whoosh index
as a backend for a spell-checking engine.
"""

from collections import defaultdict

import analysis, fields, index, query, searching, writing
from support.levenshtein import relative, distance
from util import UtilityIndex

class SpellChecker(UtilityIndex):
    """
    Implements a spell-checking engine with a Whoosh-based backend
    dictionary. This class is based on the Lucene spell-checker
    contributed code.
    """
    
    def __init__(self, storage, indexname = "SPELL",
                 booststart = 2.0, boostend = 1.0,
                 mingram = 3, maxgram = 4,
                 minscore = 0.5):
        """
        storage is a Whoosh storage object in which to create
        the spelling dictionary index.  indexname is the name
        of the sub-index; the default is "SPELL". booststart is
        a floating point value describing how much to boost matches
        of the first N-gram (the beginning of the word); default is
        2.0. boostend is how much to boost matches of the last N-gram
        (the end of the word); default is 1.0 (no boost). mingram is
        the shortest N-gram to store. maxgram is the longest N-gram to
        store. minscore is the minimum score matches must achieve to
        be returned; default is 0.5.
        """
        
        self.storage = storage
        self.indexname = indexname
        
        self._index = None
        
        self.booststart = booststart
        self.boostend = boostend
        self.mingram = mingram
        self.maxgram = maxgram
    
    def schema(self):
        fls = [fields.StoredField("word"),
               fields.StoredField("score")]
        for size in xrange(self.mingram, self.maxgram + 1):
            fls.extend([fields.IDField("start%s" % size, analysis.SimpleAnalyzer()),
                        fields.IDField("end%s" % size, analysis.SimpleAnalyzer()),
                        fields.FrequencyField("gram%s" % size, analysis.SimpleAnalyzer())
                        ])
        return index.Schema(*fls)
    
    def suggest(self, text, number = 3, usescores = False):
        """
        Suggests alternate spellings for a word. text is the text of the word
        to check. number is the number of suggestions to return. fieldname.
        if morepopular is True, the suggestions are computed based on their frequency
        in the source index, rather than distance from the original text.
        """
        
        grams = defaultdict(list)
        for size in xrange(self.mingram, self.maxgram + 1):
            key = "gram%s" % size
            nga = analysis.NgramAnalyzer(size)
            for gram in nga.words(text):
                grams[key].append(gram)
        
        queries = []
        for size in xrange(self.mingram, min(self.maxgram + 1, len(text))):
            key = "gram%s" % size
            gramlist = grams[key]
            queries.append(query.Term("start%s" % size, gramlist[0], boost = self.booststart))
            queries.append(query.Term("end%s" % size, gramlist[-1], boost = self.boostend))
            for gram in gramlist:
                queries.append(query.Term(key, gram))
        
        q = query.Or(queries)
        ix = self.index()
        
        s = searching.Searcher(ix)
        try:
            results = s.search(q)
            
            if len(results) > number*2:
                fieldlist = results[:len(results)//2]
            
            suggestions = []
            for fields in fieldlist:
                word = fields["word"]
                if word == text: continue
                suggestions.append((word, fields["score"]))
            
            if usescores:
                def keyfn(a):
                    return 0 - (1/distance(text, a[0])) * a[1]
            else:
                def keyfn(a):
                    return distance(text, a[0])
            
            suggestions.sort(key = keyfn)
        finally:
            s.close()
        
        return [word for word, _ in suggestions[:number]]
        
    def add_field(self, ix, fieldname):
        """
        Adds the terms in a field from another index to the backend dictionary.
        term_reader is a term reader object for the source index. fieldname is the
        name of the field in the source index from which to load the terms.
        """
        
        tr = ix.term_reader()
        try:
            self.add_words(tr.field_words(fieldname))
        finally:
            tr.close()
    
    def add_words(self, ws, score = 0):
        """
        Adds words to the backend dictionary from an iterable. This method
        takes a list of words. score is the score to use for all words
        (default is 0). You can use this if you are planning to use the
        'usescores' keyword argument of the suggestions() method. However,
        in that case, you might want to use add_scored_words() instead.
        """
        self.add_ranked_words((w, 0) for w in ws)
    
    def add_scored_words(self, ws):
        """
        Adds words to the backend dictionary from a sequence of
        (word, score) tuples. You can use this if you are planning to
        use the 'usescores' keyword argument of the suggestions() method.
        Otherwise, just use add_words().
        """
        
        writer = writing.IndexWriter(self.index())
        for text, score in ws.iteritems():
            if text.isalpha():
                fields = {"word": text, "score": score}
                for size in xrange(self.mingram, self.maxgram + 1):
                    nga = analysis.NgramAnalyzer(size)
                    gramlist = list(nga.words(text))
                    if len(gramlist) > 0:
                        fields["start%s" % size] = gramlist[0]
                        fields["end%s" % size] = gramlist[-1]
                        fields["gram%s" % size] = " ".join(gramlist)
                writer.add_document(**fields)
        writer.close()
    
if __name__ == '__main__':
    pass
    
    

