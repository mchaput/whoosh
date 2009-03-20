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

"""This module contains functions/classes using a Whoosh index
as a backend for a spell-checking engine.
"""

from collections import defaultdict

from whoosh import analysis, fields, query, searching, writing
from whoosh.support.levenshtein import relative, distance

class SpellChecker(object):
    """Implements a spell-checking engine using a search index for the
    backend storage and lookup. This class is based on the Lucene
    contributed spell-checker code.
    
    To use this object::
    
        st = store.FileStorage("spelldict")
        sp = SpellChecker(st)
        
        sp.add_words([u"aardvark", u"manticore", u"zebra", ...])
        # or
        ix = index.open_dir("index")
        sp.add_field(ix, "content")
        
        suggestions = sp.suggest(u"ardvark", number = 2)
    """
    
    def __init__(self, storage, indexname = "SPELL",
                 booststart = 2.0, boostend = 1.0,
                 mingram = 3, maxgram = 4,
                 minscore = 0.5):
        """
        :storage: The storage object in which to create the
            spell-checker's dictionary index.
        :indexname: The name to use for the spell-checker's
            dictionary index. You only need to change this if you
            have multiple spelling indexes in the same storage.
        :booststart: How much to boost matches of the first
            N-gram (the beginning of the word).
        :boostend: How much to boost matches of the last
            N-gram (the end of the word).
        :mingram: The minimum gram length to store.
        :maxgram: The maximum gram length to store.
        :minscore: The minimum score matches much achieve to
            be returned.
        """
        
        self.storage = storage
        self.indexname = indexname
        
        self._index = None
        
        self.booststart = booststart
        self.boostend = boostend
        self.mingram = mingram
        self.maxgram = maxgram
    
    def index(self):
        """Returns the backend index of this object (instantiating it if
        it didn't already exist).
        """
        
        import index
        if not self._index:
            create = not index.exists(self.storage, indexname = self.indexname)
            self._index = index.Index(self.storage, create = create,
                                      schema = self._schema(), indexname = self.indexname)
        return self._index
    
    def _schema(self):
        # Creates a schema given this object's mingram and maxgram attributes.
        
        from fields import Schema, FieldType, Frequency, ID, STORED
        from analysis import SimpleAnalyzer
        
        idtype = ID()
        freqtype = FieldType(Frequency(SimpleAnalyzer()))
        
        fls = [("word", STORED), ("score", STORED)]
        for size in xrange(self.mingram, self.maxgram + 1):
            fls.extend([("start%s" % size, idtype),
                        ("end%s" % size, idtype),
                        ("gram%s" % size, freqtype)])
            
        return Schema(**dict(fls))
    
    def suggest(self, text, number = 3, usescores = False):
        """Returns a list of suggested alternative spellings of 'text'. You must
        add words to the dictionary (using add_field, add_words, and/or add_scored_words)
        before you can use this.
        
        :text: The word to check.
        :number: The maximum number of suggestions to return.
        :usescores: Use the per-word score to influence the suggestions.
        :*returns*: list
        """
        
        grams = defaultdict(list)
        for size in xrange(self.mingram, self.maxgram + 1):
            key = "gram%s" % size
            nga = analysis.NgramAnalyzer(size)
            for t in nga(text):
                grams[key].append(t.text)
        
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
            
            length = len(results)
            if len(results) > number*2:
                length = len(results)//2
            fieldlist = results[:length]
            
            suggestions = [(fs["word"], fs["score"])
                           for fs in fieldlist
                           if fs["word"] != text]
            
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
        """Adds the terms in a field from another index to the backend dictionary.
        This method calls add_scored_words() and uses each term's frequency as the
        score. As a result, more common words will be suggested before rare words.
        If you want to calculate the scores differently, use add_scored_words()
        directly.
        
        :ix: The index.Index object from which to add terms.
        :fieldname: The field name (or number) of a field in the source
            index. All the indexed terms from this field will be added to the
            dictionary.
        """
        
        tr = ix.term_reader()
        try:
            self.add_scored_words((w, freq) for w, _, freq in tr.iter_field(fieldname))
        finally:
            tr.close()
    
    def add_words(self, ws, score = 1):
        """Adds a list of words to the backend dictionary.
        
        :ws: A sequence of words (strings) to add to the dictionary.
        :score: An optional score to use for ALL the words in 'ws'.
        """
        self.add_scored_words((w, score) for w in ws)
    
    def add_scored_words(self, ws):
        """Adds a list of ("word", score) tuples to the backend dictionary.
        Associating words with a score lets you use the 'usescores' keyword
        argument of the suggest() method to order the suggestions using the
        scores.
        
        :ws: A sequence of ("word", score) tuples.
        """
        
        writer = writing.IndexWriter(self.index())
        for text, score in ws:
            if text.isalpha():
                fields = {"word": text, "score": score}
                for size in xrange(self.mingram, self.maxgram + 1):
                    nga = analysis.NgramAnalyzer(size)
                    gramlist = [t.text for t in nga(text)]
                    if len(gramlist) > 0:
                        fields["start%s" % size] = gramlist[0]
                        fields["end%s" % size] = gramlist[-1]
                        fields["gram%s" % size] = " ".join(gramlist)
                writer.add_document(**fields)
        writer.commit()
    
if __name__ == '__main__':
    pass
    
    

