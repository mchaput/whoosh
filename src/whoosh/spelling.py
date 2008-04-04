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

from collections import defaultdict

import analysis, fields, index, query, searching, store, writing
from support.levenshtein import relative, distance

class SpellChecker():
    def __init__(self, storage, indexname,
                 booststart = 2.0, boostend = 1.0,
                 mingram = 3, maxgram = 4,
                 minscore = 0.5):
        self.storage = storage
        self.indexname = indexname
        
        self._index = None
        self._reader = None
        
        self.booststart = booststart
        self.boostend = boostend
        self.mingram = mingram
        self.maxgram = maxgram
    
    def index(self):
        if not self._index:
            self._index = index.Index(self.storage, indexname = self.indexname)
        return self._index
    
    def schema(self):
        fls = [fields.IDField("word", analysis.LCAnalyzer(), stored = True)]
        for size in xrange(self.mingram, self.maxgram + 1):
            fls.extend([fields.IDField("start%s" % size, analysis.SimpleAnalyzer()),
                        fields.IDField("end%s" % size, analysis.SimpleAnalyzer()),
                        fields.FrequencyField("gram%s" % size, analysis.SimpleAnalyzer())
                        ])
        return index.Schema(*fls)
    
    def reader(self):
        if not self._reader:
            self._reader = self.index().reader()
        return self._reader
    
    def close_reader(self):
        if self._reader:
            self._reader.close()
    
    def suggest(self, text, number = 3, fieldname = None, morepopular = False):
        grams = defaultdict(list)
        for size in xrange(self.mingram, self.maxgram + 1):
            key = "gram%s" % size
            nga = analysis.NgramAnalyzer(size)
            for gram in nga.words(text):
                grams[key].append(gram)
        
        queries = []
        for size in xrange(self.mingram, self.maxgram + 1):
            key = "gram%s" % size
            gramlist = grams[key]
            queries.append(query.Term("start%s" % size, gramlist[0], boost = self.booststart))
            queries.append(query.Term("end%s" % size, gramlist[-1], boost = self.boostend))
            for gram in gramlist:
                queries.append(query.Term(key, gram))
        
        q = query.Or(queries)
        r = self.reader()
        dr = r.doc_reader()
        
        scorer = searching.CosineScorer()
        docset, terms = searching.find_docs(r, q)
        scored = list(scorer.score(r, terms, docset))
        scored.sort(key = lambda x: x[1], reverse = True)
        if len(scored) > number*2:
            scored = scored[:len(scored)//2]
        
        suggestions = []
        for docnum, _ in scored:
            word = dr[docnum]["word"]
            if word == text: continue
            suggestions.append(word)
        
        if morepopular:
            def keyfn(a):
                return 0-len(terms[a])
        else:
            def keyfn(a):
                return distance(text, a)
        suggestions.sort(key = keyfn)
        return suggestions[:number]
        
    def create_index(self):
        self._index = index.create(self.storage, self.schema(), self.indexname)
    
    def add_field(self, reader, fieldname):
        self.add_words(reader.field_terms(fieldname))
        
    def add_words(self, ws):
        self.close_reader()
        #exists = self.exists
        
        writer = writing.IndexWriter(self.index())
        for text in ws:
            #if not exists("word", text):
            fields = {"word": text}
            for size in xrange(self.mingram, self.maxgram + 1):
                nga = analysis.NgramAnalyzer(size)
                gramlist = list(nga.words(text))
                if len(gramlist) > 0:
                    fields["start%s" % size] = gramlist[0]
                    fields["end%s" % size] = gramlist[-1]
                    fields["gram%s" % size] = " ".join(gramlist)
            writer.add_document(**fields)
        writer.close()
    
    def exists(self, field, text):
        return self.reader.term_frequency(field, text) > 0
    
    
if __name__ == '__main__':
    import time
    storage = store.FolderStorage("c:/workspace/Help2/test_index")
    sc = SpellChecker(storage, "spell")
    
    t = time.time()
    sc.create_index()
    ix = index.Index(storage)
    sc.add_field(ix.reader(), "content")
    print time.time() - t
    
    print "-----------------------"
    t = time.time()
    print sc.suggest("renderling", number = 5)
    print time.time() - t
    
    

