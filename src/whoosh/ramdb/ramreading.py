#===============================================================================
# Copyright 2009 Matt Chaput
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

from bisect import bisect_left
from time import clock as now

from whoosh.matching import Matcher, ReadTooFar
from whoosh.reading import IndexReader


class RamIndexReader(IndexReader):
    def __init__(self, ix):
        self.ix = ix
        self.is_closed = False
        self.schema = ix.schema
        
    def __contains__(self, term):
        fieldname, text = term
        inv = self.ix.invertedindex
        return fieldname in inv and text in inv[fieldname]
        
    def close(self):
        del self.ix
        self._is_closed = True
    
    def has_deletions(self):
        return len(self.ix.deleted) > 0
    
    def is_deleted(self, docnum):
        return docnum in self.ix.deleted
    
    def stored_fields(self, docnum):
        return self.ix.storedfields[docnum]
    
    def all_stored_fields(self):
        sfs = self.ix.storedfields
        for docnum in xrange(self.ix.doc_count_all()):
            if docnum in sfs:
                yield sfs[docnum]
            
    def doc_count_all(self):
        return self.ix.doc_count_all()
    
    def doc_count(self):
        return self.ix.doc_count()
    
    def field_length(self, fieldname):
        return self.ix.field_length(fieldname)
    
    def max_field_length(self, fieldname):
        return self.ix.max_field_length(fieldname)
    
    def doc_field_length(self, docnum, fieldname, default=0):
        return self.ix.fieldlengths.get((docnum, fieldname), default)
    
    def has_vector(self, docnum, fieldname):
        return (docnum, fieldname) in self.ix.vectors
    
    def vector(self, docnum, fieldname):
        vformat = self.schema[fieldname].vector
        return RamPostingReader(vformat, self.ix.vectors[(docnum, fieldname)])
    
    def __iter__(self):
        tls = self.ix.termlists
        inv = self.ix.invertedindex
        ixf = self.ix.indexfreqs
        for fieldname in sorted(tls.iterkeys()):
            for text in tls[fieldname]:
                docfreq = len(inv[fieldname][text])
                indexfreq = ixf[(fieldname, text)]
                yield (fieldname, text, docfreq, indexfreq)
                
    def doc_frequency(self, fieldname, text):
        return len(self.ix.invertedindex[fieldname][text])
    
    def frequency(self, fieldname, text):
        return self.ix.indexfreqs[(fieldname, text)]
    
    def iter_from(self, fieldname, text):
        tls = self.ix.termlists
        inv = self.ix.invertedindex
        ixf = self.ix.indexfreqs
        
        fnms = sorted(fn for fn in tls.iterkeys() if fn >= fieldname)
        for fn in fnms:
            texts = tls[fn]
            start = 0
            if fn == fieldname:
                start = bisect_left(texts, text)
                
            for text in texts[start:]:
                docfreq = len(inv[fieldname][text])
                indexfreq = ixf[(fieldname, text)]
                yield (fieldname, text, docfreq, indexfreq)
                
    def lexicon(self, fieldname):
        return self.ix.termlists[fieldname]
    
    def iter_field(self, fieldname, prefix=''):
        inv = self.ix.invertedindex
        ixf = self.ix.indexfreqs
        
        fieldtexts = self.ix.termlists[fieldname]
        start = bisect_left(fieldtexts, prefix)
        for text in fieldtexts[start:]:
            docfreq = len(inv[fieldname][text])
            indexfreq = ixf[(fieldname, text)]
            yield (text, docfreq, indexfreq)
    
    def expand_prefix(self, fieldname, prefix):
        fieldtexts = self.ix.termlists[fieldname]
        start = bisect_left(fieldtexts, prefix)
        for text in fieldtexts[start:]:
            if text.startswith(prefix):
                yield text
            else:
                break
            
    def postings(self, fieldname, text, scorer=None):
        excludeset = self.ix.deleted
        inv = self.ix.invertedindex
        format = self.schema[fieldname].format
        postings = inv[fieldname][text]
        if excludeset:
            postings = [(docnum, weight, stringvalue) for docnum, weight, stringvalue
                        in postings if docnum not in excludeset]
        return RamPostingReader(format, postings, scorer=scorer)


class RamPostingReader(Matcher):
    def __init__(self, format, postings, scorer=None, stringids=False):
        self.format = format
        self.postings = postings
        self.i = 0
        
        self.scorer = scorer
    
    def is_active(self):
        return self.i < len(self.postings)
    
    def id(self):
        return self.postings[self.i][0]
    
    def all_items(self):
        return self.postings
    
    def all_ids(self):
        return (x[0] for x in self.postings)
    
    def next(self):
        if not self.is_active():
            raise ReadTooFar
        self.i += 1
    
    def skip_to(self, target):
        if not self.is_active():
            raise ReadTooFar
        
        if target <= self.id:
            return
        
        postings = self.postings
        i = self.i
        
        while i < len(postings):
            i += 1
            if i == len(postings):
                break
            elif postings[i][0] >= target:
                break
            
        self.i = i
    
    def weight(self):
        if not self.is_active():
            raise ReadTooFar
        
        return self.postings[self.i][1]
    
    def value(self):
        if not self.is_active():
            raise ReadTooFar
        
        return self.postings[self.i][2]
    
    def score(self):
        return self.scorer.score(self)
    
    def quality(self):
        return self.scorer.quality(self)
    
    def block_quality(self):
        return self.scorer.block_quality(self)
    
    

















    
    
