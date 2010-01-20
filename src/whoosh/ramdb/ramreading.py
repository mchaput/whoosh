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

from whoosh.postings import PostingReader, ReadTooFar
from whoosh.reading import IndexReader


class RamIndexReader(IndexReader):
    def __init__(self, ix):
        self.ix = ix
        self.schema = ix.schema
        self._stored_field_names = ix.schema.stored_field_names()
        self._scorable_fields = ix.schema.scorable_fields()
        
    def __contains__(self, term):
        fieldid, text = term
        fieldnum = self.ix.schema.to_number(fieldid)
        inv = self.ix.invertedindex
        return fieldnum in inv and text in inv[fieldnum]
    
    def close(self):
        pass
    
    def has_deletions(self):
        return len(self.ix.deleted) > 0
    
    def is_deleted(self, docnum):
        return docnum in self.ix.deleted
    
    def stored_fields(self, docnum):
        return dict(zip(self._stored_field_names, self.ix.storedfields[docnum]))
    
    def all_stored_fields(self):
        sfn = self._stored_field_names
        for sfs in self.ix.storedfields:
            yield dict(zip(sfn, sfs))
            
    def doc_count_all(self):
        return self.ix.doc_count_all()
    
    def doc_count(self):
        return self.ix.doc_count()
    
    def field_length(self, fieldid):
        fieldnum = self.ix.schema.to_number(fieldid)
        return self.ix.fieldlength_totals[fieldnum]
    
    def doc_field_length(self, docnum, fieldid):
        fieldnum = self.ix.schema.to_number(fieldid)
        return self.ix.fieldlengths[(docnum, fieldnum)]
    
    def doc_field_lengths(self, docnum):
        dfl = self.doc_field_length
        return [dfl(docnum, fnum) for fnum in self._scorable_fields]
    
    def has_vector(self, docnum, fieldnum):
        return (docnum, fieldnum) in self.ix.vectors
    
    def __iter__(self):
        tls = self.ix.termlists
        inv = self.ix.invertedindex
        ixf = self.ix.indexfreqs
        for fieldnum in sorted(tls.keys()):
            for text in tls[fieldnum]:
                docfreq = len(inv[fieldnum][text])
                indexfreq = ixf[(fieldnum, text)]
                yield (fieldnum, text, docfreq, indexfreq)
                
    def doc_frequency(self, fieldid, text):
        fieldnum = self.ix.schema.to_number(fieldid)
        return len(self.ix.invertedindex[fieldnum][text])
    
    def frequency(self, fieldid, text):
        fieldnum = self.ix.schema.to_number(fieldid)
        return self.ix.indexfreq[(fieldnum, text)]
    
    def iter_from(self, fieldid, text):
        tls = self.ix.termlists
        inv = self.ix.invertedindex
        ixf = self.ix.indexfreqs
        for fieldnum in sorted(tls.keys()):
            fieldtexts = tls[fieldnum]
            start = bisect_left(fieldtexts, text)
            for text in fieldtexts[start:]:
                docfreq = len(inv[fieldnum][text])
                indexfreq = ixf[(fieldnum, text)]
                yield (fieldnum, text, docfreq, indexfreq)
    
    def lexicon(self, fieldid):
        fieldnum = self.ix.schema.to_number(fieldid)
        return self.ix.termlists[fieldnum]
    
    def expand_prefix(self, fieldid, prefix):
        fieldnum = self.ix.schema.to_number(fieldid)
        fieldtexts = self.ix.termlists[fieldnum]
        start = bisect_left(fieldtexts, prefix)
        for text in fieldtexts[start:]:
            if text.startswith(prefix):
                yield text
            else:
                break
            
    def postings(self, fieldid, text, exclude_docs = None):
        fieldnum = self.ix.schema.to_number(fieldid)
        if not exclude_docs:
            exclude_docs = frozenset()
        excludeset = self.ix.deleted | exclude_docs
        inv = self.ix.invertedindex
        format = self.schema[fieldnum].format
        postings = inv[fieldnum][text]
        if excludeset:
            postings = [(docnum, stringvalue) for docnum, stringvalue
                        in postings if docnum not in excludeset]
        return RamPostingReader(format, postings)


class RamPostingReader(PostingReader):
    def __init__(self, format, postings):
        self.format = format
        self.postings = postings
        self.reset()
    
    def reset(self):
        self.i = 0
        self.id = self.postings[0][0]
    
    def all_items(self):
        print "all_items"
        return self.postings
    
    def all_ids(self):
        return (id for id, _ in self.postings)
    
    def next(self):
        if self.id is None:
            raise ReadTooFar
        
        postings = self.postings
        i = self.i
        
        i += 1
        if i < len(postings):
            self.id = postings[i][0]
        else:
            self.id = None
        self.i = i
    
    def skip_to(self, target):
        if target <= self.id:
            return
        if self.id is None:
            raise ReadTooFar
        
        postings = self.postings
        i = self.i
        
        while i < len(postings):
            i += 1
            if i == len(postings):
                self.id = None
                break
            elif postings[i][0] >= target:
                self.id = postings[i][0]
                break
            
        self.i = i
    
    def value(self):
        if self.id is None:
            raise ReadTooFar
        
        return self.postings[self.i][1]
    
    

















    
    
