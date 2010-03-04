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

from bisect import insort
from collections import defaultdict
from threading import Lock

from whoosh.fields import UnknownFieldError
from whoosh.index import Index
from whoosh.ramdb.ramreading import RamIndexReader
from whoosh.writing import IndexWriter
from whoosh.util import protected


class RamIndex(Index):
    def __init__(self, schema):
        self.schema = schema
        self.maxdoc = 0
        self._sync_lock = Lock()
        self.is_closed = False
        
        self.clear()
    
    def clear(self):
        # Maps fieldnum -> a sorted list of term texts in that field
        self.termlists = defaultdict(list)
        
        # Maps field numbers to dictionaries of term -> posting list
        self.invertedindex = {}
        for fieldnum in xrange(len(self.schema)):
            self.invertedindex[fieldnum] = defaultdict(list)
        
        # Maps terms -> index frequencies
        self.indexfreqs = defaultdict(int)
        
        # Maps docnum -> stored field lists
        self.storedfields = {}
        
        # Maps (docnum, fieldnum) -> field length
        self.fieldlengths = defaultdict(int)
        
        # Maps fieldnum -> total field length
        self.fieldlength_totals = defaultdict(int)
        
        # Maps fieldnum -> maximum field length in a document
        self.fieldlength_maxes = {}
        
        # Maps (docnum, fieldnum) -> posting list
        self.vectors = {}
        
        # Contains docnums of deleted documents
        self.deleted = set()
        
        self._stored_to_pos = dict((fnum, i) for i, fnum
                                   in enumerate(self.schema.stored_fields()))
    
    def close(self):
        del self.termlists
        del self.invertedindex
        del self.indexfreqs
        del self.storedfields
        del self.fieldlengths
        del self.fieldlength_totals
        del self.vectors
        del self.deleted
        self.is_closed = True
    
    def doc_count_all(self):
        return len(self.storedfields)
    
    def doc_count(self):
        return len(self.storedfields) - len(self.deleted)
        
    def reader(self):
        return RamIndexReader(self)
    
    def writer(self):
        return self
    
    @protected
    def optimize(self):
        schema = self.schema
        deleted = self.deleted
        
        # Remove documents from stored fields
        storedfields = self.storedfields
        for docnum in deleted:
            del storedfields[docnum]
        
        # Remove documents from inverted index
        removedterms = defaultdict(set)
        for fieldnum in xrange(len(schema)):
            inv = self.invertedindex[fieldnum]
            for term, postlist in inv.iteritems():
                inv[term] = [x for x in postlist if x[0] not in deleted]
            
            # Remove terms that no longer have any postings after the
            # documents are deleted
            for term in inv.keys():
                if not inv[term]:
                    removedterms[fieldnum].add(term)
                    del inv[term]
        
        # If terms were removed as a result of document deletion,
        # update termlists and indexfreqs
        termlists = self.termlists
        for fieldnum, removed in removedterms.iteritems():
            termlists[fieldnum] = [t for t in termlists[fieldnum]
                                   if t not in removed]
            for text in removed:
                del self.indexfreqs[(fieldnum, text)]
        
        # Remove documents from field lengths
        fieldlengths = self.fieldlengths
        fieldlength_totals = self.fieldlength_totals
        for docnum_fieldnum in fieldlengths.keys():
            if docnum_fieldnum[0] in deleted:
                fieldlength_totals[docnum_fieldnum[1]] -= fieldlengths[docnum_fieldnum]
                del fieldlengths[docnum_fieldnum]
                
        # Remove documents from vectors
        vectors = self.vectors
        for docnum_fieldnum in vectors.keys():
            if docnum_fieldnum[0] in deleted: del vectors[docnum_fieldnum]
            
        # Reset deleted list
        self.deleted = set()
        
    @protected
    def add_document(self, **fields):
        schema = self.schema
        invertedindex = self.invertedindex
        indexfreqs = self.indexfreqs
        fieldlengths = self.fieldlengths
        fieldlength_totals = self.fieldlength_totals
        maxdoc = self.maxdoc
        
        fieldnames = [name for name in fields.keys() if not name.startswith("_")]
        fieldnames.sort(key = schema.name_to_number)
        
        stored_to_pos = dict((fnum, i) for i, fnum
                             in enumerate(schema.stored_fields()))
        storedvalues = [None] * len(stored_to_pos)
        
        for name in fieldnames:
            if name not in schema:
                raise UnknownFieldError("There is no field named %r" % name)
            
        for name in fieldnames:
            value = fields.get(name)
            if value:
                fieldnum = schema.name_to_number(name)
                field = schema.field_by_number(fieldnum)
                
                fieldlist = self.termlists[fieldnum]
                fielddict = invertedindex[fieldnum]
                
                # If the field is indexed, add the words in the value to the
                # index
                if field.indexed:
                    # Count of all terms in the value
                    count = 0
                    # Count of UNIQUE terms in the value
                    unique = 0
                    
                    for w, freq, valuestring in field.index(value):
                        if w not in fielddict:
                            insort(fieldlist, w)
                        fielddict[w].append((maxdoc, valuestring))
                        indexfreqs[(fieldnum, w)] += freq
                        count += freq
                        unique += 1
                
                    if field.scorable:
                        fieldlength_totals[fieldnum] += count
                        fieldlengths[(maxdoc, fieldnum)] = count
                        if count > self.fieldlength_maxes.get(fieldnum, 0):
                            self.fieldlength_maxes[fieldnum] = count
                    
            vector = field.vector
            if vector:
                vlist = sorted((w, valuestring) for w, freq, valuestring
                               in vector.word_values(value))
                self.vectors[(maxdoc, fieldnum)] = vlist
            
            if field.stored:
                storedname = "_stored_" + name
                if storedname in fields:
                    stored_value = fields[storedname]
                else :
                    stored_value = value
                
                storedvalues[stored_to_pos[fieldnum]] = stored_value
        
        self.storedfields[maxdoc] = storedvalues
        self.maxdoc += 1
    
    @protected
    def delete_document(self, docnum, delete=True):
        if delete:
            self.deleted.add(docnum)
        else:
            self.deleted.remove(docnum)

    def has_deletions(self):
        return bool(self.deleted)
    





    