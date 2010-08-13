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

from collections import defaultdict
from threading import Lock

from whoosh.fields import UnknownFieldError
from whoosh.index import Index
from whoosh.ramdb.ramreading import RamIndexReader
from whoosh.util import protected


class RamIndex(Index):
    def __init__(self, schema):
        self.schema = schema
        self.docnum = 0
        self._sync_lock = Lock()
        self.is_closed = False
        
        self.clear()
    
    def clear(self):
        # Maps fieldname -> a sorted list of term texts in that field
        self.termlists = defaultdict(list)
        
        # Maps fieldnames to dictionaries of term -> posting list
        self.invertedindex = {}
        for fieldname in self.schema.names():
            self.invertedindex[fieldname] = defaultdict(list)
        
        # Maps terms -> index frequencies
        self.indexfreqs = defaultdict(int)
        
        # Maps docnum -> stored field dicts
        self.storedfields = {}
        
        # Maps (docnum, fieldname) -> field length
        self.fieldlengths = defaultdict(int)
        
        # Maps (docnum, fieldname) -> posting list
        self.vectors = {}
        
        # Contains docnums of deleted documents
        self.deleted = set()
        
    def close(self):
        del self.termlists
        del self.invertedindex
        del self.indexfreqs
        del self.storedfields
        del self.fieldlengths
        del self.vectors
        del self.deleted
        self.is_closed = True
    
    def doc_count_all(self):
        return len(self.storedfields)
    
    def doc_count(self):
        return len(self.storedfields) - len(self.deleted)
    
    def field_length(self, fieldname):
        return sum(l for docnum_fieldname, l in self.fieldlengths.iteritems()
                   if docnum_fieldname[1] == fieldname)
        
    def max_field_length(self, fieldname):
        return max(l for docnum_fieldname, l in self.fieldlengths.iteritems()
                   if docnum_fieldname[1] == fieldname)
    
    def reader(self):
        return RamIndexReader(self)
    
    def writer(self):
        return self
    
    @protected
    def add_field(self, *args, **kwargs):
        self.schema.add_field(*args, **kwargs)
    
    @protected
    def remove_field(self, fieldname):
        self.schema.remove_field(fieldname)
        if fieldname in self.termlists:
            del self.termlists[fieldname]
        for fn, text in self.indexfreqs.iterkeys():
            if fn == fieldname:
                del self.indexfreqs[(fn, text)]
        for sfields in self.storedfields.itervalues():
            if fieldname in sfields:
                del sfields[fieldname]
        for docnum, fn in self.fieldlengths.iterkeys():
            if fn == fieldname:
                del self.fieldlengths[(docnum, fn)]
        if fieldname in self.fieldlength_maxes:
            del self.fieldlength_maxes[fieldname]
        for docnum, fn in self.vectors.iterkeys():
            if fn == fieldname:
                del self.vectors[(docnum, fn)]
    
    @protected
    def delete_document(self, docnum, delete=True):
        if delete:
            self.deleted.add(docnum)
        else:
            self.deleted.remove(docnum)
    
    @protected
    def delete_by_term(self, fieldname, text):
        inv = self.invertedindex
        if fieldname in inv:
            terms = inv[fieldname]
            if text in terms:
                postings = terms[text]
                for p in postings:
                    self.deleted.add(p[0])
    
    @protected
    def delete_by_query(self, q, searcher=None):
        s = self.searcher()
        for docnum in q.docs(s):
            self.deleted.add(docnum)
    
    def has_deletions(self):
        return bool(self.deleted)
    
    @protected
    def optimize(self):
        deleted = self.deleted
        
        # Remove deleted documents from stored fields
        storedfields = self.storedfields
        for docnum in deleted:
            del storedfields[docnum]
        
        # Remove deleted documents from inverted index
        removedterms = defaultdict(set)
        for fieldname in self.schema.names():
            inv = self.invertedindex[fieldname]
            for term, postlist in inv.iteritems():
                inv[term] = [x for x in postlist if x[0] not in deleted]
            
            # Remove terms that no longer have any postings after the
            # documents are deleted
            for term in inv.keys():
                if not inv[term]:
                    removedterms[fieldname].add(term)
                    del inv[term]
        
        # If terms were removed as a result of document deletion,
        # update termlists and indexfreqs
        termlists = self.termlists
        for fieldname, removed in removedterms.iteritems():
            termlists[fieldname] = [t for t in termlists[fieldname]
                                   if t not in removed]
            for text in removed:
                del self.indexfreqs[(fieldname, text)]
        
        # Remove documents from field lengths
        fieldlengths = self.fieldlengths
        for docnum, fieldname in fieldlengths.keys():
            if docnum in deleted:
                del fieldlengths[(docnum, fieldname)]
                
        # Remove documents from vectors
        vectors = self.vectors
        for docnum, fieldname in vectors.keys():
            if docnum in deleted:
                del vectors[(docnum, fieldname)]
            
        # Reset deleted list
        self.deleted = set()
        
    @protected
    def add_document(self, **fields):
        schema = self.schema
        invertedindex = self.invertedindex
        indexfreqs = self.indexfreqs
        fieldlengths = self.fieldlengths
        
        fieldnames = [name for name in sorted(fields.keys())
                      if not name.startswith("_")]
        
        storedvalues = {}
        
        for name in fieldnames:
            if name not in schema:
                raise UnknownFieldError("There is no field named %r" % name)
        
        for name in fieldnames:
            value = fields.get(name)
            if value:
                field = schema[name]
                
                newwords = set()
                fielddict = invertedindex[name]
                
                # If the field is indexed, add the words in the value to the
                # index
                if field.indexed:
                    # Count of all terms in the value
                    count = 0
                    # Count of UNIQUE terms in the value
                    unique = 0
                    
                    for w, freq, weight, valuestring in field.index(value):
                        if w not in fielddict:
                            newwords.add(w)
                        fielddict[w].append((self.docnum, weight, valuestring))
                        indexfreqs[(name, w)] += freq
                        count += freq
                        unique += 1
                        
                    self.termlists[name] = sorted(set(self.termlists[name]) | newwords)
                
                    if field.scorable:
                        fieldlengths[(self.docnum, name)] = count
                    
            vector = field.vector
            if vector:
                vlist = sorted((w, weight, valuestring) for w, freq, weight, valuestring
                               in vector.word_values(value))
                self.vectors[(self.docnum, name)] = vlist
            
            if field.stored:
                storedname = "_stored_" + name
                if storedname in fields:
                    stored_value = fields[storedname]
                else :
                    stored_value = value
                
                storedvalues[name] = stored_value
        
        self.storedfields[self.docnum] = storedvalues
        self.docnum += 1
        
    @protected
    def add_reader(self, reader):
        startdoc = self.docnum
        
        has_deletions = reader.has_deletions()
        if has_deletions:
            docmap = {}
        
        fieldnames = set(self.schema.names())
        
        for docnum in xrange(reader.doc_count_all()):
            if (not has_deletions) or (not reader.is_deleted(docnum)):
                d = dict(item for item
                         in reader.stored_fields(docnum).iteritems()
                         if item[0] in fieldnames)
                self.storedfields[self.docnum] = d
                
                if has_deletions:
                    docmap[docnum] = self.docnum
                
                for fieldname, length in reader.doc_field_lengths(docnum):
                    if fieldname in fieldnames:
                        self.fieldlengths[(self.docnum, fieldname)] = length
                        
                for fieldname in reader.vector_names():
                    if (fieldname in fieldnames
                        and reader.has_vector(docnum, fieldname)):
                        vpostreader = reader.vector(docnum, fieldname)
                        self.vectors[(self.docnum, fieldname)] = list(vpostreader.all_items())
                        vpostreader.close()
                
                self.docnum += 1
                
        for fieldname, text, _, _ in reader:
            if fieldname in fieldnames:
                postreader = reader.postings(fieldname, text)
                while postreader.is_active():
                    docnum = postreader.id()
                    valuestring = postreader.value()
                    weight = postreader.weight()
                    if has_deletions:
                        newdoc = docmap[docnum]
                    else:
                        newdoc = startdoc + docnum
                    self.invertedindex[fieldname][text].append((newdoc,
                                                                weight,
                                                                valuestring))
                    postreader.next()
    
    
    





    