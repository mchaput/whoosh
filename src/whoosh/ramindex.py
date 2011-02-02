#===============================================================================
# Copyright 2011 Matt Chaput
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
from bisect import bisect_left
from threading import RLock

from whoosh.fields import UnknownFieldError
from whoosh.matching import ListMatcher, NullMatcher
from whoosh.reading import IndexReader, TermNotFound
from whoosh.writing import IndexWriter
from whoosh.util import synchronized


class RamIndex(IndexReader, IndexWriter):
    def __init__(self, schema):
        self.schema = schema
        self.docnum = 0
        self._sync_lock = RLock()
        self.is_closed = False
        self.clear()
    
    @synchronized
    def clear(self):
        self.invindex = {}
        self.indexfreqs = defaultdict(int)
        self.storedfields = []
        self.fieldlengths = defaultdict(int)
        self.vectors = {}
        self.deleted = set()
        self.usage = 0
    
    @synchronized
    def __contains__(self, term):
        try:
            self.invindex[term[0]][term[1]]
            return True
        except KeyError:
            return False
    
    @synchronized
    def __iter__(self):
        invindex = self.invindex
        indexfreqs = self.indexfreqs
        for fn in sorted(invindex.keys()):
            for text in sorted(self.invindex[fn].keys()):
                docfreq = len(invindex[fn][text])
                indexfreq = indexfreqs[fn, text]
                yield (fn, text, docfreq, indexfreq)
    
    def close(self):
        pass
    
    @synchronized
    def has_deletions(self):
        return bool(self.deleted)
    
    @synchronized
    def is_deleted(self, docnum):
        return docnum in self.deleted
    
    @synchronized
    def delete_document(self, docnum, delete=True):
        if delete:
            self.deleted.add(docnum)
        else:
            self.deleted.remove(docnum)
    
    @synchronized
    def stored_fields(self, docnum):
        return self.storedfields[docnum]
    
    @synchronized
    def all_stored_fields(self):
        deleted = self.deleted
        return (sf for i, sf in enumerate(self.storedfields)
                if i not in deleted)
    
    @synchronized
    def field_length(self, fieldname):
        if fieldname not in self.schema or not self.schema[fieldname].scorable:
            return 0
        return sum(l for docnum_fieldname, l in self.fieldlengths.iteritems()
                   if docnum_fieldname[1] == fieldname)
    
    @synchronized
    def max_field_length(self, fieldname):
        if fieldname not in self.schema or not self.schema[fieldname].scorable:
            return 0
        return max(l for docnum_fieldname, l in self.fieldlengths.iteritems()
                   if docnum_fieldname[1] == fieldname)
    
    def doc_field_length(self, docnum, fieldname, default=0):
        return self.fieldlengths.get((docnum, fieldname), default)
    
    def has_vector(self, docnum, fieldname):
        return (docnum, fieldname) in self.vectors
    
    @synchronized
    def vector(self, docnum, fieldname):
        vformat = self.schema[fieldname].vector
        ids, weights, values = zip(*self.vectors[docnum, fieldname])
        return ListMatcher(ids, weights, values, format=vformat)
    
    def doc_frequency(self, fieldname, text):
        return len(self.invindex[fieldname][text])
    
    def frequency(self, fieldname, text):
        return self.indexfreqs[fieldname, text]
    
    @synchronized
    def iter_from(self, fieldname, text):
        invindex = self.invindex
        indexfreqs = self.indexfreqs
        
        for fn in sorted(key for key in self.invindex.keys() if key >= fieldname):
            texts = sorted(invindex[fn])
            start = 0
            if fn == fieldname:
                start = bisect_left(texts, text)
            for t in texts[start:]:
                docfreq = len(invindex[fn][t])
                indexfreq = indexfreqs[fn, t]
                yield (fn, t, docfreq, indexfreq)
    
    def lexicon(self, fieldname):
        return sorted(self.invindex[fieldname].keys())
    
    @synchronized
    def expand_prefix(self, fieldname, prefix):
        texts = self.lexicon(fieldname)
        start = 0 if not prefix else bisect_left(texts, prefix)
        for text in texts[start:]:
            if text.startswith(prefix):
                yield text
            else:
                break
    
    @synchronized
    def first_id(self, fieldname, text):
        try:
            plist = self.invindex[fieldname][text]
        except KeyError:
            raise TermNotFound((fieldname, text))
        else:
            deleted = self.deleted
            for x in plist:
                docnum = x[0]
                if docnum not in deleted:
                    return docnum
    
    @synchronized
    def postings(self, fieldname, text, scorer=None):
        try:
            postings = self.invindex[fieldname][text]
        except KeyError:
            raise TermNotFound((fieldname, text))
        
        excludeset = self.deleted
        format = self.schema[fieldname].format
        if excludeset:
            postings = [x for x in postings if x[0] not in excludeset]
            if not postings:
                return NullMatcher()
        ids, weights, values = zip(*postings)
        return ListMatcher(ids, weights, values, format=format)
    
    def reader(self):
        return self
    
    def searcher(self, **kwargs):
        from whoosh.searching import Searcher
        return Searcher(self.reader(), **kwargs)
    
    def writer(self, **kwargs):
        return self
    
    def doc_count_all(self):
        return len(self.storedfields)
    
    def doc_count(self):
        return len(self.storedfields) - len(self.deleted)
    
    @synchronized
    def update_document(self, **fields):
        super(RamIndex, self).update_document(**fields)
    
    @synchronized
    def add_document(self, **fields):
        schema = self.schema
        invindex = self.invindex
        indexfreqs = self.indexfreqs
        fieldlengths = self.fieldlengths
        usage = 0
        
        fieldnames = [name for name in sorted(fields.keys())
                      if not name.startswith("_")]
        
        for name in fieldnames:
            if name not in schema:
                raise UnknownFieldError("There is no field named %r" % name)
            if name not in invindex:
                invindex[name] = {}
        
        storedvalues = {}
        
        for name in fieldnames:
            value = fields.get(name)
            if value:
                field = schema[name]
                
                fielddict = invindex[name]
                
                # If the field is indexed, add the words in the value to the
                # index
                if field.indexed:
                    # Count of all terms in the value
                    count = 0
                    # Count of UNIQUE terms in the value
                    unique = 0
                    
                    for w, freq, weight, valuestring in field.index(value):
                        if w not in fielddict:
                            fielddict[w] = []
                        fielddict[w].append((self.docnum, weight, valuestring))
                        indexfreqs[name, w] += freq
                        count += freq
                        unique += 1
                        
                        usage += 44 + len(valuestring)
                    
                    if field.scorable:
                        fieldlengths[self.docnum, name] = count
                        usage += 36
                    
            vector = field.vector
            if vector:
                vlist = sorted((w, weight, valuestring) for w, freq, weight, valuestring
                               in vector.word_values(value))
                self.vectors[self.docnum, name] = vlist
                usage += 28
                for x in vlist:
                    usage += 44 + len(x[2])
            
            if field.stored:
                storedname = "_stored_" + name
                if storedname in fields:
                    stored_value = fields[storedname]
                else :
                    stored_value = value
                
                storedvalues[name] = stored_value
                usage += 28 + len(name)# + len(stored_value)
        
        self.storedfields.append(storedvalues)
        self.usage += usage
        self.docnum += 1
    
#    @synchronized
#    def optimize(self):
#        deleted = self.deleted
#        
#        # Remove deleted documents from stored fields
#        self.storedfields = [sf for i, sf in enumerate(self.storedfields)
#                             if i not in deleted]
#        
#        # Remove deleted documents from inverted index
#        removedterms = defaultdict(set)
#        for fn in self.invindex:
#            termdict = self.invindex[fn]
#            for text, postlist in termdict.items():
#                newlist = [x for x in postlist if x[0] not in deleted]
#                if newlist:
#                    termdict[text] = newlist
#                else:
#                    removedterms[fn].add(text)
#                    del termdict[text]
#        
#        # If terms were removed as a result of document deletion, update
#        # indexfreqs
#        for fn, removed in removedterms.iteritems():
#            for text in removed:
#                del self.indexfreqs[fn, text]
#        
#        # Remove documents from field lengths
#        fieldlengths = self.fieldlengths
#        for docnum, fieldname in fieldlengths.keys():
#            if docnum in deleted:
#                del fieldlengths[docnum, fieldname]
#                
#        # Remove documents from vectors
#        vectors = self.vectors
#        for docnum, fieldname in vectors.keys():
#            if docnum in deleted:
#                del vectors[docnum, fieldname]
#        
#        # Reset deleted list
#        self.deleted = set()
        
    def commit(self):
        pass
        
    




