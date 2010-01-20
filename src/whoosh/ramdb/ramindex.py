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


class RamIndex(Index):
    def __init__(self, schema):
        self.schema = schema
        self.maxdoc = 0
        self._sync_lock = Lock()
        
        self.termlists = defaultdict(list)
        self.invertedindex = {}
        for fieldnum in xrange(len(schema)):
            self.invertedindex[fieldnum] = defaultdict(list)
        self.indexfreqs = defaultdict(int)
        
        self.storedfields = {}
        self.fieldlengths = defaultdict(int)
        self.fieldlength_totals = defaultdict(int)
        self.vectors = {}
        self.deleted = set()
        
        self._stored_to_pos = dict((fnum, i) for i, fnum in enumerate(self.schema.stored_fields()))
    
    def doc_count_all(self):
        return self.maxdoc
    
    def doc_count(self):
        return self.maxdoc - len(self.deleted)
        
    def reader(self):
        return RamIndexReader(self)
    
    def writer(self):
        return RamIndexWriter(self)
    
    def optimize(self):
        # TODO: Write this
        pass


class RamIndexWriter(IndexWriter):
    def __init__(self, ix):
        self.ix = ix
        self.schema = ix.schema
        self._stored_to_pos = dict((fnum, i) for i, fnum in enumerate(self.schema.stored_fields()))
        
    def add_document(self, **fields):
        schema = self.ix.schema
        invertedindex = self.ix.invertedindex
        indexfreqs = self.ix.indexfreqs
        fieldlengths = self.ix.fieldlengths
        fieldlength_totals = self.ix.fieldlength_totals
        termlists = self.ix.termlists
        maxdoc = self.ix.maxdoc
        
        fieldnames = [name for name in fields.keys() if not name.startswith("_")]
        fieldnames.sort(key = schema.name_to_number)
        
        stored_to_pos = self._stored_to_pos
        storedvalues = [None] * len(stored_to_pos)
        
        for name in fieldnames:
            if name not in schema:
                raise UnknownFieldError("There is no field named %r" % name)
            
        for name in fieldnames:
            value = fields.get(name)
            if value:
                fieldnum = schema.name_to_number(name)
                field = schema.field_by_number(fieldnum)
                format = field.format
                
                if format.analyzer:
                    if format.textual and not isinstance(value, unicode):
                        raise ValueError("%r in field %s is not unicode" % (value, name))
                    
                count = 0
                unique = 0
                
                fieldlist = termlists[fieldnum]
                fielddict = invertedindex[fieldnum]
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
                    
            vector = field.vector
            if vector:
                vlist = sorted((w, valuestring) for w, freq, valuestring
                               in vector.word_values(value))
                self.ix.vectors[(maxdoc, fieldnum)] = vlist
            
            if field.stored:
                storedname = "_stored_" + name
                if storedname in fields:
                    stored_value = fields[storedname]
                else :
                    stored_value = value
                
                storedvalues[stored_to_pos[fieldnum]] = stored_value
        
        self.ix.storedfields[maxdoc] = storedvalues
        self.ix.maxdoc += 1
    
    def delete_document(self, docnum, delete=True):
        if delete:
            self.ix.deleted.add(docnum)
        else:
            self.ix.deleted.remove(docnum)
    
    def commit(self):
        # No op
        pass
    
    def cancel(self):
        # No op
        pass





    