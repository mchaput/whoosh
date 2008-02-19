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
from bisect import bisect_right

import reading, writing
from support.bitvector import BitVector


_toc_filename = re.compile("_toc([0-9]+)")
_segment_filename = re.compile("(_[0-9]+)\\.(dcs|dcx|pst|tix)")


class OutOfDateError(Exception): pass


def _last_generation(storage):
    """
    Utility function to find the most recent
    generation number of the index. The index will use
    this to start a new generation, and a reader can use
    this to check if it's up to date.
    """
    
    max = -1
    for filename in storage.list():
        m = _toc_filename.match(filename)
        if m:
            num = int(m.group(1))
            if num > max: max = num
    return max

def create(storage, schema):
    """
    Creates an index in the specified storage object,
    using the specified field schema.
    """
    
    storage.clean()
    write_index_file(storage, 0, [], schema, 0)
    return Index(storage)

def write_index_file(storage, generation, segments, schema, counter):
    stream = storage.create_file("_toc%s" % generation)
    stream.write_pickle((segments, schema, counter))
    stream.close()

def read_index_file(storage, generation):
    stream = storage.open_file("_toc%s" % generation)
    segments, schema, counter = stream.read_pickle()
    stream.close()
    return segments, schema, counter


class Schema(object):
    def __init__(self, *fields):
        self.by_number = []
        self.by_name = {}
        
        for field in fields:
            self.add(field)
    
    def has_name(self, name):
        return self.by_name.has_key(name)
    
    def has_field(self, field):
        return self.has_name(field.name) and self.by_name[field.name] == field
    
    def add(self, field):
        if self.by_name.has_key(field.name):
            raise Exception("Schema already has a field named %s" % field.name)
        
        num = len(self.by_number)
        field.number = num
        self.by_number.append(field)
        self.by_name[field.name] = field


class Index(object):
    def __init__(self, storage):
        self.storage = storage
        
        self.generation = _last_generation(storage)
        if self.generation >= 0:
            self.reload()
    
    def field_by_name(self, name):
        if name in self.schema.by_name:
            return self.schema.by_name[name]
        else:
            raise ValueError("No field named '%s'" % name)
    
    def doc_count(self):
        return sum([s.max_doc - s.deleted_count() for s in self.segments])
    
    def reader(self):
        segs = self.segments
        if len(segs) == 0: return None
        if len(segs) == 1:
            return reading.SegmentReader(self, segs[0])
        else:
            return reading.MultiSegmentReader(self, segs)
    
    def up_to_date(self):
        return self.generation == _last_generation(self.storage)
    
    def next_segment_name(self):
        self.counter += 1
        return "_%s" % self.counter
    
    def reload(self):
        segments, self.schema, self.counter = read_index_file(self.storage, self.generation)
        self._set_segments(segments)
    
    def _set_segments(self, segments):
        self.segments = segments
        
        self.doc_offsets = []
        self.max_doc = 0
        self.term_count_multiplied = 0.0
        self.term_count_actual = 0
        
        for segment in self.segments:
            self.doc_offsets.append(self.max_doc)
            self.max_doc += segment.max_doc
            self.term_count_multiplied += segment.term_count_multiplied
            self.term_count_actual += segment.term_count_actual
    
    def _document_segment(self, docnum):
        if len(self.doc_offsets) == 1: return 0
        return bisect_right(self.doc_offsets, docnum) - 1
    
    def delete_document(self, docnum):
        segmentnum = self._document_segment(docnum)
        offset = self.doc_offsets[segmentnum]
        segment = self.segments[segmentnum]
        segment.delete_document(docnum - offset)
    
    def delete_by_term(self, fieldname, text):
        r = self.reader()
        tr = r.term_reader()
        fieldnum = self.field_by_name(fieldname).number
        try:
            tr.find_term(fieldnum, text)
            for docnum, data in tr.postings(): #@UnusedVariable
                print "Deleting", docnum
                self.delete_document(docnum)
            return tr.doc_freq
        except reading.TermNotFound:
            return 0
    
    def has_deletions(self):
        for segment in self.segments:
            if segment.has_deletions(): return True
        return False
    
    def optimize(self):
        if len(self.segments) < 2 and not self.has_deletions():
            return
        w = writing.IndexWriter(self)
        w.optimize()
        w.close()
    
    def checkpoint(self):
        if not self.up_to_date():
            raise OutOfDateError
        
        self.generation += 1
        write_index_file(self.storage, self.generation, self.segments, self.schema, self.counter)
        self.clean_files()
    
    def clean_files(self):
        storage = self.storage
        current_segment_names = set([s.name for s in self.segments])
        
        for filename in storage.list():
            m = _toc_filename.match(filename)
            if m:
                num = int(m.group(1))
                if num != self.generation:
                    storage.delete_file(filename)
            else:
                m = _segment_filename.match(filename)
                if m:
                    name = m.group(1)
                    if name not in current_segment_names:
                        storage.delete_file(filename)
                else:
                    storage.delete_file(filename)


class Segment(object):
    def __init__(self, name, max_doc, term_count_multiplied, term_count_actual, deleted = None):
        self.name = name
        self.max_doc = max_doc
        self.term_count_multiplied = term_count_multiplied
        self.term_count_actual = term_count_actual
        self.deleted = deleted
    
    def __repr__(self):
        return "%s(\"%s\")" % (self.__class__.__name__, self.name)
    
    def has_deletions(self):
        return self.deleted_count() > 0
    
    def deleted_count(self):
        if self.deleted is None: return 0
        return self.deleted.count()
    
    def delete_document(self, docnum):
        if self.deleted is None:
            self.deleted = BitVector(self.max_doc)
            
        self.deleted.set(docnum)
    
    def is_deleted(self, docnum):
        if self.deleted is None: return False
        return self.deleted.get(docnum)
    

def dump_index(ix):
    print "Index stored in", ix.storage
    print "Index has %s segments:" % len(ix.segments)
    for seg in ix.segments:
        print "Segment", seg.name
        reader = reading.SegmentReader(ix, seg)
        
        print "  Documents:"
        docnum = 0
        for tcm, tca, payload in reader.doc_reader():
            d = "DEL" if reader.is_deleted(docnum) else "   "
            print "    ", d, "tcm=", tcm, "tca=", tca, "payload=", repr(payload)
            docnum += 1
            
        print "  Terms:"
        tr = reader.term_reader()
        by_number = ix.schema.by_number
        for fieldnum, text, freq in tr:
            print "    %s:%s" % (by_number[fieldnum].name, text), "freq=", freq
            for docnum, data in tr.postings():
                print "      docnum=", docnum, "data=", repr(data)
    
    
    
    
    
    
    
    
    
    
    