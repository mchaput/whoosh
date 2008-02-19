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

from bisect import bisect_left, insort_left

import structfile

_unsignedlong_size = structfile._unsignedlong_size
_int_size = structfile._int_size

class TermNotFound(Exception): pass

class SegmentReader(object):
    def __init__(self, index, segment):
        self.storage = index.storage
        self.schema = index.schema
        self.segment = segment
        
        self.doc_index = self.storage.open_file(segment.name + ".dcx")
        self.doc_file = self.storage.open_file(segment.name + ".dcs")
        self.term_index = self.storage.open_file(segment.name + ".tix")
        self.post_file = self.storage.open_file(segment.name + ".pst")
    
    def close(self):
        self.docs_index.close()
        self.docs_file.close()
        self.term_index.close()
        self.post_file.close()
    
    def has_deletions(self):
        return self.segment.has_deletions()
    
    def is_deleted(self, docnum):
        return self.segment.is_deleted(docnum)
    
    def doc_reader(self):
        return DocReader(self.doc_index, self.doc_file)
    
    def term_reader(self):
        return TermReader(self.segment, self.schema, self.term_index, self.post_file)
    
    def run_query(self, q):
        return q.run(self.term_reader())

class MultiSegmentReader(SegmentReader):
    def __init__(self, index, segments):
        self.index = index
        self.segments = segments
        self.readers = None
        
    def close(self):
        if self.readers:
            for r in self.readers: r.close()
    
    def _get_readers(self):
        if not self.readers:
            self.readers = [SegmentReader(self.index, s) for s in self.segments]
        return self.readers
    
    def doc_reader(self):
        return MultiDocReader([s.doc_reader() for s in self._get_readers()])
    
    def term_reader(self):
        return MultiTermReader([s.term_reader() for s in self._get_readers()],
                               self.index.doc_offsets)                           


class DocReader(object):
    def __init__(self, doc_index, doc_file):
        self.doc_index = doc_index
        self.doc_file = doc_file
        
    def next(self):
        control = self.doc_file.read_byte()
        if control == 0:
            self.term_count_multiplied = self.term_count_actual = self.doc_file.read_int()
        else:
            self.term_count_multiplied = self.doc_file.read_float()
            self.term_count_actual = self.doc_file.read_int()
        
        self.payload = self.doc_file.read_pickle()
        return (self.term_count_multiplied, self.term_count_actual, self.payload)
    
    def reset(self):
        self.doc_file.seek(0)
        
    def __iter__(self):
        self.reset()
        try:
            while True:
                yield self.next()
        except structfile.EndOfFile:
            raise StopIteration
        
    def __getitem__(self, docnum):
        self.doc_index.seek(docnum * _unsignedlong_size)
        self.doc_file.seek(self.doc_index.read_ulong())
        return self.next()
    
class MultiDocReader(object):
    def __init__(self, doc_readers):
        self.doc_readers = doc_readers
        self.term_count_multiplied = None
        self.term_count_actual = None
        self.payload = None
        self.current = 0
        self.reset()
    
    def reset(self):
        for r in self.docReaders:
            r.reset()
        self.current = 0
    
    def next(self):
        if self.current > len(self.doc_readers):
            return
        
        try:
            self.term_count_multiplied, self.term_count_actual, self.payload = self.doc_readers[self.current].next()
            return (self.term_count_multiplied, self.term_count_actual, self.payload)
        except structfile.EndOfFile:
            self.current += 1
            if self.current >= len(self.doc_readers):
                return
            return self.next()

class TermReader(object):
    def __init__(self, segment, schema, term_index, post_file):
        self.segment = segment
        self.schema = schema
        self.term_index = term_index
        self.post_file = post_file
        
        self.version = None
        self.postfile_offset = None
        
        self.index_skips()
        self.reset()
    
    def reset(self):
        term_index = self.term_index
        term_index.seek(0)
        
        self.version = term_index.read_int()
        assert self.version == -100
        
        term_index.read_int() # Reserved
        term_index.read_int() # Reserved
        term_index.read_int() # Reserved
        term_index.read_int() # Reserved
        
        self.state = 0 # 0 = on skip pointer, 1 = in block, -1 = last block
        self.next_block = 0
    
    def index_skips(self):
        self.reset()
        term_index = self.term_index
        
        skiplist = None #[(0, '', term_index.tell())]
        
        while True:
            here = term_index.tell()
            pointer = term_index.read_ulong()
            
            text = term_index.read_string()
            field_num = term_index.read_varint()
            
            if skiplist is None: skiplist = [(0, '', here)]
            skiplist.append((field_num, text, here))
            
            if pointer == 0:
                break
            term_index.seek(pointer)
        
        self.skiplist = skiplist
    
    def find_term(self, field_num, text):
        try:
            #print "find_term:", field_num, text
            self.seek_term(field_num, text)
            #print "       at:", self.field_num, self.text 
            if not(self.field_num == field_num and self.text == text):
                raise TermNotFound
        except structfile.EndOfFile:
            raise TermNotFound
    
    def seek_term(self, field_num, text):
        skipindex = bisect_left(self.skiplist, (field_num, text)) - 1
        assert skipindex >= 0
        self.term_index.seek(self.skiplist[skipindex][2])
        self.state = 0
        self.next()
        
        if not (self.field_num == field_num and self.text == text):
            while self.field_num < field_num or (self.field_num == field_num and self.text < text):
                self.next()
    
    def __iter__(self):
        try:
            while True:
                yield self.next()
        except structfile.EndOfFile:
            raise StopIteration
    
    def next(self):
        term_index = self.term_index
        
        if self.state == 1 and term_index.tell() == self.next_block:
            self.state = 0
            
        if self.state == 0:
            self.next_block = term_index.read_ulong()
            if self.next_block == 0:
                self.state = -1
            else:
                self.state = 1
                
        self.text = term_index.read_string().decode("utf8")
        self.field_num = term_index.read_varint()
        self.current_field = self.schema.by_number[self.field_num]
        
        self.doc_freq = term_index.read_varint()
        self.postfile_offset = term_index.read_ulong()
    
        return (self.field_num, self.text, self.doc_freq)
    
    def postings(self, exclude_docs = set()):
        is_deleted = self.segment.is_deleted
        
        post_file = self.post_file
        post_file.seek(self.postfile_offset)
        
        docnum = 0
        for i in xrange(0, self.doc_freq): #@UnusedVariable
            delta = post_file.read_varint()
            docnum += delta
            
            data = self.current_field.read_postvalue(post_file)
            
            if not is_deleted(docnum) and not docnum in exclude_docs:
                yield docnum, data
    
    def weights(self, exclude_docs = set(), boost = 1.0):
        field = self.current_field
        for docnum, data in self.postings(exclude_docs = exclude_docs):
            yield (docnum, field.data_to_weight(data) * boost)

    def positions(self, exclude_docs = set(), boost = 1.0):
        field = self.current_field
        for docnum, data in self.postings(exclude_docs = exclude_docs):
            yield (docnum, field.data_to_positions(data))

class MultiTermReader(TermReader):
    def __init__(self, term_readers, doc_offsets):
        self.term_readers = term_readers
        self.doc_offsets = doc_offsets
        self.reset()
        
    def reset(self):
        self.waitlist = None
        self.current_readers = None
        
        for r in self.term_readers:
            r.reset()
    
    def index_skips(self):
        raise NotImplemented
    
    def seek_term(self, field_num, text):
        for r in self.term_readers:
            r.reset()
            r.seek_term(field_num, text)
            
    def __iter__(self):
        while True:
            # This isn't really an infinite loop... it will
            # eventually pass through a StopIteration exception
            # from the next() method.
            
            yield self.next()
            
    def next(self):
        # This method does a merge sort of the terms coming off
        # the sub-readers.

        # The waiting list is a list of the "head" term from each
        # sub-reader, so we can sort them. On the first call to
        # next(), this is None.
        waitlist = self.waitlist
        
        # On first run, we need to fill in the waiting list. We do
        # this by making the code that follows, which replenishes
        # the list, replenish from ALL readers.
        if waitlist is None:
            waitlist = []
            self.current_readers = self.term_readers[:]
        
        # Replace the terms taken in the last iteration with new
        # terms. On the first call to next(), thanks to the code
        # above, this initializes the waiting list.
        
        if self.current_readers:
            for r in self.current_readers:
                field_num, text, doc_count = r.next()
                insort_left(waitlist, (field_num, text, doc_count, r))
        
        # Take the lowest term from the head of the waiting list.
        current = waitlist[0]
        
        # Set this reader's attributes to those of the term
        
        self.field_num = current[0]
        self.field = self.schema.by_number[self.field_num]
        self.text = current[1]
        doc_count = current[2]
        
        # We need to calculate the doc_count (doc frequency) by
        # adding up the doc counts from each reader with this
        # term. (If several readers include the same term, each
        # copy of the term should be at the head of the waiting list
        # right now).
        
        right = 1
        while right < len(waitlist) and waitlist[right][0] == field_num and waitlist[right][1] == text:
            doc_count += waitlist[right][2]
            right += 1
        
        self.doc_count = doc_count
        
        # Remember the readers that have the "current" term.
        # We'll need to iterate through them to get postings.
        # And on the next call to next(), we'll need to get
        # new terms from them to replenish the waiting list.
        
        self.currentReaders = [r for
                               fieldNum, text, docCount, r
                               in waitlist[:right] ]
        
        # Now that the readers are recorded, we can remove
        # the (one or more copies of the) current term from the
        # waiting list.
        self.waitlist = waitlist[right:]
        
        return (fieldNum, text, docCount)
        
    def postings(self):
        for i, r in enumerate(self.current_readers):
            for docnum, data in r.postings():
                self.docnum = docnum + self.doc_offsets[i]
                self.data = data
                yield self.docnum, data
                
    

















    
    
    