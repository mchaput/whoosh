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

from bisect import bisect_left, bisect_right, insort_left
from whoosh.structfile import StructFile

import structfile

_unsignedlong_size = structfile._unsignedlong_size
_int_size = structfile._int_size

class TermNotFound(Exception): pass

class SegmentReader(object):
    def __init__(self, index, segment):
        self.index = index
        self.storage = index.storage
        self.schema = index.schema
        self.segment = segment
        
        self.doc_index = self.storage.open_file(segment.name + ".dcx")
        self.doc_file = self.storage.open_file(segment.name + ".dcs")
        self.term_index = self.storage.open_file(segment.name + ".tix")
        self.post_file = self.storage.open_file(segment.name + ".pst")
    
    def doc_count(self):
        return self.index.doc_count()
    def max_weight(self):
        return self.index.max_weight()
    def term_total(self):
        return self.index.term_total()
    def term_count(self):
        return self.index.term_count()
    
    def close(self):
        del self.index
        
        self.doc_index.close()
        self.doc_file.close()
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
    
    def term_frequency(self, fieldname, text):
        tr = self.term_reader()
        try:
            tr.find_term(self.schema.name_to_number(fieldname), text)
            return tr.doc_freq
        except TermNotFound:
            return 0
        
    
    def field_terms(self, fieldname):
        tr = self.term_reader()
        field_num = self.schema.name_to_number(fieldname)
        tr.seek_term(field_num, '')
        while tr.field_num == field_num:
            yield tr.text
            tr.next()
    
    def run_query(self, q):
        return q.run(self.term_reader())
    
    def stored(self, docnum):
        return self.doc_reader()[docnum]
    
    def doc(self, **kw):
        for p in self.docs(**kw):
            return p
        
    def docs(self, **kw):
        tr = self.term_reader()
        results = set()
        for k, v in kw.iteritems():
            fieldnum = self.schema.name_to_number(k)
            
            if isinstance(v, unicode):
                v = (v, )
            elif isinstance(v, (tuple, list)):
                pass
            elif isinstance(v, str):
                raise ValueError("Search values must be unicode ('%s' for field '%s')" % (v, k))
            else:
                raise ValueError("Don't know what to do with value '%s' for field '%s'" % (v, k))
            
            for value in v:
                tr.find_term(fieldnum, value)
                results &= set([docnum for docnum, _ in tr.postings()])
        
        dr = self.doc_reader()
        for docnum in results:
            if not self.is_deleted(docnum):
                yield dr[docnum]
    
class MultiSegmentReader(SegmentReader):
    def __init__(self, index, segments):
        self.index = index
        self.segments = segments
        self.readers = None
    
    def is_deleted(self, docnum):
        return self.index.is_deleted(docnum)
    
    def close(self):
        if self.readers:
            for r in self.readers: r.close()
    
    def _get_readers(self):
        if not self.readers:
            self.readers = [SegmentReader(self.index, s) for s in self.segments]
        return self.readers
    
    def doc_reader(self):
        return MultiDocReader([s.doc_reader() for s in self._get_readers()],
                              self.index.doc_offsets)
    
    def term_reader(self):
        return MultiTermReader([s.term_reader() for s in self._get_readers()],
                               self.index.doc_offsets)                           


class DocReader(object):
    def __init__(self, doc_index, doc_file):
        self.doc_index = doc_index
        self.doc_file = doc_file
    
    def find(self, docnum):
        self.doc_index.seek(docnum * _unsignedlong_size)
        self.doc_file.seek(self.doc_index.read_ulong())
        return self.next()
    
    def next(self):
        try:
            control = self.doc_file.read_byte()
            if control == 0:
                self.term_total = self.term_count = self.doc_file.read_int()
            else:
                self.term_total = self.doc_file.read_float()
                self.term_count = self.doc_file.read_int()
            
            return (self.term_total, self.term_count)
        except structfile.EndOfFile:
            return None
    
    def reset(self):
        self.doc_file.seek(0)
    
    def payload(self):
        return self.doc_file.read_pickle()
    
    def total(self, docnum):
        return self.find(docnum)[0]
    
    def count(self, docnum):
        return self.find(docnum)[1]
    
    def __iter__(self):
        self.reset()
        try:
            while True:
                yield self.next()
        except structfile.EndOfFile:
            raise StopIteration
        
    def __getitem__(self, docnum):
        self.find(docnum)
        return self.payload()
    
class MultiDocReader(DocReader):
    def __init__(self, doc_readers, doc_offsets):
        self.doc_readers = doc_readers
        self.doc_offsets = doc_offsets
        self.term_total = None
        self.term_count = None
        self._payload = None
        self.current = 0
        self.reset()
    
    def _document_segment(self, docnum):
        if len(self.doc_offsets) == 1: return 0
        return bisect_left(self.doc_offsets, docnum)
    
    def _segment_and_docnum(self, docnum):
        segmentnum = self._document_segment(docnum)
        offset = self.doc_offsets[segmentnum]
        return segmentnum, docnum - offset
    
    def find(self, docnum):
        current, docn = self._document_segment(docnum)
        self.current = current
        return self.doc_readers[current].find(docn)
    
    def next(self):
        if self.current > len(self.doc_readers):
            return
        
        try:
            self.term_total, self.term_count = self.doc_readers[self.current].next()
            self._payload = self.doc_readers[self.current].payload()
            return (self.term_total, self.term_count)
        except structfile.EndOfFile:
            self.current += 1
            if self.current >= len(self.doc_readers):
                return
            return self.next()
    
    def payload(self):
        return self.doc_readers[self.current].payload()
    
    def reset(self):
        for r in self.docReaders:
            r.reset()
        self.current = 0
    
class TermReader(object):
    def __init__(self, segment, schema, term_index, post_file, preindex = True):
        self.segment = segment
        self.schema = schema
        self.term_index = term_index
        self.post_file = post_file
        
        self.version = None
        self.postfile_offset = None
        
        self.skiplist = None
        if preindex:
            self.index_skips()
        
        self.reset()
    
    def reset(self):
        term_index = self.term_index
        term_index.seek(0)
        
        self.version = term_index.read_int()
        assert self.version == -100 # Version
        assert term_index.read_int() == 0 # Reserved
        assert term_index.read_int() == 0 # Reserved
        assert term_index.read_int() == 0 # Reserved
        assert term_index.read_int() == 0 # Reserved
        
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
            self.seek_term(field_num, text)
            if not(self.field_num == field_num and self.text == text):
                raise TermNotFound
        except structfile.EndOfFile:
            raise TermNotFound
    
    def seek_term(self, field_num, text):
        if not self.skiplist:
            self.index_skips()
        
        skipindex = bisect_left(self.skiplist, (field_num, text)) - 1
        
        if skipindex >= 0:
            self.term_index.seek(self.skiplist[skipindex][2])
            self.state = 0
            self.next()
        else:
            self.reset()
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
        self.total_weight = term_index.read_float()
        self.postfile_offset = term_index.read_ulong()
        
        return (self.field_num, self.text, self.doc_freq)
    
    def postings(self, exclude_docs = set()):
        is_deleted = self.segment.is_deleted
        
        post_file = self.post_file
        post_file.seek(self.postfile_offset)
        
        readfn = self.current_field.read_postvalue
        
        docnum = 0
        for i in xrange(0, self.doc_freq): #@UnusedVariable
            delta = post_file.read_varint()
            docnum += delta
            
            data = readfn(post_file)
            
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
        raise NotImplementedError
    
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
                field_num, text, doc_freq = r.next()
                insort_left(waitlist, (field_num, text, doc_freq, r))
        
        # Take the lowest term from the head of the waiting list.
        current = waitlist[0]
        
        # Set this reader's attributes to those of the term
        
        self.field_num = current[0]
        self.field = self.schema.by_number[self.field_num]
        self.text = current[1]
        
        doc_freq = current[2]
        total_weight = current[3].total_weight
        
        # We need to calculate the doc_freq (doc frequency) by
        # adding up the doc counts from each reader with this
        # term. (If several readers include the same term, each
        # copy of the term should be at the head of the waiting list
        # right now).
        
        right = 1
        while right < len(waitlist) and waitlist[right][0] == field_num and waitlist[right][1] == text:
            doc_freq += waitlist[right][2]
            total_weight += waitlist[right][3].total_weight
            right += 1
        
        self.doc_count = doc_freq
        self.total_weight = total_weight
        
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
                
    
def read_all_docs(reader):
    dr = reader.doc_reader()
    dr.reset()
    cache = {}
    try:
        while True:
            dr.next()
            fields = dr.payload()
            cache[fields["path"]] = fields
    except EOFError:
        pass
    return cache

def create_quick_index(reader, fieldname, outfile):
    import array
    arr = array.array('l')
    
    dr = reader.doc_reader()
    dr.reset()
    try:
        while True:
            dr.next()
            fields = dr.payload()
            arr.append(o.tell())
            outfile.write_string(fields[fieldname].encode("utf-8"))
    except EOFError:
        pass

    return arr

def create_stem_map(reader, fieldnames):
    if isinstance(fieldnames, basestring):
        fieldnames = [fieldnames]
    
    from support.porter import stem
    from collections import defaultdict
    map = defaultdict(set)
    for fieldname in fieldnames:
        for w in reader.field_terms(fieldname):
            s = stem(w)
            if s != w:
                map[s].add(w)
    return map


if __name__ == '__main__':
    import time
    import index
    ix = index.open_dir("c:/workspace/Help2/test_index")
    r = ix.reader()
    
    o = ix.storage.create_file("title.qix")
    t = time.time()
    arr = create_quick_index(r, "title", o)
    print time.time() - t
    
    o.close()
    
    o = ix.storage.open_file("title.qix")
    
    import random
    ls = [random.randint(0, 6000) for i in xrange(0, 100)]
    
    dr = r.doc_reader()
    t = time.clock()
    for dn in ls:
        f = dr[dn]["title"]
    print time.clock() - t
    
    t = time.clock()
    for dn in ls:
        o.seek(arr[dn])
        f = o.read_string().decode("utf-8")
    print time.clock() - t
    













    
    
    