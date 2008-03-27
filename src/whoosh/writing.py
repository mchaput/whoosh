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

import cPickle
from collections import defaultdict

import postpool, reading, index
from util import fib


_min_skip_size = 4 * 1024


class IndexWriter(object):
    def __init__(self, index):
        self.index = index
        self.segment_writer = None
    
    def get_segment_writer(self):
        if not self.segment_writer:
            self.segment_writer = SegmentWriter(self.index)
        return self.segment_writer
    
    def add_document(self, **fields):
        writer = self.get_segment_writer()
        writer.add_document(fields)
    
    def optimize(self):
        self._merge_segments(True)
    
    def close(self):
        if self.segment_writer:
            self._finish()
        del self.index
    
    def _finish(self):
        if self.segment_writer:
            self._merge_segments(False)
    
    def _merge_segments(self, all):
        # This method is called in two different ways.
        # It's called with all=False when the user has used this
        # writer to add documents to a new segment, and then calls
        # close(). In that case, we'll look for sparse segments to
        # merge in at the same time before we write out the new
        # segment.
        # 
        # It's also called when someone creates an IndexWriter and
        # calls optimize() on it (usually through Index.optimize()).
        # In that case, all = True, which means merge all segments
        # into a new segment no matter how big or small.
        
        sw = self.get_segment_writer()
        
        segment_list = self.index.segments[:]
        if not all:
            segment_list.sort(key = lambda x: x.doc_count())
        new_segment_list = []
        
        if all:
            # Merge all segments
            for seg in segment_list:
                sw.add_segment(reading.SegmentReader(self.index, seg))
        else:
            # Find sparse segments
            total_docs = 0
            for i, seg in enumerate(segment_list):
                total_docs += seg.max_doc
                if total_docs < fib(i + 5):
                    sw.add_segment(reading.SegmentReader(self.index, seg))
                else:
                    new_segment_list.append(seg)
        
        new_segment_list.append(index.Segment(sw.name,
                                              sw.max_doc,
                                              sw.term_total,
                                              sw.term_count,
                                              sw.max_weight))
        
        del sw
        self.segment_writer.close()
        self.segment_writer = None
        
        self.index._set_segments(new_segment_list)
        self.index.checkpoint()

class SegmentWriter(object):
    def __init__(self, index, name = None):
        self.index = index
        self.schema = index.schema
        self.storage = index.storage
        self.name = name or index.next_segment_name()
        
        #self.lexicon = {}
        #self.wordnum = 0
        
        self.max_doc = 0
        self.term_total = 0
        self.term_count = 0
        self.max_weight = 0.0
        
        self.pool = postpool.PostingPool(self.storage)

        self.docs_index = self.storage.create_file(self.name + ".dcx")
        self.docs_file = self.storage.create_file(self.name + ".dcs")
        self.term_index = self.storage.create_file(self.name + ".tix")
        self.post_file = self.storage.create_file(self.name + ".pst")
        self.forward_index = self.storage.create_file(self.name + ".fix")
        
        self.last_skip_pointer = None
        
    def close(self):
        self.flush_pool()
        
        self.docs_index.close()
        self.docs_file.close()
        self.term_index.close()
        self.post_file.close()
        self.forward_index.close()

    def add_index(self, other_ix):
        for s in other_ix.segments:
            self.add_segment(reading.SegmentReader(other_ix, s))

    def add_segment(self, reader):
        start_doc = self.max_doc
        has_deletions = reader.has_deletions()
        
        if has_deletions:
            doc_map = {}
        
        dr = reader.doc_reader()
        docnum = 0
        for term_total, term_count, payload in dr:
            if not reader.is_deleted(docnum):
                if has_deletions:
                    doc_map[docnum] = self.max_doc
                self.write_doc_entry(term_total, term_count, payload)
                self.max_doc += 1
            
            docnum += 1
        
        tr = reader.term_reader()
        for field_num, text, doc_count in tr: #@UnusedVariable
            for docnum, data in tr.postings():
                if has_deletions:
                    newdoc = doc_map[docnum]
                else:
                    newdoc = start_doc + docnum
                
                self.pool.add_posting(field_num, text, newdoc, data)

    def write_term_ix_header(self):
        self.term_index.write_int(-100) # version
        self.term_index.write_int(0) # reserved
        self.term_index.write_int(0) # reserved
        self.term_index.write_int(0) # reserved
        self.term_index.write_int(0) # reserved

    def write_doc_entry(self, term_total, term_count, payload):
        docs_file = self.docs_file
        
        self.docs_index.write_ulong(docs_file.tell())
        
        if term_total == term_count:
            docs_file.write_byte(0)
        else:
            docs_file.write_byte(1)
            docs_file.write_float(term_total)
        docs_file.write_int(term_count)
        docs_file.write_pickle(payload)

    def write_term_entry(self, field_num, text, doc_count, total_weight, post_offset):
        term_index = self.term_index
        te = text.encode("utf-8")
        
        lsp = self.last_skip_pointer
        here = term_index.tell()
        
        if lsp is None:
            # At the very beginning of writing
            term_index.write_ulong(0)
            self.last_skip_pointer = here
        else:
            new_skip_start = distance = here
            distance -= lsp
            
            if distance > _min_skip_size:
                new_lsp = term_index.tell()
                
                if lsp:
                    term_index.seek(lsp)
                    term_index.write_ulong(new_skip_start)
                    term_index.seek(new_lsp)
                
                term_index.write_ulong(0)
                self.last_skip_pointer = new_lsp
        
        term_index.write_string(te)
        term_index.write_varint(field_num)
        term_index.write_varint(doc_count)
        term_index.write_float(total_weight)
        term_index.write_ulong(post_offset)

    def flush_pool(self):
        fields_by_number = self.schema.by_number
        
        self.pool.finish()
        post_file = self.post_file
        
        write_posting_method = None
        current_field_num = None # Field number of the current term
        current_text = None # Text of the current term
        doc_freq = None
        doc_base = None # Base for doc frequency deltas
        post_offset = None # Offset into the postings file
        
        self.write_term_ix_header()
        
        term_weight = 0.0
        
        for field_num, text, docnum, data in self.pool:
            # If we're starting a new term...
            if write_posting_method is None or field_num > current_field_num or text > current_text:
                if term_weight > self.max_weight:
                    self.max_weight = term_weight
                
                write_posting_method = fields_by_number[field_num].write_postvalue
                
                # If this is not the very first term...
                if post_offset is not None:
                    assert doc_freq > 0
                    
                    # Write the term index entry for the PREVIOUS term.
                    # This lets us write out doc_freq (the number of documents
                    # the term appears in).
                    self.write_term_entry(current_field_num,
                                          current_text,
                                          doc_freq,
                                          term_weight,
                                          post_offset)
                
                # Reset term variables
                post_offset = post_file.tell()
                current_field_num = field_num
                current_text = text
                doc_freq = 0
                doc_base = 0
                term_weight = 0.0
            
            elif field_num < current_field_num or (field_num == current_field_num and text < current_text):
                raise Exception("Postings are out of order: %s:%s .. %s:%s" %
                                (current_field_num, current_text, field_num, text))
            
            # Docnum delta
            post_file.write_varint(docnum - doc_base)
            # Posting data
            term_weight += write_posting_method(post_file, data)
            
            doc_base = docnum
            doc_freq += 1
        
        self.pool.delete_runs()
    
    def add_document(self, fields):
        docnum = self.max_doc
        self.max_doc += 1
        
        payload = {}
        tcm = 0
        tca = 0
        
        for name, value in fields.iteritems():
            if value is not None:
                field = self.schema.by_name[name]
                
                if isinstance(value, (list, tuple)):
                    if len(value) != 2:
                        raise ValueError("Sequence should only have two items (found %s)" % repr(value))
                    elif not (field.indexed and field.stored):
                        raise ValueError("Sequence only valid when a field is both indexed and stored")
                    
                    value, storedvalue = value
                else:
                    storedvalue = value
                
                if field.indexed:
                    if not isinstance(value, unicode):
                        raise ValueError("Indexable content must be unicode (found %s of type %s in field %s)" % (repr(value), type(value), field.name))
                    
                    #map = defaultdict(float)
                    field_tcm, field_tca = self.add_words(docnum, field, value)
                    
                    #if field.options.get("forward_indexed"):
                    #    self.forward_index.write_int(len(map))
                    #    for w in sorted(map.keys()):
                    #        self.forward_index.write_int(self.lexicon[w])
                    #        self.forward_index.write_float(map[w])
                    
                    tcm += field_tcm
                    tca += field_tca
                
                if field.stored:
                    payload[name] = storedvalue
        
        self.write_doc_entry(tcm, tca, payload)
    
    def add_words(self, docnum, field, value, start_pos = 0):
        total = 0
        count = 0
        
        if isinstance(field, basestring):
            field = self.schema.by_name[field]
        boost = field.field_boost
        
        for w, data in field.word_datas(value, start_pos = start_pos):
            #if w not in self.lexicon:
            #    self.lexicon[w] = self.wordnum
            #    self.wordnum += 1
            
            assert w != ""
            self.pool.add_posting(field.number, w, docnum, data)
            
            #map[w] += boost
            total += boost
            count += 1
            
        self.term_total += total
        self.term_count += count
        return (total, count)

        
        
        
