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

"""
This module contains classes for writing to an index.
"""

from collections import defaultdict

from whoosh import postpool, reading, index
from whoosh.reading import UnknownFieldError
from whoosh.tables import TableWriter, PostingTableWriter #, RecordWriter
from whoosh.util import fib

# Exceptions

class IndexingError(Exception):
    pass

# Constants

# TODO: it might be better style to have these classes actually
# implement the given merge policy, rather than just using them
# as constants.

class NO_MERGE(object): pass
class MERGE_SMALL(object): pass
class OPTIMIZE(object): pass

# Writing classes

class IndexWriter(object):
    """
    High-level object for writing to an index. This object takes care of
    instantiating a SegmentWriter to create a new segment as you add documents,
    as well as merging existing segments (if necessary) when you finish.
    """
    
    # This class is mostly a shell for SegmentWriter. It exists to handle
    # multiple SegmentWriters during merging/optimizing.
    
    def __init__(self, ix, blocksize = 32 * 1024):
        """
        index is the Index object representing the index you want to write to.
        """
        
        # Obtain a lock
        self.locked = ix.lock()
        
        self.index = ix
        self.segments = ix.segments.copy()
        self.blocksize = blocksize
        self.segment_writer = None
    
    def get_segment_writer(self):
        """
        Returns the underlying SegmentWriter object.
        """
        
        if not self.segment_writer:
            self.segment_writer = SegmentWriter(self.index, blocksize = self.blocksize)
        return self.segment_writer
    
    def start_document(self):
        """
        Starts recording information for a new document. This should be followed by
        add_field() calls, and must be followed by an end_document() call.
        Alternatively you can use add_document() to add all fields at once.
        """
        self.get_segment_writer().start_document()
        
    def add_field(self, fieldname, value, stored_value = None):
        """
        Adds a the value of a field to the document opened with start_document().
        """
        self.segment_writer.add_field(fieldname, value, stored_value = stored_value)
        
    def end_document(self):
        """
        Closes a document opened with start_document().
        """
        self.segment_writer.end_document()
    
    def add_document(self, **fields):
        """
        Adds all the fields of a document at once. This is an alternative to calling
        start_document(), add_field() [...], end_document().
        
        The keyword args map field names to the values to store and/or index.
        
        The default for stored and indexed fields is to store the indexed
        source text, but you can optionally specify a specific string to store by
        including a "_stored_<fieldname>" key in the 'fields' dictionary.
        """
        self.get_segment_writer().add_document(fields)
    
    def optimize(self):
        """
        If the index has multiple segments, merges them into a single
        segment.
        """
        self._merge_segments(OPTIMIZE)
    
    def close(self, mergetype = MERGE_SMALL):
        """
        Finishes writing and unlocks the index.
        """
        
        if self.segment_writer:
            self._merge_segments(mergetype)
        
        # Release the lock
        if self.locked:
            self.index.unlock()
    
    def _merge_segments(self, mergetype):
        sw = self.get_segment_writer()
        
        segments = self.segments
        new_segments = index.SegmentSet()
        
        if mergetype is OPTIMIZE:
            # Merge all segments
            for seg in segments:
                sw.add_segment(self.index, seg)
        else:
            # Find sparse segments and merge them into the segment
            # currently being written.
            
            sorted_segment_list = sorted((s.doc_count_all(), s) for s in segments)
            total_docs = 0
            
            if mergetype is not NO_MERGE:
                # Merge sparse segments into the one we're
                # currently writing
                for i, (count, seg) in enumerate(sorted_segment_list):
                    if count > 0:
                        total_docs += count
                        if total_docs < fib(i + 5):
                            sw.add_segment(self.index, seg)
                        else:
                            new_segments.append(seg)
            else:
                new_segments = segments
        
        self.segment_writer.close()
        new_segments.append(sw.segment())
        
        # TODO: This means iw.optimize() silently commits! Is there a better way?
        self.segment_writer = None
        self.index.commit(new_segments)


class SegmentWriter(object):
    """
    Do not instantiate this object directly; it is created by the IndexWriter object.
    
    Handles the actual writing of new documents to the index: writes stored fields,
    handles the posting pool, and writes out the term index.
    """
    
    class DocumentState(object):
        def __init__(self):
            self.reset()
        
        def reset(self):
            self.active = False
            # term_count: the number of terms in the document (a.k.a. document length)
            self.term_count = 0
            # unique_count: the number of UNIQUE terms in the document
            self.unique_count = 0
            # field_counts: number of terms in each scorable field. The positions in
            # the list correspond to the positions in the scorable_fields list.
            self.field_counts = defaultdict(int)
            # stored_fields: Maps field name -> stored field content for this document
            self.stored_fields = {}
            # Shortcut if you have the docinfo already.
            self.docinfo = None
            # Keep track of the last field that was added.
            self.prev_fieldnum = None
    
    def __init__(self, ix, name = None, blocksize = 32 * 1024):
        """
        index is the Index object representing the index in which to write the new segment.
        name is the name of the segment. zipmode is either zipfile.ZIP_DEFLATED or
        zipfile.ZIP_STORED; this whether stored fields are compressed. The default is
        ZIP_DEFLATED.
        """
        
        self.index = ix
        self.schema = ix.schema
        self.storage = ix.storage
        self.name = name or ix.next_segment_name()
        
        self.max_doc = 0
        self.term_count = 0
        self.max_count = 0
        self.field_counts = defaultdict(int)
        
        # Records the state of the writer wrt start_document/end_document.
        # None == not "in" a document.
        self._doc_state = SegmentWriter.DocumentState()
        self._scorable_fields = self.schema.scorable_fields()
        
        self.pool = postpool.PostingPool(self.name)
        
        # Create a temporary segment object just so we can access
        # its *_filename attributes (so if we want to change the
        # naming convention, we only have to do it in one place).
        tempseg = index.Segment(self.name, 0, 0, 0, None)
        
        # Open files for writing
        term_file = self.storage.create_file(tempseg.term_filename)
        self.term_table = PostingTableWriter(term_file, blocksize = blocksize)
        
        doclength_file = self.storage.create_file(tempseg.doclen_filename)
        
        #recordfmt = "!ii" + ("i" * len(self.schema.scorable_fields()))
        self.doclength_records = TableWriter(doclength_file) #RecordWriter(doclength_file, recordfmt)
        
        docs_file = self.storage.create_file(tempseg.docs_filename)
        self.docs_table = TableWriter(docs_file, blocksize = blocksize, compressed = 9)
        
        self.vector_table = None
        if self.schema.has_vectored_fields():
            vector_file = self.storage.create_file(tempseg.vector_filename)
            self.vector_table = PostingTableWriter(vector_file, stringids = True)
            
    def segment(self):
        """
        Returns an index.Segment object for the segment being written.
        """
        return index.Segment(self.name, self.max_doc,
                             self.term_count, self.max_count,
                             dict(self.field_counts))
    
    def close(self):
        """
        Finishes writing the segment (flushes the posting pool out to disk) and
        closes all open files.
        """
        
        if self._doc_state.active:
            raise IndexingError("Called SegmentWriter.close() with a document still opened")
        
        self._flush_pool()
        
        self.doclength_records.close()
        self.docs_table.close()
        self.term_table.close()
        
        if self.vector_table:
            self.vector_table.close()
        
    def add_index(self, other_ix):
        """
        Adds the contents of another Index object to this segment.
        This currently does NO checking of whether the schemas match up.
        """
        
        for seg in other_ix.segments:
            self.add_segment(other_ix, seg)

    def add_segment(self, ix, segment):
        """
        Adds the contents of another segment to this one. This is used
        to merge existing segments into the new one before deleting them.
        """
        
        start_doc = self.max_doc
        has_deletions = segment.has_deletions()
        
        if has_deletions:
            doc_map = {}
        
        # Merge document info
        docnum = 0
        schema = ix.schema
        
        try:
            doc_reader = reading.DocReader(ix.storage, segment, schema)
            _doc_info = doc_reader._doc_info
            
            vectored_fieldnums = ix.schema.vectored_fields()
            if vectored_fieldnums:
                doc_reader._open_vectors()
                inv = doc_reader.vector_table
                outv = self.vector_table
        
            ds = SegmentWriter.DocumentState()
            for docnum in xrange(0, segment.max_doc):
                if not segment.is_deleted(docnum):
                    doclen = doc_reader.doc_length(docnum)
                    ds.term_count = doclen
                    ds.unique_count = doc_reader.unique_count(docnum)
                    ds.stored_fields = doc_reader[docnum]
                    
                    self.term_count += doclen
                    
                    if has_deletions:
                        doc_map[docnum] = self.max_doc
                    
                    for fieldnum in vectored_fieldnums:
                        if (docnum, fieldnum) in inv:
                            data, count, postings = inv._raw_data((docnum, fieldnum))
                            outv._add_raw_data((self.max_doc, fieldnum), data, count, postings)
                    
                    fcs = ds.field_counts
                    for fieldnum in self._scorable_fields:
                        fcs[fieldnum] = doc_reader.doc_field_length(docnum, fieldnum)
                    
                    self._write_doc_entry(ds)
                    self.max_doc += 1
                
                docnum += 1
        finally:
            doc_reader.close()
        
        # Merge terms
        term_reader = reading.TermReader(ix.storage, segment, ix.schema)
        try:
            for fieldnum, text, _, _ in term_reader:
                for docnum, data in term_reader.postings(fieldnum, text):
                    if has_deletions:
                        newdoc = doc_map[docnum]
                    else:
                        newdoc = start_doc + docnum
                    
                    self.pool.add_posting(fieldnum, text, newdoc, data)
        finally:
            term_reader.close()

    def start_document(self):
        ds = self._doc_state
        if ds.active:
            raise IndexingError("Called start_document() when a document was already opened")
        ds.active = True
    
    def end_document(self):
        ds = self._doc_state
        if not ds.active:
            raise IndexingError("Called end_document() when a document was not opened")
        
        self._write_doc_entry(ds)
        ds.reset()
        self.max_doc += 1

    def add_document(self, fields):
        self.start_document()
        fieldnames = [name for name in fields.keys() if not name.startswith("_")]
        
        schema = self.schema
        for name in fieldnames:
            if name not in schema:
                raise UnknownFieldError("There is no field named %r" % name)
        
        fieldnames.sort(key = schema.name_to_number)
        for name in fieldnames:
            self.add_field(name, fields[name], stored_value = fields.get("_stored_%s" % name))
        self.end_document()
    
    def add_field(self, fieldname, value, stored_value = None,
                  start_pos = 0, start_char = 0, **kwargs):
        if value is None:
            return
        
        # Get the field information
        schema = self.schema
        if fieldname not in schema:
            raise UnknownFieldError("There is no field named %r" % fieldname)
        fieldnum = schema.name_to_number(fieldname)
        field = schema.field_by_name(fieldname)
        format = field.format
        
        # Check that the user added the fields in lexical order
        docstate = self._doc_state
        if fieldnum < docstate.prev_fieldnum:
            raise IndexingError("Added field %r out of order (add fields in schema order)" % fieldname)
        docstate.prev_fieldnum = fieldnum

        # If the field is indexed, add the words in the value to the index
        if format.analyzer:
            if not isinstance(value, unicode):
                raise ValueError("%r in field %s is not unicode" % (value, fieldname))
            
            # Count of all terms in the value
            count = 0
            # Count of UNIQUE terms in the value
            unique = 0
            for w, freq, data in format.word_datas(value,
                                                   start_pos = start_pos, start_char = start_char,
                                                   **kwargs):
                assert w != ""
                self.pool.add_posting(fieldnum, w, self.max_doc, data)
                count += freq
                unique += 1
            
            # Add the term count to the total for this field
            self.field_counts[fieldnum] += count
            # Add the term count to the total for the entire index
            self.term_count += count
            
            if field.scorable:
                # Add the term count to the field length for this document
                docstate.field_counts[fieldnum] += count
            
            # Add the term count to the total for this document
            docstate.term_count += count
            # Add to the number of unique terms in this document
            docstate.unique_count += unique
        
        # If the field is vectored, add the words in the value to
        # the vector table
        vector = field.vector
        if vector:
            vtable = self.vector_table
            vdata = dict((w, data) for w, freq, data
                          in vector.word_datas(value,
                                               start_pos = start_pos, start_char = start_char,
                                               **kwargs))
            write_postvalue = vector.write_postvalue
            for word in sorted(vdata.keys()):
                vtable.write_posting(word, vdata[word], writefn = write_postvalue)
            vtable.add_row((self.max_doc, fieldnum))
        
        # If the field is stored, add the value to the doc state
        if field.stored:
            if stored_value is None: stored_value = value
            docstate.stored_fields[fieldname] = stored_value
        
    def _write_doc_entry(self, ds):
        #if ds.docinfo:
        #    docinfo = ds.docinfo
        #else:
        #    fc = ds.field_counts
        #    docinfo = [ds.term_count, ds.unique_count] + [fc[n] for n in sorted(fc.keys())]
        #self.doclength_records.add(*docinfo)
        docnum = self.max_doc
        
        self.doclength_records.add_row((docnum, -2), ds.unique_count)
        self.doclength_records.add_row((docnum, -1), ds.term_count)
        fcs = ds.field_counts
        for fieldnum in self._scorable_fields:
            self.doclength_records.add_row((docnum, fieldnum), fcs[fieldnum])
            
        self.docs_table.add_row(docnum, ds.stored_fields)

    def _flush_pool(self):
        # This method pulls postings out of the posting pool (built up
        # as documents are added) and writes them to the posting file.
        # Each time it encounters a posting for a new term, it writes
        # the previous term to the term index (by waiting to write the
        # term entry, we can easily count the document frequency and
        # sum the terms by looking at the postings).
        
        term_table = self.term_table
        
        write_posting_method = None
        current_fieldnum = None # Field number of the current term
        current_text = None # Text of the current term
        first = True
        term_count = 0
        
        # Loop through the postings in the pool.
        # Postings always come out of the pool in field number/alphabetic order.
        for fieldnum, text, docnum, data in self.pool:
            # If we're starting a new term, reset everything
            
            if write_posting_method is None or fieldnum > current_fieldnum or text > current_text:
                if fieldnum != current_fieldnum:
                    write_posting_method = self.schema.field_by_number(fieldnum).format.write_postvalue
                
                # If we've already written at least one posting, write the
                # previous term to the index.
                if not first:
                    term_table.add_row((current_fieldnum, current_text), term_count)
                    
                    if term_count > self.max_count:
                        self.max_count = term_count
                
                # Reset term variables
                current_fieldnum = fieldnum
                current_text = text
                term_count = 0
                first = False
            
            elif fieldnum < current_fieldnum or (fieldnum == current_fieldnum and text < current_text):
                # This should never happen!
                raise Exception("Postings are out of order: %s:%s .. %s:%s" %
                                (current_fieldnum, current_text, fieldnum, text))
            
            term_count += term_table.write_posting(docnum, data, write_posting_method)
        
        # Finish up the last term
        if not first:
            term_table.add_row((current_fieldnum, current_text), term_count)
            if term_count > self.max_count:
                self.max_count = term_count


if __name__ == '__main__':
    pass


        
        
