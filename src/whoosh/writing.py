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

from array import array
from collections import defaultdict
from tempfile import TemporaryFile

from whoosh import index, postpool, reading, structfile, tables
from whoosh.fields import UnknownFieldError
from whoosh.util import fib

# Exceptions

class IndexingError(Exception):
    pass


DOCLENGTH_TYPE = "H"
DOCLENGTH_LIMIT = 2**16-1


# Merge policies

# A merge policy is a callable that takes the Index object,
# the SegmentWriter object, and the current SegmentSet
# (not including the segment being written), and returns an
# updated SegmentSet (not including the segment being
# written).

def NO_MERGE(ix, writer, segments):
    """This policy does not merge any existing segments.
    """
    return segments


def MERGE_SMALL(ix, writer, segments):
    """This policy merges small segments, where small is
    defined using a heuristic based on the fibonacci sequence.
    """
    
    newsegments = index.SegmentSet()
    sorted_segment_list = sorted((s.doc_count_all(), s) for s in segments)
    total_docs = 0
    for i, (count, seg) in enumerate(sorted_segment_list):
        if count > 0:
            total_docs += count
            if total_docs < fib(i + 5):
                writer.add_segment(ix, seg)
            else:
                newsegments.append(seg)
    return newsegments


def OPTIMIZE(ix, writer, segments):
    """This policy merges all existing segments.
    """
    for seg in segments:
        writer.add_segment(ix, seg)
    return index.SegmentSet()


# Writing classes

class IndexWriter(index.DeletionMixin):
    """High-level object for writing to an index. This object takes care of
    instantiating a SegmentWriter to create a new segment as you add documents,
    as well as merging existing segments (if necessary) when you finish.
    
    You can use this object as a context manager. If an exception is thrown
    from within the context it calls cancel(), otherwise it calls commit()
    when the context ends.
    """
    
    # This class is mostly a shell for SegmentWriter. It exists to handle
    # multiple SegmentWriters during merging/optimizing.
    
    def __init__(self, ix, postlimit = 4 * 1024 * 1024,
                 term_blocksize = 1 * 1024, doc_blocksize = 8 * 1024,
                 vector_blocksize = 8 * 1024):
        """
        :ix: the Index object you want to write to.
        :blocksize: the block size for tables created by this writer.
        """
        
        # Obtain a lock
        self.locked = ix.lock()
        
        self.index = ix
        self.segments = ix.segments.copy()
        self.postlimit = postlimit
        self.term_blocksize = term_blocksize
        self.doc_blocksize = doc_blocksize
        self.vector_blocksize = vector_blocksize
        self._segment_writer = None
        self._searcher = None
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.cancel()
        else:
            self.commit()
    
    def _finish(self):
        self._segment_writer = None
        # Release the lock
        if self.locked:
            self.index.unlock()
    
    def segment_writer(self):
        """Returns the underlying SegmentWriter object."""
        
        if not self._segment_writer:
            self._segment_writer = SegmentWriter(self.index, self.postlimit,
                                                 self.term_blocksize,
                                                 self.doc_blocksize, self.vector_blocksize)
        return self._segment_writer
    
    def searcher(self):
        """Returns a searcher for the existing index."""
        if not self._searcher:
            self._searcher = self.index.searcher()
        return self._searcher
    
    def start_document(self):
        """Starts recording information for a new document. This should be followed by
        add_field() calls, and must be followed by an end_document() call.
        Alternatively you can use add_document() to add all fields at once.
        """
        self.segment_writer().start_document()
        
    def add_field(self, fieldname, text, stored_value = None):
        """Adds a the value of a field to the document opened with start_document().
        
        :fieldname: The name of the field in which to index/store the text.
        :text: The unicode text to index.
        """
        self.segment_writer().add_field(fieldname, text, stored_value = stored_value)
        
    def end_document(self):
        """
        Closes a document opened with start_document().
        """
        self.segment_writer().end_document()
    
    def add_document(self, **fields):
        """Adds all the fields of a document at once. This is an alternative to calling
        start_document(), add_field() [...], end_document().
        
        The keyword arguments map field names to the values to index/store.
        
        For fields that are both indexed and stored, you can specify an alternate
        value to store using a keyword argument in the form "_stored_<fieldname>".
        For example, if you have a field named "title" and you want to index the
        text "a b c" but store the text "e f g", use keyword arguments like this::
        
            add_document(title=u"a b c" _stored_title=u"e f g)
        """
        self.segment_writer().add_document(fields)
    
    def update_document(self, **fields):
        """Adds or replaces a document. At least one of the fields for which you
        supply values must be marked as 'unique' in the index's schema.
        
        The keyword arguments map field names to the values to index/store.
        
        For fields that are both indexed and stored, you can specify an alternate
        value to store using a keyword argument in the form "_stored_<fieldname>".
        For example, if you have a field named "title" and you want to index the
        text "a b c" but store the text "e f g", use keyword arguments like this::
        
            update_document(title=u"a b c" _stored_title=u"e f g)
        """
        
        # Check which of the supplied fields are unique
        unique_fields = [name for name, field
                         in self.index.schema.fields()
                         if name in fields and field.unique]
        if not unique_fields:
            raise IndexingError("None of the fields in %r are unique" % fields.keys())
        
        # Delete documents in which the supplied unique fields match
        searcher = self.searcher()
        for name in unique_fields:
            self.delete_by_term(name, fields[name], searcher = searcher)
        
        # Add the given fields
        self.add_document(**fields)
    
    def commit(self, mergetype = MERGE_SMALL):
        """Finishes writing and unlocks the index.
        
        :mergetype: How to merge existing segments. One of
            writing.NO_MERGE, writing.MERGE_SMALL, or writing.OPTIMIZE.
        """
        
        if self._segment_writer or mergetype is OPTIMIZE:
            self._merge_segments(mergetype)
        self.index.commit(self.segments)
        self._finish()
        
    def cancel(self):
        """Cancels any documents/deletions added by this object
        and unlocks the index.
        """
        self._finish()
    
    def _merge_segments(self, mergetype):
        sw = self.segment_writer()
        new_segments = mergetype(self.index, sw, self.segments)
        sw.close()
        new_segments.append(sw.segment())
        self.segments = new_segments


class SegmentWriter(object):
    """
    Do not instantiate this object directly; it is created by the IndexWriter object.
    
    Handles the actual writing of new documents to the index: writes stored fields,
    handles the posting pool, and writes out the term index.
    """
    
    class DocumentState(object):
        def __init__(self, scorable_fields):
            self._scorable_fields = scorable_fields
            self._fieldnum_to_pos = dict((fnum, i) for i, fnum in enumerate(scorable_fields))
            self.reset()
        
        def add_to_length(self, fieldnum, n):
            pos = self._fieldnum_to_pos[fieldnum]
            current = self.field_lengths[pos]
            if current >= DOCLENGTH_LIMIT: return
            self.field_lengths[pos] = min(current + n, DOCLENGTH_LIMIT)
        
        def reset(self):
            #: Whether a document is currently in progress
            self.active = False
            #: Maps field names to stored field contents for this document
            self.stored_fields = {}
            #: Keeps track of the last field that was added
            self.prev_fieldnum = None
            #: Keeps track of field lengths in this document
            self.field_lengths = array(DOCLENGTH_TYPE, [0] * len(self._scorable_fields))
    
    def __init__(self, ix, postlimit,
                 term_blocksize, doc_blocksize, vector_blocksize,
                 name = None):
        """
        :ix: the Index object in which to write the new segment.
        :postlimit: the maximum size for a run in the posting pool.
        :name: the name of the segment.
        :blocksize: the block size to use for tables created by this writer.
        """
        
        self.index = ix
        self.schema = ix.schema
        self.storage = ix.storage
        self.name = name or ix._next_segment_name()
        
        self.max_doc = 0
        self.max_weight = 0
        
        self.pool = postpool.PostingPool(limit = postlimit)
        self._scorable_fields = self.schema.scorable_fields()
        
        # Create a temporary segment object just so we can access
        # its *_filename attributes (so if we want to change the
        # naming convention, we only have to do it in one place).
        tempseg = index.Segment(self.name, 0, 0, None)
        
        # Open files for writing
        self.term_table = self.storage.create_table(tempseg.term_filename, postings = True,
                                                    blocksize = term_blocksize)
        
        self.doclength_table = self.storage.create_records(tempseg.doclen_filename,
                                                           DOCLENGTH_TYPE,
                                                           len(self._scorable_fields))
        
        self.docs_table = self.storage.create_table(tempseg.docs_filename,
                                                    blocksize = doc_blocksize, compressed = 9)
        
        self.vector_table = None
        if self.schema.has_vectored_fields():
            self.vector_table = self.storage.create_table(tempseg.vector_filename,
                                                          postings = True,
                                                          stringids = True,
                                                          blocksize = vector_blocksize)
        
        # Keep track of the total number of tokens (across all docs)
        # in each field
        self.field_length_totals = defaultdict(int)
        # Records the state of the writer's current document
        self._doc_state = SegmentWriter.DocumentState(self._scorable_fields)
            
    def segment(self):
        """Returns an index.Segment object for the segment being written."""
        return index.Segment(self.name, self.max_doc, self.max_weight,
                             dict(self.field_length_totals))
    
    def close(self):
        """Finishes writing the segment (flushes the posting pool out to disk) and
        closes all open files.
        """
        
        if self._doc_state.active:
            raise IndexingError("Called SegmentWriter.close() with a document still opened")
        
        self._flush_pool()
        
        self.doclength_table.close()
        
        self.docs_table.close()
        self.term_table.close()
        
        if self.vector_table:
            self.vector_table.close()
        
    def add_index(self, other_ix):
        """Adds the contents of another Index object to this segment.
        This currently does NO checking of whether the schemas match up.
        """
        
        for seg in other_ix.segments:
            self.add_segment(other_ix, seg)

    def add_segment(self, ix, segment):
        """Adds the contents of another segment to this one. This is used
        to merge existing segments into the new one before deleting them.
        
        :ix: The index.Index object containing the segment to merge.
        :segment: The index.Segment object to merge into this one.
        """
        
        start_doc = self.max_doc
        has_deletions = segment.has_deletions()
        
        if has_deletions:
            doc_map = {}
        
        # Merge document info
        docnum = 0
        schema = ix.schema
        
        doc_reader = reading.DocReader(ix.storage, segment, schema)
        try:
            vectored_fieldnums = ix.schema.vectored_fields()
            if vectored_fieldnums:
                doc_reader._open_vectors()
                inv = doc_reader.vector_table
                outv = self.vector_table
            
            ds = SegmentWriter.DocumentState(self._scorable_fields)
            for docnum in xrange(0, segment.max_doc):
                if not segment.is_deleted(docnum):
                    ds.stored_fields = doc_reader[docnum]
                    ds.field_lengths = doc_reader.doc_field_lengths(docnum)
                    
                    if has_deletions:
                        doc_map[docnum] = self.max_doc
                    
                    for fieldnum in vectored_fieldnums:
                        if (docnum, fieldnum) in inv:
                            tables.copy_data(inv, (docnum, fieldnum),
                                             outv, (self.max_doc, fieldnum),
                                             postings = True)
                    
                    self._write_doc_entry(ds)
                    self.max_doc += 1
                
                docnum += 1
        
            # Add field length totals
            for fieldnum, total in segment.field_length_totals.iteritems():
                self.field_length_totals[fieldnum] += total
        
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
            value = fields.get(name)
            if value:
                self.add_field(name, value, stored_value = fields.get("_stored_%s" % name))
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
        
        # Check that the user added the fields in schema order
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
            self.field_length_totals[fieldnum] += count
            # Add the term count to the per-document field length
            if field.scorable:
                docstate.add_to_length(fieldnum, count)
        
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
            vtable.add_row((self.max_doc, fieldnum), None)
        
        # If the field is stored, add the value to the doc state
        if field.stored:
            if stored_value is None: stored_value = value
            docstate.stored_fields[fieldname] = stored_value
        
    def _write_doc_entry(self, ds):
        docnum = self.max_doc
        self.doclength_table.append(ds.field_lengths)
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
        current_weight = 0
        
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
                    term_table.add_row((current_fieldnum, current_text), current_weight)
                    
                    if current_weight > self.max_weight:
                        self.max_weight = current_weight
                
                # Reset term variables
                current_fieldnum = fieldnum
                current_text = text
                current_weight = 0
                first = False
            
            elif fieldnum < current_fieldnum or (fieldnum == current_fieldnum and text < current_text):
                # This should never happen!
                raise Exception("Postings are out of order: %s:%s .. %s:%s" %
                                (current_fieldnum, current_text, fieldnum, text))
            
            current_weight += term_table.write_posting(docnum, data, write_posting_method)
        
        # Finish up the last term
        if not first:
            term_table.add_row((current_fieldnum, current_text), current_weight)
            if current_weight > self.max_weight:
                self.max_weight = current_weight



if __name__ == '__main__':
    pass


        
        
