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

from array import array
from collections import defaultdict

from whoosh.fields import UnknownFieldError
from whoosh.writing import IndexWriter, IndexingError
from whoosh.filedb import postpool
from whoosh.filedb.fileindex import SegmentDeletionMixin, Segment, SegmentSet
from whoosh.filedb.filereading import FileTermReader, FileDocReader
from whoosh.filedb.filetables import copy_postings
from whoosh.filedb.filetables import create_docs_table, create_term_table, create_vector_table
from whoosh.util import fib


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
    
    newsegments = SegmentSet()
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
    return SegmentSet()


# Writing classes

class FileIndexWriter(SegmentDeletionMixin, IndexWriter):
    # This class is mostly a shell for SegmentWriter. It exists to handle
    # multiple SegmentWriters during merging/optimizing.
    
    def __init__(self, ix, postlimit = 4 * 1024 * 1024):
        """
        :param ix: the Index object you want to write to.
        :param postlimit: Essentially controls the maximum amount of memory the
            indexer uses at a time, in bytes (the actual amount of memory used by
            the Python process will be much larger because of other overhead).
            The default (4MB) is quite small. You should increase this value
            for large collections, e.g. ``postlimit=32*1024*1024``.
        """
        
        # Obtain a lock
        self.locked = ix.lock()
        
        self.index = ix
        self.segments = ix.segments.copy()
        self.postlimit = postlimit
        self._segment_writer = None
        self._searcher = ix.searcher()
    
    def _finish(self):
        self._close_searcher()
        self._segment_writer = None
        # Release the lock
        if self.locked:
            self.index.unlock()
    
    def segment_writer(self):
        """Returns the underlying SegmentWriter object."""
        
        if not self._segment_writer:
            self._segment_writer = SegmentWriter(self.index, self.postlimit)
        return self._segment_writer
    
    def add_document(self, **fields):
        self.segment_writer().add_document(fields)
    
    def commit(self, mergetype = MERGE_SMALL):
        """Finishes writing and unlocks the index.
        
        :param mergetype: How to merge existing segments. One of
            :class:`whoosh.filedb.filewriting.NO_MERGE`,
            :class:`whoosh.filedb.filewriting.MERGE_SMALL`,
            or :class:`whoosh.filedb.filewriting.OPTIMIZE`.
        """
        
        self._close_searcher()
        if self._segment_writer or mergetype is OPTIMIZE:
            self._merge_segments(mergetype)
        self.index.commit(self.segments)
        self._finish()
        
    def cancel(self):
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
    
    def __init__(self, ix, postlimit, name = None):
        """
        :param ix: the Index object in which to write the new segment.
        :param postlimit: the maximum size for a run in the posting pool.
        :param name: the name of the segment.
        """
        
        self.index = ix
        self.schema = ix.schema
        self.storage = ix.storage
        self.name = name or ix._next_segment_name()
        
        self.max_doc = 0

        self.pool = postpool.PostingPool(limit = postlimit)
        self._scorable_to_pos = dict((fnum, i) for i, fnum in enumerate(self.schema.scorable_fields()))
        self._stored_to_pos = dict((fnum, i) for i, fnum in enumerate(self.schema.stored_fields()))
        
        # Create a temporary segment object just so we can access
        # its *_filename attributes (so if we want to change the
        # naming convention, we only have to do it in one place).
        tempseg = Segment(self.name, 0, 0, None)
        
        # Open files for writing
        self.term_table = create_term_table(self.storage, tempseg)
        self.docs_table = create_docs_table(self.storage, tempseg)
        
        recordformat = "<" + DOCLENGTH_TYPE * len(self._scorable_to_pos)
        self.doclength_table = self.storage.create_records(tempseg.doclen_filename,
                                                           recordformat)
        
        self.vector_table = None
        if self.schema.has_vectored_fields():
            self.vector_table = create_vector_table(self.storage, tempseg)
        
        # Keep track of the total number of tokens (across all docs)
        # in each field
        self.field_length_totals = defaultdict(int)
            
    def segment(self):
        """Returns an index.Segment object for the segment being written."""
        return Segment(self.name, self.max_doc, dict(self.field_length_totals))
    
    def close(self):
        """Finishes writing the segment (flushes the posting pool out to disk) and
        closes all open files.
        """
        
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
        
        :param ix: The index.Index object containing the segment to merge.
        :param segment: The index.Segment object to merge into this one.
        """
        
        start_doc = self.max_doc
        has_deletions = segment.has_deletions()
        
        if has_deletions:
            doc_map = {}
        
        # Merge document info
        docnum = 0
        schema = ix.schema
        name2num = schema.name_to_number
        stored_to_pos = self._stored_to_pos
        
        def storedkeyhelper(item):
            return stored_to_pos[name2num(item[0])]
        
        doc_reader = FileDocReader(ix.storage, segment, schema)
        try:
            vectored_fieldnums = ix.schema.vectored_fields()
            if vectored_fieldnums:
                doc_reader._open_vectors()
                inv = doc_reader.vector_table
                outv = self.vector_table
            
            for docnum in xrange(segment.max_doc):
                if not segment.is_deleted(docnum):
                    # Copy the stored fields and field lengths from the other segment into this one
                    storeditems = doc_reader[docnum].items()
                    storedvalues = [v for k, v in sorted(storeditems, key=storedkeyhelper)]
                    self._add_doc_data(storedvalues, doc_reader.doc_field_lengths(docnum))
                    
                    if has_deletions:
                        doc_map[docnum] = self.max_doc
                    
                    # Copy term vectors
                    for fieldnum in vectored_fieldnums:
                        if (docnum, fieldnum) in inv:
                            copy_postings(inv, (docnum, fieldnum),
                                          outv, (self.max_doc, fieldnum))
                
                    self.max_doc += 1
            
            # Add field length totals
            for fieldnum, total in segment.field_length_totals.iteritems():
                self.field_length_totals[fieldnum] += total
        
        finally:
            doc_reader.close()
        
        # Merge terms
        term_reader = FileTermReader(ix.storage, segment, ix.schema)
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

    def add_document(self, fields):
        scorable_to_pos = self._scorable_to_pos
        stored_to_pos = self._stored_to_pos
        
        fieldnames = [name for name in fields.keys() if not name.startswith("_")]
        
        schema = self.schema
        for name in fieldnames:
            if name not in schema:
                raise UnknownFieldError("There is no field named %r" % name)
        
        fieldlengths = array(DOCLENGTH_TYPE, [0] * len(scorable_to_pos))
        storedvalues = [None] * len(stored_to_pos)
        
        fieldnames.sort(key = schema.name_to_number)
        for name in fieldnames:
            value = fields.get(name)
            
            if value:
                fieldnum = schema.name_to_number(name)
                field = schema.field_by_name(name)
                format = field.format
                
                # If the field is indexed, add the words in the value to the index
                if format.analyzer:
                    if not isinstance(value, unicode):
                        raise ValueError("%r in field %s is not unicode" % (value, name))
                    
                    # Count of all terms in the value
                    count = 0
                    # Count of UNIQUE terms in the value
                    unique = 0
                    
                    # TODO: Method for adding progressive field values, ie setting
                    # start_pos/start_char?
                    for w, freq, data in format.word_datas(value):
                        #assert w != ""
                        self.pool.add_posting(fieldnum, w, self.max_doc, data)
                        count += freq
                        unique += 1
                    
                    if field.scorable:
                        # Add the term count to the total for this field
                        self.field_length_totals[fieldnum] += count
                        # Set the term count to the per-document field length
                        fieldlengths[scorable_to_pos[fieldnum]] = count
                
                # If the field is vectored, add the words in the value to
                # the vector table
                vector = field.vector
                if vector:
                    vtable = self.vector_table
                    # TODO: Method for adding progressive field values, ie setting
                    # start_pos/start_char?
                    vdata = dict((w, data) for w, freq, data in vector.word_datas(value,))
                    write_postvalue = vector.write_postvalue
                    for word in sorted(vdata.keys()):
                        vtable.write_posting(word, vdata[word], writefn = write_postvalue)
                    vtable.add((self.max_doc, fieldnum), '')
                
                # If the field is stored, add the value to the doc state
                if field.stored:
                    storedname = "_stored_" + name
                    if storedname in fields:
                        stored_value = fields[storedname]
                    else :
                        stored_value = value
                    storedvalues[stored_to_pos[fieldnum]] = stored_value
        
        self._add_doc_data(storedvalues, fieldlengths)
        self.max_doc += 1
    
    def _add_doc_data(self, storedvalues, fieldlengths):
        self.docs_table.append(storedvalues)
        self.doclength_table.append(fieldlengths)
    
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
                    term_table.add((current_fieldnum, current_text), current_weight)
                    
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
            term_table.add((current_fieldnum, current_text), current_weight)




        
        
