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

from collections import defaultdict
from marshal import dumps

from whoosh.fields import UnknownFieldError
from whoosh.filedb.fileindex import SegmentDeletionMixin, Segment, SegmentSet
from whoosh.filedb.filepostings import FilePostingWriter
from whoosh.filedb.filetables import (FileListWriter, FileTableWriter,
                                      StructHashWriter, LengthWriter)
from whoosh.filedb import misc
from whoosh.filedb.pools import TempfilePool, MultiPool
from whoosh.store import LockError
from whoosh.support.filelock import try_for
from whoosh.util import fib
from whoosh.writing import IndexWriter


# Merge policies

# A merge policy is a callable that takes the Index object, the SegmentWriter
# object, and the current SegmentSet (not including the segment being written),
# and returns an updated SegmentSet (not including the segment being written).

def NO_MERGE(ix, writer, segments):
    """This policy does not merge any existing segments.
    """
    return segments


def MERGE_SMALL(ix, writer, segments):
    """This policy merges small segments, where "small" is defined using a
    heuristic based on the fibonacci sequence.
    """

    from whoosh.filedb.filereading import SegmentReader
    newsegments = SegmentSet()
    sorted_segment_list = sorted((s.doc_count_all(), s) for s in segments)
    total_docs = 0
    for i, (count, seg) in enumerate(sorted_segment_list):
        if count > 0:
            total_docs += count
            if total_docs < fib(i + 5):
                writer.add_reader(SegmentReader(ix.storage, seg, ix.schema))
            else:
                newsegments.append(seg)
    return newsegments


def OPTIMIZE(ix, writer, segments):
    """This policy merges all existing segments.
    """

    from whoosh.filedb.filereading import SegmentReader
    for seg in segments:
        writer.add_reader(SegmentReader(ix.storage, seg, ix.schema))
    return SegmentSet()


class SegmentWriter(SegmentDeletionMixin, IndexWriter):
    def __init__(self, ix, poolclass=None, procs=0, blocklimit=128,
                 timeout=0.0, delay=0.1, **poolargs):
        self.lock = ix.storage.lock(ix.indexname + "_LOCK")
        if not try_for(self.lock.acquire, timeout=timeout, delay=delay):
            raise LockError
        
        self.index = ix
        self.segments = ix.segments.copy()
        self.blocklimit = 128
        
        self.schema = ix.schema
        self.name = ix._next_segment_name()
        
        # Create a temporary segment to use its .*_filename attributes
        segment = Segment(self.name, 0, 0, None, None)
        
        self._searcher = ix.searcher()
        self.docnum = 0
        self.fieldlength_totals = defaultdict(int)
        
        storedfieldnames = ix.schema.stored_field_names()
        def encode_storedfields(fielddict):
            return dumps([fielddict.get(k) for k in storedfieldnames])
        
        storage = ix.storage
        
        # Terms index
        tf = storage.create_file(segment.termsindex_filename)
        self.termsindex = FileTableWriter(tf,
                                          keycoder=misc.encode_termkey,
                                          valuecoder=misc.encode_terminfo)
        
        # Term postings file
        pf = storage.create_file(segment.termposts_filename)
        self.postwriter = FilePostingWriter(pf, blocklimit=blocklimit)
        
        if ix.schema.has_vectored_fields():
            # Vector index
            vf = storage.create_file(segment.vectorindex_filename)
            self.vectorindex = StructHashWriter(vf, "<IH", "<I")
            
            # Vector posting file
            vpf = storage.create_file(segment.vectorposts_filename)
            self.vpostwriter = FilePostingWriter(vpf, stringids=True)
        else:
            self.vectorindex = None
            self.vpostwriter = None
        
        # Stored fields file
        sf = storage.create_file(segment.storedfields_filename)
        self.storedfields = FileListWriter(sf,
                                           valuecoder=encode_storedfields)
        
        # Field length file
        self.fieldlengths = storage.create_file(segment.fieldlengths_filename)
        
        # Create the pool
        if poolclass is None:
            if procs > 1:
                poolclass = MultiPool
                poolargs["procs"] = procs
            else:
                poolclass = TempfilePool
        self.pool = poolclass(self.fieldlengths, **poolargs)
    
    def searcher(self):
        return self.index.searcher()
    
    def add_reader(self, reader):
        startdoc = self.docnum
        
        has_deletions = reader.has_deletions()
        if has_deletions:
            docmap = {}
            
        schema = self.schema
        vectored_fieldnums = schema.vectored_fields()
        scorable_fieldnums = schema.scorable_fields()
        
        # Add stored documents, vectors, and field lengths
        for docnum in xrange(reader.doc_count_all()):
            if (not has_deletions) or (not reader.is_deleted(docnum)):
                stored = reader.stored_fields(docnum)
                self._add_stored_fields(stored)
                
                if has_deletions:
                    docmap[docnum] = self.docnum
                
                for fieldnum in scorable_fieldnums:
                    self.pool.add_field_length(self.docnum, fieldnum,
                                               reader.doc_field_length(docnum, fieldnum))
                for fieldnum in vectored_fieldnums:
                    if reader.has_vector(docnum, fieldnum):
                        self._add_vector(fieldnum,
                                         reader.vector(docnum, fieldnum).items())
                self.docnum += 1
        
        current_fieldnum = None
        decoder = None
        for fieldnum, text, _, _ in reader:
            if fieldnum != current_fieldnum:
                current_fieldnum = fieldnum
                decoder = schema[fieldnum].format.decode_frequency
                
            postreader = reader.postings(fieldnum, text)
            for docnum, valuestring in postreader.all_items():
                if has_deletions:
                    newdoc = docmap[docnum]
                else:
                    newdoc = startdoc + docnum
                
                # TODO: Is there a faster way to do this?
                freq = decoder(valuestring)
                self.pool.add_posting(fieldnum, text, newdoc, freq, valuestring)
        
    def add_document(self, **fields):
        schema = self.schema
        name2num = schema.name_to_number
        
        # Sort the keys by their order in the schema
        fieldnames = [name for name in fields.keys() if not name.startswith("_")]
        fieldnames.sort(key=name2num)
        
        # Check if the caller gave us a bogus field
        for name in fieldnames:
            if name not in schema:
                raise UnknownFieldError("There is no field named %r" % name)
            
        storedvalues = {}
        
        docnum = self.docnum
        for name in fieldnames:
            value = fields.get(name)
            if value:
                fieldnum = name2num(name)
                field = schema.field_by_number(fieldnum)
                
                if field.indexed:
                    self.pool.add_content(docnum, fieldnum, field, value)
                
                vformat = field.vector
                if vformat:
                    vlist = sorted((w, valuestring) for w, freq, valuestring
                                   in vformat.word_values(value, mode="index"))
                    self._add_vector(fieldnum, vlist)
                    
                if field.stored:
                    # Caller can override the stored value by including a key
                    # _stored_<fieldname>
                    storedname = "_stored_" + name
                    if storedname in fields:
                        storedvalues[name] = fields[storedname]
                    else:
                        storedvalues[name] = value
                        
        self._add_stored_fields(storedvalues)
        self.docnum += 1
    
    def _add_stored_fields(self, storeddict):
        self.storedfields.append(storeddict)
        
    def _add_vector(self, fieldnum, vlist):
        vpostwriter = self.vpostwriter
        vformat = self.schema[fieldnum].vector
        
        offset = vpostwriter.start(vformat)
        for text, valuestring in vlist:
            assert isinstance(text, unicode), "%r is not unicode" % text
            vpostwriter.write(text, valuestring)
        vpostwriter.finish()
        
        self.vectorindex.add((self.docnum, fieldnum), offset)
    
    def _close_all(self):
        self.termsindex.close()
        self.postwriter.close()
        if self.vectorindex:
            self.vectorindex.close()
        if self.vpostwriter:
            self.vpostwriter.close()
        self.storedfields.close()
        if not self.fieldlengths.is_closed:
            self.fieldlengths.close()
        
    def commit(self, mergetype=MERGE_SMALL):
        # Call the merge policy function. The policy may choose to merge other
        # segments into this writer's pool
        new_segments = mergetype(self.index, self, self.segments)
        
        # Tell the pool we're finished adding information, it should add its
        # accumulated data to the terms index and posting file.
        self.pool.finish(self.schema, self.docnum, self.termsindex, self.postwriter)
        
        # Create a Segment object for the segment created by this writer and
        # add it to the list of remaining segments returned by the merge policy
        # function
        thissegment = Segment(self.name, self.docnum,
                              self.pool.fieldlength_totals(),
                              self.pool.fieldlength_maxes())
        new_segments.append(thissegment)
        
        # Close all files, tell the index to write a new TOC with the new
        # segment list, and release the lock.
        self._close_all()
        self.index.commit(new_segments)
        self.lock.release()
        
    def cancel(self):
        self.pool.cancel()
        self._close_all()
        self.lock.release()




