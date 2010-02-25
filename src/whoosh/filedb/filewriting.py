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
                                      StructHashWriter)
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


class FileIndexWriter(SegmentDeletionMixin, IndexWriter):
    def __init__(self, ix, poolclass=None, procs=0, blocklimit=128,
                 timeout=0.0, delay=0.1, **poolargs):
        self.lock = ix.storage.lock(ix.indexname + "_LOCK")
        if not try_for(self.lock.acquire, timeout=timeout, delay=delay):
            raise LockError
        
        self.index = ix
        self.blocklimit = 128
        
        self.schema = ix.schema
        self.name = ix._next_segment_name()
        
        # Create a temporary segment to use its .*_filename attributes
        segment = Segment(self.name, 0, 0, None)
        
        self._searcher = ix.searcher()
        self.docnum = 0
        self.fieldlength_totals = defaultdict(int)
        
        storedfieldnames = ix.schema.stored_field_names()
        def encode_storedfields(fielddict):
            return dumps([fielddict.get(k) for k in storedfieldnames])
        
        storage = ix.storage
        
        # Term index
        tf = storage.create_file(segment.term_filename)
        self.termsindex = FileTableWriter(tf,
                                          keycoder=misc.encode_termkey,
                                          valuecoder=misc.encode_terminfo)
        
        # Term posting file
        pf = storage.create_file(segment.posts_filename)
        self.postwriter = FilePostingWriter(pf, blocklimit=blocklimit)
        
        # Vector index
        vf = storage.create_file(segment.vector_filename)
        self.vectorindex = StructHashWriter(vf, "!IH", "!I")
        
        # Vector posting file
        vpf = storage.create_file(segment.vectorposts_filename)
        self.vpostwriter = FilePostingWriter(vpf, stringids=True)
        
        # Stored fields file
        sf = storage.create_file(segment.docs_filename)
        self.storedfields = FileListWriter(sf,
                                           valuecoder=encode_storedfields)
        
        # Field length file
        flf = storage.create_file(segment.doclen_filename)
        self.fieldlengths = StructHashWriter(flf, "!IH", "!I")
        
        # Create the pool
        if poolclass is None:
            if procs:
                poolclass = MultiPool
                poolargs["procs"] = procs
            else:
                poolclass = TempfilePool
        self.pool = poolclass(self.fieldlengths, **poolargs)
        
    def add_reader(self, reader):
        startdoc = self.docnum
        
        has_deletions = reader.has_deletions()
        if has_deletions:
            docmap = {}
            
        schema = self.schema
        vectored_fieldnums = schema.vectored_fields()
        scored_fieldnums = schema.scored_fields()
        
        # Add stored documents, vectors, and field lengths
        for docnum in xrange(reader.doc_count_all()):
            if (not has_deletions) or (not reader.is_deleted(docnum)):
                stored = reader.stored_fields(docnum)
                self._add_stored_fields(stored)
                
                if has_deletions:
                    docmap[docnum] = self.docnum
                
                for fieldnum in scored_fieldnums:
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
    
    def _finish(self):
        self._close_reader()
        self.termsindex.close()
        self.postwriter.close()
        self.vectorindex.close()
        self.vpostwriter.close()
        self.storedfields.close()
        self.fieldlengths.close()
        
        self.lock.release()
    
    def commit(self, mergetype=MERGE_SMALL):
        self.pool.finish(self.schema, self.termsindex, self.postwriter)
        
        thissegment = Segment(self.name, self.docnum,
                              self.pool.fieldlength_totals())
        
        new_segments = mergetype(self.index, self, self.index.segments.copy())
        new_segments.append(thissegment)
        self.index.commit(new_segments)
        
        self._finish()
        
    def cancel(self):
        self.pool.cancel()
        self._finish()




