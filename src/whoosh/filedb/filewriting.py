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
from whoosh.filedb.filetables import (StoredFieldWriter, FileTableWriter,
                                      StructHashWriter)
from whoosh.filedb import misc
from whoosh.filedb.pools import TempfilePool
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
                reader = SegmentReader(ix.storage, seg)
                writer.add_reader(reader)
                reader.close()
            else:
                newsegments.append(seg)
    return newsegments


def OPTIMIZE(ix, writer, segments):
    """This policy merges all existing segments.
    """

    from whoosh.filedb.filereading import SegmentReader
    for seg in segments:
        reader = SegmentReader(ix.storage, seg)
        writer.add_reader(reader)
        reader.close()
    return SegmentSet()


class SegmentWriter(SegmentDeletionMixin, IndexWriter):
    def __init__(self, index, schema=None, poolclass=None, procs=0, blocklimit=128,
                 timeout=0.0, delay=0.1, lock=True, name=None, **poolargs):
        
        if lock:
            self.lock = index.storage.lock(index.indexname + "_LOCK")
            if not try_for(self.lock.acquire, timeout=timeout, delay=delay):
                raise LockError
        
        self.index = index
        self.schema = schema or self.index.schema
        self._name_to_num = dict((name, i) for i, name
                                 in enumerate(self.schema.names()))
        self.segments = self.index.segments.copy()
        self.blocklimit = 128
        
        self.name = name or self.index._next_segment_name()
        
        # Create a temporary segment to use its .*_filename attributes
        segment = Segment(self.name, self.schema, 0, 0, None, None)
        
        self.docnum = 0
        self.fieldlength_totals = defaultdict(int)
        
        storage = self.index.storage
        
        # Terms index
        tf = storage.create_file(segment.termsindex_filename)
        self.termsindex = FileTableWriter(tf,
                                          keycoder=misc.encode_termkey,
                                          valuecoder=misc.encode_terminfo)
        
        # Term postings file
        pf = storage.create_file(segment.termposts_filename)
        self.postwriter = FilePostingWriter(pf, blocklimit=blocklimit)
        
        if self.schema.has_vectored_fields():
            # Vector index
            vf = storage.create_file(segment.vectorindex_filename)
            self.vectorindex = StructHashWriter(vf, "!IH", "!I")
            
            # Vector posting file
            vpf = storage.create_file(segment.vectorposts_filename)
            self.vpostwriter = FilePostingWriter(vpf, stringids=True)
        else:
            self.vectorindex = None
            self.vpostwriter = None
        
        # Stored fields file
        sf = storage.create_file(segment.storedfields_filename)
        self.storedfields = StoredFieldWriter(sf, self.schema.stored_field_names())
        
        # Field lengths file
        self.lengthfile = storage.create_file(segment.fieldlengths_filename)
        
        # Create the pool
        if poolclass is None:
            if procs > 1:
                from whoosh.filedb.multiproc import MultiPool
                poolclass = MultiPool
            else:
                poolclass = TempfilePool
        self.pool = poolclass(self.schema, procs=procs, **poolargs)
    
    def searcher(self):
        return self.index.searcher()
    
    def add_reader(self, reader):
        startdoc = self.docnum
        
        has_deletions = reader.has_deletions()
        if has_deletions:
            docmap = {}
        
        schema = self.schema
        vectored_fieldnames = schema.vectored_field_names()
        scorable_fieldnames = schema.scorable_field_names()
        stored_fieldnames = schema.stored_field_names()
        
        # Add stored documents, vectors, and field lengths
        for docnum in xrange(reader.doc_count_all()):
            if (not has_deletions) or (not reader.is_deleted(docnum)):
                storeddict = reader.stored_fields(docnum)
                valuelist = [storeddict.get(name) for name in stored_fieldnames]
                self.storedfields.append(valuelist)
                
                if has_deletions:
                    docmap[docnum] = self.docnum
                
                for fieldname in scorable_fieldnames:
                    l = reader.doc_field_length(docnum, fieldname)
                    if l:
                        self.pool.add_field_length(self.docnum, fieldname)
                
                for fieldname in vectored_fieldnames:
                    if reader.has_vector(docnum, fieldname):
                        vpostreader = reader.vector(docnum, fieldname)
                        self.add_vector_reader(self.docnum, fieldname, vpostreader)
                
                self.docnum += 1
        
        fieldnames = set(schema.names())
        for fieldname, text, _, _ in reader:
            if fieldname in fieldnames:
                postreader = reader.postings(fieldname, text)
                while postreader.is_active():
                    docnum = postreader.id()
                    valuestring = postreader.value()
                    if has_deletions:
                        newdoc = docmap[docnum]
                    else:
                        newdoc = startdoc + docnum
                    
                    self.pool.add_posting(fieldname, text, newdoc,
                                          postreader.weight(), valuestring)
                    postreader.next()
    
    def add_document(self, **fields):
        schema = self.schema
        
        # Sort the keys
        fieldnames = sorted([name for name in fields.keys()
                             if not name.startswith("_")])
        
        # Check if the caller gave us a bogus field
        for name in fieldnames:
            if name not in schema:
                raise UnknownFieldError("No field named %r in %s" % (name, schema))
        
        storedvalues = []
        
        docnum = self.docnum
        for fieldname in fieldnames:
            value = fields.get(fieldname)
            if value is not None:
                field = schema[fieldname]
                
                if field.indexed:
                    self.pool.add_content(docnum, fieldname, field, value)
                
                vformat = field.vector
                if vformat:
                    vlist = sorted((w, weight, valuestring)
                                   for w, freq, weight, valuestring
                                   in vformat.word_values(value, mode="index"))
                    self.add_vector(docnum, fieldname, vlist)
                
                if field.stored:
                    # Caller can override the stored value by including a key
                    # _stored_<fieldname>
                    storedname = "_stored_" + name
                    if storedname in fields:
                        storedvalues.append(fields[storedname])
                    else:
                        storedvalues.append(value)
        
        self.storedfields.append(storedvalues)
        self.docnum += 1
    
    def add_vector(self, docnum, fieldid, vlist):
        vpostwriter = self.vpostwriter
        offset = vpostwriter.start(self.schema[fieldid].vector)
        for text, weight, valuestring in vlist:
            assert isinstance(text, unicode), "%r is not unicode" % text
            vpostwriter.write(text, weight, valuestring, 0)
        vpostwriter.finish()
        
        fnum = self._name_to_num[fieldid]
        self.vectorindex.add((docnum, fnum), offset)
    
    def add_vector_reader(self, docnum, fieldid, vreader):
        vpostwriter = self.vpostwriter
        offset = vpostwriter.start(self.schema[fieldid].vector)
        while vreader.is_active():
            # text, weight, valuestring, fieldlen
            vpostwriter.write(vreader.id(), vreader.weight(), vreader.value(), 0)
            vreader.next()
        vpostwriter.finish()
        
        fnum = self._name_to_num[fieldid]
        self.vectorindex.add((docnum, fnum), offset)
    
    def _close_all(self):
        self.termsindex.close()
        self.postwriter.close()
        self.storedfields.close()
        if not self.lengthfile.is_closed:
            self.lengthfile.close()
        if self.vectorindex:
            self.vectorindex.close()
        if self.vpostwriter:
            self.vpostwriter.close()
    
    def _getsegment(self):
        return Segment(self.name, self.schema, self.docnum,
                       self.pool.fieldlength_totals(),
                       self.pool.fieldlength_maxes())
    
    def commit(self, mergetype=MERGE_SMALL):
        # Call the merge policy function. The policy may choose to merge other
        # segments into this writer's pool
        new_segments = mergetype(self.index, self, self.segments)
        
        # Tell the pool we're finished adding information, it should add its
        # accumulated data to the lengths, terms index, and posting files.
        self.pool.finish(self.docnum, self.lengthfile, self.termsindex, self.postwriter)
        
        # Create a Segment object for the segment created by this writer and
        # add it to the list of remaining segments returned by the merge policy
        # function
        new_segments.append(self._getsegment())
        
        # Close all files, tell the index to write a new TOC with the new
        # segment list, and release the lock.
        self._close_all()
        self.index.commit(new_segments)
        self.lock.release()
        
    def cancel(self):
        self.pool.cancel()
        self._close_all()
        self.lock.release()




