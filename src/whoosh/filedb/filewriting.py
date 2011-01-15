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

from bisect import bisect_right
from collections import defaultdict

from whoosh.fields import UnknownFieldError
from whoosh.filedb.fileindex import Segment
from whoosh.filedb.filepostings import FilePostingWriter
from whoosh.filedb.filetables import (TermIndexWriter, StoredFieldWriter,
                                      TermVectorWriter)
from whoosh.filedb.pools import TempfilePool
from whoosh.reading import TermNotFound
from whoosh.store import LockError
from whoosh.support.filelock import try_for
from whoosh.util import fib
from whoosh.writing import IndexWriter, IndexingError


# Merge policies

# A merge policy is a callable that takes the Index object, the SegmentWriter
# object, and the current segment list (not including the segment being written),
# and returns an updated segment list (not including the segment being written).

def NO_MERGE(writer, segments):
    """This policy does not merge any existing segments.
    """
    return segments


def MERGE_SMALL(writer, segments):
    """This policy merges small segments, where "small" is defined using a
    heuristic based on the fibonacci sequence.
    """

    from whoosh.filedb.filereading import SegmentReader
    newsegments = []
    sorted_segment_list = sorted((s.doc_count_all(), s) for s in segments)
    total_docs = 0
    for i, (count, seg) in enumerate(sorted_segment_list):
        if count > 0:
            total_docs += count
            if total_docs < fib(i + 5):
                reader = SegmentReader(writer.storage, writer.schema, seg)
                writer.add_reader(reader)
                reader.close()
            else:
                newsegments.append(seg)
    return newsegments


def OPTIMIZE(writer, segments):
    """This policy merges all existing segments.
    """

    from whoosh.filedb.filereading import SegmentReader
    for seg in segments:
        reader = SegmentReader(writer.storage, writer.schema, seg)
        writer.add_reader(reader)
        reader.close()
    return []


# Writer object

class SegmentWriter(IndexWriter):
    def __init__(self, ix, poolclass=None, procs=0, blocklimit=128,
                 timeout=0.0, delay=0.1, name=None, lock=True, **poolargs):
        
        self.writelock = None
        if lock:
            self.writelock = ix.lock("WRITELOCK")
            if not try_for(self.writelock.acquire, timeout=timeout, delay=delay):
                raise LockError
        
        self.ix = ix
        self.storage = ix.storage
        self.indexname = ix.indexname
        self.is_closed = False
        
        info = ix._read_toc()
        self.schema = info.schema
        self.segments = info.segments
        self.blocklimit = blocklimit
        self.segment_number = info.segment_counter + 1
        self.generation = info.generation + 1
        
        self._doc_offsets = []
        base = 0
        for s in self.segments:
            self._doc_offsets.append(base)
            base += s.doc_count_all()
        
        self.name = name or Segment.basename(self.indexname, self.segment_number)
        self.docnum = 0
        self.fieldlength_totals = defaultdict(int)
        self._added = False
        self._unique_cache = {}
    
        # Create a temporary segment to use its .*_filename attributes
        segment = Segment(self.name, self.generation, 0, None, None)
        
        # Terms index
        tf = self.storage.create_file(segment.termsindex_filename)
        self.termsindex = TermIndexWriter(tf)
        
        # Term postings file
        pf = self.storage.create_file(segment.termposts_filename)
        self.postwriter = FilePostingWriter(pf, blocklimit=blocklimit)
        
        if self.schema.has_vectored_fields():
            # Vector index
            vf = self.storage.create_file(segment.vectorindex_filename)
            self.vectorindex = TermVectorWriter(vf)
            
            # Vector posting file
            vpf = self.storage.create_file(segment.vectorposts_filename)
            self.vpostwriter = FilePostingWriter(vpf, stringids=True)
        else:
            self.vectorindex = None
            self.vpostwriter = None
        
        # Stored fields file
        sf = self.storage.create_file(segment.storedfields_filename)
        self.storedfields = StoredFieldWriter(sf, self.schema.stored_names())
        
        # Field lengths file
        self.lengthfile = self.storage.create_file(segment.fieldlengths_filename)
        
        # Create the pool
        if poolclass is None:
            if procs > 1:
                from whoosh.filedb.multiproc import MultiPool
                poolclass = MultiPool
            else:
                poolclass = TempfilePool
        self.pool = poolclass(self.schema, procs=procs, **poolargs)
    
    def _check_state(self):
        if self.is_closed:
            raise IndexingError("This writer is closed")
    
    def add_field(self, fieldname, fieldspec):
        self._check_state()
        if self._added:
            raise Exception("Can't modify schema after adding data to writer")
        super(SegmentWriter, self).add_field(fieldname, fieldspec)
    
    def remove_field(self, fieldname):
        self._check_state()
        if self._added:
            raise Exception("Can't modify schema after adding data to writer")
        super(SegmentWriter, self).remove_field(fieldname)
    
    def _document_segment(self, docnum):
        #Returns the index.Segment object containing the given document
        #number.

        offsets = self._doc_offsets
        if len(offsets) == 1: return 0
        return bisect_right(offsets, docnum) - 1

    def _segment_and_docnum(self, docnum):
        #Returns an (index.Segment, segment_docnum) pair for the segment
        #containing the given document number.

        segmentnum = self._document_segment(docnum)
        offset = self._doc_offsets[segmentnum]
        segment = self.segments[segmentnum]
        return segment, docnum - offset

    def has_deletions(self):
        """
        :returns: True if this index has documents that are marked deleted but
            haven't been optimized out of the index yet.
        """
        
        return any(s.has_deletions() for s in self.segments)

    def delete_document(self, docnum, delete=True):
        self._check_state()
        segment, segdocnum = self._segment_and_docnum(docnum)
        segment.delete_document(segdocnum, delete=delete)

    def deleted_count(self):
        """
        :returns: the total number of deleted documents in the index.
        """
        
        return sum(s.deleted_count() for s in self.segments)

    def is_deleted(self, docnum):
        segment, segdocnum = self._segment_and_docnum(docnum)
        return segment.is_deleted(segdocnum)

    def searcher(self):
        self._check_state()
        from whoosh.filedb.fileindex import FileIndex
        return FileIndex(self.storage, indexname=self.indexname).searcher()
    
    def add_reader(self, reader):
        self._check_state()
        startdoc = self.docnum
        
        has_deletions = reader.has_deletions()
        if has_deletions:
            docmap = {}
        
        fieldnames = set(self.schema.names())
        
        # Add stored documents, vectors, and field lengths
        for docnum in xrange(reader.doc_count_all()):
            if (not has_deletions) or (not reader.is_deleted(docnum)):
                d = dict(item for item
                         in reader.stored_fields(docnum).iteritems()
                         if item[0] in fieldnames)
                # We have to append a dictionary for every document, even if
                # it's empty.
                self.storedfields.append(d)
                
                if has_deletions:
                    docmap[docnum] = self.docnum
                
                for fieldname, length in reader.doc_field_lengths(docnum):
                    if fieldname in fieldnames:
                        self.pool.add_field_length(self.docnum, fieldname, length)
                
                for fieldname in reader.schema.vector_names():
                    if (fieldname in fieldnames
                        and reader.has_vector(docnum, fieldname)):
                        vpostreader = reader.vector(docnum, fieldname)
                        self._add_vector_reader(self.docnum, fieldname, vpostreader)
                
                self.docnum += 1
        
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
                    
        self._added = True
    
    def add_document(self, **fields):
        #from whoosh.util import now
        #t = now()
        self._check_state()
        schema = self.schema
        
        # Sort the keys
        fieldnames = sorted([name for name in fields.keys()
                             if not name.startswith("_")])
        
        # Check if the caller gave us a bogus field
        for name in fieldnames:
            if name not in schema:
                raise UnknownFieldError("No field named %r in %s" % (name, schema))
        
        storedvalues = {}
        
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
                    self._add_vector(docnum, fieldname, vlist)
                
                if field.stored:
                    # Caller can override the stored value by including a key
                    # _stored_<fieldname>
                    storedvalue = value
                    storedname = "_stored_" + fieldname
                    if storedname in fields:
                        storedvalue = fields[storedname]
                    storedvalues[fieldname] = storedvalue
        
        self._added = True
        self.storedfields.append(storedvalues)
        self.docnum += 1
        #print "%f" % (now() - t)
    
    def update_document(self, **fields):
        self._check_state()
        _unique_cache = self._unique_cache
        
        # Check which of the supplied fields are unique
        unique_fields = [name for name, field in self.schema.items()
                         if name in fields and field.unique]
        if not unique_fields:
            raise IndexingError("None of the fields in %r"
                                " are unique" % fields.keys())
        
        # Delete documents matching the unique terms
        delset = set()
        for name in unique_fields:
            field = self.schema[name]
            text = field.to_text(fields[name])
            
            # If we've seen an update_document with this unique field before...
            if name in _unique_cache:
                # Get the cache for this field
                term2docnum = _unique_cache[name]
                
                # If the cache is None, that means we've seen this field once
                # before but didn't cache it the first time. Cache it now.
                if term2docnum is None:
                    # Read the first document number found for every term in
                    # this field and cache the mapping from term to doc num
                    term2docnum = {}
                    s = self.searcher()
                    term2docnum = dict(s.reader().first_ids(name))
                    s.close()
                    _unique_cache[name] = term2docnum
                
                # Look up the cached document number for this term
                if text in term2docnum:
                    delset.add(term2docnum[text])
            else:
                # This is the first time we've seen an update_document with
                # this field. Mark it by putting None in the cache for this
                # field, but don't cache it. We'll only build the cache if we
                # see an update_document on this field again. This is to
                # prevent caching a field even when the user is only going to
                # call update_document once.
                reader = self.searcher().reader()
                try:
                    delset.add(reader.postings(name, text).id())
                    _unique_cache[name] = None
                except TermNotFound:
                    pass
                finally:
                    reader.close()
            
        # Delete the old docs
        for docnum in delset:
            self.delete_document(docnum)
        
        # Add the given fields
        self.add_document(**fields)
    
    def _add_vector(self, docnum, fieldname, vlist):
        vpostwriter = self.vpostwriter
        offset = vpostwriter.start(self.schema[fieldname].vector)
        for text, weight, valuestring in vlist:
            assert isinstance(text, unicode), "%r is not unicode" % text
            vpostwriter.write(text, weight, valuestring, 0)
        vpostwriter.finish()
        
        self.vectorindex.add((docnum, fieldname), offset)
    
    def _add_vector_reader(self, docnum, fieldname, vreader):
        vpostwriter = self.vpostwriter
        offset = vpostwriter.start(self.schema[fieldname].vector)
        while vreader.is_active():
            # text, weight, valuestring, fieldlen
            vpostwriter.write(vreader.id(), vreader.weight(), vreader.value(), 0)
            vreader.next()
        vpostwriter.finish()
        
        self.vectorindex.add((docnum, fieldname), offset)
    
    def _close_all(self):
        self.is_closed = True
        
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
        return Segment(self.name, self.generation, self.docnum,
                       self.pool.fieldlength_totals(),
                       self.pool.fieldlength_maxes())
    
    def commit(self, mergetype=None, optimize=False, merge=True):
        """Finishes writing and saves all additions and changes to disk.
        
        There are four possible ways to use this method::
        
            # Merge small segments but leave large segments, trying to
            # balance fast commits with fast searching:
            writer.commit()
        
            # Merge all segments into a single segment:
            writer.commit(optimize=True)
            
            # Don't merge any existing segments:
            writer.commit(merge=False)
            
            # Use a custom merge function
            writer.commit(mergetype=my_merge_function)
        
        :param mergetype: a custom merge function taking an Index object,
            Writer object, and segment list as arguments, and returning a
            new segment list. If you supply a ``mergetype`` function,
            the values of the ``optimize`` and ``merge`` arguments are ignored.
        :param optimize: if True, all existing segments are merged with the
            documents you've added to this writer (and the value of the
            ``merge`` argument is ignored).
        :param merge: if False, do not merge small segments.
        """
        
        self._check_state()
        try:
            if mergetype:
                pass
            elif optimize:
                mergetype = OPTIMIZE
            elif not merge:
                mergetype = NO_MERGE
            else:
                mergetype = MERGE_SMALL
            
            # Call the merge policy function. The policy may choose to merge other
            # segments into this writer's pool
            new_segments = mergetype(self, self.segments)
            
            # Tell the pool we're finished adding information, it should add its
            # accumulated data to the lengths, terms index, and posting files.
            if self._added:
                self.pool.finish(self.docnum, self.lengthfile, self.termsindex,
                                 self.postwriter)
            
                # Create a Segment object for the segment created by this writer and
                # add it to the list of remaining segments returned by the merge policy
                # function
                new_segments.append(self._getsegment())
            
            # Close all files, write a new TOC with the new segment list, and
            # release the lock.
            self._close_all()
            
            from whoosh.filedb.fileindex import _write_toc, _clean_files
            _write_toc(self.storage, self.schema, self.indexname, self.generation,
                       self.segment_number, new_segments)
            
            readlock = self.ix.lock("READLOCK")
            readlock.acquire(True)
            try:
                _clean_files(self.storage, self.indexname, self.generation, new_segments)
            finally:
                readlock.release()
        
        finally:
            if self.writelock:
                self.writelock.release()
        
    def cancel(self):
        self._check_state()
        try:
            self.pool.cancel()
            self._close_all()
        finally:
            if self.writelock:
                self.writelock.release()




