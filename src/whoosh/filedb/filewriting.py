# Copyright 2007 Matt Chaput. All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#    1. Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#
#    2. Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY MATT CHAPUT ``AS IS'' AND ANY EXPRESS OR
# IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
# EVENT SHALL MATT CHAPUT OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
# OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
# EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
# The views and conclusions contained in the software and documentation are
# those of the authors and should not be interpreted as representing official
# policies, either expressed or implied, of Matt Chaput.

from __future__ import with_statement
from bisect import bisect_right
from collections import defaultdict

from whoosh.fields import UnknownFieldError
from whoosh.filedb.fileindex import Segment
from whoosh.filedb.filepostings import FilePostingWriter
from whoosh.filedb.filetables import (TermIndexWriter, StoredFieldWriter,
                                      TermVectorWriter)
from whoosh.filedb.pools import TempfilePool
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


def MERGE_SQUARES(writer, segments):
    """This is an alternative merge policy similar to Lucene's. It is less
    optimal than the default MERGE_SMALL.
    """
    
    from whoosh.filedb.filereading import SegmentReader
    
    sizedsegs = [(s.doc_count_all(), s) for s in segments]
    tomerge = []
    for size in (10, 100, 1000, 10000, 100000):
        smaller = [seg for segsize, seg in sizedsegs
                   if segsize < size - 1 and segsize >= size//10]
        if len(smaller) >= 10:
            tomerge.extend(smaller)
            for seg in smaller:
                segments.remove(seg)
    
    for seg in tomerge:
        reader = SegmentReader(writer.storage, writer.schema, seg)
        writer.add_reader(reader)
        reader.close()
    
    return segments


# Writer object

class SegmentWriter(IndexWriter):
    def __init__(self, ix, poolclass=None, procs=0, blocklimit=128,
                 timeout=0.0, delay=0.1, name=None, _l=True, **poolargs):
        
        self.writelock = None
        if _l:
            self.writelock = ix.lock("WRITELOCK")
            if not try_for(self.writelock.acquire, timeout=timeout, delay=delay):
                raise LockError
        self.readlock = ix.lock("READLOCK")
        
        info = ix._read_toc()
        self.schema = info.schema
        self.segments = info.segments
        self.storage = ix.storage
        self.indexname = ix.indexname
        self.is_closed = False
        
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
        ti = TermIndexWriter(tf)
        # Term postings file
        pf = self.storage.create_file(segment.termposts_filename)
        pw = FilePostingWriter(pf, blocklimit=blocklimit)
        # Terms writer
        self.termswriter = TermsWriter(self.schema, ti, pw)
        
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
        if len(offsets) == 1:
            return 0
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
        if docnum >= sum(seg.doccount for seg in self.segments):
            raise IndexingError("No document ID %r in this index" % docnum)
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

    def reader(self, reuse=None):
        self._check_state()
        from whoosh.filedb.fileindex import FileIndex
        
        return FileIndex._reader(self.storage, self.schema, self.segments,
                                 self.generation, reuse=reuse)
    
    def add_reader(self, reader):
        self._check_state()
        startdoc = self.docnum
        
        has_deletions = reader.has_deletions()
        if has_deletions:
            docmap = {}
        
        fieldnames = set(self.schema.names())
        
        # Add stored documents, vectors, and field lengths
        for docnum in reader.all_doc_ids():
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
    
    #def update_document(self, **fields):
    
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
        
        self.termswriter.close()
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
        
        :param mergetype: a custom merge function taking a Writer object and
            segment list as arguments, and returning a new segment list. If you
            supply a ``mergetype`` function, the values of the ``optimize`` and
            ``merge`` arguments are ignored.
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
                self.pool.finish(self.termswriter, self.docnum, self.lengthfile)
            
                # Create a Segment object for the segment created by this writer and
                # add it to the list of remaining segments returned by the merge policy
                # function
                new_segments.append(self._getsegment())
            else:
                self.pool.cleanup()
            
            # Close all files, write a new TOC with the new segment list, and
            # release the lock.
            self._close_all()
            
            from whoosh.filedb.fileindex import _write_toc, _clean_files
            _write_toc(self.storage, self.schema, self.indexname, self.generation,
                       self.segment_number, new_segments)
            
            self.readlock.acquire(True)
            try:
                _clean_files(self.storage, self.indexname, self.generation, new_segments)
            finally:
                self.readlock.release()
        
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


class TermsWriter(object):
    def __init__(self, schema, termsindex, postwriter, inlinelimit=1):
        self.schema = schema
        self.termsindex = termsindex
        self.postwriter = postwriter
        self.inlinelimit = inlinelimit
        
        self.lastfn = None
        self.lasttext = None
        self.format = None
        self.offset = None
    
    def _new_term(self, fieldname, text):
        lastfn = self.lastfn
        lasttext = self.lasttext
        if fieldname < lastfn or (fieldname == lastfn and text < lasttext):
            raise Exception("Postings are out of order: %r:%s .. %r:%s" %
                            (lastfn, lasttext, fieldname, text))
    
        if fieldname != lastfn:
            self.format = self.schema[fieldname].format
    
        if fieldname != lastfn or text != lasttext:
            self._finish_term()
            # Reset the term attributes
            self.weight = 0
            self.offset = self.postwriter.start(self.format)
            self.lasttext = text
            self.lastfn = fieldname
    
    def _finish_term(self):
        postwriter = self.postwriter
        if self.lasttext is not None:
            postcount = postwriter.posttotal
            if postcount <= self.inlinelimit and postwriter.blockcount < 1:
                offset = postwriter.as_inline()
                postwriter.cancel()
            else:
                offset = self.offset
                postwriter.finish()
            
            self.termsindex.add((self.lastfn, self.lasttext),
                                (self.weight, offset, postcount))
    
    def add_postings(self, fieldname, text, matcher, getlen, offset=0, docmap=None):
        self._new_term(fieldname, text)
        postwrite = self.postwriter.write
        totalweight = 0
        while matcher.is_active():
            docnum = matcher.id()
            weight = matcher.weight()
            valuestring = matcher.value()
            if docmap:
                newdoc = docmap[docnum]
            else:
                newdoc = offset + docnum
            totalweight += weight
            postwrite(newdoc, weight, valuestring, getlen(docnum, fieldname))
            matcher.next()
        self.weight += totalweight
    
    def add_iter(self, postiter, getlen, offset=0, docmap=None):
        _new_term = self._new_term
        postwrite = self.postwriter.write
        for fieldname, text, docnum, weight, valuestring in postiter:
            _new_term(fieldname, text)
            if docmap:
                newdoc = docmap[docnum]
            else:
                newdoc = offset + docnum
            self.weight += weight
            postwrite(newdoc, weight, valuestring, getlen(docnum, fieldname))
    
    def add(self, fieldname, text, docnum, weight, valuestring, fieldlen):
        self._new_term(fieldname, text)
        self.weight += weight
        self.postwriter.write(docnum, weight, valuestring, fieldlen)
        
    def close(self):
        self._finish_term()
        self.termsindex.close()
        self.postwriter.close()
        
        
        
        
            
        













