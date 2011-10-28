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
import os, tempfile
from bisect import bisect_right
from collections import defaultdict

try:
    import sqlite3  #@UnusedImport
    has_sqlite = True
except ImportError:
    has_sqlite = False

from whoosh.compat import integer_types, iteritems, text_type, next
from whoosh.fields import UnknownFieldError
from whoosh.filedb.fileindex import Segment
from whoosh.filedb.filepostings import FilePostingWriter
from whoosh.filedb.filetables import (TermIndexWriter, StoredFieldWriter,
                                      TermVectorWriter, Lengths)
from whoosh.store import LockError
from whoosh.support.dawg import DawgBuilder, flatten
from whoosh.support.filelock import try_for
from whoosh.support.externalsort import SortingPool
from whoosh.util import fib
from whoosh.writing import IndexWriter, IndexingError


# Merge policies

# A merge policy is a callable that takes the Index object, the SegmentWriter
# object, and the current segment list (not including the segment being
# written), and returns an updated segment list (not including the segment
# being written).

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


#def MERGE_SQUARES(writer, segments):
#    """This is an alternative merge policy similar to Lucene's. It is less
#    optimal than the default MERGE_SMALL.
#    """
#
#    from whoosh.filedb.filereading import SegmentReader
#
#    sizedsegs = [(s.doc_count_all(), s) for s in segments]
#    tomerge = []
#    for size in (10, 100, 1000, 10000, 100000):
#        smaller = [seg for segsize, seg in sizedsegs
#                   if segsize < size - 1 and segsize >= size // 10]
#        if len(smaller) >= 10:
#            tomerge.extend(smaller)
#            for seg in smaller:
#                segments.remove(seg)
#
#    for seg in tomerge:
#        reader = SegmentReader(writer.storage, writer.schema, seg)
#        writer.add_reader(reader)
#        reader.close()
#
#    return segments


class PostingPool(SortingPool):
    # Subclass whoosh.support.externalsort.SortingPool to use knowledge of
    # postings to set run size in bytes instead of items

    def __init__(self, limitmb=128, **kwargs):
        SortingPool.__init__(self, **kwargs)
        self.limit = limitmb * 1024 * 1024
        self.currentsize = 0

    def add(self, item):
        # item = (fieldname, text, docnum, weight, valuestring)
        size = (28 + 4 * 5  # tuple = 28 + 4 * length
                + 21 + len(item[0])  # fieldname = str = 21 + length
                + 26 + len(item[1]) * 2  # text = unicode = 26 + 2 * length
                + 18  # docnum = long = 18
                + 16  # weight = float = 16
                + 21 + len(item[4]))  # valuestring
        self.currentsize += size
        if self.currentsize > self.limit:
            self.save()

    def save(self):
        SortingPool.save(self)
        self.currentsize = 0


# Writer object

class SegmentWriter(IndexWriter):
    def __init__(self, ix, poolclass=None, blocklimit=128, timeout=0.0,
                 delay=0.1, _lk=True, limitmb=128, docbase=0, **kwargs):
        self.is_closed = False
        self.writelock = None
        self._added = False
        if _lk:
            self.writelock = ix.lock("WRITELOCK")
            if not try_for(self.writelock.acquire, timeout=timeout,
                           delay=delay):
                raise LockError

        self.storage = storage = ix.storage
        self.indexname = ix.indexname

        info = ix._read_toc()
        self.schema = info.schema
        self.segments = info.segments
        self._doc_offsets = []
        base = 0
        for s in self.segments:
            self._doc_offsets.append(base)
            base += s.doc_count_all()

        self.blocklimit = blocklimit
        self.generation = info.generation + 1
        self.newsegment = Segment(self.indexname, 0)
        self.docnum = self.docbase = docbase

        # Spelling
        self.dawg = None
        if any(field.spelling for field in self.schema):
            self.dawgfile = storage.create_file(self.newsegment.dawg_filename)
            self.dawg = DawgBuilder(field_root=True)

        # Terms index
        tf = storage.create_file(self.newsegment.termsindex_filename)
        ti = TermIndexWriter(tf)
        # Term postings file
        pf = storage.create_file(self.newsegment.termposts_filename)
        pw = FilePostingWriter(pf, blocklimit=blocklimit)
        # Terms writer
        self.termswriter = TermsWriter(self.schema, ti, pw, self.dawg)

        if self.schema.has_vectored_fields():
            # Vector index
            vf = storage.create_file(self.newsegment.vectorindex_filename)
            self.vectorindex = TermVectorWriter(vf)

            # Vector posting file
            vpf = storage.create_file(self.newsegment.vectorposts_filename)
            self.vpostwriter = FilePostingWriter(vpf, stringids=True)
        else:
            self.vectorindex = None
            self.vpostwriter = None

        # Stored fields file
        sf = storage.create_file(self.newsegment.storedfields_filename)
        self.storedfields = StoredFieldWriter(sf, self.schema.stored_names())

        # Field lengths
        self.lengths = Lengths()

        # Create the posting pool
        self.pool = PostingPool(limitmb=limitmb,
                                prefix="whoosh_%s_" % self.indexname)

    def _check_state(self):
        if self.is_closed:
            raise IndexingError("This writer is closed")

    def add_field(self, fieldname, fieldspec, **kwargs):
        self._check_state()
        if self._added:
            raise Exception("Can't modify schema after adding data to writer")
        super(SegmentWriter, self).add_field(fieldname, fieldspec, **kwargs)

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
        lengths = self.lengths

        has_deletions = reader.has_deletions()
        if has_deletions:
            docmap = {}

        fieldnames = set(self.schema.names())

        # Add stored documents, vectors, and field lengths
        for docnum in reader.all_doc_ids():
            newdoc = self.docnum
            if (not has_deletions) or (not reader.is_deleted(docnum)):
                d = dict(item for item
                         in iteritems(reader.stored_fields(docnum))
                         if item[0] in fieldnames)
                # We have to append a dictionary for every document, even if
                # it's empty.
                self.storedfields.append(d)

                if has_deletions:
                    docmap[docnum] = newdoc

                for fieldname in reader.schema.scorable_names():
                    length = reader.doc_field_length(docnum, fieldname)
                    if length and fieldname in fieldnames:
                        lengths.add(newdoc, fieldname, length)

                for fieldname in reader.schema.vector_names():
                    if (fieldname in fieldnames
                        and reader.has_vector(docnum, fieldname)):
                        vpostreader = reader.vector(docnum, fieldname)
                        self._add_vector_reader(newdoc, fieldname, vpostreader)

                self.docnum += 1

        # Add postings
        for fieldname, text in reader.all_terms():
            if fieldname in fieldnames:
                postreader = reader.postings(fieldname, text)
                while postreader.is_active():
                    docnum = postreader.id()
                    valuestring = postreader.value()
                    if has_deletions:
                        newdoc = docmap[docnum]
                    else:
                        newdoc = startdoc + docnum

                    self.pool.add((fieldname, text, newdoc,
                                   postreader.weight(), valuestring))
                    postreader.next()

        self._added = True

    def add_document(self, **fields):
        self._check_state()
        schema = self.schema
        lengths = self.lengths
        add_post = self.pool.add
        docboost = self._doc_boost(fields)

        # Sort the keys
        fieldnames = sorted([name for name in fields.keys()
                             if not name.startswith("_")])

        # Check if the caller gave us a bogus field
        for name in fieldnames:
            if name not in schema:
                raise UnknownFieldError("No field named %r in %s"
                                        % (name, schema))

        storedvalues = {}
        docnum = self.docnum
        for fieldname in fieldnames:
            value = fields.get(fieldname)
            if value is None:
                continue
            field = schema[fieldname]

            if field.indexed:
                fieldboost = self._field_boost(fields, fieldname, docboost)
                length = 0
                # TODO: Method for adding progressive field values, ie
                # setting start_pos/start_char?
                for w, freq, weight, valuestring in field.index(value):
                    #assert w != ""
                    weight *= fieldboost
                    add_post((fieldname, w, docnum, weight, valuestring))
                    length += freq

                if field.scorable:
                    lengths.add(docnum, fieldname, length)

            if field.separate_spelling():
                # For fields which use different tokens for spelling, insert
                # fake postings for the spellable words
                for w in field.spellable_words(value):
                    add_post((fieldname + " ", w, None, None, None))

            vformat = field.vector
            if vformat:
                wvs = vformat.word_values(value, field.analyzer, mode="index")
                vlist = sorted((w, weight, valuestring)
                               for w, _, weight, valuestring in wvs)
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

    def _add_vector(self, docnum, fieldname, vlist):
        vpostwriter = self.vpostwriter
        offset = vpostwriter.start(self.schema[fieldname].vector)
        for text, weight, valuestring in vlist:
            #assert isinstance(text, text_type), "%r is not unicode" % text
            vpostwriter.write(text, weight, valuestring, 0)
        vpostwriter.finish(inlinelimit=0)
        self.vectorindex.add((docnum, fieldname), offset)

    def _add_vector_reader(self, docnum, fieldname, vreader):
        vpostwriter = self.vpostwriter
        offset = vpostwriter.start(self.schema[fieldname].vector)
        while vreader.is_active():
            # text, weight, valuestring, fieldlen
            vpostwriter.write(vreader.id(), vreader.weight(), vreader.value(),
                              0)
            vreader.next()
        vpostwriter.finish(inlinelimit=0)
        self.vectorindex.add((docnum, fieldname), offset)

    def _close_all(self):
        self.is_closed = True

        self.termswriter.close()
        self.storedfields.close()
        if self.vectorindex:
            self.vectorindex.close()
        if self.vpostwriter:
            self.vpostwriter.close()

    def doc_count(self):
        return self.docnum - self.docbase

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

        from whoosh.filedb.fileindex import TOC, _clean_files

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

            # Call the merge policy function. The policy may choose to merge
            # other segments into this writer's pool
            finalsegments = mergetype(self, self.segments)

            if self._added:
                # Create a Segment object for the segment created by this
                # writer
                newsegment = self.newsegment
                newsegment.doccount = self.doc_count()

                # Copy the sorted pool postings to the terms index and posting
                # files
                self.termswriter.add_iter(self.pool.items(), self.lengths)
                # Write out lengths file
                lf = self.storage.create_file(newsegment.fieldlengths_filename)
                self.lengths.to_file(lf, newsegment.doccount)
                # Write out spelling files
                if self.dawg:
                    self.dawg.write(self.dawgfile)

                # Add new segment to the list of remaining segments returned by
                # the merge policy function
                finalsegments.append(newsegment)
            else:
                self.pool.cleanup()

            # Close all files, write a new TOC with the new segment list, and
            # release the lock.
            self._close_all()

            # Write the TOC for the new generation
            toc = TOC(self.schema, finalsegments, self.generation)
            toc.write(self.storage, self.indexname)

            # Delete leftover files
            _clean_files(self.storage, self.indexname, self.generation,
                         finalsegments)

        finally:
            if self.writelock:
                self.writelock.release()

    def cancel(self):
        self._check_state()
        try:
            self.pool.cleanup()
            self._close_all()
        finally:
            if self.writelock:
                self.writelock.release()


class TermsWriter(object):
    def __init__(self, schema, termsindex, postwriter, dawg, inlinelimit=1):
        self.schema = schema
        # This file maps terms to TermInfo structures
        self.termsindex = termsindex

        # This object writes postings to the posting file and keeps track of
        # blocks
        self.postwriter = postwriter

        # Spelling
        self.dawg = dawg

        # Posting lists with <= this number of postings will be inlined into
        # the terms index instead of being written to the posting file
        assert isinstance(inlinelimit, integer_types)
        self.inlinelimit = inlinelimit

        self.lastfn = None
        self.lasttext = None

    def _finish_term(self, fn, text):
        if fn is not None and fn[-1] != " " and text is not None:
            terminfo = self.postwriter.finish(self.inlinelimit)
            self.termsindex.add((fn, text), terminfo)

    def add_iter(self, postiter, lengths):
        dawg = self.dawg
        postwriter = self.postwriter
        lastfn = self.lastfn
        lasttext = self.lasttext
        spelling = False
        fmt = None

        for fieldname, text, docnum, weight, valuestring in postiter:
            if fieldname < lastfn or (fieldname == lastfn and text < lasttext):
                raise Exception("Postings are out of order: %r:%s .. %r:%s" %
                                (lastfn, lasttext, fieldname, text))

            if (fieldname[-1] == " "
                and (fieldname != lastfn or text != lasttext)):
                # Spelling word placeholder
                if fieldname != lastfn and not lastfn[-1] == " ":
                    self._finish_term(lastfn, lasttext)
                fn = fieldname[:-1]
                dawg.insert((fn,) + tuple(text))
            else:
                # Is the fieldname of this posting different from the last one?
                if fieldname != lastfn:
                    # Store information we need about the new field
                    field = self.schema[fieldname]
                    fmt = field.format
                    spelling = (field.spelling
                                and not field.separate_spelling())

                # Is the term of this posting different from the last one?
                if fieldname != lastfn or text != lasttext:
                    # Finish up the last term before starting a new one
                    self._finish_term(lastfn, lasttext)

                    # If this field has spelling, add to the word graph
                    if spelling:
                        dawg.insert((fieldname,) + tuple(text))

                    # Set up postwriter for a new term
                    postwriter.start(fmt)

                length = lengths.get(docnum, fieldname)
                postwriter.write(docnum, weight, valuestring, length)

            lastfn = fieldname
            lasttext = text

        self.lastfn = lastfn
        self.lasttext = lasttext

    def close(self):
        self._finish_term(self.lastfn, self.lasttext)
        self.termsindex.close()
        self.postwriter.close()


# Retroactively add spelling files to an existing index

def add_spelling(ix, fieldnames, commit=True):
    """Adds spelling files to an existing index that was created without
    them, and modifies the schema so the given fields have the ``spelling``
    attribute. Only works on filedb indexes.
    
    >>> ix = index.open_dir("testindex")
    >>> add_spelling(ix, ["content", "tags"])
    
    :param ix: a :class:`whoosh.filedb.fileindex.FileIndex` object.
    :param fieldnames: a list of field names to create word graphs for.
    :param force: if True, overwrites existing word graph files. This is only
        useful for debugging.
    """

    from whoosh.filedb.filereading import SegmentReader

    writer = ix.writer()
    storage = writer.storage
    schema = writer.schema
    segments = writer.segments

    for segment in segments:
        filename = segment.dawg_filename
        r = SegmentReader(storage, schema, segment)
        f = storage.create_file(filename)
        dawg = DawgBuilder(field_root=True)
        for fieldname in fieldnames:
            ft = (fieldname,)
            for word in r.lexicon(fieldname):
                dawg.insert(ft + tuple(word))
        dawg.write(f)

    for fieldname in fieldnames:
        schema[fieldname].spelling = True

    if commit:
        writer.commit(merge=False)




