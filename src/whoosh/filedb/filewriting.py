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

from whoosh.fields import UnknownFieldError
from whoosh.store import LockError
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
    sorted_segment_list = sorted(segments, key=lambda s: s.doc_count_all())
    total_docs = 0
    for i, seg in enumerate(sorted_segment_list):
        count = seg.doc_count_all()
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
                + 21 + len(item[4] or ''))  # valuestring
        self.currentsize += size
        if self.currentsize > self.limit:
            self.save()
        self.current.append(item)

    def iter_postings(self):
        # This is just an alias for items() to be consistent with the
        # iter_postings()/add_postings() interface of a lot of other classes
        return self.items()

    def save(self):
        SortingPool.save(self)
        self.currentsize = 0


def renumber_postings(reader, startdoc, docmap):
    for fieldname, text, docnum, weight, value in reader.iter_postings():
        newdoc = docmap[docnum] if docmap else startdoc + docnum
        yield (fieldname, text, newdoc, weight, value)


# Writer object

class SegmentWriter(IndexWriter):
    def __init__(self, ix, poolclass=None, timeout=0.0, delay=0.1, _lk=True,
                 limitmb=128, docbase=0, codec=None, compound=True, **kwargs):
        # Lock the index
        self.writelock = None
        if _lk:
            self.writelock = ix.lock("WRITELOCK")
            if not try_for(self.writelock.acquire, timeout=timeout,
                           delay=delay):
                raise LockError

        if codec is None:
            from whoosh.codec import default_codec
            codec = default_codec()
        self.codec = codec

        # Get info from the index
        self.storage = ix.storage
        self.indexname = ix.indexname
        info = ix._read_toc()
        self.generation = info.generation + 1
        self.schema = info.schema
        self.segments = info.segments
        self.docnum = self.docbase = docbase
        self._setup_doc_offsets()

        # Internals
        self.compound = compound
        poolprefix = "whoosh_%s_" % self.indexname
        self.pool = PostingPool(limitmb=limitmb, prefix=poolprefix)
        newsegment = self.newsegment = codec.new_segment(self.storage,
                                                         self.indexname)
        self.is_closed = False
        self._added = False

        # Set up writers
        self.perdocwriter = codec.per_document_writer(self.storage, newsegment)
        self.fieldwriter = codec.field_writer(self.storage, newsegment)

    def __repr__(self):
        return "<%s %r>" % (self.__class__.__name__, self.newsegment)

    def _setup_doc_offsets(self):
        self._doc_offsets = []
        base = 0
        for s in self.segments:
            self._doc_offsets.append(base)
            base += s.doc_count_all()

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
        Returns True if this index has documents that are marked deleted but
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
        from whoosh.filedb.fileindex import FileIndex

        self._check_state()
        return FileIndex._reader(self.storage, self.schema, self.segments,
                                 self.generation, reuse=reuse)

    def iter_postings(self):
        return self.pool.iter_postings()

    def add_postings(self, lengths, items, startdoc, docmap):
        # items = (fieldname, text, docnum, weight, valuestring) ...
        schema = self.schema

        # Make a generator to strip out deleted fields and renumber the docs
        # before passing them down to the field writer
        def gen():
            for fieldname, text, docnum, weight, valuestring in items:
                if fieldname not in schema:
                    continue
                if docmap is not None:
                    newdoc = docmap[docnum]
                else:
                    newdoc = startdoc + docnum
                yield (fieldname, text, newdoc, weight, valuestring)

        self.fieldwriter.add_postings(schema, lengths, gen())

    def _make_docmap(self, reader, newdoc):
        # If the reader has deletions, make a dictionary mapping the docnums
        # of undeleted documents to new sequential docnums starting at newdoc
        hasdel = reader.has_deletions()
        if hasdel:
            docmap = {}
            for docnum in reader.all_doc_ids():
                if reader.is_deleted(docnum):
                    continue
                docmap[docnum] = newdoc
                newdoc += 1
        else:
            docmap = None
            newdoc += reader.doc_count_all()
        # Return the map and the new lowest unused document number
        return docmap, newdoc

    def _merge_per_doc(self, reader, docmap):
        schema = self.schema
        newdoc = self.docnum
        perdocwriter = self.perdocwriter
        sharedfields = set(schema.names()) & set(reader.schema.names())

        for docnum in reader.all_doc_ids():
            # Skip deleted documents
            if docmap and docnum not in docmap:
                continue
            # Renumber around deletions
            if docmap:
                newdoc = docmap[docnum]

            # Get the stored fields
            d = reader.stored_fields(docnum)
            # Start a new document in the writer
            perdocwriter.start_doc(newdoc)
            # For each field in the document, copy its stored value,
            # length, and vectors (if any) to the writer
            for fieldname in sharedfields:
                field = schema[fieldname]
                length = (reader.doc_field_length(docnum, fieldname, 0)
                          if field.scorable else 0)
                perdocwriter.add_field(fieldname, field, d.get(fieldname),
                                       length)
                if field.vector and reader.has_vector(docnum, fieldname):
                    v = reader.vector(docnum, fieldname)
                    perdocwriter.add_vector_matcher(fieldname, field, v)
            # Finish the new document
            perdocwriter.finish_doc()
            newdoc += 1

    def _merge_fields(self, reader, docmap):
        # Add inverted index postings to the pool, renumbering document number
        # references as necessary
        add_post = self.pool.add
        # Note: iter_postings() only yields postings for undeleted docs
        for p in renumber_postings(reader, self.docnum, docmap):
            add_post(p)

    def add_reader(self, reader):
        self._check_state()

        # Make a docnum map to renumber around deleted documents
        docmap, newdoc = self._make_docmap(reader, self.docnum)
        # Add per-document values
        self._merge_per_doc(reader, docmap)
        # Add field postings
        self._merge_fields(reader, docmap)

        self.docnum = newdoc
        self._added = True

    def _check_fields(self, schema, fieldnames):
        # Check if the caller gave us a bogus field
        for name in fieldnames:
            if name not in schema:
                raise UnknownFieldError("No field named %r in %s"
                                        % (name, schema))

    def add_document(self, **fields):
        self._check_state()
        perdocwriter = self.perdocwriter
        schema = self.schema
        docnum = self.docnum
        add_post = self.pool.add

        docboost = self._doc_boost(fields)
        fieldnames = sorted([name for name in fields.keys()
                             if not name.startswith("_")])
        self._check_fields(schema, fieldnames)

        perdocwriter.start_doc(docnum)
        # For each field...
        for fieldname in fieldnames:
            value = fields.get(fieldname)
            if value is None:
                continue
            field = schema[fieldname]

            length = 0
            if field.indexed:
                # TODO: Method for adding progressive field values, ie
                # setting start_pos/start_char?
                fieldboost = self._field_boost(fields, fieldname, docboost)
                # Ask the field to return a list of (text, weight, valuestring)
                # tuples and the number of terms in the field
                items = field.index(value)
                # Only store the length if the field is marked scorable
                scorable = field.scorable
                # Add the terms to the pool
                for text, freq, weight, valuestring in items:
                    #assert w != ""
                    weight *= fieldboost
                    if scorable:
                        length += freq
                    add_post((fieldname, text, docnum, weight, valuestring))

            if field.separate_spelling():
                # For fields which use different tokens for spelling, insert
                # fake postings for the spellable words, where docnum=None
                # means "this is a spelling word"

                # TODO: think of something less hacktacular
                for text in field.spellable_words(value):
                    add_post((fieldname, text, None, None, None))

            vformat = field.vector
            if vformat:
                analyzer = field.analyzer
                vitems = sorted(vformat.word_values(value, analyzer,
                                                    mode="index"))
                perdocwriter.add_vector_items(fieldname, field, vitems)

            # Figure out what value to store for this field
            storedval = None
            if field.stored:
                storedkey = "_stored_%s" % fieldname
                if storedkey in fields:
                    storedval = fields.get(storedkey)
                else:
                    storedval = value

            # Add the stored value and length for this field to the per-
            # document writer
            perdocwriter.add_field(fieldname, field, storedval, length)
        perdocwriter.finish_doc()
        self._added = True
        self.docnum += 1

    def doc_count(self):
        return self.docnum - self.docbase

    def get_segment(self):
        newsegment = self.newsegment
        newsegment.doccount = self.doc_count()
        return newsegment

    def _merge_segments(self, mergetype, optimize, merge):
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
        return mergetype(self, self.segments)

    def _flush_segment(self):
        lengths = self.perdocwriter.lengths_reader()
        postings = self.pool.iter_postings()
        self.fieldwriter.add_postings(self.schema, lengths, postings)

    def _close_segment(self):
        self.perdocwriter.close()
        self.fieldwriter.close()
        self.pool.cleanup()

    def _assemble_segment(self):
        if self.compound:
            # Assemble the segment files into a compound file
            newsegment = self.get_segment()
            newsegment.create_compound_file(self.storage)
            newsegment.compound = True

    def _commit_toc(self, segments):
        # Write a new TOC with the new segment list (and delete old files)
        self.codec.commit_toc(self.storage, self.indexname, self.schema,
                              segments, self.generation)

    def _finish(self):
        if self.writelock:
            self.writelock.release()
        self.is_closed = True
        #self.storage.close()

    def _partial_segment(self):
        # For use by a parent multiprocessing writer: Closes out the segment
        # but leaves the pool files intact so the parent can access them
        self._check_state()
        self.perdocwriter.close()
        self.fieldwriter.close()
        # Don't call self.pool.cleanup()! We want to grab the pool files.
        return self.get_segment()

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
            # Merge old segments if necessary
            finalsegments = self._merge_segments(mergetype, optimize, merge)
            if self._added:
                # Finish writing segment
                self._flush_segment()
                # Close segment files
                self._close_segment()
                # Assemble compound segment if necessary
                self._assemble_segment()

                # Add the new segment to the list of remaining segments
                # returned by the merge policy function
                finalsegments.append(self.get_segment())
            else:
                # Close segment files
                self._close_segment()
            # Write TOC
            self._commit_toc(finalsegments)
        finally:
            # Final cleanup
            self._finish()

    def cancel(self):
        self._check_state()
        self._close_segment()
        self._finish()


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
    from whoosh.support import dawg

    writer = ix.writer()
    storage = writer.storage
    schema = writer.schema
    segments = writer.segments

    for segment in segments:
        r = SegmentReader(storage, schema, segment)
        f = segment.create_file(storage, ".dag")
        gw = dawg.GraphWriter(f)
        for fieldname in fieldnames:
            gw.start_field(fieldname)
            for word in r.lexicon(fieldname):
                gw.insert(word)
            gw.finish_field()
        gw.close()

    for fieldname in fieldnames:
        schema[fieldname].spelling = True

    if commit:
        writer.commit(merge=False)




