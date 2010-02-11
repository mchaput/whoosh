#===============================================================================
# Copyright 2009 Matt Chaput
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

import cPickle, re
from bisect import bisect_right
from time import time
from threading import Lock

from whoosh import __version__
from whoosh.fields import Schema
from whoosh.index import Index
from whoosh.index import EmptyIndexError, OutOfDateError, IndexVersionError
from whoosh.index import _DEF_INDEX_NAME
from whoosh.store import LockError
from whoosh.support.bitvector import BitVector
from whoosh.system import _INT_SIZE, _FLOAT_SIZE


_INDEX_VERSION = -105
_EXTENSIONS = "dci|dcz|tiz|fvz|pst|vps"


# A mix-in that adds methods for deleting
# documents from self.segments. These methods are on IndexWriter as
# well as Index for convenience, so they're broken out here.

class SegmentDeletionMixin(object):
    """Mix-in for classes that support deleting documents from self.segments.
    """

    def delete_document(self, docnum, delete=True):
        """Deletes a document by number."""
        self.segments.delete_document(docnum, delete=delete)

    def deleted_count(self):
        """Returns the total number of deleted documents in this index.
        """
        return self.segments.deleted_count()

    def is_deleted(self, docnum):
        """Returns True if a given document number is deleted but
        not yet optimized out of the index.
        """
        return self.segments.is_deleted(docnum)

    def has_deletions(self):
        """Returns True if this index has documents that are marked
        deleted but haven't been optimized out of the index yet.
        """
        return self.segments.has_deletions()


class FileIndex(SegmentDeletionMixin, Index):
    def __init__(self, storage, schema, create=False,
                 indexname=_DEF_INDEX_NAME):
        self.storage = storage
        self.indexname = indexname

        if schema is not None and not isinstance(schema, Schema):
            raise ValueError("%r is not a Schema object" % schema)

        self.generation = self.latest_generation()

        if create:
            if schema is None:
                raise IndexError("To create an index you must specify a schema")

            self.schema = schema
            self.generation = 0
            self.segment_counter = 0
            self.segments = SegmentSet()

            # Clear existing files
            prefix = "_%s_" % self.indexname
            for filename in self.storage:
                if filename.startswith(prefix):
                    storage.delete_file(filename)

            self._write()
        elif self.generation >= 0:
            self._read(schema)
        else:
            raise EmptyIndexError("No index named %r in storage %r" % (indexname, storage))

        # Open a reader for this index. This is used by the
        # deletion methods, but mostly it's to keep the underlying
        # files open so they don't get deleted from underneath us.
        self._searcher = self.searcher()

        self.segment_num_lock = Lock()

    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__,
                               self.storage, self.indexname)

    def __del__(self):
        if (hasattr(self, "_searcher")
            and self._searcher
            and not self._searcher.is_closed):
            self._searcher.close()

    def close(self):
        self._searcher.close()

    def latest_generation(self):
        pattern = _toc_pattern(self.indexname)

        max = -1
        for filename in self.storage:
            m = pattern.match(filename)
            if m:
                num = int(m.group(1))
                if num > max: max = num
        return max

    def refresh(self):
        if not self.up_to_date():
            return self.__class__(self.storage, self.schema,
                                  indexname=self.indexname)
        else:
            return self

    def up_to_date(self):
        return self.generation == self.latest_generation()

    def _write(self):
        # Writes the content of this index to the .toc file.
        for field in self.schema:
            field.clean()
        #stream = self.storage.create_file(self._toc_filename())

        # Use a temporary file for atomic write.
        tocfilename = self._toc_filename()
        tempfilename = '%s.%s' % (tocfilename, time())
        stream = self.storage.create_file(tempfilename)

        stream.write_varint(_INT_SIZE)
        stream.write_varint(_FLOAT_SIZE)
        stream.write_int(-12345)

        stream.write_int(_INDEX_VERSION)
        for num in __version__[:3]:
            stream.write_varint(num)

        stream.write_string(cPickle.dumps(self.schema, -1))
        stream.write_int(self.generation)
        stream.write_int(self.segment_counter)
        stream.write_pickle(self.segments)
        stream.close()

        # Rename temporary file to the proper filename
        self.storage.rename_file(tempfilename, self._toc_filename(), safe=True)

    def _read(self, schema):
        # Reads the content of this index from the .toc file.
        stream = self.storage.open_file(self._toc_filename())

        if stream.read_varint() != _INT_SIZE or \
           stream.read_varint() != _FLOAT_SIZE:
            raise IndexError("Index was created on an architecture with different data sizes")

        if not stream.read_int() == -12345:
            raise IndexError("Number misread: byte order problem")

        version = stream.read_int()
        if version != _INDEX_VERSION:
            raise IndexVersionError("Can't read format %s" % version, version)
        self.version = version
        self.release = (stream.read_varint(),
                        stream.read_varint(),
                        stream.read_varint())

        # If the user supplied a schema object with the constructor, don't load
        # the pickled schema from the saved index.
        if schema:
            self.schema = schema
            stream.skip_string()
        else:
            self.schema = cPickle.loads(stream.read_string())

        generation = stream.read_int()
        assert generation == self.generation
        self.segment_counter = stream.read_int()
        self.segments = stream.read_pickle()
        stream.close()

    def _next_segment_name(self):
        #Returns the name of the next segment in sequence.
        if self.segment_num_lock.acquire():
            try:
                self.segment_counter += 1
                return "_%s_%s" % (self.indexname, self.segment_counter)
            finally:
                self.segment_num_lock.release()
        else:
            raise LockError

    def _toc_filename(self):
        # Returns the computed filename of the TOC for this index name and
        # generation.
        return "_%s_%s.toc" % (self.indexname, self.generation)

    def last_modified(self):
        return self.storage.file_modified(self._toc_filename())

    def is_empty(self):
        return len(self.segments) == 0

    def optimize(self):
        if len(self.segments) < 2 and not self.segments.has_deletions():
            return

        from whoosh.filedb.filewriting import OPTIMIZE
        w = self.writer()
        w.commit(OPTIMIZE)

    def commit(self, new_segments=None):
        self._searcher.close()

        if not self.up_to_date():
            raise OutOfDateError

        if new_segments:
            if not isinstance(new_segments, SegmentSet):
                raise ValueError("FileIndex.commit() called with something other than a SegmentSet: %r" % new_segments)
            self.segments = new_segments

        self.generation += 1
        self._write()
        self._clean_files()

        self._searcher = self.searcher()

    def _clean_files(self):
        # Attempts to remove unused index files (called when a new generation
        # is created). If existing Index and/or reader objects have the files
        # open, they may not get deleted immediately (i.e. on Windows) but will
        # probably be deleted eventually by a later call to clean_files.

        storage = self.storage
        current_segment_names = set(s.name for s in self.segments)

        tocpattern = _toc_pattern(self.indexname)
        segpattern = _segment_pattern(self.indexname)

        for filename in storage:
            m = tocpattern.match(filename)
            if m:
                num = int(m.group(1))
                if num != self.generation:
                    try:
                        storage.delete_file(filename)
                    except OSError:
                        # Another process still has this file open
                        pass
            else:
                m = segpattern.match(filename)
                if m:
                    name = m.group(1)
                    if name not in current_segment_names:
                        try:
                            storage.delete_file(filename)
                        except OSError:
                            # Another process still has this file open
                            pass

    def doc_count_all(self):
        return self.segments.doc_count_all()

    def doc_count(self):
        return self.segments.doc_count()

    def field_length(self, fieldid):
        fieldnum = self.schema.to_number(fieldid)
        return sum(s.field_length(fieldnum) for s in self.segments)

    def reader(self):
        return self.segments.reader(self.storage, self.schema)

    def writer(self, **kwargs):
        from whoosh.filedb.filewriting import FileIndexWriter
        return FileIndexWriter(self, **kwargs)


# SegmentSet object

class SegmentSet(object):
    """This class is never instantiated by the user. It is used by the Index
    object to keep track of the segments in the index.
    """

    def __init__(self, segments=None):
        if segments is None:
            self.segments = []
        else:
            self.segments = segments

        self._doc_offsets = self.doc_offsets()

    def __repr__(self):
        return repr(self.segments)

    def __len__(self):
        """
        :returns: the number of segments in this set.
        """
        return len(self.segments)

    def __iter__(self):
        return iter(self.segments)

    def __getitem__(self, n):
        return self.segments.__getitem__(n)

    def append(self, segment):
        """Adds a segment to this set."""

        self.segments.append(segment)
        self._doc_offsets = self.doc_offsets()

    def _document_segment(self, docnum):
        """Returns the index.Segment object containing the given document
        number.
        """

        offsets = self._doc_offsets
        if len(offsets) == 1: return 0
        return bisect_right(offsets, docnum) - 1

    def _segment_and_docnum(self, docnum):
        """Returns an (index.Segment, segment_docnum) pair for the segment
        containing the given document number.
        """

        segmentnum = self._document_segment(docnum)
        offset = self._doc_offsets[segmentnum]
        segment = self.segments[segmentnum]
        return segment, docnum - offset

    def copy(self):
        """:returns: a deep copy of this set."""
        return self.__class__([s.copy() for s in self.segments])

    def doc_offsets(self):
        # Recomputes the document offset list. This must be called if you
        # change self.segments.
        offsets = []
        base = 0
        for s in self.segments:
            offsets.append(base)
            base += s.doc_count_all()
        return offsets

    def doc_count_all(self):
        """
        :returns: the total number of documents, DELETED or UNDELETED, in this
            set.
        """
        return sum(s.doc_count_all() for s in self.segments)

    def doc_count(self):
        """
        :returns: the number of undeleted documents in this set.
        """
        return sum(s.doc_count() for s in self.segments)


    def has_deletions(self):
        """
        :returns: True if this index has documents that are marked deleted but
            haven't been optimized out of the index yet. This includes
            deletions that haven't been written to disk with Index.commit()
            yet.
        """
        return any(s.has_deletions() for s in self.segments)

    def delete_document(self, docnum, delete=True):
        """Deletes a document by number.

        You must call Index.commit() for the deletion to be written to disk.
        """

        segment, segdocnum = self._segment_and_docnum(docnum)
        segment.delete_document(segdocnum, delete=delete)

    def deleted_count(self):
        """
        :returns: the total number of deleted documents in this index.
        """
        return sum(s.deleted_count() for s in self.segments)

    def is_deleted(self, docnum):
        """
        :returns: True if a given document number is deleted but not yet
            optimized out of the index.
        """

        segment, segdocnum = self._segment_and_docnum(docnum)
        return segment.is_deleted(segdocnum)

    def reader(self, storage, schema):
        from whoosh.filedb.filereading import SegmentReader
        segments = self.segments
        if len(segments) == 1:
            return SegmentReader(storage, segments[0], schema)
        else:
            from whoosh.reading import MultiReader
            readers = [SegmentReader(storage, segment, schema)
                       for segment in segments]
            return MultiReader(readers, self._doc_offsets, schema)


class Segment(object):
    """Do not instantiate this object directly. It is used by the Index object
    to hold information about a segment. A list of objects of this class are
    pickled as part of the TOC file.
    
    The TOC file stores a minimal amount of information -- mostly a list of
    Segment objects. Segments are the real reverse indexes. Having multiple
    segments allows quick incremental indexing: just create a new segment for
    the new documents, and have the index overlay the new segment over previous
    ones for purposes of reading/search. "Optimizing" the index combines the
    contents of existing segments into one (removing any deleted documents
    along the way).
    """

    def __init__(self, name, max_doc, field_length_totals, deleted=None):
        """
        :param name: The name of the segment (the Index object computes this
            from its name and the generation).
        :param max_doc: The maximum document number in the segment.
        :param term_count: Total count of all terms in all documents.
        :param field_length_totals: A dictionary mapping field numbers to the
            total number of terms in that field across all documents in the
            segment.
        :param deleted: A set of deleted document numbers, or None if no
            deleted documents exist in this segment.
        """

        self.name = name
        self.max_doc = max_doc
        self.field_length_totals = field_length_totals
        self.deleted = deleted

        self.doclen_filename = self.name + ".dci"
        self.docs_filename = self.name + ".dcz"
        self.term_filename = self.name + ".tiz"
        self.vector_filename = self.name + ".fvz"
        self.posts_filename = self.name + ".pst"
        self.vectorposts_filename = self.name + ".vps"

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.name)

    def copy(self):
        if self.deleted:
            deleted = set(self.deleted)
        else:
            deleted = None
        return Segment(self.name, self.max_doc,
                       self.field_length_totals,
                       deleted)

    def doc_count_all(self):
        """
        :returns: the total number of documents, DELETED OR UNDELETED, in this
            segment.
        """
        return self.max_doc

    def doc_count(self):
        """
        :returns: the number of (undeleted) documents in this segment.
        """
        return self.max_doc - self.deleted_count()

    def has_deletions(self):
        """
        :returns: True if any documents in this segment are deleted.
        """
        return self.deleted_count() > 0

    def deleted_count(self):
        """
        :returns: the total number of deleted documents in this segment.
        """
        if self.deleted is None: return 0
        return len(self.deleted)

    def field_length(self, fieldnum):
        """
        :param fieldnum: the internal number of the field.
        :returns: the total number of terms in the given field across all
            documents in this segment.
        """
        return self.field_length_totals.get(fieldnum, 0)

    def delete_document(self, docnum, delete=True):
        """Deletes the given document number. The document is not actually
        removed from the index until it is optimized.

        :param docnum: The document number to delete.
        :param delete: If False, this undeletes a deleted document.
        """

        if delete:
            if self.deleted is None:
                self.deleted = set()
            elif docnum in self.deleted:
                raise KeyError("Document %s in segment %r is already deleted"
                               % (docnum, self.name))

            self.deleted.add(docnum)
        else:
            if self.deleted is None or docnum not in self.deleted:
                raise KeyError("Document %s is not deleted" % docnum)

            self.deleted.clear(docnum)

    def is_deleted(self, docnum):
        """:returns: True if the given document number is deleted."""

        if self.deleted is None: return False
        return docnum in self.deleted


# Utility functions

def _toc_pattern(indexname):
    """Returns a regular expression object that matches TOC filenames.
    name is the name of the index.
    """

    return re.compile("_%s_([0-9]+).toc" % indexname)

def _segment_pattern(indexname):
    """Returns a regular expression object that matches segment filenames.
    name is the name of the index.
    """

    return re.compile("(_%s_[0-9]+).(%s)" % (indexname, _EXTENSIONS))























