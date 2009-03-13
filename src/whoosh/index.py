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

"""Contains the main functions/classes for creating, maintaining, and using
an index.
"""

from __future__ import division
import os.path, re
from bisect import bisect_right
import cPickle
from threading import Lock

from whoosh import fields, store


_DEF_INDEX_NAME = "MAIN"
_EXTENSIONS = "dci|dcz|tiz|fvz"

# Exceptions

class OutOfDateError(Exception):
    """Raised when you try to commit changes to an index which is not
    the latest generation.
    """
    pass

class EmptyIndexError(Exception):
    """Raised when you try to work with an index that has no indexed terms.
    """
    pass

class IndexLockedError(Exception):
    """Raised when you try to write to or lock an already-locked index (or
    one that was accidentally left in a locked state).
    """
    pass

class IndexError(Exception):
    """Generic index error."""
    pass

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

def create_in(dirname, schema = None, indexname = None, **kwargs):
    """Convenience function to create an index in a directory. Takes care of creating
    a FileStorage object for you. dirname is the filename of the directory in
    which to create the index. schema is a fields.Schema object describing the
    index's fields. indexname is the name of the index to create; you only need to
    specify this if you are creating multiple indexes within the
    same storage object.
    
    If you specify both a schema and keyword arguments, the schema wins.
    
    Returns an Index object.
    """
    
    if not indexname:
        indexname = _DEF_INDEX_NAME
    
    storage = store.FileStorage(dirname)
    if kwargs and not schema:
        schema = fields.Schema(**kwargs)
    elif not schema and not kwargs:
        raise Exception("You must specify either a schema or keyword arguments.")
    
    return Index(storage, schema = schema, indexname = indexname, create = True)

def open_dir(dirname, indexname = None):
    """Convenience function for opening an index in a directory. Takes care of creating
    a FileStorage object for you. dirname is the filename of the directory in
    containing the index. indexname is the name of the index to create; you only need to
    specify this if you have multiple indexes within the same storage object.
    
    Returns an Index object.
    """
    
    if indexname is None:
        indexname = _DEF_INDEX_NAME
    
    return Index(store.FileStorage(dirname), indexname = indexname)

def exists(dirname, indexname = None):
    """Returns True if dirname contains a Whoosh index."""
    
    if indexname is None:
        indexname = _DEF_INDEX_NAME
    
    if os.path.exists(dirname):
        try:
            ix = open_dir(dirname)
            return ix.latest_generation() > -1
        except EmptyIndexError:
            pass

    return False


# A mix-in that adds methods for deleting
# documents from self.segments. These methods are on IndexWriter as
# well as Index for convenience, so they're broken out here.

class DeletionMixin(object):
    """Mix-in for classes that support deleting documents from self.segments."""
    
    def delete_document(self, docnum, delete = True):
        """Deletes a document by number."""
        self.segments.delete_document(docnum, delete = delete)
    
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
    
    def delete_by_term(self, fieldname, text, searcher = None):
        """Deletes any documents containing "term" in the "fieldname"
        field. This is useful when you have an indexed field containing
        a unique ID (such as "pathname") for each document.
        
        Note that this method opens and closes a Searcher. If you are calling
        this method repeatedly (for example, deleting changed documents before
        reindexing them), you will want to open your own Searcher object and
        pass it in with the 'searcher' keyword argument for efficiency.
        
        :*returns*: the number of documents deleted.
        """
        
        from whoosh.query import Term
        q = Term(fieldname, text)
        return self.delete_by_query(q, searcher = searcher)
    
    def delete_by_query(self, q, searcher = None):
        """Deletes any documents matching a query object.
        
        Note that this method opens and closes a Searcher. If you are calling
        this method repeatedly (for example, deleting changed documents before
        reindexing them), you should open your own Searcher object and
        pass it in with the 'searcher' keyword argument for efficiency.
        
        :*returns*: the number of documents deleted.
        """
        
        if searcher is None:
            from whoosh.searching import Searcher
            s = Searcher(self)
        else:
            s = searcher  
        
        count = 0
        try:
            for docnum in q.docs(s):
                self.delete_document(docnum)
                count += 1
            return count
        
        finally:
            if searcher is None:
                s.close()
        
        return count

# Index class

class Index(DeletionMixin):
    """Represents an indexed collection of documents.
    """
    
    def __init__(self, storage, schema = None, create = False, indexname = _DEF_INDEX_NAME):
        """
        :storage: The store.Storage object in which this index resides.
            See the store module for more details.
        :schema: A fields.Schema object defining the fields of this index. If you omit
            this argument for an existing index, the object will load the pickled Schema
            object that was saved with the index. If you are creating a new index
            (create = True), you must supply this argument.
        :create: Whether to create a new index. If this is True, you must supply
            a Schema instance using the schema keyword argument.
        :indexname: An optional name to use for the index. Use this if you need
            to keep multiple indexes in the same storage object.
        """
        
        self.storage = storage
        self.indexname = indexname
        
        if schema is not None and not isinstance(schema, fields.Schema):
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
            self.unlock()
            prefix = "_%s_" % self.indexname
            for filename in self.storage:
                if filename.startswith(prefix):
                    storage.delete_file(filename)
            
            self._write()
        elif self.generation >= 0:
            self._read(schema)
        else:
            raise EmptyIndexError
            
        self.segment_num_lock = Lock()
            
    def latest_generation(self):
        """Returns the generation number of the latest generation of this
        index.
        """
        
        pattern = _toc_pattern(self.indexname)
        
        max = -1
        for filename in self.storage:
            m = pattern.match(filename)
            if m:
                num = int(m.group(1))
                if num > max: max = num
        return max
    
    def refresh(self):
        """Returns a new Index object representing the latest generation
        of this index (if this object is the latest generation, returns
        self).
        :*returns*: index.Index
        """
        
        if not self.up_to_date():
            return self.__class__(self.storage, indexname = self.indexname)
        else:
            return self
    
    def up_to_date(self):
        """Returns True if this object represents the latest generation of
        this index. Returns False if this object is not the latest
        generation (that is, someone else has updated the index since
        you opened this object).
        """
        return self.generation == self.latest_generation()
    
    def _write(self):
        # Writes the content of this index to the .toc file.
        for field in self.schema:
            field.clean()
        stream = self.storage.create_file(self._toc_filename())
        stream.write_string(cPickle.dumps(self.schema, -1))
        stream.write_int(self.generation)
        stream.write_int(self.segment_counter)
        stream.write_pickle(self.segments)
        stream.close()
    
    def _read(self, schema):
        # Reads the content of this index from the .toc file.
        stream = self.storage.open_file(self._toc_filename())
        
        # If the user supplied a schema object with the constructor,
        # don't load the pickled schema from the saved index.
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
            raise IndexLockedError
    
    def _toc_filename(self):
        # Returns the computed filename of the TOC for this
        # index name and generation.
        return "_%s_%s.toc" % (self.indexname, self.generation)
    
    def last_modified(self):
        """Returns the last modified time of the .toc file.
        """
        return self.storage.file_modified(self._toc_filename())
    
    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__, self.storage, self.indexname)
    
    def lock(self):
        """Locks this index for writing, or raises an error if the index
        is already locked. Returns true if the index was successfully
        locked.
        """
        return self.storage.lock("_%s_LOCK" % self.indexname)
    
    def unlock(self):
        """Unlocks the index. Only call this if you were the one who locked
        it (without getting an exception) in the first place!
        """
        self.storage.unlock("_%s_LOCK" % self.indexname)
    
    def is_empty(self):
        """Returns True if this index is empty (that is, it has never
        had any documents successfully written to it.
        """
        return len(self.segments) == 0
    
    def optimize(self):
        """Optimizes this index's segments. This will fail if the index
        is already locked for writing.
        """
        
        if len(self.segments) < 2 and not self.segments.has_deletions():
            return
        
        from whoosh import writing
        w = writing.IndexWriter(self)
        w.commit(writing.OPTIMIZE)
    
    def commit(self, new_segments = None):
        """Commits pending edits (such as deletions) to this index object.
        Raises OutOfDateError if this index is not the latest generation
        (that is, if someone has updated the index since you opened
        this object).
        
        :new_segments: a replacement SegmentSet. This is used by
            IndexWriter to update the index after it finishes
            writing.
        """
        
        if not self.up_to_date():
            raise OutOfDateError
        
        if new_segments:
            self.segments = new_segments
        
        self.generation += 1
        self._write()
        self.clean_files()
    
    def clean_files(self):
        """Attempts to remove unused index files (called when a new generation
        is created). If existing Index and/or reader objects have the files
        open, they may not get deleted immediately (i.e. on Windows)
        but will probably be deleted eventually by a later call to clean_files.
        """
        
        storage = self.storage
        current_segment_names = set([s.name for s in self.segments])
        
        tocpattern = _toc_pattern(self.indexname)
        segpattern = _segment_pattern(self.indexname)
        
        for filename in storage:
            m = tocpattern.match(filename)
            if m:
                num = int(m.group(1))
                if num != self.generation:
                    storage.delete_file(filename)
            else:
                m = segpattern.match(filename)
                if m:
                    name = m.group(1)
                    if name not in current_segment_names:
                        storage.delete_file(filename)
    
    def doc_count_all(self):
        """Returns the total number of documents, DELETED OR UNDELETED,
        in this index.
        """
        return self.segments.doc_count_all()
    
    def doc_count(self):
        """Returns the total number of UNDELETED documents in this index.
        """
        return self.segments.doc_count()
    
    def max_weight(self):
        """Returns the maximum term weight in this index.
        This is used by some scoring algorithms.
        """
        return self.segments.max_weight()
    
    def total_term_count(self):
        """Returns the total term count across all fields in all documents.
        This is used by some scoring algorithms. Note that this
        necessarily includes terms in deleted documents.
        """
        return self.segments.total_term_count()
    
    def field_length(self, fieldnum):
        """Returns the total number of terms in a given field.
        This is used by some scoring algorithms. Note that this
        necessarily includes terms in deleted documents.
        """
        
        if isinstance(fieldnum, basestring):
            fieldnum = self.schema.number_to_name(fieldnum)
        
        return sum(s.field_length(fieldnum) for s in self.segments)
    
    def term_reader(self):
        """Returns a TermReader object for this index.
        
        :*returns*: reading.TermReader
        """
        
        from whoosh import reading
        segments = self.segments
        
        if len(segments) == 1:
            return reading.TermReader(self.storage, segments[0], self.schema)
        else:
            term_readers = [reading.TermReader(self.storage, s, self.schema)
                            for s in segments]
            doc_offsets = segments.doc_offsets()
            return reading.MultiTermReader(term_readers, doc_offsets, self.schema)
    
    def doc_reader(self):
        """Returns a DocReader object for this index.
        
        :*returns*: reading.DocReader
        """
        
        from whoosh import reading
        schema = self.schema
        segments = self.segments
        if len(segments) == 1:
            return reading.DocReader(self.storage, segments[0], schema)
        else:
            doc_readers = [reading.DocReader(self.storage, s, self.schema)
                           for s in segments]
            doc_offsets = segments.doc_offsets()
            return reading.MultiDocReader(doc_readers, doc_offsets, schema)
    
    def searcher(self, **kwargs):
        """Returns a Searcher object for this index. Keyword arguments
        are passed to the Searcher object's constructor.
        
        :*returns*: searching.Searcher
        """
        
        from whoosh.searching import Searcher
        return Searcher(self, **kwargs)
    
    def writer(self, **kwargs):
        """Returns an IndexWriter object for this index.
        
        :*returns*: writing.IndexWriter
        """
        from whoosh.writing import IndexWriter
        return IndexWriter(self, **kwargs)
    
    def find(self, querystring, parser = None, **kwargs):
        """Parses querystring, runs the query in this index, and returns a
        Result object. Any additional keyword arguments are passed to
        Searcher.search() along with the parsed query.

        :querystring: The query string to parse and search for.
        :parser: A Parser object to use to parse 'querystring'.
            The default is to use a standard qparser.QueryParser.
            This object must implement a parse(str) method which returns a
            query.Query instance.
        :*returns*: searching.Results
        """

        if parser is None:
            from whoosh.qparser import QueryParser
            parser = QueryParser(self.schema)
            
        return self.searcher().search(parser.parse(querystring), **kwargs)
    
    


# SegmentSet object


class SegmentSet(object):
    """This class is never instantiated by the user. It is used by the Index
    object to keep track of the segments in the index.
    """

    def __init__(self, segments = None):
        if segments is None:
            self.segments = []
        else:
            self.segments = segments
        
        self._doc_offsets = self.doc_offsets()
    
    def __repr__(self):
        return repr(self.segments)
    
    def __len__(self):
        """:*returns*: the number of segments in this set."""
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
        """Returns an (index.Segment, segment_docnum) pair for the
        segment containing the given document number.
        """
        
        segmentnum = self._document_segment(docnum)
        offset = self._doc_offsets[segmentnum]
        segment = self.segments[segmentnum]
        return segment, docnum - offset
    
    def copy(self):
        """:*returns*: a deep copy of this set."""
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
        :*returns*: the total number of documents, DELETED or
            UNDELETED, in this set.
        """
        return sum(s.doc_count_all() for s in self.segments)
    
    def doc_count(self):
        """
        :*returns*: the number of undeleted documents in this set.
        """
        return sum(s.doc_count() for s in self.segments)
    
    
    def max_weight(self):
        """
        :*returns*: the maximum frequency of any term in the set.
        """
        
        if not self.segments:
            return 0
        return max(s.max_weight for s in self.segments)
    
    def total_term_count(self):
        """
        :*returns*: the total number of terms in the set. Note that this
            necessarily includes deleted documents.
        """
        return sum(s.term_count for s in self.segments)
    
    def has_deletions(self):
        """
        :*returns*: True if this index has documents that are marked
            deleted but haven't been optimized out of the index yet.
            This includes deletions that haven't been written to disk
            with Index.commit() yet.
        """
        return any(s.has_deletions() for s in self.segments)
    
    def delete_document(self, docnum, delete = True):
        """Deletes a document by number.

        You must call Index.commit() for the deletion to be written to disk.
        """
        
        segment, segdocnum = self._segment_and_docnum(docnum)
        segment.delete_document(segdocnum, delete = delete)
    
    def deleted_count(self):
        """
        :*returns*: the total number of deleted documents in this index.
        """
        return sum(s.deleted_count() for s in self.segments)
    
    def is_deleted(self, docnum):
        """
        :*returns*: True if a given document number is deleted but not yet
            optimized out of the index.
        """
        
        segment, segdocnum = self._segment_and_docnum(docnum)
        return segment.is_deleted(segdocnum)
    

class Segment(object):
    """Do not instantiate this object directly. It is used by the Index
    object to hold information about a segment. A list of objects of this
    class are pickled as part of the TOC file.
    
    The TOC file stores a minimal amount of information -- mostly a list of
    Segment objects. Segments are the real reverse indexes. Having multiple
    segments allows quick incremental indexing: just create a new segment for
    the new documents, and have the index overlay the new segment over previous
    ones for purposes of reading/search. "Optimizing" the index combines the
    contents of existing segments into one (removing any deleted documents
    along the way).
    """
    
    def __init__(self, name, max_doc,
                 term_count, max_weight, field_length_totals,
                 deleted = None):
        """
        :name: The name of the segment (the Index object computes this from its
            name and the generation).
        :max_doc: The maximum document number in the segment.
        :term_count: Total count of all terms in all documents.
        :max_weight: The maximum weight of any term in the segment. This is used
            by some scoring algorithms.
        :field_length_totals: A dictionary mapping field numbers to the total
            number of terms in that field across all documents in the segment.
        :deleted: A collection of deleted document numbers, or None
            if no deleted documents exist in this segment.
        """
        
        self.name = name
        self.max_doc = max_doc
        self.term_count = term_count
        self.max_weight = max_weight
        self.field_length_totals = field_length_totals
        self.deleted = deleted
        
        self.doclen_filename = self.name + ".dci"
        self.docs_filename = self.name + ".dcz"
        self.term_filename = self.name + ".tiz"
        self.vector_filename = self.name + ".fvz"
    
    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.name)
    
    def copy(self):
        return Segment(self.name, self.max_doc,
                       self.term_count, self.max_weight, self.field_length_totals,
                       self.deleted)
    
    def doc_count_all(self):
        """
        :*returns*: the total number of documents, DELETED OR UNDELETED,
            in this segment.
        """
        return self.max_doc
    
    def doc_count(self):
        """:*returns*: the number of (undeleted) documents in this segment."""
        return self.max_doc - self.deleted_count()
    
    def has_deletions(self):
        """:*returns*: True if any documents in this segment are deleted."""
        return self.deleted_count() > 0
    
    def deleted_count(self):
        """:*returns*: the total number of deleted documents in this segment."""
        if self.deleted is None: return 0
        return len(self.deleted)
    
    def field_length(self, fieldnum):
        """
        :fieldnum: the internal number of the field.
        :*returns*: the total number of terms in the given field across all
            documents in this segment.
        """
        return self.field_length_totals.get(fieldnum, 0)
    
    def delete_document(self, docnum, delete = True):
        """Deletes the given document number. The document is not actually
        removed from the index until it is optimized.

        :docnum: The document number to delete.
        :delete: If False, this undeletes a deleted document.
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
            
            self.deleted.remove(docnum)
    
    def is_deleted(self, docnum):
        """:*returns*: True if the given document number is deleted."""
        
        if self.deleted is None: return False
        return docnum in self.deleted

# Debugging functions

        
if __name__ == '__main__':
    pass
