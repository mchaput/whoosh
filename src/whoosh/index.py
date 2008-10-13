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

"""
This module contains the main functions/classes for maintaining an index.
"""

from __future__ import division
import re
from bisect import bisect_right

import reading, store, writing


_DEF_INDEX_NAME = "MAIN"
_EXTENSIONS = "dci|dcz|pst|tiz|fvz"

# Exceptions

class OutOfDateError(Exception):
    """
    Raised when you try to commit changes to an index which is not
    the latest generation.
    """
    pass

class EmptyIndex(Exception):
    """
    Raised when you try to work with an index that has no indexed terms.
    """
    pass

class IndexLocked(Exception):
    """
    Raised when you try to write to or lock an already-locked index (or
    one that was accidentally left in a locked state).
    """
    pass

class IndexError(Exception):
    """
    Generic index error.
    """
    pass

# Utility functions

def toc_pattern(indexname):
    """
    Returns a regular expression object that matches TOC filenames.
    name is the name of the index.
    """
    
    return re.compile("_%s_([0-9]+).toc" % indexname)

def segment_pattern(indexname):
    """
    Returns a regular expression object that matches segment filenames.
    name is the name of the index.
    """
    
    return re.compile("(_%s_[0-9]+).(%s)" % (indexname, _EXTENSIONS))

def _last_generation(storage, indexname):
    """
    Utility function to find the most recent generation number of the index.
    storage is the storage object containing the index. indexname is the name of
    the index.
    """
    
    pattern = toc_pattern(indexname)
    
    max = -1
    for filename in storage:
        m = pattern.match(filename)
        if m:
            num = int(m.group(1))
            if num > max: max = num
    return max

def clear_index(storage, indexname):
    """
    Clears all information from an index!
    storage is the storage object containing the index. indexname is the name of
    the index.
    """
    
    prefix = "_%s_" % indexname
    for filename in storage:
        if filename.startswith(prefix):
            storage.delete_file(filename)

def create(storage, schema, indexname = _DEF_INDEX_NAME):
    """
    Initializes necessary files for a new index.
    storage is the storage object in which to create the index.
    schema is an index.Schema object describing the index's fields.
    indexname is the name of the index to create; you only need to
    specify this if you are creating multiple indexes within the
    same storage object.
    
    Returns an index.Index object.
    """
    
    clear_index(storage, indexname)
    _write_index_file(storage, indexname, 0, [], schema, 0)
    ix = Index(storage, indexname)
    if ix.is_locked():
        ix.unlock()
    return ix

def _write_index_file(storage, indexname, generation, segments, schema, counter):
    """
    Utility function writes an index TOC file using the informaiton supplied in the
    arguments.
    """
    stream = storage.create_file("_%s_%s.toc" % (indexname, generation))
    stream.write_pickle((segments, schema, counter))
    stream.close()

def _toc_name(name, generation):
    """
    Utility function returns the filename for the TOC file given an index name
    and a generation number.
    """
    
    return "_%s_%s.toc" % (name, generation)

def _read_index_file(storage, name, generation):
    """
    Utility function reads the contents of an index TOC file and returns the
    information inside as a tuple of ([index.Segment], index.Schema, counter)
    """
    
    stream = storage.open_file(_toc_name(name, generation))
    segments, schema, counter = stream.read_pickle()
    stream.close()
    return segments, schema, counter

def _last_modified(storage, name):
    """
    Utility function takes a storage object and the name of an index an returns
    the last modified time of the index.
    """
    
    gen = _last_generation(storage, name)
    return storage.file_modified(_toc_name(name, gen))


def create_index_in(dirname, schema, indexname = None):
    """
    Convenience function to create an index in a directory. Takes care of creating
    a FileStorage object for you. dirname is the filename of the directory in
    which to create the index. schema is an index.Schema object describing the
    index's fields. indexname is the name of the index to create; you only need to
    specify this if you are creating multiple indexes within the
    same storage object.
    
    Returns an index.Index object.
    """
    
    if indexname is None:
        indexname = _DEF_INDEX_NAME
    
    storage = store.FileStorage(dirname)
    return create(storage, schema, indexname = indexname)

def open_dir(dirname, indexname = None):
    """
    Convenience function for opening an index in a directory. Takes care of creating
    a FileStorage object for you. dirname is the filename of the directory in
    containing the index. indexname is the name of the index to create; you only need to
    specify this if you have multiple indexes within the same storage object.
    
    Returns an index.Index object.
    """
    
    if indexname is None:
        indexname = _DEF_INDEX_NAME
    
    return Index(store.FileStorage(dirname), indexname)

def has_index(dirname, indexname = None):
    """
    Returns whether a given directory contains a valid index.
    indexname is the name of the index to create; you only need to
    specify this if you have multiple indexes within the same storage object.
    """
    
    if indexname is None:
        indexname = _DEF_INDEX_NAME
        
    gen = _last_generation(store.FileStorage(dirname), indexname)
    return gen >= 0

# Classes

class Schema(object):
    """
    Represents the fields in an index.
    """
    
    def __init__(self, *fields):
        """
        The positional arguments to he constructor must be INSTANTIATED fields.Field
        objects (not classes) representing the fields of an index.
        """
        
        self.by_number = []
        self.by_name = {}
        
        for field in fields:
            self.add(field)
    
    def __repr__(self):
        return "<Schema: %r>" % self.by_number
    
    def __iter__(self):
        return iter(self.by_number)
    
    def add(self, field):
        """
        Adds a fields.Field object to this schema.
        """
        
        if self.by_name.has_key(field.name):
            raise Exception("Schema already has a field named %s" % field.name)
        
        num = len(self.by_number)
        field.number = num
        self.by_number.append(field)
        self.by_name[field.name] = field
    
    def field_names(self):
        """
        Returns a list of the names of the fields in this schema.
        """
        return self.by_name.keys()
    
    def name_to_number(self, name):
        """
        Given a field name, returns the field's number.
        """
        return self.by_name[name].number
    
    def number_to_name(self, number):
        """
        Given a field number, returns the field's name.
        """
        return self.by_number[number].name
    
    def has_name(self, name):
        """
        Returns True if this schema has a field by the given name.
        """
        return name in self.by_name
    
    def has_field(self, field):
        """
        Returns True if this schema contains the given fields.Field object.
        """
        return self.has_name(field.name) and self.by_name[field.name] == field
    
    def has_vectors(self):
        """
        Returns True if any of the fields in this schema store term vectors.
        """
        return any(field.vector for field in self)
    
    def vectored_fields(self):
        """
        Returns a list of field numbers corresponding to the fields that are
        vectored.
        """
        return [field.number for field in self if field.vector]


class Index(object):
    """
    Represents (a generation of) an index. You must create the index using
    index.create() or index.create_index_in() before you can instantiate this
    object (otherwise it will raise index.EmptyIndex).
    """
    
    def __init__(self, storage, indexname = None):
        """
        storage is a storage object in which this index is stored.
        indexname is the name of the index; you only need to
        specify this if you have multiple indexes within the
        same storage object.
        """
        
        if indexname is None:
            indexname = _DEF_INDEX_NAME
        
        self.storage = storage
        self.name = indexname
        
        self.generation = _last_generation(storage, indexname)
        if self.generation >= 0:
            self.reload()
        else:
            raise EmptyIndex
        
        self._dr = self._tr = None
        
    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__, self.storage, self.name)
    
    def lock(self):
        """
        Locks this index for writing, or raises IndexLocked if the index
        is already locked.
        """
        
        try:
            self.storage.make_dir("_%s_LOCK" % self.name)
            return True
        except:
            raise IndexLocked
    
    def is_locked(self):
        """
        Returns True if this index is currently locked for writing.
        """
        
        return self.storage.file_exists("_%s_LOCK" % self.name)
    
    def unlock(self):
        """
        Unlocks the index. Only call this if you were the one who locked
        it (without getting an IndexLocked exception) in the first place!
        """
        
        try:
            self.storage.remove_dir("_%s_LOCK" % self.name)
        except:
            pass
    
    def is_empty(self):
        """
        Returns True if this index is empty (that is, it has never
        had any documents sucessfully written to it.
        """
        
        return len(self.segments) == 0
    
    def field_by_name(self, name):
        """
        Given a field name, returns the fields.Field object
        from this index's schema.
        """
        return self.schema.by_name[name]
    
    def fieldnum_by_name(self, name):
        """
        Given a field name, returns the field number in this
        index's schema.
        """
        return self.schema.name_to_number(name)
    
    def doc_count_all(self):
        """
        Returns the total number of documents, DELETED OR UNDELETED,
        in this index.
        """
        return sum(s.max_doc for s in self.segments)
    
    def doc_count(self):
        """
        Returns the total number of UNDELETED documents in this index.
        """
        return sum(s.doc_count() for s in self.segments)
    
    def max_count(self):
        """
        Returns the maximum term weight in this index.
        This is used by some scoring algorithms.
        """
        return max(s.max_count for s in self.segments)
    
    def term_count(self):
        """
        Returns the total term count across all fields in all documents.
        This is used by some scoring algorithms.
        """
        return sum(s.term_count for s in self.segments)
    
    def field_length(self, fieldnum):
        """
        Returns the total number of terms in a given field (the "field length").
        This is used by some scoring algorithms.
        """
        
        if isinstance(fieldnum, basestring):
            fieldnum = self.schema.number_to_name(fieldnum)
        
        return sum(s.field_counts.get(fieldnum, 0) for s in self.segments)
    
    def sibling(self, indexname):
        """
        Convenience function to get another index in the same storage
        object as this one. This is only useful if you have multiple
        indexes in the same storage object.
        
        Returns an index.Index object.
        """
        
        return Index(self.storage, indexname = indexname)
    
    def term_reader(self):
        segs = self.segments
        
        if len(segs) == 1:
            segment = segs[0]
            return reading.TermReader(self.storage, segment, self.schema)
        else:
            term_readers = [reading.TermReader(self.storage, s, self.schema)
                            for s in segs]
            return reading.MultiTermReader(term_readers, self.doc_offsets)
    
    def doc_reader(self):
        schema = self.schema
        if len(self.segments) == 1:
            return reading.DocReader(self.storage, self.segments[0], schema)
        else:
            doc_readers = [reading.DocReader(self.storage, s, schema)
                           for s in self.segments]
            return reading.MultiDocReader(doc_readers, self.doc_offsets)
    
    def find(self, querystring):
        import searching, qparser
        s = searching.Searcher(self)
        pq = qparser.QueryParser(self.schema).parse(querystring)
        return s.search(pq)
    
    def doc(self, **kw):
        """
        Convenience function returns the stored fields of a document
        matching the given keyword arguments, where the keyword keys are
        field names and the values are terms that must appear in the field.
        
        Where Index.docs() returns a generator, this function returns either
        a dictionary or None. Use it when you assume the given keyword arguments
        either match zero or one documents (i.e. at least one of the fields is
        a unique key).
        
        This method opens and closes a temporary searcher for each call and
        forwards to its equivalent method. If you are calling it multiple times
        in a row, you should open your own searcher instead.
        """
        
        for p in self.docs(**kw):
            return p
    
    def docs(self, **kw):
        """
        Convenience function returns the stored fields of a document
        matching the given keyword arguments, where the keyword keys are
        field names and the values are terms that must appear in the field.
        
        Returns a list (not a generator, so as not to keep the readers open)
        of dictionaries containing the stored fields of any documents matching
        the keyword arguments.
        
        This method opens and closes a temporary searcher for each call and
        forwards to its equivalent method. If you are calling it multiple times
        in a row, you should open your own searcher instead.
        """
        
        import searching
        s = searching.Searcher(self)
        try:
            return s.docs(**kw)
        finally:
            s.close()
        
    
    def term_exists(self, fieldname, text):
        """
        Returns True if the given term exists in this index.
        
        Note that this convenience method opens and closes a temporary TermReader.
        If you are planning to call this multiple times, it's more efficient to
        create your own TermReader and use 'term in term_reader'.
        """
        
        tr = self.term_reader()
        try:
            return (fieldname, text) in tr
        finally:
            tr.close()
    
    def stored(self, docnum):
        """
        Returns the stored fields of the given document number.
        
        Note that this convenience method opens and closes a temporary DocReader.
        If you are planning to call it multiple times, it's more efficient to
        create your own DocReader.
        """
        
        dr = self.doc_reader()
        try:
            fields = dr[docnum]
        finally:
            dr.close()
        
        return fields
    
    def up_to_date(self):
        """
        Returns true if this object represents the current generation of
        the index.
        """
        
        return self.generation == _last_generation(self.storage, self.name)
    
    def last_modified(self):
        """
        Returns the last modified time of this index.
        """
        
        return _last_modified(self.storage, self.name)
    
    def next_segment_name(self):
        """
        Returns the name of the next segment in sequence.
        """
        
        self.counter += 1
        return "_%s_%s" % (self.name, self.counter)
    
    def reload(self):
        """
        Reloads information from this index/generation's files on disk.
        This will NOT update the object to a later generation.
        """
        
        segments, self.schema, self.counter = _read_index_file(self.storage, self.name, self.generation)
        self._set_segments(segments)
    
    def refresh(self):
        """
        Returns the latest generation of this index.
        """
        return self.__class__(self.storage, indexname = self.name)
    
    def _set_segments(self, segments):
        """
        Sets this object's segment information. This is called by a writer
        to update the Index object's information after the writer commits.
        """
        
        self.segments = segments
        
        self.doc_offsets = []
        self.max_doc = 0
        
        for segment in self.segments:
            self.doc_offsets.append(self.max_doc)
            self.max_doc += segment.max_doc
    
    def _add_segment_tuples(self, segtuples):
        segments = [Segment(name, maxdoc, termcount, maxcount, dict(fieldcounts))
                    for name, maxdoc, termcount, maxcount, fieldcounts
                    in segtuples]
        self._set_segments(self.segments + segments)
    
    def _document_segment(self, docnum):
        """
        Returns the index.Segment object containing the given document
        number.
        """
        
        if len(self.doc_offsets) == 1: return 0
        return bisect_right(self.doc_offsets, docnum) - 1
    
    def _segment_and_docnum(self, docnum):
        """
        Returns an (index.Segment, segment_docnum) tuple for the
        given document number.
        """
        
        segmentnum = self._document_segment(docnum)
        offset = self.doc_offsets[segmentnum]
        segment = self.segments[segmentnum]
        return segment, docnum - offset
    
    def delete_document(self, docnum):
        """
        Deletes a document by number.

        You must call Index.commit() for the deletion to be written to disk.
        """
        
        segment, segdocnum = self._segment_and_docnum(docnum)
        segment.delete_document(segdocnum)
    
    def is_deleted(self, docnum):
        """
        Returns True if a given document number is deleted but
        not yet optimized out of the index.
        
        You must call Index.() for the deletion to be written to disk.
        """
        
        segment, segdocnum = self._segment_and_docnum(docnum)
        return segment.is_deleted(segdocnum)
    
    def delete_by_term(self, fieldname, text, searcher = None):
        """
        Deletes any documents containing "term" in the "fieldname"
        field. This is useful when you have an indexed field containing
        a unique ID (such as "pathname") for each document.
        
        You must call Index.commit() for the deletion to be written to disk.
        
        Note that this method opens and closes a Searcher. If you are calling
        this method repeatedly (for example, deleting changed documents before
        reindexing them), you will want to open your own Searcher object and
        pass it in with the 'searcher' keyword argument for efficiency.
        
        Returns the number of documents deleted.
        """
        
        import query
        q = query.Term(fieldname, text, searcher = searcher)
        return self.delete_by_query(q)
    
    def delete_by_query(self, q, searcher = None):
        """
        Deletes any documents matching a query object.
        
        You must call Index.commit() for the deletion to be written to disk.
        
        Note that this method opens and closes a Searcher. If you are calling
        this method repeatedly (for example, deleting changed documents before
        reindexing them), you should open your own Searcher object and
        pass it in with the 'searcher' keyword argument for efficiency.
        
        Returns the number of documents deleted.
        """
        
        if searcher is None:
            from searching import Searcher
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
    
    def has_deletions(self):
        """
        Returns True if this index has documents that are marked
        deleted but haven't been optimized out of the index yet.
        This includes deletions that haven't been written to disk
        with Index.commit() yet.
        """
        
        for segment in self.segments:
            if segment.has_deletions(): return True
        return False
    
    def optimize(self):
        """
        Optimizes this index's segments.
        
        This opens and closes a writing.IndexWriter object, so it may
        fail if the index is already locked for writing.
        """
        
        if len(self.segments) < 2 and not self.has_deletions():
            return
        w = writing.IndexWriter(self)
        w.optimize()
        w.close()
    
    def commit(self):
        """
        Commits pending edits (such as deletions) to this index object.
        Raises OutOfDateError if this index is not the latest generation
        (that is, if some code has written to the index since you opened
        this object).
        """
        
        if not self.up_to_date():
            raise OutOfDateError
        
        self.generation += 1
        _write_index_file(self.storage, self.name, self.generation, self.segments, self.schema, self.counter)
        self.clean_files()
    
    def clean_files(self):
        """
        Attempts to remove unused index files (called when a new generation
        is created). If existing Index and/or reader objects have the files
        open, they may not get deleted immediately (i.e. on Windows)
        but will probably be deleted eventually by a later call to clean_files.
        """
        
        storage = self.storage
        current_segment_names = set([s.name for s in self.segments])
        
        tocpattern = toc_pattern(self.name)
        segpattern = segment_pattern(self.name)
        
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


class Segment(object):
    """
    This object is never instantiated by the user. It is used by the Index
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
    
    def __init__(self, name, max_doc, term_count, max_count, field_counts, deleted = None):
        """
        name is the name of the segment (the Index object computes this from its
        name and the generation). max_doc is the maximum document number in the
        segment.
        term_count is the total count of all terms in all documents. max_count is
        the maximum count of any term in the segment. deleted is a set of deleted
        document numbers, or None if no documents are deleted in this segment.
        """
        
        self.name = name
        self.max_doc = max_doc
        self.term_count = term_count
        self.max_count = max_count
        self.field_counts = field_counts
        self.deleted = deleted
        
        self.doclen_filename = self.name + ".dci"
        self.docs_filename = self.name + ".dcz"
        self.term_filename = self.name + ".tiz"
        self.vector_filename = self.name + ".fvz"
    
    def __repr__(self):
        return "%s(\"%s\")" % (self.__class__.__name__, self.name)
    
    def doc_count(self):
        """
        Returns the number of (undeleted) documents in this segment.
        """
        return self.max_doc - self.deleted_count()
    
    def has_deletions(self):
        """
        Returns True if any documents in this segment are deleted.
        """
        return self.deleted_count() > 0
    
    def deleted_count(self):
        """
        Returns the total number of deleted documents in this segment.
        """
        if self.deleted is None: return 0
        return len(self.deleted)
    
    def delete_document(self, docnum, delete = True):
        """
        Deletes the given document number. The document is not actually
        removed from the index until it is optimized.
        if delete = False, this undeletes a deleted document.
        """
        
        if delete:
            if self.deleted is None:
                self.deleted = set()
            elif docnum in self.deleted:
                raise KeyError("Document is already deleted" % docnum)
            
            self.deleted.add(docnum)
        else:
            if self.deleted is None or docnum not in self.deleted:
                raise KeyError("Document is not deleted" % docnum)
            
            self.deleted.remove(docnum)
    
    def is_deleted(self, docnum):
        """
        Returns True if the given document number is deleted.
        """
        if self.deleted is None: return False
        return docnum in self.deleted

# Debugging functions

        
if __name__ == '__main__':
    pass
    
    
    
    
    
    
    