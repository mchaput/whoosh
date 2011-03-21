# Copyright 2009 Matt Chaput. All rights reserved.
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

import cPickle
import re
import uuid
from time import time
from threading import Lock

from whoosh import __version__
from whoosh.fields import ensure_schema
from whoosh.index import Index, EmptyIndexError, IndexVersionError, _DEF_INDEX_NAME
from whoosh.reading import EmptyReader, MultiReader
from whoosh.store import Storage, LockError
from whoosh.system import _INT_SIZE, _FLOAT_SIZE, _LONG_SIZE


_INDEX_VERSION = -110


# TOC read/write functions

def _toc_filename(indexname, gen):
    return "_%s_%s.toc" % (indexname, gen)


def _toc_pattern(indexname):
    """Returns a regular expression object that matches TOC filenames.
    name is the name of the index.
    """

    return re.compile("^_%s_([0-9]+).toc$" % indexname)


def _segment_pattern(indexname):
    """Returns a regular expression object that matches segment filenames.
    name is the name of the index.
    """

    return re.compile("(_%s_[0-9]+)\\..*" % indexname)


def _latest_generation(storage, indexname):
    pattern = _toc_pattern(indexname)

    max = -1
    for filename in storage:
        m = pattern.match(filename)
        if m:
            num = int(m.group(1))
            if num > max:
                max = num
    return max


def _create_index(storage, schema, indexname=_DEF_INDEX_NAME):
    # Clear existing files
    prefix = "_%s_" % indexname
    for filename in storage:
        if filename.startswith(prefix):
            storage.delete_file(filename)
    
    schema = ensure_schema(schema)
    # Write a TOC file with an empty list of segments
    _write_toc(storage, schema, indexname, 0, 0, [])


def _write_toc(storage, schema, indexname, gen, segment_counter, segments):
    schema = ensure_schema(schema)
    schema.clean()

    # Use a temporary file for atomic write.
    tocfilename = _toc_filename(indexname, gen)
    tempfilename = '%s.%s' % (tocfilename, time())
    stream = storage.create_file(tempfilename)

    stream.write_varint(_INT_SIZE)
    stream.write_varint(_LONG_SIZE)
    stream.write_varint(_FLOAT_SIZE)
    stream.write_int(-12345)

    stream.write_int(_INDEX_VERSION)
    for num in __version__[:3]:
        stream.write_varint(num)

    stream.write_string(cPickle.dumps(schema, -1))
    stream.write_int(gen)
    stream.write_int(segment_counter)
    stream.write_pickle(segments)
    stream.close()

    # Rename temporary file to the proper filename
    storage.rename_file(tempfilename, tocfilename, safe=True)


class Toc(object):
    def __init__(self, **kwargs):
        for name, value in kwargs.iteritems():
            setattr(self, name, value)
        

def _read_toc(storage, schema, indexname):
    gen = _latest_generation(storage, indexname)
    if gen < 0:
        raise EmptyIndexError("Index %r does not exist in %r" % (indexname, storage))
    
    # Read the content of this index from the .toc file.
    tocfilename = _toc_filename(indexname, gen)
    stream = storage.open_file(tocfilename)

    def check_size(name, target):
        sz = stream.read_varint()
        if sz != target:
            raise IndexError("Index was created on different architecture:"
                             " saved %s = %s, this computer = %s" % (name, sz, target))

    check_size("int", _INT_SIZE)
    check_size("long", _LONG_SIZE)
    check_size("float", _FLOAT_SIZE)

    if not stream.read_int() == -12345:
        raise IndexError("Number misread: byte order problem")

    version = stream.read_int()
    if version != _INDEX_VERSION:
        raise IndexVersionError("Can't read format %s" % version, version)
    release = (stream.read_varint(), stream.read_varint(), stream.read_varint())
    
    # If the user supplied a schema object with the constructor, don't load
    # the pickled schema from the saved index.
    if schema:
        stream.skip_string()
    else:
        schema = cPickle.loads(stream.read_string())
    schema = ensure_schema(schema)
    
    # Generation
    index_gen = stream.read_int()
    assert gen == index_gen
    
    segment_counter = stream.read_int()
    segments = stream.read_pickle()
    
    stream.close()
    return Toc(version=version, release=release, schema=schema,
               segment_counter=segment_counter, segments=segments,
               generation=gen)


def _next_segment_name(self):
    #Returns the name of the next segment in sequence.
    if self.segment_num_lock is None:
        self.segment_num_lock = Lock()
    
    if self.segment_num_lock.acquire():
        try:
            self.segment_counter += 1
            return 
        finally:
            self.segment_num_lock.release()
    else:
        raise LockError


def _clean_files(storage, indexname, gen, segments):
    # Attempts to remove unused index files (called when a new generation
    # is created). If existing Index and/or reader objects have the files
    # open, they may not be deleted immediately (i.e. on Windows) but will
    # probably be deleted eventually by a later call to clean_files.

    current_segment_names = set(s.name for s in segments)

    tocpattern = _toc_pattern(indexname)
    segpattern = _segment_pattern(indexname)

    todelete = set()
    for filename in storage:
        tocm = tocpattern.match(filename)
        segm = segpattern.match(filename)
        if tocm:
            if int(tocm.group(1)) != gen:
                todelete.add(filename)
        elif segm:
            name = segm.group(1)
            if name not in current_segment_names:
                todelete.add(filename)
    
    for filename in todelete:
        try:
            storage.delete_file(filename)
        except OSError:
            # Another process still has this file open
            pass


# Index placeholder object

class FileIndex(Index):
    def __init__(self, storage, schema=None, indexname=_DEF_INDEX_NAME):
        if not isinstance(storage, Storage):
            raise ValueError("%r is not a Storage object" % storage)
        if not isinstance(indexname, (str, unicode)):
            raise ValueError("indexname %r is not a string" % indexname)
        
        if schema:
            schema = ensure_schema(schema)
        
        self.storage = storage
        self._schema = schema
        self.indexname = indexname
        
        # Try reading the TOC to see if it's possible
        _read_toc(self.storage, self._schema, self.indexname)

    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__,
                               self.storage, self.indexname)

    def close(self):
        pass

    # add_field
    # remove_field
    
    def latest_generation(self):
        return _latest_generation(self.storage, self.indexname)
    
    # refresh
    # up_to_date
    
    def last_modified(self):
        gen = self.latest_generation()
        filename = _toc_filename(self.indexname, gen)
        return self.storage.file_modified(filename)

    def is_empty(self):
        return len(self._read_toc().segments) == 0
    
    def optimize(self):
        w = self.writer()
        w.commit(optimize=True)

    # searcher
    
    def writer(self, **kwargs):
        from whoosh.filedb.filewriting import SegmentWriter
        return SegmentWriter(self, **kwargs)

    def lock(self, name):
        """Returns a lock object that you can try to call acquire() on to
        lock the index.
        """
        
        return self.storage.lock(self.indexname + "_" + name)

    def _read_toc(self):
        return _read_toc(self.storage, self._schema, self.indexname)

    def _segments(self):
        return self._read_toc().segments
    
    def _current_schema(self):
        return self._read_toc().schema
    
    @property
    def schema(self):
        return self._current_schema()

    @classmethod
    def _reader(self, storage, schema, segments, generation, reuse=None):
        from whoosh.filedb.filereading import SegmentReader
        
        reusable = {}
        try:
            if len(segments) == 0:
                # This index has no segments! Return an EmptyReader object,
                # which simply returns empty or zero to every method
                return EmptyReader(schema)
            
            if reuse:
                # Put all atomic readers in a dictionary keyed by their
                # generation, so we can re-use them if them if possible
                if reuse.is_atomic():
                    readers = [reuse]
                else:
                    readers = [r for r, offset in reuse.leaf_readers()]
                reusable = dict((r.generation(), r) for r in readers)
            
            # Make a function to open readers, which reuses reusable readers.
            # It removes any readers it reuses from the "reusable" dictionary,
            # so later we can close any remaining readers.
            def segreader(segment):
                gen = segment.generation
                if gen in reusable:
                    r = reusable[gen]
                    del reusable[gen]
                    return r
                else:
                    return SegmentReader(storage, schema, segment)
            
            if len(segments) == 1:
                # This index has one segment, so return a SegmentReader object
                # for the segment
                return segreader(segments[0])
            else:
                # This index has multiple segments, so create a list of
                # SegmentReaders for the segments, then composite them with a
                # MultiReader
                
                readers = [segreader(segment) for segment in segments]
                return MultiReader(readers, generation=generation)
        finally:
            for r in reusable.values():
                r.close()

    def reader(self, reuse=None):
        # Lock the index so nobody can delete a segment while we're in the
        # middle of creating the reader
        lock = self.lock("READLOCK")
        lock.acquire(True)
        try:
            # Read the information from the TOC file
            info = self._read_toc()
            return self._reader(self.storage, info.schema, info.segments,
                                info.generation, reuse=reuse)
        finally:
            lock.release()    


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

    EXTENSIONS = {"fieldlengths": "fln", "storedfields": "sto",
                  "termsindex": "trm", "termposts": "pst",
                  "vectorindex": "vec", "vectorposts": "vps"}
    
    generation = 0
    
    def __init__(self, name, generation, doccount, fieldlength_totals,
                 fieldlength_maxes, deleted=None):
        """
        :param name: The name of the segment (the Index object computes this
            from its name and the generation).
        :param doccount: The maximum document number in the segment.
        :param term_count: Total count of all terms in all documents.
        :param fieldlength_totals: A dictionary mapping field numbers to the
            total number of terms in that field across all documents in the
            segment.
        :param deleted: A set of deleted document numbers, or None if no
            deleted documents exist in this segment.
        """

        assert isinstance(name, basestring)
        assert isinstance(doccount, (int, long))
        assert fieldlength_totals is None or isinstance(fieldlength_totals, dict), "fl_totals=%r" % fieldlength_totals
        assert fieldlength_maxes is None or isinstance(fieldlength_maxes, dict), "fl_maxes=%r" % fieldlength_maxes
        
        self.name = name
        self.generation = generation
        self.doccount = doccount
        self.fieldlength_totals = fieldlength_totals
        self.fieldlength_maxes = fieldlength_maxes
        self.deleted = deleted
        self.uuid = uuid.uuid4()
        
    def __repr__(self):
        return "<%s %r %s>" % (self.__class__.__name__, self.name,
                               getattr(self, "uuid", ""))

    def __getattr__(self, name):
        # Capture accesses to e.g. Segment.fieldlengths_filename and return
        # the appropriate filename
        ext = "_filename"
        if name.endswith(ext):
            basename = name[:0 - len(ext)]
            if basename in self.EXTENSIONS:
                return self.make_filename(self.EXTENSIONS[basename])
        
        raise AttributeError(name)

    def copy(self):
        return Segment(self.name, self.generation, self.doccount,
                       self.fieldlength_totals, self.fieldlength_maxes,
                       self.deleted)

    def make_filename(self, ext):
        return "%s.%s" % (self.name, ext)

    @classmethod
    def basename(cls, indexname, segment_number):
        return "_%s_%s" % (indexname, segment_number)

    def doc_count_all(self):
        """
        :returns: the total number of documents, DELETED OR UNDELETED, in this
            segment.
        """
        return self.doccount

    def doc_count(self):
        """
        :returns: the number of (undeleted) documents in this segment.
        """
        return self.doccount - self.deleted_count()

    def has_deletions(self):
        """
        :returns: True if any documents in this segment are deleted.
        """
        return self.deleted_count() > 0

    def deleted_count(self):
        """
        :returns: the total number of deleted documents in this segment.
        """
        if self.deleted is None:
            return 0
        return len(self.deleted)

    def field_length(self, fieldname, default=0):
        """Returns the total number of terms in the given field across all
        documents in this segment.
        """
        return self.fieldlength_totals.get(fieldname, default)

    def max_field_length(self, fieldname, default=0):
        """Returns the maximum length of the given field in any of the
        documents in the segment.
        """
        return self.fieldlength_maxes.get(fieldname, default)

    def delete_document(self, docnum, delete=True):
        """Deletes the given document number. The document is not actually
        removed from the index until it is optimized.

        :param docnum: The document number to delete.
        :param delete: If False, this undeletes a deleted document.
        """

        if delete:
            if self.deleted is None:
                self.deleted = set()
            self.deleted.add(docnum)
        elif self.deleted is not None and docnum in self.deleted:
            self.deleted.clear(docnum)

    def is_deleted(self, docnum):
        """:returns: True if the given document number is deleted."""

        if self.deleted is None:
            return False
        return docnum in self.deleted


























