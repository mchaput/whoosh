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

import re, sys
from base64 import b64encode, b64decode
from datetime import datetime
from os import urandom
from time import time, sleep

from whoosh import __version__
from whoosh.compat import pickle, integer_types, string_type, iteritems
from whoosh.fields import ensure_schema, merge_schemas
from whoosh.index import Index, EmptyIndexError, IndexVersionError, _DEF_INDEX_NAME
from whoosh.reading import EmptyReader, MultiReader
from whoosh.store import Storage, LockError
from whoosh.system import _INT_SIZE, _FLOAT_SIZE, _LONG_SIZE


_INDEX_VERSION = -110


# TOC read/write functions

ROOT_ID = "\x00" * 16


class Revision(object):
    def __init__(self, indexname, id, schema, parentids=None, segments=None,
                 release=None, created=None):
        self.indexname = indexname
        self.id = id if id is not None else self._make_id()
        self.schema = schema
        self.parentids = parentids or ()
        self.segments = segments or ()
        self.release = release
        self.created = created
    
    @staticmethod
    def _make_id():
        return urandom(12)
    
    @staticmethod
    def _filename(indexname, id):
        i = b64encode(id, "-_")
        return "%s.%s.toc" % (indexname, i)
    
    @staticmethod
    def regex(indexname):
        pat = r"^%s\.(?P<id>.{12})\.toc$" % indexname
        return re.compile(pat)
    
    @classmethod
    def create(cls, storage, indexname, schema, parentids=None, segments=None):
        rev = cls(indexname, cls._make_id(), schema, parentids, segments)
        rev.store(storage)
        return rev
    
    @classmethod
    def load(cls, storage, indexname, id, schema=None):
        fname = cls._filename(indexname, id)
        stream = storage.open_file(fname)
    
        # Check size of data types
        def check_size(name, target):
            sz = stream.read_varint()
            if sz != target:
                raise IndexError("Index was created on different architecture:"
                                 " saved %s=%s, this computer=%s" % (name, sz, target))
        check_size("int", _INT_SIZE)
        check_size("long", _LONG_SIZE)
        check_size("float", _FLOAT_SIZE)
        
        # Check byte order
        if not stream.read_int() == -12345:
            raise IndexError("Number misread: byte order problem?")
        
        # Check format version data
        version = stream.read_int()
        if version != _INDEX_VERSION:
            raise IndexVersionError("Can't read format %s" % version, version)
        # Load Whoosh version that created this TOC
        release = stream.read_pickle()
        # Read the list of parent IDs
        parentids = stream.read_pickle()
        # Check that the filename and internal ID match
        _id = stream.read(16)
        if _id != id:
            raise Exception("ID in %s is %s" % (fname, b64encode(_id)))
        # Creation date
        created = stream.read_pickle()
        # If a schema was supplied, use it instead of reading the one on disk
        if schema:
            stream.skip_string()
        else:
            schema = pickle.loads(stream.read_string())
        # Load the segments
        segments = stream.read_pickle()
        stream.close()
        return cls(indexname, id, schema, parentids, segments, release, created)
    
    @classmethod
    def find_all(cls, storage, indexname):
        regex = cls.regex(indexname)
        for fname in storage:
            m = regex.match(fname)
            if m:
                yield b64decode(m.group("id"))
    
    @classmethod
    def load_all(cls, storage, indexname, schema=None, suppress=False):
        for id in cls.find_all(storage, indexname):
            try:
                yield cls.load(storage, indexname, id, schema=schema)
            except OSError:
                if not suppress:
                    raise
    
    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, self.filename())
    
    def filename(self):
        return self._filename(self.indexname, self.id)
    
    def store(self, storage):
        schema = ensure_schema(self.schema)
        schema.clean()
        
        tocfilename = self.filename()
        tempfilename = '%s.%s' % (tocfilename, time())
        stream = storage.create_file(tempfilename)
        
        # Write the sizes of datatypes to check system compatibility
        stream.write_varint(_INT_SIZE)
        stream.write_varint(_LONG_SIZE)
        stream.write_varint(_FLOAT_SIZE)
        # Write a dummy value to check byte order
        stream.write_int(-12345)
        # Write the index format version and Whoosh version
        stream.write_int(_INDEX_VERSION)
        stream.write_pickle(__version__)
        # Write self
        stream.write_pickle(tuple(self.parentids))
        stream.write(self.id)
        stream.write_pickle(datetime.utcnow())
        stream.write_string(pickle.dumps(self.schema, -1))
        stream.write_pickle(self.segments)
        stream.close()
        
        # Rename temporary file to the proper filename
        storage.rename_file(tempfilename, tocfilename, safe=True)
    
    def delete_files(self, storage, suppress=True):
        try:
            storage.delete_file(self.filename())
        except OSError:
            if not suppress:
                raise


class Segment(object):
    EXTENSIONS = {"dawg": "dag",
                  "fieldlengths": "fln",
                  "storedfields": "sto",
                  "termsindex": "trm",
                  "termposts": "pst",
                  "vectorindex": "vec",
                  "vectorposts": "vps"}
    
    def __init__(self, indexname, id=None, doccount=0, fieldlength_totals=None,
                 fieldlength_mins=None, fieldlength_maxes=None, deleted=None):
        self.indexname = indexname
        self.id = id or self._make_id()
        self.doccount = doccount
        self.fieldlength_totals = fieldlength_totals or {}
        self.fieldlength_mins = fieldlength_mins or {}
        self.fieldlength_maxes = fieldlength_maxes or {}
        self.deleted = deleted
        
    @staticmethod
    def _make_id():
        return urandom(12)
    
    @classmethod
    def _idstring(cls, segid):
        return b64encode(segid)
    
    @classmethod
    def _basename(cls, indexname, segid):
        return "%s.%s" % (indexname, cls._idstring(segid))
    
    @classmethod
    def _make_filename(cls, indexname, segid, ext):
        return "%s.%s" % (cls._basename(indexname, segid), ext)
    
    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, b64encode(self.id))

    def __getattr__(self, name):
        # Capture accesses to e.g. Segment.fieldlengths_filename and return
        # the appropriate filename
        ext = "_filename"
        if name.endswith(ext):
            basename = name[:0 - len(ext)]
            if basename in self.EXTENSIONS:
                return self.make_filename(self.EXTENSIONS[basename])
        
        raise AttributeError(name)
    
    def exists_in(self, storage):
        return any(storage.file_exists(self.make_filename(ext))
                   for ext in self.EXTENSIONS.values())
    
    def delete_files(self, storage, suppress=True):
        for ext in self.EXTENSIONS.values():
            fname = self.make_filename(ext)
            if storage.file_exists(fname):
                try:
                    storage.delete_file(fname)
                except OSError:
                    if not suppress:
                        raise
    
    def basename(self):
        return self._basename(self.indexname, self.id)

    def make_filename(self, ext):
        return self._make_filename(self.indexname, self.id, ext)
    
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

    def min_field_length(self, fieldname, default=0):
        """Returns the maximum length of the given field in any of the
        documents in the segment.
        """
        
        return self.fieldlength_mins.get(fieldname, default)

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


def _leaf_revs(storage, indexname, parentids=None):
    parentids = parentids or set()
    revs = list(Revision.load_all(storage, indexname, suppress=True))
    for rev in revs:
        parentids.update(rev.parentids)
    return [rev for rev in revs if rev.id not in parentids]


def _create_index(storage, schema, indexname=_DEF_INDEX_NAME):
    # Clear existing files
    prefix = "%s." % indexname
    for filename in storage:
        if filename.startswith(prefix):
            storage.delete_file(filename)
    
    # Create and store the root revision
    schema = ensure_schema(schema)
    return Revision.create(storage, indexname, schema)



    
    

# Index placeholder object

class FileIndex(Index):
    def __init__(self, storage, schema=None, indexname=_DEF_INDEX_NAME):
        if not isinstance(storage, Storage):
            raise ValueError("%r is not a Storage object" % storage)
        if not isinstance(indexname, string_type):
            raise ValueError("indexname %r is not a string" % indexname)
        
        if schema:
            schema = ensure_schema(schema)
        
        self.storage = storage
        self.indexname = indexname
        self._schema = schema
        
        # Try reading the TOC to see if it's possible
        #self._revision()

    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__,
                               self.storage, self.indexname)

    def close(self):
        pass

    def _leaf_revisions(self):
        return _leaf_revs(self.storage, self.indexname)

    def _segments(self):
        segs = {}
        for rev in self._leaf_revisions():
            for seg in rev.segments:
                if seg.id in segs:
                    raise Exception
                segs[seg.id] = seg
        return list(segs.values())

    # add_field
    # remove_field
    
    def latest_generation(self):
        return tuple(rev.id for rev in self._leaf_revisions())
    
    # refresh
    # up_to_date
    
    def last_modified(self):
        return max(rev.created for rev in self._leaf_revisions())

    def is_empty(self):
        return sum(len(rev.segments) for rev in self._leaf_revisions()) == 0
    
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

    @property
    def schema(self):
        return (self._schema
                or merge_schemas([rev.schema for rev
                                  in self._leaf_revisions()]))

    @classmethod
    def _reader(self, storage, schema, segments, reuse=None):
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
                readers = [r for r, _ in reuse.leaf_readers()]
                reusable = dict((r.generation(), r) for r in readers)
            
            # Make a function to open readers, which reuses reusable readers.
            # It removes any readers it reuses from the "reusable" dictionary,
            # so later we can close any readers left in the dictionary.
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
                return MultiReader(readers)
        finally:
            for r in reusable.values():
                r.close()

    def reader(self, reuse=None):
        retries = 10
        while retries > 0:
            # Read the information from the TOC file
            try:
                segments = self._segments()
                return self._reader(self.storage, self.schema, segments,
                                    reuse=reuse)
            except IOError:
                # Presume that we got a "file not found error" because a writer
                # deleted one of the files just as we were trying to open it,
                # and so retry a few times before actually raising the
                # exception
                e = sys.exc_info()[1]
                retries -= 1
                if retries <= 0:
                    raise e
                sleep(0.05)


























