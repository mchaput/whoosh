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
from time import time, sleep

from whoosh import __version__
from whoosh.compat import pickle, string_type, xrange
from whoosh.fields import ensure_schema
from whoosh.index import (Index, EmptyIndexError, IndexVersionError,
                          _DEF_INDEX_NAME)
from whoosh.reading import EmptyReader, MultiReader
from whoosh.store import Storage
from whoosh.system import _INT_SIZE, _FLOAT_SIZE, _LONG_SIZE


_INDEX_VERSION = -110


class TOC(object):
    """Object representing the state of the complete index after a commit.
    """

    @classmethod
    def _filename(cls, indexname, gen):
        return "_%s_%s.toc" % (indexname, gen)

    @classmethod
    def _pattern(cls, indexname):
        return re.compile("^_%s_([0-9]+).toc$" % indexname)

    @classmethod
    def _segment_pattern(cls, indexname):
        return re.compile("(%s_[0-9a-z]+)[.][a-z]+" % indexname)

    @classmethod
    def _latest_generation(cls, storage, indexname):
        pattern = cls._pattern(indexname)

        mx = -1
        for filename in storage:
            m = pattern.match(filename)
            if m:
                mx = max(int(m.group(1)), mx)
        return mx

    @classmethod
    def create(cls, storage, schema, indexname=_DEF_INDEX_NAME):
        schema = ensure_schema(schema)

        # Clear existing files
        prefix = "_%s_" % indexname
        for filename in storage:
            if filename.startswith(prefix):
                storage.delete_file(filename)

        # Write a TOC file with an empty list of segments
        toc = cls(schema, [], 0)
        toc.write(storage, indexname)

    @classmethod
    def read(cls, storage, indexname, gen=None, schema=None):
        if gen is None:
            gen = cls._latest_generation(storage, indexname)
            if gen < 0:
                raise EmptyIndexError("Index %r does not exist in %r"
                                      % (indexname, storage))

        # Read the content of this index from the .toc file.
        tocfilename = cls._filename(indexname, gen)
        stream = storage.open_file(tocfilename)

        def check_size(name, target):
            sz = stream.read_varint()
            if sz != target:
                raise IndexError("Index was created on different architecture:"
                                 " saved %s = %s, this computer = %s"
                                 % (name, sz, target))

        check_size("int", _INT_SIZE)
        check_size("long", _LONG_SIZE)
        check_size("float", _FLOAT_SIZE)

        if not stream.read_int() == -12345:
            raise IndexError("Number misread: byte order problem")

        version = stream.read_int()
        if version != _INDEX_VERSION:
            raise IndexVersionError("Can't read format %s" % version, version)
        release = (stream.read_varint(), stream.read_varint(),
                   stream.read_varint())

        # If the user supplied a schema object with the constructor, don't load
        # the pickled schema from the saved index.
        if schema:
            stream.skip_string()
        else:
            schema = pickle.loads(stream.read_string())
        schema = ensure_schema(schema)

        # Generation
        index_gen = stream.read_int()
        assert gen == index_gen

        _ = stream.read_int()  # Unused
        segments = stream.read_pickle()

        stream.close()
        return cls(schema, segments, gen, version=version, release=release)

    def __init__(self, schema, segments, generation,
                 version=_INDEX_VERSION, release=__version__):
        self.schema = schema
        self.segments = segments
        self.generation = generation
        self.version = version
        self.release = release

    def write(self, storage, indexname):
        schema = ensure_schema(self.schema)
        schema.clean()

        # Use a temporary file for atomic write.
        tocfilename = self._filename(indexname, self.generation)
        tempfilename = '%s.%s' % (tocfilename, time())
        stream = storage.create_file(tempfilename)

        stream.write_varint(_INT_SIZE)
        stream.write_varint(_LONG_SIZE)
        stream.write_varint(_FLOAT_SIZE)
        stream.write_int(-12345)

        stream.write_int(_INDEX_VERSION)
        for num in __version__[:3]:
            stream.write_varint(num)

        stream.write_string(pickle.dumps(schema, -1))
        stream.write_int(self.generation)
        stream.write_int(0)  # Unused
        stream.write_pickle(self.segments)
        stream.close()

        # Rename temporary file to the proper filename
        storage.rename_file(tempfilename, tocfilename, safe=True)


def clean_files(storage, indexname, gen, segments):
    # Attempts to remove unused index files (called when a new generation
    # is created). If existing Index and/or reader objects have the files
    # open, they may not be deleted immediately (i.e. on Windows) but will
    # probably be deleted eventually by a later call to clean_files.

    current_segment_names = set(s.segment_id() for s in segments)
    tocpattern = TOC._pattern(indexname)
    segpattern = TOC._segment_pattern(indexname)

    todelete = set()
    for filename in storage:
        if filename.startswith("."):
            continue
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
            # Another process still has this file open, I guess
            pass


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
        self._schema = schema
        self.indexname = indexname

        # Try reading the TOC to see if it's possible
        TOC.read(self.storage, self.indexname, schema=self._schema)

    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__,
                               self.storage, self.indexname)

    def close(self):
        pass

    # add_field
    # remove_field

    def latest_generation(self):
        return TOC._latest_generation(self.storage, self.indexname)

    # refresh
    # up_to_date

    def last_modified(self):
        gen = self.latest_generation()
        filename = TOC._filename(self.indexname, gen)
        return self.storage.file_modified(filename)

    def is_empty(self):
        return len(self._read_toc().segments) == 0

    def optimize(self, **kwargs):
        w = self.writer(**kwargs)
        w.commit(optimize=True)

    # searcher

    def writer(self, procs=1, **kwargs):
        if procs > 1:
            from whoosh.filedb.multiproc import MpWriter
            return MpWriter(self, procs=procs, **kwargs)
        else:
            from whoosh.filedb.filewriting import SegmentWriter
            return SegmentWriter(self, **kwargs)

    def lock(self, name):
        """Returns a lock object that you can try to call acquire() on to
        lock the index.
        """

        return self.storage.lock(self.indexname + "_" + name)

    def _read_toc(self):
        return TOC.read(self.storage, self.indexname, schema=self._schema)

    def _segments(self):
        return self._read_toc().segments

    def _current_schema(self):
        return self._read_toc().schema

    @property
    def schema(self):
        return self._current_schema()

    @classmethod
    def _reader(self, storage, schema, segments, generation, reuse=None):
        # Returns a reader for the given segments, possibly reusing already
        # opened readers
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
                segid = segment.segment_id()
                if segid in reusable:
                    r = reusable[segid]
                    del reusable[segid]
                    return r
                else:
                    return SegmentReader(storage, schema, segment,
                                         generation=generation)

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
        retries = 10
        while retries > 0:
            # Read the information from the TOC file
            try:
                info = self._read_toc()
                return self._reader(self.storage, info.schema, info.segments,
                                    info.generation, reuse=reuse)
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


# Fix for old indexes pickled with whoosh.fileindex.Segment objects
from whoosh.codec.whoosh2 import W2Segment as Segment  # @UnusedImport

