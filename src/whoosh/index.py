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

"""Contains the main functions/classes for creating, maintaining, and using
an index.
"""

from __future__ import division, absolute_import

import re
import struct
import sys
import typing
from concurrent import futures
from datetime import datetime
from time import sleep
from typing import Dict, Sequence, Tuple
from typing.re import Pattern

from whoosh import __version__, fields, storage, writing
from whoosh.codec import codecs
from whoosh.metadata import MetaData
from whoosh.util.times import datetime_to_long, long_to_datetime

# Typing imports
if typing.TYPE_CHECKING:
    from whoosh import reading, searching


# Constants

DEFAULT_INDEX_NAME = "MAIN"
CURRENT_TOC_VERSION = -113


# Exceptions

class LockError(Exception):
    pass


class WhooshIndexError(Exception):
    pass


class IndexVersionError(WhooshIndexError):
    """
    Raised when you try to open an index using a format that the current
    version of Whoosh cannot read. That is, when the index you're trying to
    open is either not backward or forward compatible with this version of
    Whoosh.
    """

    def __init__(self, msg, version, release=None):
        Exception.__init__(self, msg)
        self.version = version
        self.release = release


class OutOfDateError(WhooshIndexError):
    """
    Raised when you try to commit changes to an index which is not the
    latest generation.
    """


class EmptyIndexError(WhooshIndexError):
    """
    Raised when you try to work with an index that has no indexed terms.
    """


# Configuration

def from_json(data: dict) -> 'Index':
    from whoosh import storage
    from whoosh.codec import codecs

    indexname = data.get("indexname")
    store = storage.from_json(data["storage"])
    codec = codecs.from_json(data["codec"])
    ix = store.open_index(indexname=indexname, codec=codec)

    return ix


# Filename functions

def make_toc_filename(indexname: str, generation: int, ext: str="toc") -> str:
    name = "_%s_%d.%s" % (indexname, generation, ext)
    assert toc_regex(indexname, ext).match(name)
    return name


def toc_regex(indexname: str, ext: str="toc") -> Pattern:
    return re.compile("^_%s_(?P<gen>[0-9]+)[.]%s$" % (indexname, ext))


def make_segment_filename(indexname: str, segid: str, ext: str,
                          subname: str=None,) -> str:
    if subname:
        name = "%s_%s-%s.%s" % (indexname, segid, subname, ext)
    else:
        name = "%s_%s.%s" % (indexname, segid, ext)
    assert segment_regex(indexname).match(name)
    return name


def segment_regex(indexname: str) -> Pattern:
    return re.compile("^%s_(?P<id>\\d+)(-[^.]*)?[.](?P<ext>[A-Za-z0-9_.]+)$" %
                      (indexname,))


# TOC

# Length of codec json, length of segment bytes
segment_entry = struct.Struct("<Hi")


class TocHeader(MetaData):
    magic_bytes = b"Indx"
    field_order = ("toc_version release_major release_minor release_build "
                   "generation created schema_len segment_count")

    toc_version = "i"  # TOC format revision number
    release_major = "B"  # Major version number
    release_minor = "B"  # Minor version number
    release_build = "H"  # Build version number
    generation = "I"  # current generation number
    created = "q"  # long representation of creation datetime
    schema_len = "i"  # length of the encoded schema in bytes
    segment_count = "i"  # number of segments


class Toc:
    """
    Holds information about a particular revision of the index, including the
    schema and a list of segments.
    """

    def __init__(self, schema: 'fields.Schema',
                 segments: 'Sequence[codecs.Segment]',
                 generation: int, toc_version: int=CURRENT_TOC_VERSION,
                 release: Tuple[int, int, int]=__version__,
                 created: datetime=None):
        self.schema = schema
        self.segments = segments
        self.generation = generation
        self.toc_version = toc_version
        self.release = release
        self.created = created or datetime.utcnow()
        self.filename = None

    def to_bytes(self) -> bytes:
        import json
        output = bytearray()

        schema_bytes = self.schema.to_bytes()
        created_int = datetime_to_long(self.created)
        assert self.generation >= 0

        # Generate the header
        output += TocHeader(
            toc_version=self.toc_version,
            release_major=self.release[0],
            release_minor=self.release[1],
            release_build=self.release[2],
            generation=self.generation, created=created_int,
            schema_len=len(schema_bytes),
            segment_count=len(self.segments)
        ).encode()

        # Add the schema
        output += schema_bytes

        # Add the segments
        for segment in self.segments:
            codec_bytes = json.dumps(segment.codec_json()).encode("utf8")
            segment_bytes = segment.to_bytes()
            output += segment_entry.pack(len(codec_bytes), len(segment_bytes))
            output += codec_bytes
            output += segment_bytes

        return output

    @classmethod
    def from_bytes(cls, bs: bytes, offset: int=0) -> 'Toc':
        import json

        head = TocHeader.decode(bs, offset)
        release = head.release_major, head.release_minor, head.release_build
        created = long_to_datetime(head.created)

        # Read the schema
        schema_start = offset + head.get_size()
        schema_end = schema_start + head.schema_len
        schema = fields.Schema.from_bytes(bs[schema_start:schema_end])

        # Read the segments
        segments = []
        pos = schema_end
        for _ in range(head.segment_count):
            namestart = pos + segment_entry.size
            namelen, seglen = segment_entry.unpack(bs[pos:namestart])
            jsonstr = bytes(bs[namestart:namestart + namelen]).decode("utf8")
            data = json.loads(jsonstr)

            c = codecs.from_json(data)
            segstart = namestart + namelen
            segment = c.segment_from_bytes(bs[segstart:segstart + seglen])
            segments.append(segment)

            pos = segstart + seglen

        return cls(
            schema=schema, segments=segments, generation=head.generation,
            toc_version=head.toc_version, release=release, created=created
        )


# Index class

class Index:
    """
    Represents an indexed collection of documents.
    """

    def __init__(self, store: 'storage.Storage', indexname: str,
                 schema: 'fields.Schema'=None):
        self.store = store
        self.indexname = indexname

        if schema and not isinstance(schema, fields.Schema):
            raise TypeError("%r is not a schema")
        self._schema = schema

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not exc_type:
            self.close()

    def json_info(self) -> dict:
        toc = self.toc
        return {
            "indexname": self.indexname,
            # "schema": toc.schema.json_info(),
            "segments": [seg.json_info() for seg in toc.segments],
            "generation": toc.generation,
            "toc_version": toc.toc_version,
            "release": toc.release,
            "created": toc.created,
            "filename": toc.filename,
            "doc_count": self.doc_count(),
            "doc_count_all": self.doc_count_all(),
            "up_to_date": self.up_to_date(),
            "is_empty": self.is_empty(),
        }

    @property
    def toc(self):
        with self.store.open(self.indexname) as session:
            return self.store.load_toc(session)

    @property
    def schema(self):
        return self._schema if self._schema else self.toc.schema

    def storage(self) -> 'storage.Storage':
        return self.store

    def segments(self) -> 'Sequence[codecs.Segment]':
        return self.toc.segments

    def release_version(self) -> Tuple[int, int, int]:
        """
        Returns the version of Whoosh that created this index as tuple of
        ``(major_ver, minor_ver, build_ver)``.
        """

        return self.toc.release

    def toc_version(self) -> int:
        """
        Returns the version number of the index format.
        """

        return self.toc.toc_version

    def close(self):
        pass

    def add_field(self, fieldname: str, fieldspec: 'fields.FieldType'):
        """
        Adds a field to the index's schema.

        :param fieldname: the name of the field to add.
        :param fieldspec: an instantiated :class:`whoosh.fields.FieldType`
            object.
        """

        with self.writer() as w:
            w.add_field(fieldname, fieldspec)

    def remove_field(self, fieldname: str):
        """
        Removes the named field from the index's schema. Depending on the
        backend implementation, this may or may not actually remove existing
        data for the field from the index. Optimizing the index should always
        clear out existing data for a removed field.
        """

        with self.writer() as w:
            w.remove_field(fieldname)

    def latest_generation(self) -> int:
        """
        Returns the generation number of the latest generation of this
        index, or -1 if the backend doesn't support versioning.
        """

        with self.store.open(indexname=self.indexname) as session:
            return self.store.latest_generation(session)

    def last_modified(self):
        return self.store.last_modified()

    def up_to_date(self) -> bool:
        """
        Returns True if this object represents the latest generation of
        this index. Returns False if this object is not the latest generation
        (that is, someone else has updated the index since you opened this
        object).
        """

        return self.toc.generation == self.latest_generation()

    def creation_time(self) -> datetime:
        """
        Returns the creation time of the index.
        """

        return self.toc.created

    def is_empty(self) -> bool:
        """
        Returns True if this index is "fresh" (that is, it has never had any
        documents successfully written to it.
        """

        return self.doc_count() == 0

    def doc_count(self) -> int:
        segments = self.toc.segments
        if not segments:
            return 0
        return sum(seg.doc_count() for seg in segments)

    def doc_count_all(self) -> int:
        segments = self.toc.segments
        if not segments:
            return 0
        return sum(seg.doc_count_all() for seg in segments)

    def optimize(self):
        """
        Optimizes this index, if necessary.
        """

        with self.writer() as w:
            w.optimize = True

    def searcher(self, **kwargs) -> 'searching.Searcher':
        """
        Returns a Searcher object for this index. Keyword arguments are
        passed to the Searcher object's constructor.

        :rtype: :class:`whoosh.searching.Searcher`
        """

        from whoosh.searching import SearcherType
        reader = self.reader()
        return SearcherType.from_reader(reader, fromindex=self, **kwargs)

    def _reader(self, schema: 'fields.Schema',
                segments: 'Sequence[codecs.Segment]',
                generation: int, reuse: 'reading.IndexReader'):
        # Returns a reader for the given segments, possibly reusing already
        # opened readers
        from whoosh.reading import EmptyReader, SegmentReader, MultiReader

        if not segments:
            if reuse:
                reuse.close()
            return EmptyReader(schema)

        reusable = {}  # type: Dict[str, reading.IndexReader]
        try:
            # Put all atomic readers in a dictionary keyed by their segment ID,
            # so we can re-use them if possible
            if reuse:
                for r, _ in reuse.leaf_readers():
                    segid = r.segment_id()
                    if not segid:
                        raise Exception("Reader %r has no segment ID" % r)
                    reusable[segid] = r

            # Make a function to get a reader for a segment, which reuses
            # readers from the old reader when available.
            # It removes any readers it reuses from the "reusable" dictionary,
            # so later we can close any readers left in the dictionary.
            def segreader(segment):
                segid = segment.segment_id()
                if segid in reusable:
                    return reusable.pop(segid)
                else:
                    return SegmentReader(self.store, schema, segment,
                                         generation=generation)

            if len(segments) == 1:
                reader = segreader(segments[0])
            else:
                rs = [segreader(segment) for segment in segments]
                reader = MultiReader(rs, generation=generation)
            return reader
        finally:
            for r in reusable.values():
                r.close()

    def reader_for(self, segment: 'codecs.Segment',
                   schema: 'fields.Schema'=None) -> 'reading.IndexReader':
        from whoosh.reading import SegmentReader

        schema = schema or self.schema
        return SegmentReader(self.store, schema, segment)

    def reader(self, reuse: 'reading.IndexReader'=None
               ) -> 'reading.IndexReader':
        """
        Returns an IndexReader object for this index.

        :param reuse: an existing reader. Some implementations may recycle
            resources from this existing reader to create the new reader. Note
            that any resources in the "recycled" reader that are not used by
            the new reader will be CLOSED, so you CANNOT use it afterward.
        """

        retries = 10
        while retries > 0:
            try:
                toc = self.toc
                return self._reader(toc.schema, toc.segments, toc.generation,
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

    def writer(self,
               executor: futures.Executor=None,
               multiproc: bool=False, multithreaded: bool=False,
               procs: int=None, threads: int=None,
               codec: 'codecs.Codec'=None,
               schema: 'fields.Schema'=None,
               **kwargs
               ) -> 'writing.IndexWriter':
        """
        Returns an writer object for this index.

        :param executor: a ``conccurent.futures.Executor`` object for the writer
            to use for concurrent operation. If you pass this argument, it
            overrides any default executor implied by the other arguments.
        :param multiproc: use a multi-processing executor to index in background
            processes.
        :param multithreaded: use a multi-threaded executor to index in
            background threads.
        :param procs: when multiproc is True, configure the executor to use a
            pool of this many processes. The default (None) uses the process
            pool executor's default (usually the number of CPUs).
        :param threads: when multithreaded is True, configure the executor to
            use a pool of this many threads. The default (None) uses the
            thread pool executor's default (the number of CPUs times 5).
        :param schema: if you pass a Schema object, the writer will use that
            schema when writing the new segment(s).
        :param codec: use this codec to write into storage. If you don't pass
            a codec the writer simply uses the default.
        :param kwargs: keyword arguments are passed to the writer's constructor.
            See :class:`whoosh.writing.IndexWriter` for the options available.
        """

        return writing.IndexWriter(self, executor=executor, multiproc=multiproc,
                                   multithreaded=multithreaded, procs=procs,
                                   threads=threads, codec=codec, schema=schema,
                                   **kwargs)


class MultiIndex(Index):
    """
    This presents the contents of two or more separate indexes as a single
    index, with all writes going to the _last_ index in the list. The main use
    for this is to overlay a small, dynamic index over a large, static index.

    Note that currently, this implementation only allows writing to an "overlay"
    index, so you can't delete documents from the static index(es). A future
    version may provide a way to "tombstone" documents so they don't appear in
    search results.
    """

    def __init__(self, indexes: Sequence[Index]):
        self._indexes = indexes

    def indexes(self) -> Tuple[Index]:
        return tuple(self._indexes)

    def reader(self, reuse: 'reading.IndexReader'=None
               ) -> 'reading.IndexReader':
        from whoosh.reading import leaf_readers, EmptyReader, MultiReader

        readers = leaf_readers([ix.reader() for ix in self._indexes
                                if not ix.is_empty()])
        if not readers:
            return EmptyReader()
        elif len(readers) == 1:
            return readers[0]
        else:
            return MultiReader(readers)

    def last_index(self) -> 'Index':
        return self._indexes[-1]

    def writer(self, *args, **kwargs):
        self.last_index().writer(*args, **kwargs)

    @property
    def schema(self):
        return self.last_index().schema

    def close(self):
        for ix in self._indexes:
            ix.close()

    def add_field(self, fieldname: str, fieldspec: 'fields.FieldType'):
        self.last_index().add_field(fieldname, fieldspec)

    def remove_field(self, fieldname: str):
        self.last_index().remove_field(fieldname)

    def latest_generation(self) -> int:
        return max(ix.latest_generation() for ix in self.indexes())

    def last_modified(self):
        return max(ix.last_modified() for ix in self.indexes())

    def up_to_date(self) -> bool:
        return all(ix.up_to_date() for ix in self.indexes())

    def creation_time(self) -> datetime:
        return min(ix.creation_time() for ix in self.indexes())

    def is_empty(self):
        return all(ix.is_empty() for ix in self.indexes())

    def doc_count(self) -> int:
        return sum(ix.doc_count() for ix in self.indexes())

    def doc_count_all(self) -> int:
        return sum(ix.doc_count_all() for ix in self.indexes())


# Convenience functions

def create_in(dirname: str, schema: 'fields.Schema',
              indexname: str=None) -> Index:
    """
    Convenience function to create an index in a directory. Takes care of
    creating a FileStorage object for you.

    :param dirname: the path string of the directory in which to create the
        index.
    :param schema: a :class:`whoosh.fields.Schema` object describing the
        index's fields.
    :param indexname: the name of the index to create; you only need to specify
        this if you are creating multiple indexes within the same storage
        object.
    :returns: :class:`Index`
    """

    from whoosh.filedb import filestore

    store = filestore.FileStorage(dirname)
    indexname = indexname or DEFAULT_INDEX_NAME
    return store.create_index(schema, indexname)


def open_dir(dirname: str, indexname: str=None, readonly: bool=False,
             use_mmap: bool=True, schema: 'fields.Schema'=None):
    """
    Convenience function for opening an index in a directory. Takes care of
    creating a FileStorage object for you. dirname is the filename of the
    directory in containing the index. indexname is the name of the index to
    create; you only need to specify this if you have multiple indexes within
    the same storage object.

    :param dirname: the path string of the directory in which to create the
        index.
    :param indexname: the name of the index to create; you only need to specify
        this if you have multiple indexes within the same storage object.
    :param readonly: open the directory as read-only (not currently used).
    :param schema: use this schema instead of the one saved with the index.
    """

    from whoosh.filedb import filestore

    store = filestore.FileStorage(dirname, readonly=readonly,
                                  supports_mmap=use_mmap)
    indexname = indexname or DEFAULT_INDEX_NAME
    return store.open_index(indexname, schema=schema)


def exists_in(dirname: str, indexname: str=None):
    """
    Returns True if dirname contains a Whoosh index.

    :param dirname: the file path of a directory.
    :param indexname: the name of the index. If None, the default index name is
        used.
    """

    from whoosh.filedb import filestore

    store = filestore.FileStorage(dirname)
    indexname = indexname or DEFAULT_INDEX_NAME
    return store.index_exists(indexname)


def version_in(dirname: str, indexname: str=None):
    """
    Returns a tuple of (release_version, format_version), where
    release_version is the release version number of the Whoosh code that
    created the index -- e.g. (0, 1, 24) -- and format_version is the version
    number of the on-disk format used for the index -- e.g. -102.

    You should avoid attaching significance to the second number (the index
    version). This is simply a version number for the TOC file and probably
    should not have been exposed in a public interface. The best way to check
    if the current version of Whoosh can open an index is to actually try to
    open it and see if it raises a ``whoosh.index.IndexVersionError`` exception.

    Note that the release and format version are available as attributes on the
    Index object in Index.release and Index.version.

    :param dirname: the file path of a directory containing an index.
    :param indexname: the name of the index. If None, the default index name is
        used.
    :returns: ((major_ver, minor_ver, build_ver), format_ver)
    """

    from whoosh.filedb import filestore

    store = filestore.FileStorage(dirname)
    indexname = indexname or DEFAULT_INDEX_NAME
    return version(store, indexname=indexname)


def version(store: 'storage.Storage', indexname: str=None
            ) -> Tuple[Tuple[int, int, int], int]:
    """
    Returns a tuple of (release_version, format_version), where
    release_version is the release version number of the Whoosh code that
    created the index -- e.g. (0, 1, 24) -- and format_version is the version
    number of the on-disk format used for the index -- e.g. -102.

    You should avoid attaching significance to the second number (the index
    version). This is simply a version number for the TOC file and probably
    should not have been exposed in a public interface. The best way to check
    if the current version of Whoosh can open an index is to actually try to
    open it and see if it raises a ``whoosh.index.IndexVersionError`` exception.

    Note that the release and format version are available as attributes on the
    Index object in Index.release and Index.version.

    :param store: a Storage object.
    :param indexname: the name of the index. If None, the default index name is
        used.
    """

    indexname = indexname or DEFAULT_INDEX_NAME
    with store.open(indexname) as session:
        toc = store.load_toc(session)
        return toc.release, toc.toc_version






