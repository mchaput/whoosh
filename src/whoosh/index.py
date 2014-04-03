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

"""
Contains the main functions/classes for creating, maintaining, and using
an index.
"""

from __future__ import division, with_statement
import os.path, re, sys
from datetime import datetime
from time import time, sleep

from whoosh import __version__
from whoosh.compat import pickle, string_type
from whoosh.codec import default_codec
from whoosh.fields import ensure_schema
from whoosh.kv import default_db_class
from whoosh.legacy import toc_loaders
from whoosh.system import _INT_SIZE, _FLOAT_SIZE, _LONG_SIZE
from whoosh.util import now


_DEF_INDEX_NAME = "MAIN"
_CURRENT_TOC_VERSION = -111


# Exceptions

class LockError(Exception):
    pass


class IndexVersionError(IndexError):
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


class OutOfDateError(IndexError):
    """
    Raised when you try to commit changes to an index which is not the
    latest generation.
    """


class EmptyIndexError(IndexError):
    """
    Raised when you try to work with an index that has no indexed terms.
    """


# Convenience functions


def open_dir(dirname, indexname=None, codec=None, dbclass=None):
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
    """

    indexname = indexname or _DEF_INDEX_NAME
    codec = codec or default_codec()
    dbclass = dbclass or default_db_class

    dirpath = os.path.join(dirname, indexname)
    db = dbclass(dirpath)
    return Index(db, codec)


def create_in(dirname, schema, indexname=None, codec=None, dbclass=None,
              clear=False):
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

    indexname = indexname or _DEF_INDEX_NAME
    codec = codec or default_codec()
    dbclass = dbclass or default_db_class

    dirpath = os.path.join(dirname, indexname)
    db = dbclass(dirpath)
    return Index.create(db, codec, schema, clear=clear)


# def exists_in(dirname, indexname=None, codec=None, dbclass=None):
#     """
#     Convenience function returns True if dirname contains a Whoosh index.
#
#     :param dirname: the file path of a directory.
#     :param indexname: the name of the index. If None, the default index name is
#         used.
#     """
#
#     ix = open_dir(dirname, indexname, codec=codec, dbclass=dbclass)
#     return ix.exists()


# Index base class

class Index(object):
    """
    Represents an indexed collection of documents.
    """

    def __init__(self, db, codec):
        self.db = db
        self.codec = codec
        # Try reading the index metadata to see if it's possible
        self._ixinfo = self.info()

    @classmethod
    def create(cls, db, codec, schema, clear=False):
        if clear:
            db.destroy()
        db.create()
        with db.open(write=True) as txn:
            codec.write_info(txn, IndexInfo(schema))
        ix = cls(db, codec)
        return ix

    def reader(self, reuse=None, schema=None):
        """
        Returns an IndexReader object for this index.

        :param reuse: an existing reader. Some implementations may recycle
            resources from this existing reader to create the new reader. Note
            that any resources in the "recycled" reader that are not used by
            the new reader will be CLOSED, so you CANNOT use it afterward.
        :rtype: :class:`whoosh.reading.IndexReader`
        """

        from whoosh.reading import DBReader

        txn = self.db.open()
        schema = schema or self.schema
        return DBReader(txn, self.codec, schema)

    def searcher(self, **kwargs):
        """
        Returns a Searcher object for this index. Keyword arguments are
        passed to the Searcher object's constructor.

        :rtype: :class:`whoosh.searching.Searcher`
        """

        from whoosh.searching import Searcher
        return Searcher(self.reader(), fromindex=self, **kwargs)

    def writer(self, procs=1, schema=None, **kwargs):
        """
        Returns an IndexWriter object for this index.

        :rtype: :class:`whoosh.writing.IndexWriter`
        """

        if procs > 1:
            raise NotImplementedError
        else:
            from whoosh.writing import DBWriter

            txn = self.db.open(write=True)
            schema = schema or self.schema
            return DBWriter(txn, self.codec, schema, self._ixinfo, **kwargs)

    def add_field(self, fieldname, fieldspec):
        """
        Adds a field to the index's schema.

        :param fieldname: the name of the field to add.
        :param fieldspec: an instantiated :class:`whoosh.fields.FieldType`
            object.
        """

        w = self.writer()
        w.add_field(fieldname, fieldspec)
        w.commit()

    def remove_field(self, fieldname):
        """
        Removes the named field from the index's schema. Depending on the
        backend implementation, this may or may not actually remove existing
        data for the field from the index. Optimizing the index should always
        clear out existing data for a removed field.
        """

        w = self.writer()
        w.remove_field(fieldname)
        w.commit()

    def info(self):
        with self.db.open() as txn:
            return self.codec.info(txn)

    def latest_generation(self):
        """
        Returns the generation number of the latest generation of this
        index, or -1 if the backend doesn't support versioning.
        """

        return self.info().generation

    def last_modified(self):
        """
        Returns the last modified time of the index, or -1 if the backend
        doesn't support last-modified times.
        """

        return self.info().last_modified

    def version(self):
        """
        Returns the version number of the Whoosh that created the index.
        """

        return self.info().release

    @property
    def schema(self):
        return self.info().schema

    def refresh(self):
        """
        Returns a new Index object representing the latest generation
        of this index (if this object is the latest generation, or the backend
        doesn't support versioning, returns self).

        :returns: :class:`Index`
        """

        return self

    def up_to_date(self):
        """
        Returns True if this object represents the latest generation of
        this index. Returns False if this object is not the latest generation
        (that is, someone else has updated the index since you opened this
        object).
        """

        return True

    def is_empty(self):
        """
        Returns True if this index is empty (that is, it has never had any
        documents successfully written to it.
        """

        with self.reader() as r:
            for _ in r.all_doc_ids():
                return False
            else:
                return True

    def optimize(self):
        """
        Optimizes this index, if necessary.
        """

        with self.writer() as w:
            w.optimize()

    def doc_count(self):
        """
        Returns the total number of **undeleted** documents in this index.
        """

        with self.reader() as r:
            return r.doc_count()

    def close(self):
        """
        Closes any open resources held by the Index object itself. This may
        not close all resources being used everywhere, for example by a
        Searcher object.
        """

        pass


class IndexInfo(object):
    def __init__(self, schema, generation=-1, last_modified=None, release=None,
                 layout=None):
        self.schema = schema
        self.generation = generation

        if last_modified:
            self.last_modified = last_modified
        else:
            self.touch()

        if release is None:
            from whoosh import __version__
            release = __version__
        self.release = release

    def touch(self):
        self.last_modified = datetime.utcnow()
        self.generation += 1





