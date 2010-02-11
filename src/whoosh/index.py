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
import os.path

from whoosh import fields, store


_DEF_INDEX_NAME = "MAIN"


# Exceptions

class IndexError(Exception):
    """Generic index error."""

class IndexVersionError(IndexError):
    """Raised when you try to open an index using a format that the current
    version of Whoosh cannot read. That is, when the index you're trying to
    open is either not backward or forward compatible with this version of
    Whoosh.
    """
    
    def __init__(self, msg, version, release=None):
        Exception.__init__(self, msg)
        self.version = version
        self.release = release

class OutOfDateError(IndexError):
    """Raised when you try to commit changes to an index which is not the
    latest generation.
    """

class EmptyIndexError(IndexError):
    """Raised when you try to work with an index that has no indexed terms.
    """


# Convenience functions

def create_in(dirname, schema, indexname=None):
    """Convenience function to create an index in a directory. Takes care of
    creating a FileStorage object for you.
    
    :param dirname: the path string of the directory in which to create the index.
    :param schema: a :class:`whoosh.fields.Schema` object describing the index's fields.
    :param indexname: the name of the index to create; you only need to specify this if
        you are creating multiple indexes within the same storage object.
    :returns: :class:`Index`
    """
    
    if not indexname:
        indexname = _DEF_INDEX_NAME
    
    from whoosh.filedb.filestore import FileStorage
    storage = FileStorage(dirname)
    return storage.create_index(schema, indexname)

def open_dir(dirname, indexname = None, mapped=True):
    """Convenience function for opening an index in a directory. Takes care of
    creating a FileStorage object for you. dirname is the filename of the
    directory in containing the index. indexname is the name of the index to
    create; you only need to specify this if you have multiple indexes within
    the same storage object.
    
    :param dirname: the path string of the directory in which to create the
        index.
    :param indexname: the name of the index to create; you only need to specify
        this if you have multiple indexes within the same storage object.
    :param mapped: whether to use memory mapping to speed up disk reading.
    :returns: :class:`Index`
    """
    
    if indexname is None:
        indexname = _DEF_INDEX_NAME
    
    from whoosh.filedb.filestore import FileStorage
    storage = FileStorage(dirname, mapped=mapped)
    return storage.open_index(indexname)

def exists_in(dirname, indexname = None):
    """Returns True if dirname contains a Whoosh index.
    
    :param dirname: the file path of a directory.
    :param indexname: the name of the index. If None, the default index name is
        used.
    :param rtype: bool
    """
    
    if os.path.exists(dirname):
        try:
            ix = open_dir(dirname, indexname=indexname)
            return ix.latest_generation() > -1
        except EmptyIndexError:
            pass

    return False

def exists(storage, indexname = None):
    """Returns True if the given Storage object contains a Whoosh index.
    
    :param storage: a store.Storage object.
    :param indexname: the name of the index. If None, the default index name is
        used.
    :param rtype: bool
    """
    
    if indexname is None:
        indexname = _DEF_INDEX_NAME
        
    try:
        ix = storage.open_index(indexname)
        return ix.latest_generation() > -1
    except EmptyIndexError:
        pass
    
    return False

def version_in(dirname, indexname = None):
    """Returns a tuple of (release_version, format_version), where
    release_version is the release version number of the Whoosh code that
    created the index -- e.g. (0, 1, 24) -- and format_version is the version
    number of the on-disk format used for the index -- e.g. -102.
    
    The second number (format version) may be useful for figuring out if you
    need to recreate an index because the format has changed. However, you can
    just try to open the index and see if you get an IndexVersionError
    exception.
    
    Note that the release and format version are available as attributes on the
    Index object in Index.release and Index.version.
    
    :param dirname: the file path of a directory containing an index.
    :param indexname: the name of the index. If None, the default index name is
        used.
    :returns: ((major_ver, minor_ver, build_ver), format_ver)
    """
    
    from whoosh.filedb.filestore import FileStorage
    storage = FileStorage(dirname)
    return version(storage, indexname=indexname)
    

def version(storage, indexname = None):
    """Returns a tuple of (release_version, format_version), where
    release_version is the release version number of the Whoosh code that
    created the index -- e.g. (0, 1, 24) -- and format_version is the version
    number of the on-disk format used for the index -- e.g. -102.
    
    The second number (format version) may be useful for figuring out if you
    need to recreate an index because the format has changed. However, you can
    just try to open the index and see if you get an IndexVersionError
    exception.
    
    Note that the release and format version are available as attributes on the
    Index object in Index.release and Index.version.
    
    :param storage: a store.Storage object.
    :param indexname: the name of the index. If None, the default index name is
        used.
    :returns: ((major_ver, minor_ver, build_ver), format_ver)
    """
    
    try:
        if indexname is None:
            indexname = _DEF_INDEX_NAME
        
        ix = storage.open_index(indexname)
        return (ix.release, ix.version)
    except IndexVersionError, e:
        return (None, e.version)


# 

class DeletionMixin(object):
    def delete_by_term(self, fieldname, text):
        """Deletes any documents containing "term" in the "fieldname" field.
        This is useful when you have an indexed field containing a unique ID
        (such as "pathname") for each document.
        
        :returns: the number of documents deleted.
        """
        
        from whoosh.query import Term
        q = Term(fieldname, text)
        return self.delete_by_query(q)
    
    def delete_by_query(self, q):
        """Deletes any documents matching a query object.
        
        :returns: the number of documents deleted.
        """
        
        count = 0
        for docnum in q.docs(self.searcher()):
            self.delete_document(docnum)
            count += 1
        return count

# Index class

class Index(DeletionMixin):
    """Represents an indexed collection of documents.
    """
    
    def __init__(self, storage, schema = None, indexname = _DEF_INDEX_NAME):
        """
        :param storage: The :class:`whoosh.store.Storage` object in which this
            index resides. See the store module for more details.
        :param schema: A :class:`whoosh.fields.Schema` object defining the
            fields of this index.
        :param indexname: An optional name to use for the index. Use this if
            you need to keep multiple indexes in the same storage object.
        """
        
        self.storage = storage
        self.indexname = indexname
        
        if schema is not None and not isinstance(schema, fields.Schema):
            raise ValueError("%r is not a Schema object" % schema)
        
        self.schema = schema
    
    def close(self):
        """Closes any open resources held by the Index object itself. This may
        not close all resources being used everywhere, for example by a
        Searcher object.
        """
        pass
    
    def delete_document(self, docnum, delete=True):
        """Deletes a document by number."""
        raise NotImplementedError
    
    def latest_generation(self):
        """Returns the generation number of the latest generation of this
        index, or -1 if the backend doesn't support versioning.
        """
        return -1
    
    def refresh(self):
        """Returns a new Index object representing the latest generation
        of this index (if this object is the latest generation, or the backend
        doesn't support versioning, returns self).
        
        :returns: :class:`Index`
        """
        return self
    
    def up_to_date(self):
        """Returns True if this object represents the latest generation of
        this index. Returns False if this object is not the latest generation
        (that is, someone else has updated the index since you opened this
        object).
        
        :param rtype: bool
        """
        return True
    
    def last_modified(self):
        """Returns the last modified time of the index, or -1 if the backend
        doesn't support last-modified times.
        """
        return -1
    
    def is_empty(self):
        """Returns True if this index is empty (that is, it has never had any
        documents successfully written to it.
        
        :param rtype: bool
        """
        raise NotImplementedError
    
    def optimize(self):
        """Optimizes this index, if necessary.
        """
        pass
    
    def commit(self):
        """Commits pending edits (such as deletions) to this index object.
        """
        pass
    
    def doc_count_all(self):
        """Returns the total number of documents, DELETED OR UNDELETED,
        in this index.
        """
        raise NotImplementedError
    
    def doc_count(self):
        """Returns the total number of UNDELETED documents in this index.
        """
        raise NotImplementedError
    
    def field_length(self, fieldid):
        """Returns the total number of terms in a given field. This is used by
        some scoring algorithms. Note that this necessarily includes terms in
        deleted documents.
        """
        raise NotImplementedError
    
    def searcher(self, **kwargs):
        """Returns a Searcher object for this index. Keyword arguments are
        passed to the Searcher object's constructor.
        
        :rtype: :class:`whoosh.searching.Searcher`
        """
        
        from whoosh.searching import Searcher
        return Searcher(self.reader(), **kwargs)
    
    def reader(self):
        """Returns an IndexReader object for this index.
        
        :rtype: :class:`whoosh.reading.IndexReader`
        """
        raise NotImplementedError
    
    def writer(self, **kwargs):
        """Returns an IndexWriter object for this index.
        
        :rtype: :class:`whoosh.writing.IndexWriter`
        """
        raise NotImplementedError
    

# Debugging functions

        
if __name__ == '__main__':
    pass
