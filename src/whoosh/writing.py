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

import threading, time

from whoosh.index import DeletionMixin
from whoosh.store import LockError

# Exceptions

class IndexingError(Exception):
    pass


# Base class

class IndexWriter(DeletionMixin):
    """High-level object for writing to an index.
    
    To get a writer for a particular index, call
    :meth:`~whoosh.index.Index.writer` on the Index object.
    
    >>> writer = my_index.writer()
    
    You can use this object as a context manager. If an exception is thrown
    from within the context it calls cancel(), otherwise it calls commit() when
    the context exits.
    """
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.cancel()
        else:
            self.commit()
    
    def searcher(self, **kwargs):
        """Returns a searcher for the existing index.
        """
        
        if not self._searcher:
            self._searcher = self.index.searcher(**kwargs)
        return self._searcher
    
    def _close_reader(self):
        if self._searcher:
            self._searcher.close()
            self._searcher = None
    
    def delete_document(self, docnum, delete=True):
        """Deletes a document by number.
        """
        raise NotImplementedError
    
    def add_document(self, **fields):
        """Adds all the fields of a document at once. This is an alternative to
        calling start_document(), add_field() [...], end_document().
        
        The keyword arguments map field names to the values to index/store.
        
        For fields that are both indexed and stored, you can specify an
        alternate value to store using a keyword argument in the form
        "_stored_<fieldname>". For example, if you have a field named "title"
        and you want to index the text "a b c" but store the text "e f g", use
        keyword arguments like this::
        
            writer.add_document(title=u"a b c", _stored_title=u"e f g")
        """
        raise NotImplementedError
    
    def update_document(self, **fields):
        """Adds or replaces a document. At least one of the fields for which
        you supply values must be marked as 'unique' in the index's schema.
        
        The keyword arguments map field names to the values to index/store.
        
        Note that this method will only replace a *committed* document;
        currently it cannot replace documents you've added to the IndexWriter
        but haven't yet committed. For example, if you do this:
        
        >>> writer.update_document(unique_id=u"1", content=u"Replace me")
        >>> writer.update_document(unique_id=u"1", content=u"Replacement")
        
        ...this will add two documents with the same value of ``unique_id``,
        instead of the second document replacing the first.
        
        For fields that are both indexed and stored, you can specify an
        alternate value to store using a keyword argument in the form
        "_stored_<fieldname>". For example, if you have a field named "title"
        and you want to index the text "a b c" but store the text "e f g", use
        keyword arguments like this::
        
            writer.update_document(title=u"a b c", _stored_title=u"e f g")
        """
        
        # Check which of the supplied fields are unique
        unique_fields = [name for name, field
                         in self.index.schema.fields()
                         if name in fields and field.unique]
        if not unique_fields:
            raise IndexingError("None of the fields in %r are unique" % fields.keys())
        
        # Delete documents in which the supplied unique fields match
        from whoosh import query
        delquery = query.Or([query.Term(name, fields[name])
                             for name in unique_fields])
        delquery = delquery.normalize()
        self.delete_by_query(delquery)
        
        # Add the given fields
        self.add_document(**fields)
    
    def commit(self):
        """Finishes writing and unlocks the index.
        """
        pass
        
    def cancel(self):
        """Cancels any documents/deletions added by this object
        and unlocks the index.
        """
        pass
    

class AsyncWriter(threading.Thread, DeletionMixin):
    """Convenience wrapper for a writer object that might fail due to locking
    (i.e. the ``filedb`` writer). This object will attempt once to obtain the
    underlying writer, and if it's successful, will simply pass method calls on
    to it.
    
    If this object *can't* obtain a writer immediately, it will *buffer*
    delete, add, and update method calls in memory until you call ``commit()``.
    At that point, this object will start running in a separate thread, trying
    to obtain the writer over and over, and once it obtains it, "replay" all
    the buffered method calls on it.
    
    In a typical scenario where you're adding a single or a few documents to
    the index as the result of a Web transaction, this lets you just create the
    writer, add, and commit, without having to worry about index locks,
    retries, etc.
    
    The first argument is a callable which returns the actual writer. Usually
    this will be the ``writer`` method of your Index object. Any additional
    keyword arguments to the initializer are passed into the callable.
    
    For example, to get an aynchronous writer, instead of this:
    
    >>> writer = myindex.writer(postlimit=128 * 1024 * 1024)
    
    Do this:
    
    >>> from whoosh.writing import AsyncWriter
    >>> writer = AsyncWriter(myindex.writer, postlimit=128 * 1024 * 1024)
    """
    
    def __init__(self, writerfn, delay=0.25, **writerargs):
        """
        :param writerfn: a callable object (function or method) which returns
            the actual writer.
        :param delay: the delay (in seconds) between attempts to instantiate
            the actual writer.
        """
        
        threading.Thread.__init__(self)
        self.running = False
        self.writerfn = writerfn
        self.writerargs = writerargs
        self.delay = delay
        self.events = []
        try:
            self.writer = writerfn(**writerargs)
        except LockError:
            self.writer = None
    
    def _record(self, method, *args, **kwargs):
        if self.writer:
            getattr(self.writer, method)(*args, **kwargs)
        else:
            self.events.add(method, args, kwargs)
    
    def run(self):
        self.running = True
        writer = self.writer
        while writer is None:
            try:
                writer = self.writerfn(**self.writerargs)
            except LockError:
                time.sleep(self.delay)
        for method, args, kwargs in self.events:
            getattr(writer, method)(*args, **kwargs)
        writer.commit(*self.commitargs, **self.commitkwargs)
    
    def delete_document(self, docnum):
        self._record("delete_document", docnum)
    
    def add_document(self, *args, **kwargs):
        self._record("add_document", *args, **kwargs)
        
    def update_document(self, *args, **kwargs):
        self._record("update_document", *args, **kwargs)
    
    def commit(self, *args, **kwargs):
        if self.writer:
            self.writer.commit(*args, **kwargs)
        else:
            self.commitargs, self.commitkwargs = args, kwargs
            self.start()
    
    def cancel(self, *args, **kwargs):
        if self.writer:
            self.writer.cancel(*args, **kwargs)
    
    






