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

from whoosh.store import LockError

# Exceptions

class IndexingError(Exception):
    pass


# Base class

class IndexWriter(object):
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
    
    def add_field(self, fieldname, fieldtype, **kwargs):
        """Adds a field to the index's schema.
        
        :param fieldname: the name of the field to add.
        :param fieldtype: an instantiated :class:`whoosh.fields.FieldType`
            object.
        """
        
        self.schema.add(fieldname, fieldtype, **kwargs)
    
    def remove_field(self, fieldname, **kwargs):
        """Removes the named field from the index's schema. Depending on the
        backend implementation, this may or may not actually remove existing
        data for the field from the index. Optimizing the index should always
        clear out existing data for a removed field.
        """
        
        self.schema.remove(fieldname, **kwargs)
        
    def searcher(self, **kwargs):
        """Returns a searcher for the existing index.
        """
        
        raise NotImplementedError
    
    def delete_by_term(self, fieldname, text, searcher=None):
        """Deletes any documents containing "term" in the "fieldname" field.
        This is useful when you have an indexed field containing a unique ID
        (such as "pathname") for each document.
        
        :returns: the number of documents deleted.
        """
        
        from whoosh.query import Term
        q = Term(fieldname, text)
        return self.delete_by_query(q, searcher=searcher)
    
    def delete_by_query(self, q, searcher=None):
        """Deletes any documents matching a query object.
        
        :returns: the number of documents deleted.
        """
        
        if searcher:
            s = searcher
        else:
            s = self.searcher()
        
        count = 0
        for docnum in q.docs(s):
            if not self.is_deleted(docnum):
                self.delete_document(docnum)
                count += 1
        
        if not searcher:
            s.close()
        
        return count
    
    def delete_document(self, docnum, delete=True):
        """Deletes a document by number.
        """
        raise NotImplementedError
    
    def add_document(self, **fields):
        """The keyword arguments map field names to the values to index/store.
        
        For fields that are both indexed and stored, you can specify an
        alternate value to store using a keyword argument in the form
        "_stored_<fieldname>". For example, if you have a field named "title"
        and you want to index the text "a b c" but store the text "e f g", use
        keyword arguments like this::
        
            writer.add_document(title=u"a b c", _stored_title=u"e f g")
        """
        raise NotImplementedError
    
    def update_document(self, **fields):
        """The keyword arguments map field names to the values to index/store.
        
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
        unique_fields = [name for name, field in self.schema.items()
                         if name in fields and field.unique]
        if not unique_fields:
            raise IndexingError("None of the fields in %r"
                                " are unique" % fields.keys())
        
        # Find the set of documents matching the unique terms
        delset = set()
        reader = self.searcher().reader()
        for name in unique_fields:
            field = self.schema[name]
            text = field.to_text(fields[name])
            docnum = reader.postings(name, text).id()
            delset.add(docnum)
        reader.close()
        
        # Delete the old docs
        for docnum in delset:
            self.delete_document(docnum)
        
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
    

class PostingWriter(object):
    def start(self, format):
        """Start a new set of postings for a new term. Implementations may
        raise an exception if this is called without a corresponding call to
        finish().
        """
        raise NotImplementedError
    
    def write(self, id, weight, valuestring):
        """Add a posting with the given ID and value.
        """
        raise NotImplementedError
    
    def finish(self):
        """Finish writing the postings for the current term. Implementations
        may raise an exception if this is called without a preceding call to
        start().
        """
        pass
    
    def close(self):
        """Finish writing all postings and close the underlying file.
        """
        pass


class AsyncWriter(threading.Thread, IndexWriter):
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
    
    For example, to get an aynchronous writer, instead of this:
    
    >>> writer = myindex.writer(postlimitmb=128)
    
    Do this:
    
    >>> from whoosh.writing import AsyncWriter
    >>> writer = AsyncWriter(myindex, )
    """
    
    def __init__(self, index, delay=0.25, writerargs=None):
        """
        :param index: the :class:`whoosh.index.Index` to write to.
        :param delay: the delay (in seconds) between attempts to instantiate
            the actual writer.
        :param writerargs: an optional dictionary specifying keyword arguments
            to to be passed to the index's ``writer()`` method.
        """
        
        threading.Thread.__init__(self)
        self.running = False
        self.index = index
        self.writerargs = writerargs or {}
        self.delay = delay
        self.events = []
        try:
            self.writer = self.index.writer(**self.writerargs)
        except LockError:
            self.writer = None
    
    def searcher(self):
        return self.index.searcher()
    
    def _record(self, method, args, kwargs):
        if self.writer:
            getattr(self.writer, method)(*args, **kwargs)
        else:
            self.events.append((method, args, kwargs))
    
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
    
    def delete_document(self, *args, **kwargs):
        self._record("delete_document", args, kwargs)
    
    def add_document(self, *args, **kwargs):
        self._record("add_document", args, kwargs)
        
    def update_document(self, *args, **kwargs):
        self._record("update_document", args, kwargs)
    
    def add_field(self, *args, **kwargs):
        self._record("add_field", args, kwargs)
        
    def remove_field(self, *args, **kwargs):
        self._record("remove_field", args, kwargs)
    
    def delete_by_term(self, *args, **kwargs):
        self._record("delete_by_term", args, kwargs)
    
    def commit(self, *args, **kwargs):
        if self.writer:
            self.writer.commit(*args, **kwargs)
        else:
            self.commitargs, self.commitkwargs = args, kwargs
            self.start()
    
    def cancel(self, *args, **kwargs):
        if self.writer:
            self.writer.cancel(*args, **kwargs)
    
    
class BatchWriter(object):
    """Convenience wrapper that batches up calls to ``add_document()``,
    ``update_document()``, and/or ``delete_document()``, and commits them
    whenever a maximum amount of time passes or a maximum number of batched
    changes accumulate.
    
    This is useful when you're adding documents one at a time, in rapid
    succession (e.g. a web app). The more documents you add per commit, the
    more efficient Whoosh is. This class batches multiple documents and adds
    them all at once. If you're adding a bunch of documents at a time, just use
    a regular writer -- you're already committing a "batch" of documents, so
    you don't need this class.
    
    In scenarios where you are continuously adding single documents very
    rapidly (for example a web application where lots of users are adding
    content simultaneously), and you don't mind a delay between documents being
    added and becoming searchable, using a BatchWriter is *much* faster than
    opening and committing a writer for each document you add.
    
    >>> from whoosh.writing import BatchWriter
    >>> writer = BatchWriter(myindex)
    
    Calling ``commit()`` on this object opens a writer and commits any batched
    up changes. You can continue to make changes after calling ``commit()``,
    and you can call ``commit()`` multiple times.
    
    You should explicitly call ``commit()`` on this object before it goes out
    of scope to make sure any uncommitted changes are saved.
    """

    def __init__(self, index, period=60, limit=10, writerargs=None,
                 commitargs=None):
        """
        :param index: the :class:`whoosh.index.Index` to write to.
        :param period: the maximum amount of time (in seconds) between commits.
        :param limit: the maximum number of changes to accumulate before
            committing.
        :param writerargs: dictionary specifying keyword arguments to be passed
            to the index's ``writer()`` method.
        :param commitargs: dictionary specifying keyword arguments to be passed
            to the writer's ``commit()`` method.
        """
        self.index = index
        self.period = period
        self.limit = limit
        self.writerargs = writerargs or {}
        self.commitargs = commitargs or {}
        
        self.events = []
        self.timer = threading.Timer(self.period, self.commit)
    
    def __del__(self):
        self.commit(restart=False)
    
    def commit(self, restart=True):
        self.timer.cancel()
        if self.events:
            writer = self.index.writer(**self.writerargs)
            for method, args, kwargs in self.events:
                getattr(writer, method)(*args, **kwargs)
            writer.commit(**self.commitargs)
            self.events = []
        
        if restart:
            self.timer = threading.Timer(self.period, self.commit)
    
    def _record(self, method, args, kwargs):
        self.events.append((method, args, kwargs))
        if len(self.events) >= self.limit:
            self.commit()
        
    def delete_document(self, *args, **kwargs):
        self._record("delete_document", args, kwargs)
        
    def add_document(self, *args, **kwargs):
        self._record("add_document", args, kwargs)

    def update_document(self, *args, **kwargs):
        self._record("update_document", args, kwargs)

    def add_field(self, *args, **kwargs):
        self._record("add_field", args, kwargs)
        
    def remove_field(self, *args, **kwargs):
        self._record("remove_field", args, kwargs)




