==============================
How to implement a new backend
==============================

Storage
=======

* Subclass :class:`whoosh.store.Storage`.

* Storages must implement the following methods.

  * :meth:`whoosh.store.Storage.create_index` -- create an object implementing the
    :class:`whoosh.index.Index` interface and returns it.
    
  * :meth:`whoosh.store.Storage.open_index` -- returns an object implementing the
    :class:`whoosh.index.Index` interface.

* Storage *may* implement the following methods (the base class's versions are no-ops).

  * ``close()`` -- closes any resources in use by the Storage object.
  
  * ``optimize()`` -- cleans up and/or compacts the data stored in the Storage object.
  

Index
=====

* Subclass :class:`whoosh.index.Index`.

* Indexes must implement the following methods.

  * :meth:`whoosh.index.Index.is_empty` -- returns True if the index contains no documents.
  
  * :meth:`whoosh.index.Index.doc_count_all` -- returns the number of documents, deleted
    or undeleted, in the index. If the backend does not have delayed deletion, this returns
    the same number as ``doc_count()``.
    
  * :meth:`whoosh.index.Index.doc_count` -- returns the number of undeleted documents in
    the index.
    
  * :meth:`whoosh.index.Index.field_length` -- returns the total number of terms in a field
    across all documents.
  
  * :meth:`whoosh.index.Index.term_reader` -- returns a :class:`whoosh.reading.TermReader`
    object for the index.
  
  * :meth:`whoosh.index.Index.doc_reader` -- returns a :class:`whoosh.reading.DocReader`
    object for the index.
  
  * :meth:`whoosh.index.Index.writer` -- returns a :class:`whoosh.writing.IndexWriter`
    object for the index.

* Indexes the require/support locking must implement the following methods.

  * :meth:`whoosh.index.Index.lock`
  
  * :meth:`whoosh.index.Index.unlock`

* Indexes that support deletion must implement the following methods.

  * :meth:`whoosh.index.Index.delete_document` -- deletes a document by number.
  
* Indexes that require/support versioning/transactions *may* implement the following methods.

  * :meth:`whoosh.index.Index.latest_generation` -- returns the generation number of the
    latest version of the index.

  * :meth:`whoosh.index.Index.up_to_date` -- returns True if the Index object represents
    the latest generation of the index.
    
  * :meth:`whoosh.index.Index.refresh` -- returns a new Index representing the latest
    generation of the index.
  
* Index *may* implement the following methods (the base class's versions are no-ops).

  * :meth:`whoosh.index.Index.optimize` -- cleans and/or compacts data contained in the index.
  
  * :meth:`whoosh.index.Index.close` -- closes any open resources associated with the index.


IndexWriter
===========

* Subclass :class:`whoosh.writing.IndexWriter`.

* IndexWriters must implement the following methods.

  * :meth:`whoosh.reading.IndexWriter.add_document` -- 

* IndexWriters that support deletion must implement the following methods.

  * :meth:`whoosh.writing.IndexWriter.delete_document` -- deletes a document by number.
  
  * :meth:`whoosh.reading.IndexWriter.update_document` -- 
  
* IndexWriters that work as transactions must implement the following methods.

  * :meth:`whoosh.reading.IndexWriter.commit` -- Save the additions/deletions done with
    this IndexWriter to the main index, and release any resources used by the IndexWriter.
  
  * :meth:`whoosh.reading.IndexWriter.cancel` -- Throw away any additions/deletions done
    with this IndexWriter, and release any resources used by the IndexWriter.


DocReader
=========

* Subclass :class:`whoosh.reading.DocReader`.

* DocReaders must implement the following methods.

  * :meth:`whoosh.reading.DocReader.__getitem__` -- 
  
  * :meth:`whoosh.reading.DocReader.__iter__` -- 
  
  * :meth:`whoosh.reading.DocReader.doc_count_all` -- 
  
  * :meth:`whoosh.reading.DocReader.doc_count` -- 
  
  * :meth:`whoosh.reading.DocReader.field_length` -- 
  
  * :meth:`whoosh.reading.DocReader.doc_field_length` -- 
  
  * :meth:`whoosh.reading.DocReader.doc_field_lengths` -- 
  
  * :meth:`whoosh.reading.DocReader.vector` -- 
  
  * :meth:`whoosh.reading.DocReader.vector_as` -- 
  
* DocReaders *may* implement the following methods.
  
  * :meth:`whoosh.reading.DocReader.close` -- closes any open resources associated with the
    reader.


TermReader
==========

* Subclass :class:`whoosh.reading.TermReader`.

* Implement the following methods.

  * :meth:`whoosh.reading.TermReader.__contains__` -- returns True if the given term tuple
    ``(fieldid, text)`` is in this reader.
  
  * :meth:`whoosh.reading.TermReader.__iter__` -- 
  
  * :meth:`whoosh.reading.TermReader.iter_from` -- 
  
  * :meth:`whoosh.reading.TermReader.doc_frequency` -- 
  
  * :meth:`whoosh.reading.TermReader.frequency` -- 
  
  * :meth:`whoosh.reading.TermReader.doc_count_all` -- 
  
  * :meth:`whoosh.reading.TermReader.postings` -- 
  
* TermReaders *may* implement the following methods.
  
  * :meth:`whoosh.reading.TermReader.close` -- closes any open resources associated with the
    reader.




