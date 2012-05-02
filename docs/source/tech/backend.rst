==============================
How to implement a new backend
==============================

Index
=====

* Subclass :class:`whoosh.index.Index`.

* Indexes must implement the following methods.

  * :meth:`whoosh.index.Index.is_empty`

  * :meth:`whoosh.index.Index.doc_count`

  * :meth:`whoosh.index.Index.reader`

  * :meth:`whoosh.index.Index.writer`

* Indexes that require/support locking must implement the following methods.

  * :meth:`whoosh.index.Index.lock`

  * :meth:`whoosh.index.Index.unlock`

* Indexes that support deletion must implement the following methods.

  * :meth:`whoosh.index.Index.delete_document`

  * :meth:`whoosh.index.Index.doc_count_all` -- if the backend has delayed
    deletion.

* Indexes that require/support versioning/transactions *may* implement the following methods.

  * :meth:`whoosh.index.Index.latest_generation`

  * :meth:`whoosh.index.Index.up_to_date`

  * :meth:`whoosh.index.Index.last_modified`

* Index *may* implement the following methods (the base class's versions are no-ops).

  * :meth:`whoosh.index.Index.optimize`

  * :meth:`whoosh.index.Index.close`


IndexWriter
===========

* Subclass :class:`whoosh.writing.IndexWriter`.

* IndexWriters must implement the following methods.

  * :meth:`whoosh.writing.IndexWriter.add_document`

  * :meth:`whoosh.writing.IndexWriter.add_reader`

* Backends that support deletion must implement the following methods.

  * :meth:`whoosh.writing.IndexWriter.delete_document`

* IndexWriters that work as transactions must implement the following methods.

  * :meth:`whoosh.reading.IndexWriter.commit` -- Save the additions/deletions done with
    this IndexWriter to the main index, and release any resources used by the IndexWriter.

  * :meth:`whoosh.reading.IndexWriter.cancel` -- Throw away any additions/deletions done
    with this IndexWriter, and release any resources used by the IndexWriter.


IndexReader
===========

* Subclass :class:`whoosh.reading.IndexReader`.

* IndexReaders must implement the following methods.

  * :meth:`whoosh.reading.IndexReader.__contains__`

  * :meth:`whoosh.reading.IndexReader.__iter__`

  * :meth:`whoosh.reading.IndexReader.iter_from`

  * :meth:`whoosh.reading.IndexReader.stored_fields`

  * :meth:`whoosh.reading.IndexReader.doc_count_all`

  * :meth:`whoosh.reading.IndexReader.doc_count`

  * :meth:`whoosh.reading.IndexReader.doc_field_length`

  * :meth:`whoosh.reading.IndexReader.field_length`

  * :meth:`whoosh.reading.IndexReader.max_field_length`

  * :meth:`whoosh.reading.IndexReader.postings`

  * :meth:`whoosh.reading.IndexReader.has_vector`

  * :meth:`whoosh.reading.IndexReader.vector`

  * :meth:`whoosh.reading.IndexReader.doc_frequency`

  * :meth:`whoosh.reading.IndexReader.frequency`

* Backends that support deleting documents should implement the following
  methods.

  * :meth:`whoosh.reading.IndexReader.has_deletions`
  * :meth:`whoosh.reading.IndexReader.is_deleted`

* Backends that support versioning should implement the following methods.

  * :meth:`whoosh.reading.IndexReader.generation`

* If the IndexReader object does not keep the schema in the ``self.schema``
  attribute, it needs to override the following methods.

  * :meth:`whoosh.reading.IndexReader.field`

  * :meth:`whoosh.reading.IndexReader.field_names`

  * :meth:`whoosh.reading.IndexReader.scorable_names`

  * :meth:`whoosh.reading.IndexReader.vector_names`

* IndexReaders *may* implement the following methods.

  * :meth:`whoosh.reading.DocReader.close` -- closes any open resources associated with the
    reader.


Matcher
=======

The :meth:`whoosh.reading.IndexReader.postings` method returns a
:class:`whoosh.matching.Matcher` object. You will probably need to implement
a custom Matcher class for reading from your posting lists.

* Subclass :class:`whoosh.matching.Matcher`.

* Implement the following methods at minimum.

  * :meth:`whoosh.matching.Matcher.is_active`

  * :meth:`whoosh.matching.Matcher.copy`

  * :meth:`whoosh.matching.Matcher.id`

  * :meth:`whoosh.matching.Matcher.next`

  * :meth:`whoosh.matching.Matcher.value`

  * :meth:`whoosh.matching.Matcher.value_as`

  * :meth:`whoosh.matching.Matcher.score`

* Depending on the implementation, you *may* implement the following methods
  more efficiently.

  * :meth:`whoosh.matching.Matcher.skip_to`

  * :meth:`whoosh.matching.Matcher.weight`

* If the implementation supports quality, you should implement the following
  methods.

  * :meth:`whoosh.matching.Matcher.supports_quality`

  * :meth:`whoosh.matching.Matcher.quality`

  * :meth:`whoosh.matching.Matcher.block_quality`

  * :meth:`whoosh.matching.Matcher.skip_to_quality`
