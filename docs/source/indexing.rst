======================
How to index documents
======================

Creating an Index object
========================

To create an index in a directory, use ``index.create_in``::

    import os, os.path
    from whoosh import index

    if not os.path.exists("indexdir"):
        os.mkdir("indexdir")

    ix = index.create_in("indexdir", schema)

To open an existing index in a directory, use ``index.open_dir``::

    import whoosh.index as index

    ix = index.open_dir("indexdir")

These are convenience methods for::

    from whoosh.filedb.filestore import FileStorage
    storage = FileStorage("indexdir")

    # Create an index
    ix = storage.create_index(schema)

    # Open an existing index
    storage.open_index()

The schema you created the index with is pickled and stored with the index.

You can keep multiple indexes in the same directory using the indexname keyword
argument::

    # Using the convenience functions
    ix = index.create_in("indexdir", schema=schema, indexname="usages")
    ix = index.open_dir("indexdir", indexname="usages")

    # Using the Storage object
    ix = storage.create_index(schema, indexname="usages")
    ix = storage.open_index(indexname="usages")


Clearing the index
==================

Calling ``index.create_in`` on a directory with an existing index will clear the
current contents of the index.

To test whether a directory currently contains a valid index, use
``index.exists_in``::

    exists = index.exists_in("indexdir")
    usages_exists = index.exists_in("indexdir", indexname="usages")

(Alternatively you can simply delete the index's files from the directory, e.g.
if you only have one index in the directory, use ``shutil.rmtree`` to remove the
directory and then recreate it.)


Indexing documents
==================

Once you've created an ``Index`` object, you can add documents to the index with an
``IndexWriter`` object. The easiest way to get the ``IndexWriter`` is to call
``Index.writer()``::

    ix = index.open_dir("index")
    writer = ix.writer()

Creating a writer locks the index for writing, so only one thread/process at
a time can have a writer open.

.. note::

    Because opening a writer locks the index for writing, in a multi-threaded
    or multi-process environment your code needs to be aware that opening a
    writer may raise an exception (``whoosh.store.LockError``) if a writer is
    already open. Whoosh includes a couple of example implementations
    (:class:`whoosh.writing.AsyncWriter` and
    :class:`whoosh.writing.BufferedWriter`) of ways to work around the write
    lock.

.. note::

    While the writer is open and during the commit, the index is still
    available for reading. Existing readers are unaffected and new readers can
    open the current index normally. Once the commit is finished, existing
    readers continue to see the previous version of the index (that is, they
    do not automatically see the newly committed changes). New readers will see
    the updated index.

The IndexWriter's ``add_document(**kwargs)`` method accepts keyword arguments
where the field name is mapped to a value::

    writer = ix.writer()
    writer.add_document(title=u"My document", content=u"This is my document!",
                        path=u"/a", tags=u"first short", icon=u"/icons/star.png")
    writer.add_document(title=u"Second try", content=u"This is the second example.",
                        path=u"/b", tags=u"second short", icon=u"/icons/sheep.png")
    writer.add_document(title=u"Third time's the charm", content=u"Examples are many.",
                        path=u"/c", tags=u"short", icon=u"/icons/book.png")
    writer.commit()

You don't have to fill in a value for every field. Whoosh doesn't care if you
leave out a field from a document.

Indexed fields must be passed a unicode value. Fields that are stored but not
indexed (i.e. the ``STORED`` field type) can be passed any pickle-able object.

Whoosh will happily allow you to add documents with identical values, which can
be useful or annoying depending on what you're using the library for::

    writer.add_document(path=u"/a", title=u"A", content=u"Hello there")
    writer.add_document(path=u"/a", title=u"A", content=u"Deja vu!")

This adds two documents to the index with identical path and title fields. See
"updating documents" below for information on the ``update_document`` method, which
uses "unique" fields to replace old documents instead of appending.


Indexing and storing different values for the same field
--------------------------------------------------------

If you have a field that is both indexed and stored, you can index a unicode
value but store a different object if necessary (it's usually not, but sometimes
this is really useful) using a "special" keyword argument ``_stored_<fieldname>``.
The normal value will be analyzed and indexed, but the "stored" value will show
up in the results::

    writer.add_document(title=u"Title to be indexed", _stored_title=u"Stored title")


Finishing adding documents
--------------------------

An ``IndexWriter`` object is kind of like a database transaction. You specify a
bunch of changes to the index, and then "commit" them all at once.

Calling ``commit()`` on the ``IndexWriter`` saves the added documents to the
index::

    writer.commit()

Once your documents are in the index, you can search for them.

If you want to close the writer without committing the changes, call
``cancel()`` instead of ``commit()``::

    writer.cancel()

Keep in mind that while you have a writer open (including a writer you opened
and is still in scope), no other thread or process can get a writer or modify
the index. A writer also keeps several open files. So you should always remember
to call either ``commit()`` or ``cancel()`` when you're done with a writer object.


Merging segments
================

A Whoosh ``filedb`` index is really a container for one or more "sub-indexes"
called segments. When you add documents to an index, instead of integrating the
new documents with the existing documents (which could potentially be very
expensive, since it involves resorting all the indexed terms on disk), Whoosh
creates a new segment next to the existing segment. Then when you search the
index, Whoosh searches both segments individually and merges the results so the
segments appear to be one unified index. (This smart design is copied from
Lucene.)

So, having a few segments is more efficient than rewriting the entire index
every time you add some documents. But searching multiple segments does slow
down searching somewhat, and the more segments you have, the slower it gets. So
Whoosh has an algorithm that runs when you call ``commit()`` that looks for small
segments it can merge together to make fewer, bigger segments.

To prevent Whoosh from merging segments during a commit, use the ``merge``
keyword argument::

    writer.commit(merge=False)

To merge all segments together, optimizing the index into a single segment,
use the ``optimize`` keyword argument::

    writer.commit(optimize=True)

Since optimizing rewrites all the information in the index, it can be slow on
a large index. It's generally better to rely on Whoosh's merging algorithm than
to optimize all the time.

(The ``Index`` object also has an ``optimize()`` method that lets you optimize the
index (merge all the segments together). It simply creates a writer and calls
``commit(optimize=True)`` on it.)

For more control over segment merging, you can write your own merge policy
function and use it as an argument to the ``commit()`` method. See the
implementation of the ``NO_MERGE``, ``MERGE_SMALL``, and ``OPTIMIZE`` functions
in the ``whoosh.writing`` module.


Deleting documents
==================

You can delete documents using the following methods on an ``IndexWriter``
object. You then need to call ``commit()`` on the writer to save the deletions
to disk.

``delete_document(docnum)``

    Low-level method to delete a document by its internal document number.

``is_deleted(docnum)``

    Low-level method, returns ``True`` if the document with the given internal
    number is deleted.

``delete_by_term(fieldname, termtext)``

    Deletes any documents where the given (indexed) field contains the given
    term. This is mostly useful for ``ID`` or ``KEYWORD`` fields.

``delete_by_query(query)``

    Deletes any documents that match the given query.

::

    # Delete document by its path -- this field must be indexed
    ix.delete_by_term('path', u'/a/b/c')
    # Save the deletion to disk
    ix.commit()

In the ``filedb`` backend, "deleting" a document simply adds the document number
to a list of deleted documents stored with the index. When you search the index,
it knows not to return deleted documents in the results. However, the document's
contents are still stored in the index, and certain statistics (such as term
document frequencies) are not updated, until you merge the segments containing
deleted documents (see merging above). (This is because removing the information
immediately from the index would essentially involving rewriting the entire
index on disk, which would be very inefficient.)


Updating documents
==================

If you want to "replace" (re-index) a document, you can delete the old document
using one of the ``delete_*`` methods on ``Index`` or ``IndexWriter``, then use
``IndexWriter.add_document`` to add the new version. Or, you can use
``IndexWriter.update_document`` to do this in one step.

For ``update_document`` to work, you must have marked at least one of the fields
in the schema as "unique". Whoosh will then use the contents of the "unique"
field(s) to search for documents to delete::

    from whoosh.fields import Schema, ID, TEXT

    schema = Schema(path = ID(unique=True), content=TEXT)

    ix = index.create_in("index")
    writer = ix.writer()
    writer.add_document(path=u"/a", content=u"The first document")
    writer.add_document(path=u"/b", content=u"The second document")
    writer.commit()

    writer = ix.writer()
    # Because "path" is marked as unique, calling update_document with path="/a"
    # will delete any existing documents where the "path" field contains "/a".
    writer.update_document(path=u"/a", content="Replacement for the first document")
    writer.commit()

The "unique" field(s) must be indexed.

If no existing document matches the unique fields of the document you're
updating, ``update_document`` acts just like ``add_document``.

"Unique" fields and ``update_document`` are simply convenient shortcuts for deleting
and adding. Whoosh has no inherent concept of a unique identifier, and in no way
enforces uniqueness when you use ``add_document``.


Incremental indexing
====================

When you're indexing a collection of documents, you'll often want two code
paths: one to index all the documents from scratch, and one to only update the
documents that have changed (leaving aside web applications where you need to
add/update documents according to user actions).

Indexing everything from scratch is pretty easy. Here's a simple example::

    import os.path
    from whoosh import index
    from whoosh.fields import Schema, ID, TEXT

    def clean_index(dirname):
      # Always create the index from scratch
      ix = index.create_in(dirname, schema=get_schema())
      writer = ix.writer()

      # Assume we have a function that gathers the filenames of the
      # documents to be indexed
      for path in my_docs():
        add_doc(writer, path)

      writer.commit()


    def get_schema()
      return Schema(path=ID(unique=True, stored=True), content=TEXT)


    def add_doc(writer, path):
      fileobj = open(path, "rb")
      content = fileobj.read()
      fileobj.close()
      writer.add_document(path=path, content=content)

Now, for a small collection of documents, indexing from scratch every time might
actually be fast enough. But for large collections, you'll want to have the
script only re-index the documents that have changed.

To start we'll need to store each document's last-modified time, so we can check
if the file has changed. In this example, we'll just use the mtime for
simplicity::

    def get_schema()
      return Schema(path=ID(unique=True, stored=True), time=STORED, content=TEXT)

    def add_doc(writer, path):
      fileobj = open(path, "rb")
      content = fileobj.read()
      fileobj.close()
      modtime = os.path.getmtime(path)
      writer.add_document(path=path, content=content, time=modtime)

Now we can modify the script to allow either "clean" (from scratch) or
incremental indexing::

    def index_my_docs(dirname, clean=False):
      if clean:
        clean_index(dirname)
      else:
        incremental_index(dirname)


    def incremental_index(dirname)
        ix = index.open_dir(dirname)

        # The set of all paths in the index
        indexed_paths = set()
        # The set of all paths we need to re-index
        to_index = set()

        with ix.searcher() as searcher:
          writer = ix.writer()

          # Loop over the stored fields in the index
          for fields in searcher.all_stored_fields():
            indexed_path = fields['path']
            indexed_paths.add(indexed_path)

            if not os.path.exists(indexed_path):
              # This file was deleted since it was indexed
              writer.delete_by_term('path', indexed_path)

            else:
              # Check if this file was changed since it
              # was indexed
              indexed_time = fields['time']
              mtime = os.path.getmtime(indexed_path)
              if mtime > indexed_time:
                # The file has changed, delete it and add it to the list of
                # files to reindex
                writer.delete_by_term('path', indexed_path)
                to_index.add(indexed_path)

          # Loop over the files in the filesystem
          # Assume we have a function that gathers the filenames of the
          # documents to be indexed
          for path in my_docs():
            if path in to_index or path not in indexed_paths:
              # This is either a file that's changed, or a new file
              # that wasn't indexed before. So index it!
              add_doc(writer, path)

          writer.commit()

The ``incremental_index`` function:

* Loops through all the paths that are currently indexed.

  * If any of the files no longer exist, delete the corresponding document from
    the index.

  * If the file still exists, but has been modified, add it to the list of paths
    to be re-indexed.

  * If the file exists, whether it's been modified or not, add it to the list of
    all indexed paths.

* Loops through all the paths of the files on disk.

  * If a path is not in the set of all indexed paths, the file is new and we
    need to index it.

  * If a path is in the set of paths to re-index, we need to index it.

  * Otherwise, we can skip indexing the file.


Clearing the index
==================

In some cases you may want to re-index from scratch. To clear the index without
disrupting any existing readers::

    from whoosh import writing

    with myindex.writer() as mywriter:
        # You can optionally add documents to the writer here
        # e.g. mywriter.add_document(...)

        # Using mergetype=CLEAR clears all existing segments so the index will
        # only have any documents you've added to this writer
        mywriter.mergetype = writing.CLEAR

Or, if you don't use the writer as a context manager and call ``commit()``
directly, do it like this::

    mywriter = myindex.writer()
    # ...
    mywriter.commit(mergetype=writing.CLEAR)

.. note::
    If you don't need to worry about existing readers, a more efficient method
    is to simply delete the contents of the index directory and start over.
