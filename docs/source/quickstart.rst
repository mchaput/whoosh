===========
Quick start
===========

Whoosh is a library of classes and functions for indexing text and then searching the index.
It allows you to develop custom search engines for your content. For example, if you were
creating blogging software, you could use Whoosh to add a search function to allow users to
search blog entries.


A quick introduction
====================

The following code should give you some of the flavor of Whoosh. It uses 

>>> from whoosh.index import create_in
>>> from whoosh.fields import *
>>> schema = Schema(title=TEXT(stored=True), path=ID(stored=True), content=TEXT)
>>> ix = create_in("indexdir", schema)
>>> writer = ix.writer()
>>> writer.add_document(title=u"First document", path=u"/a",
...                     content=u"This is the first document we've added!")
>>> writer.add_document(title=u"Second document", path=u"/b",
...                     content=u"The second one is even more interesting!")
>>> writer.commit()
>>> searcher = ix.searcher()
>>> results = searcher.find("first")
>>> results[0]
{"title": u"First document", "path": u"/a"}


Creating an index
=================

At a high level, to begin using Whoosh you need an *index object*. The first
time you create an index, you must define the index's *schema*. For example,
this schema has two fields, "title" and "content"::

	from whoosh.fields import Schema, TEXT
	
	schema = Schema(title=TEXT, content=TEXT)

A Schema object defines the fields that are indexed for each document, and
how/whether the content of the fields is indexed. You only need to do create
the schema once, when you create the index. The schema is pickled and stored with
the index.

When you create the Schema object, you use keyword arguments to map field names
to field types. The list of fields and their types defines what you are indexing and
what's searchable. Whoosh comes with some very useful predefined field types, and you
can easily create your own.

:class:`whoosh.fields.ID`
    This type simply indexes (and optionally stores) the entire value of the field as a
    single unit (that is, it doesn't break it up into individual words). This is useful
    for fields such as a file path, URL, date, category, etc.
    
:class:`whoosh.fields.STORED`
    This field is stored with the document, but not indexed. This field type is not
    indexed and not searchable. This is useful for document information you want to
    display to the user in the search results.
    
:class:`whoosh.fields.KEYWORD`
    This type is designed for space- or comma-separated keywords. This type is indexed
    and searchable (and optionally stored). To save space, it does not support phrase
    searching.
    
:class:`whoosh.fields.TEXT`
    This type is for body text. It indexes (and optionally stores) the text and stores
    term positions to allow phrase searching.

:class:`whoosh.fields.NGRAM`
    TODO

(As a shortcut, if you don't need to pass any arguments to the field type, you can just
give the class name and Whoosh will instantiate the object for you.) ::

    from whoosh.fields import Schema, STORED, ID, KEYWORD, TEXT

    schema = Schema(title=TEXT(stored=True), content=TEXT,
                    path=ID(stored=True), tags=KEYWORD, icon=STORED)

See :doc:`schema` for more information.

Once you have the schema, you can create an index using the ``create_index_in``
function::

	import os.path
	from whoosh.index import create_index_in
	
	if not os.path.exists("index"):
        os.mkdir("index")
	index = create_index_in("index", schema)

At a low level, this involves creating a *storage* object to contain the index.
A Storage object represents that medium in which the index will be stored. Usually this
will be ``FileStorage``, which stores the index as a set of files in a directory.
Whoosh includes a few other experimental storage backends. Future versions may include
additional options, such as a SQL backend.

Here's how you would create the index using a storage object directly instead of
the ``create_index_in`` convenience function::

    import os, os.path
    from whoosh.filedb.filestore import FileStorage

    if not os.path.exists("index"):
        os.mkdir("index")

    storage = FileStorage("index")
    index = storage.create_index(schema)


Opening an index
================

After you've created an index, you can open it using the ``open_dir`` convenience
function::

	from whoosh.index import open_dir
	
	index = open_dir("index")
	
Or, using a storage object::

	from whoosh.filedb.filestore import FileStorage
	
	storage = FileStorage("index")
	index = storage.open_index()


Indexing documents
==================

OK, so we've got an Index object, now we can start adding documents. The writer() method
of the Index object returns an ``IndexWriter`` object that lets you add documents to
the index. The IndexWriter's ``add_document(**kwargs)`` method accepts keyword arguments
where the field name is mapped to a value::

    writer = ix.writer()
    writer.add_document(title=u"My document", content=u"This is my document!",
                        path=u"/a", tags=u"first short", icon=u"/icons/star.png")
    writer.add_document(title=u"Second try", content=u"This is the second example.",
                        path=u"/b", tags=u"second short", icon=u"/icons/sheep.png")
    writer.add_document(title=u"Third time's the charm", content=u"Examples are many.",
                        path=u"/c", tags=u"short", icon=u"/icons/book.png")
    writer.commit()

Two important notes:

* You don't have to fill in a value for every field. Whoosh doesn't care if you leave
  out a field from a document.

* Indexed fields must be passed a unicode value. Fields that are stored but not
  indexed (STORED field type) can be passed any pickle-able object.

If you have a field that is both indexed and stored, you can even index a unicode
value but store a different object if necessary (it's usually not, but sometimes
this is really useful) using this trick::

    writer.add_document(title=u"Title to be indexed", _stored_title=u"Stored title")

Calling commit() on the ``IndexWriter`` saves the added documents to the index::

	writer.commit()

See :doc:`indexing` for more information.

Once your documents are in the index, you can search for them.


Searching
=========

So, let's say a user has typed a search into a search box and you want to run that search on
you index.

To begin searching the index, we'll need a Searcher object::

    searcher = ix.searcher()

You can use the high-level ``find()`` method to run queries on the index.
The first argument is the default field to search (for terms in the query string that
aren't explicitly qualified with a field), and the second is the query string. The
method returns a Results object.

The Results object acts like a list of dictionaries, where each dictionary
contains the stored fields of the document. The first document in the list is the most
relevant based on the scoring algorithm::

	>>> results = searcher.find("content", u"second")
    >>> print(len(results))
    1
    >>> print(results[0])
    {"title": "Second try", "path": "/b", "icon": "/icons/sheep.png"}

At a lower level, the Searcher's ``search()`` method takes Query objects instead of
a query string. You can construct query objects directly or use a query parser to
parse a query string into Query objects.

For example, this query would match documents that contain both "apple" and "bear"
in the "content" field::

	from whoosh.query import *

	myquery = And([Term("content", u"apple"), Term("content", "bear")])
	
To parse a query string into Query objects, you can use the default query parser
in the ``qparser`` module::

    from whoosh.qparser import QueryParser
    
    parser = QueryParser("content", schema = ix.schema)

The first argument, ``"content"``, specifies the default field to use when the user
doesn't specify a field for a word/phrase/clause. This is usually the "body text"
field. Specifying the schema lets the parser know which analyzers to use for which
fields. If you don't have a schema (usually when you're testing the parser), you can
omit the schema. In that case, the parser won't filter the query terms (for example,
it won't lower-case them).

The default ``QueryParser`` implements a query language very similar to Lucene's.
It lets you connect terms with AND or OR, eleminate terms with NOT, group terms
together into clauses with parentheses, do range, prefix, and wilcard queries,
and specify different fields to search. By default it joins clauses together with
AND (so by default, all terms you specify must be in the document for the document
to match)::

    >>> print(parser.parse(u"render shade animate"))
    And([Term("content", "render"), Term("content", "shade"), Term("content", "animate")])

    >>> print(parser.parse(u"render OR (title:shade keyword:animate)"))
    Or([Term("content", "render"), And([Term("title", "shade"), Term("keyword", "animate")])])

    >>> print(parser.parse(u"rend*"))
    Prefix("content", "rend")
    
We'll create a query object we can use to find a document in the index we created above::

    query = parser.parse(u"second")

Now you can use the searcher to find documents that match the query::

    results = searcher.search(query)

Whoosh includes extra features for dealing with search results, such as

* Sorting results by the value of an indexed field, instead of by relelvance.
* Highlighting the search terms in excerpts from the original documents.
* Expanding the query terms based on the top few documents found.
* Paginating the results (e.g. "Showing results 1-20, page 1 of 4").

See :doc:`searching` for more information.

