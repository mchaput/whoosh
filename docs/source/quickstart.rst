===========
Quick start
===========

Whoosh is a library of classes and functions for indexing text and then searching the index.
It allows you to develop custom search engines for your content. For example, if you were
creating blogging software, you could use Whoosh to add a search function to allow users to
search blog entries.


A quick introduction
====================

::

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
    >>> from whoosh.qparser import QueryParser
    >>> with ix.searcher() as searcher:
    ...     query = QueryParser("content", ix.schema).parse("first")
    ...     results = searcher.search(query)
    ...     results[0]
    ...
    {"title": u"First document", "path": u"/a"}


The ``Index`` and ``Schema`` objects
====================================

To begin using Whoosh, you need an *index object*. The first time you create
an index, you must define the index's *schema*. The schema lists the *fields*
in the index. A field is a piece of information for each document in the index,
such as its title or text content. A field can be *indexed* (meaning it can
be searched) and/or *stored* (meaning the value that gets indexed is returned
with the results; this is useful for fields such as the title).

This schema has two fields, "title" and "content"::

    from whoosh.fields import Schema, TEXT

    schema = Schema(title=TEXT, content=TEXT)

You only need to do create the schema once, when you create the index. The
schema is pickled and stored with the index.

When you create the ``Schema`` object, you use keyword arguments to map field names
to field types. The list of fields and their types defines what you are indexing
and what's searchable. Whoosh comes with some very useful predefined field
types, and you can easily create your own.

:class:`whoosh.fields.ID`
    This type simply indexes (and optionally stores) the entire value of the
    field as a single unit (that is, it doesn't break it up into individual
    words). This is useful for fields such as a file path, URL, date, category,
    etc.

:class:`whoosh.fields.STORED`
    This field is stored with the document, but not indexed. This field type is
    not indexed and not searchable. This is useful for document information you
    want to display to the user in the search results.

:class:`whoosh.fields.KEYWORD`
    This type is designed for space- or comma-separated keywords. This type is
    indexed and searchable (and optionally stored). To save space, it does not
    support phrase searching.

:class:`whoosh.fields.TEXT`
    This type is for body text. It indexes (and optionally stores) the text and
    stores term positions to allow phrase searching.

:class:`whoosh.fields.NUMERIC`
    This type is for numbers. You can store integers or floating point numbers.

:class:`whoosh.fields.BOOLEAN`
    This type is for boolean (true/false) values.

:class:`whoosh.fields.DATETIME`
    This type is for ``datetime`` objects. See :doc:`dates` for more
    information.

:class:`whoosh.fields.NGRAM` and :class:`whoosh.fields.NGRAMWORDS`
    These types break the field text or individual terms into N-grams.
    See :doc:`ngrams` for more information.

(As a shortcut, if you don't need to pass any arguments to the field type, you
can just give the class name and Whoosh will instantiate the object for you.) ::

    from whoosh.fields import Schema, STORED, ID, KEYWORD, TEXT

    schema = Schema(title=TEXT(stored=True), content=TEXT,
                    path=ID(stored=True), tags=KEYWORD, icon=STORED)

See :doc:`schema` for more information.

Once you have the schema, you can create an index using the ``create_in``
function::

    import os.path
    from whoosh.index import create_in

    if not os.path.exists("index"):
        os.mkdir("index")
    ix = create_in("index", schema)

(At a low level, this creates a *Storage* object to contain the index. A
``Storage`` object represents that medium in which the index will be stored.
Usually this will be ``FileStorage``, which stores the index as a set of files
in a directory.)

After you've created an index, you can open it using the ``open_dir``
convenience function::

    from whoosh.index import open_dir

    ix = open_dir("index")


The ``IndexWriter`` object
==========================

OK, so we've got an ``Index`` object, now we can start adding documents. The
``writer()`` method of the ``Index`` object returns an ``IndexWriter`` object that lets
you add documents to the index. The IndexWriter's ``add_document(**kwargs)``
method accepts keyword arguments where the field name is mapped to a value::

    writer = ix.writer()
    writer.add_document(title=u"My document", content=u"This is my document!",
                        path=u"/a", tags=u"first short", icon=u"/icons/star.png")
    writer.add_document(title=u"Second try", content=u"This is the second example.",
                        path=u"/b", tags=u"second short", icon=u"/icons/sheep.png")
    writer.add_document(title=u"Third time's the charm", content=u"Examples are many.",
                        path=u"/c", tags=u"short", icon=u"/icons/book.png")
    writer.commit()

Two important notes:

* You don't have to fill in a value for every field. Whoosh doesn't care if you
  leave out a field from a document.

* Indexed text fields must be passed a unicode value. Fields that are stored
  but not indexed (``STORED`` field type) can be passed any pickle-able object.

If you have a text field that is both indexed and stored, you can index a
unicode value but store a different object if necessary (it's usually not, but
sometimes this is really useful) using this trick::

    writer.add_document(title=u"Title to be indexed", _stored_title=u"Stored title")

Calling commit() on the ``IndexWriter`` saves the added documents to the index::

    writer.commit()

See :doc:`indexing` for more information.

Once your documents are committed to the index, you can search for them.


The ``Searcher`` object
=======================

To begin searching the index, we'll need a ``Searcher`` object::

    searcher = ix.searcher()

You'll usually want to open the searcher using a ``with`` statement so the
searcher is automatically closed when you're done with it (searcher objects
represent a number of open files, so if you don't explicitly close them and the
system is slow to collect them, you can run out of file handles)::

    with ix.searcher() as searcher:
        ...

This is of course equivalent to::

    try:
        searcher = ix.searcher()
        ...
    finally:
        searcher.close()

The Searcher's ``search()`` method takes a *Query object*. You can construct
query objects directly or use a query parser to parse a query string.

For example, this query would match documents that contain both "apple" and
"bear" in the "content" field::

    # Construct query objects directly

    from whoosh.query import *
    myquery = And([Term("content", u"apple"), Term("content", "bear")])

To parse a query string, you can use the default query parser in the ``qparser``
module. The first argument to the ``QueryParser`` constructor is the default
field to search. This is usually the "body text" field. The second optional
argument is a schema to use to understand how to parse the fields::

    # Parse a query string

    from whoosh.qparser import QueryParser
    parser = QueryParser("content", ix.schema)
    myquery = parser.parse(querystring)

Once you have a ``Searcher`` and a query object, you can use the ``Searcher``'s
``search()`` method to run the query and get a ``Results`` object::

    >>> results = searcher.search(myquery)
    >>> print(len(results))
    1
    >>> print(results[0])
    {"title": "Second try", "path": "/b", "icon": "/icons/sheep.png"}

The default ``QueryParser`` implements a query language very similar to
Lucene's. It lets you connect terms with ``AND`` or ``OR``, eleminate terms with
``NOT``, group terms together into clauses with parentheses, do range, prefix,
and wilcard queries, and specify different fields to search. By default it joins
clauses together with ``AND`` (so by default, all terms you specify must be in
the document for the document to match)::

    >>> print(parser.parse(u"render shade animate"))
    And([Term("content", "render"), Term("content", "shade"), Term("content", "animate")])

    >>> print(parser.parse(u"render OR (title:shade keyword:animate)"))
    Or([Term("content", "render"), And([Term("title", "shade"), Term("keyword", "animate")])])

    >>> print(parser.parse(u"rend*"))
    Prefix("content", "rend")

Whoosh includes extra features for dealing with search results, such as

* Sorting results by the value of an indexed field, instead of by relelvance.
* Highlighting the search terms in excerpts from the original documents.
* Expanding the query terms based on the top few documents found.
* Paginating the results (e.g. "Showing results 1-20, page 1 of 4").

See :doc:`searching` for more information.

