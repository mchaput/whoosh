==================
Designing a schema
==================

About schemas and fields
========================

The schema specifies the fields of documents in an index.

Each document can have multiple fields, such as title, content, url, date, etc.

Some fields can be indexed, and some fields can be stored with the document so
the field value is available in search results.
Some fields will be both indexed and stored.

The schema is the set of all possible fields in a document. Each individual
document might only use a subset of the available fields in the schema.

For example, a simple schema for indexing emails might have fields like
``from_addr``, ``to_addr``, ``subject``, ``body``, and ``attachments``, where
the ``attachments`` field lists the names of attachments to the email. For
emails without attachments, you would omit the attachments field.


Built-in field types
====================

Whoosh provides some useful predefined field types:

:class:`whoosh.fields.TEXT`
    This type is for body text. It indexes (and optionally stores) the text and
    stores term positions to allow phrase searching.

    ``TEXT`` fields use :class:`~whoosh.analysis.StandardAnalyzer` by default. To specify a different
    analyzer, use the ``analyzer`` keyword argument to the constructor, e.g.
    ``TEXT(analyzer=analysis.StemmingAnalyzer())``. See :doc:`analysis`.

    By default, ``TEXT`` fields store position information for each indexed term, to
    allow you to search for phrases. If you don't need to be able to search for
    phrases in a text field, you can turn off storing term positions to save
    space. Use ``TEXT(phrase=False)``.

    By default, ``TEXT`` fields are not stored. Usually you will not want to store
    the body text in the search index. Usually you have the indexed documents
    themselves available to read or link to based on the search results, so you
    don't need to store their text in the search index. However, in some
    circumstances it can be useful (see :doc:`highlight`). Use
    ``TEXT(stored=True)`` to specify that the text should be stored in the index.

:class:`whoosh.fields.KEYWORD`
    This field type is designed for space- or comma-separated keywords. This
    type is indexed and searchable (and optionally stored). To save space, it
    does not support phrase searching.

    To store the value of the field in the index, use ``stored=True`` in the
    constructor. To automatically lowercase the keywords before indexing them,
    use ``lowercase=True``.

    By default, the keywords are space separated. To separate the keywords by
    commas instead (to allow keywords containing spaces), use ``commas=True``.

    If your users will use the keyword field for searching, use ``scorable=True``.

:class:`whoosh.fields.ID`
    The ``ID`` field type simply indexes (and optionally stores) the entire value of
    the field as a single unit (that is, it doesn't break it up into individual
    terms). This type of field does not store frequency information, so it's
    quite compact, but not very useful for scoring.

    Use ``ID`` for fields like url or path (the URL or file path of a document),
    date, category -- fields where the value must be treated as a whole, and
    each document only has one value for the field.

    By default, ``ID`` fields are not stored. Use ``ID(stored=True)`` to specify that
    the value of the field should be stored with the document for use in the
    search results. For example, you would want to store the value of a url
    field so you could provide links to the original in your search results.

:class:`whoosh.fields.STORED`
    This field is stored with the document, but not indexed and not searchable.
    This is useful for document information you want to display to the user in
    the search results, but don't need to be able to search for.

:class:`whoosh.fields.NUMERIC`
    This field stores int, long, or floating point numbers in a compact,
    sortable format.

:class:`whoosh.fields.DATETIME`
    This field stores datetime objects in a compact, sortable format.

:class:`whoosh.fields.BOOLEAN`
    This simple filed indexes boolean values and allows users to search for
    ``yes``, ``no``, ``true``, ``false``, ``1``, ``0``, ``t`` or ``f``.

:class:`whoosh.fields.NGRAM`
    TBD.

Expert users can create their own field types.


Creating a Schema
=================

To create a schema::

    from whoosh.fields import Schema, TEXT, KEYWORD, ID, STORED
    from whoosh.analysis import StemmingAnalyzer

    schema = Schema(from_addr=ID(stored=True),
                    to_addr=ID(stored=True),
                    subject=TEXT(stored=True),
                    body=TEXT(analyzer=StemmingAnalyzer()),
                    tags=KEYWORD)

If you aren't specifying any constructor keyword arguments to one of the
predefined fields, you can leave off the brackets (e.g. ``fieldname=TEXT`` instead
of ``fieldname=TEXT()``). Whoosh will instantiate the class for you.

Alternatively you can create a schema declaratively using the ``SchemaClass``
base class::

    from whoosh.fields import SchemaClass, TEXT, KEYWORD, ID, STORED

    class MySchema(SchemaClass):
        path = ID(stored=True)
        title = TEXT(stored=True)
        content = TEXT
        tags = KEYWORD

You can pass a declarative class to :func:`~whoosh.index.create_in` or
:meth:`~whoosh.store.Storage.create_index()` instead of a
:class:`~whoosh.fields.Schema` instance.


Modifying the schema after indexing
===================================

After you have created an index, you can add or remove fields to the schema
using the ``add_field()`` and ``remove_field()`` methods. These methods are
on the ``Writer`` object::

    writer = ix.writer()
    writer.add_field("fieldname", fields.TEXT(stored=True))
    writer.remove_field("content")
    writer.commit()

(If you're going to modify the schema *and* add documents using the same
writer, you must call ``add_field()`` and/or ``remove_field`` *before* you
add any documents.)

These methods are also on the ``Index`` object as a convenience, but when you
call them on an ``Index``, the Index object simply creates the writer, calls
the corresponding method on it, and commits, so if you want to add or remove
more than one field, it's much more efficient to create the writer yourself::

    ix.add_field("fieldname", fields.KEYWORD)

In the ``filedb`` backend, removing a field simply removes that field from the
*schema* -- the index will not get smaller, data about that field will remain
in the index until you optimize. Optimizing will compact the index, removing
references to the deleted field as it goes::

    writer = ix.writer()
    writer.add_field("uuid", fields.ID(stored=True))
    writer.remove_field("path")
    writer.commit(optimize=True)

Because data is stored on disk with the field name, *do not* add a new field with
the same name as a deleted field without optimizing the index in between::

    writer = ix.writer()
    writer.delete_field("path")
    # Don't do this!!!
    writer.add_field("path", fields.KEYWORD)

(A future version of Whoosh may automatically prevent this error.)


Dynamic fields
==============

Dynamic fields let you associate a field type with any field name that matches
a given "glob" (a name pattern containing ``*``, ``?``, and/or ``[abc]``
wildcards).

You can add dynamic fields to a new schema using the ``add()`` method with the
``glob`` keyword set to True::

    schema = fields.Schema(...)
    # Any name ending in "_d" will be treated as a stored
    # DATETIME field
    schema.add("*_d", fields.DATETIME(stored=True), glob=True)

To set up a dynamic field on an existing index, use the same
``IndexWriter.add_field`` method as if you were adding a regular field, but
with the ``glob`` keyword argument set to ``True``::

    writer = ix.writer()
    writer.add_field("*_d", fields.DATETIME(stored=True), glob=True)
    writer.commit()

To remove a dynamic field, use the ``IndexWriter.remove_field()`` method with
the glob as the name::

    writer = ix.writer()
    writer.remove_field("*_d")
    writer.commit()

For example, to allow documents to contain any field name that ends in ``_id``
and associate it with the ``ID`` field type::

    schema = fields.Schema(path=fields.ID)
    schema.add("*_id", fields.ID, glob=True)

    ix = index.create_in("myindex", schema)

    w = ix.writer()
    w.add_document(path=u"/a", test_id=u"alfa")
    w.add_document(path=u"/b", class_id=u"MyClass")
    # ...
    w.commit()

    qp = qparser.QueryParser("path", schema=schema)
    q = qp.parse(u"test_id:alfa")
    with ix.searcher() as s:
        results = s.search(q)


Advanced schema setup
=====================

Field boosts
------------

You can specify a field boost for a field. This is a multiplier applied to the
score of any term found in the field. For example, to make terms found in the
title field score twice as high as terms in the body field::

    schema = Schema(title=TEXT(field_boost=2.0), body=TEXT)


Field types
-----------

The predefined field types listed above are subclasses of ``fields.FieldType``.
``FieldType`` is a pretty simple class. Its attributes contain information that
define the behavior of a field.

============ =============== ======================================================
Attribute     Type             Description
============ =============== ======================================================
format       fields.Format   Defines what kind of information a field records
                             about each term, and how the information is stored
                             on disk.
vector       fields.Format   Optional: if defined, the format in which to store
                             per-document forward-index information for this field.
scorable     bool            If True, the length of (number of terms in) the field in
                             each document is stored in the index. Slightly misnamed,
                             since field lengths are not required for all scoring.
                             However, field lengths are required to get proper
                             results from BM25F.
stored       bool            If True, the value of this field is stored
                             in the index.
unique       bool            If True, the value of this field may be used to
                             replace documents with the same value when the user
                             calls
                             :meth:`~whoosh.writing.IndexWriter.document_update`
                             on an ``IndexWriter``.
============ =============== ======================================================

The constructors for most of the predefined field types have parameters that let
you customize these parts. For example:

* Most of the predefined field types take a stored keyword argument that sets
  FieldType.stored.

* The ``TEXT()`` constructor takes an ``analyzer`` keyword argument that is
  passed on to the format object.

Formats
-------

A ``Format`` object defines what kind of information a field records about each
term, and how the information is stored on disk.

For example, the ``Existence`` format would store postings like this:

==== ====
Doc
==== ====
10
20
30
==== ====

Whereas the ``Positions`` format would store postings like this:

===== =============
Doc   Positions
===== =============
10    ``[1,5,23]``
20    ``[45]``
30    ``[7,12]``
===== =============

The indexing code passes the unicode string for a field to the field's ``Format``
object. The ``Format`` object calls its analyzer (see text analysis) to break the
string into tokens, then encodes information about each token.

Whoosh ships with the following pre-defined formats.

=============== ================================================================
Class name      Description
=============== ================================================================
Stored          A "null" format for fields that are stored but not indexed.
Existence       Records only whether a term is in a document or not, i.e. it
                does not store term frequency. Useful for identifier fields
                (e.g. path or id) and "tag"-type fields, where the frequency
                is expected to always be 0 or 1.
Frequency       Stores the number of times each term appears in each document.
Positions       Stores the number of times each term appears in each document,
                and at what positions.
=============== ================================================================

The ``STORED`` field type uses the ``Stored`` format (which does nothing, so ``STORED``
fields are not indexed). The ``ID`` type uses the ``Existence`` format. The ``KEYWORD`` type
uses the ``Frequency`` format. The ``TEXT`` type uses the ``Positions`` format if it is
instantiated with ``phrase=True`` (the default), or ``Frequency`` if ``phrase=False``.

In addition, the following formats are implemented for the possible convenience
of expert users, but are not currently used in Whoosh:

================= ================================================================
Class name        Description
================= ================================================================
DocBoosts         Like Existence, but also stores per-document boosts
Characters        Like Positions, but also stores the start and end character
                  indices of each term
PositionBoosts    Like Positions, but also stores per-position boosts
CharacterBoosts   Like Positions, but also stores the start and end character
                  indices of each term and per-position boosts
================= ================================================================

Vectors
-------

The main index is an inverted index. It maps terms to the documents they appear
in. It is also sometimes useful to store a forward index, also known as a term
vector, that maps documents to the terms that appear in them.

For example, imagine an inverted index like this for a field:

========== =========================================================
Term       Postings
========== =========================================================
apple      ``[(doc=1, freq=2), (doc=2, freq=5), (doc=3, freq=1)]``
bear       ``[(doc=2, freq=7)]``
========== =========================================================

The corresponding forward index, or term vector, would be:

========== ======================================================
Doc        Postings
========== ======================================================
1          ``[(text=apple, freq=2)]``
2          ``[(text=apple, freq=5), (text='bear', freq=7)]``
3          ``[(text=apple, freq=1)]``
========== ======================================================

If you set ``FieldType.vector`` to a ``Format`` object, the indexing code will use the
``Format`` object to store information about the terms in each document. Currently
by default Whoosh does not make use of term vectors at all, but they are
available to expert users who want to implement their own field types.




