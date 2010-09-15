====================
Parsing user queries
====================

Overview
========

The job of a query parser is to convert a *query string* submitted by a user
into *query objects* (objects from the :mod:`whoosh.query` module) which

For example, the user query::

.. code-block:: none

    rendering shading
    
might be parsed into query objects like this::

    And([Term("content", u"rendering"), Term("content", u"shading")])

Whoosh includes a powerful, modular parser for user queries in the
:mod:`whoosh.qparser` module. The default parser implements a query language
similar to the one that ships with Lucene. However, by changing plugins or using
functions such as :func:`whoosh.qparser.MultifieldParser`,
:func:`whoosh.qparser.SimpleParser` or :func:`whoosh.qparser.DisMaxParser`, you
can change how the parser works, get a simpler parser or change the query
language syntax.

(In previous versions of Whoosh, the query parser was based on ``pyparsing``.
The new hand-written parser is less brittle and more flexible.)

.. note::
    
    Remember that you can directly create query objects programmatically using
    the objects in the :mod:`whoosh.query` module. If you are not processing
    actual user queries, this is preferable to building a query string just to
    parse it.


Using the default parser
========================

To create a :class:`whoosh.qparser.QueryParser` object, pass it the name of the
*default field* to search and the schema of the index you'll be searching.

    from whoosh.qparser import QueryParser

    parser = QueryParser("content", schema=myindex.schema)
    
.. tip::

    You can instantiate a QueryParser object without specifying a schema,
    however the parser will not process the text of the user query (see
    :ref:`querying and indexing <index-query>` below). This is useful for
    debugging, when you want to see how QueryParser will build a query, but
    don't want to make up a schema just for testing.

Once you have a QueryParser object, you can call ``parse()`` on it to parse a
query string into a query object::

    >>> parser.parse(u"alpha OR beta gamma")
    Or([Term("content", u"alpha"), Term("content", "beta")])

See the :doc:`query language reference <querylang>` for the features and syntax
of the default parser's query language.


Searching for any terms instead of all terms by default
=======================================================

If the user doesn't explicitly specify ``AND`` or ``OR`` clauses::

    physically based rendering
    
...by default, the parser treats the words as if they were connected by ``AND``,
meaning all the terms must be present for a document to match::

    physically AND based AND rendering
    
To change the parser to use ``OR`` instead, so that any of the terms may be
present for a document to match, i.e.::

    physically OR based OR rendering
    
...configure the QueryParser using the ``group`` keyword argument like this::

    from whoosh import qparser
    
    parser = qparser.QueryParser(fieldname, schema=myindex.schema,
                                 group=qparser.OrGroup)


Letting the user search multiple fields
=======================================

The default QueryParser configuration takes terms without explicit fields and
assigns them to the default field you specified when you created the object, so
for example if you created the object with::

    parser = QueryParser("content", schema=myschema)
    
And the user entered the query:

.. code-block:: none

    three blind mice
    
The parser would treat it as:

.. code-block:: none

    content:three content:blind content:mice

However, you might want to let the user search *multiple* fields by default. For
example, you might want "unfielded" terms to search both the ``title`` and
``content`` fields.

In that case, you can use a :class:`whoosh.qparser.MultifieldParser`. This is
just like the normal QueryParser, but instead of a default field name string, it
takes a *sequence* of field names::

    from whoosh.qparser import MultifieldParser

    mparser = MultifieldParser(["title", "content"], schema=myschema)
    
When this MultifieldParser instance parses ``three blind mice``, it treats it
as:

.. code-block:: none

    (title:three OR content:three) (title:blind OR content:blind) (title:mice OR content:mice)


.. _index-query:

The relationship between indexing and querying
==============================================

TBD.


Customizing the parser
======================

QueryParser arguments
---------------------

QueryParser supports two extra keyword arguments:

group
    The query class to use to join sub-queries when the user doesn't explicitly
    specify a boolean operator, such as ``AND`` or ``OR``. This lets you change
    the default operator from ``AND`` to ``OR``.
    
    This will be the :class:`whoosh.qparser.AndGroup` or
    :class:`whoosh.qparser.OrGroup` class (*not* an instantiated object) unless
    you've written your own custom grouping syntax you want to use.
    
termclass
    The query class to use to wrap single terms.
    
    This must be a :class:`whoosh.query.Query` subclass (*not* an instantiated
    object) that accepts a fieldname string and term text unicode string in its
    ``__init__`` method. The default is :class:`whoosh.query.Term`.

    This is useful if you want to chnage the default term class to
    :class:`whoosh.query.Variations`, or if you've written a custom term class
    you want the parser to use instead of the ones shipped with Whoosh.

>>> from whoosh.qparser import QueryParser, GroupOr
>>> orparser = QueryParser("content", schema=myschema, group=GroupOr)


Configuring plugins
-------------------

TBD.





