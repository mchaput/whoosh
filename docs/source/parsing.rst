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

Whoosh includes a few pre-made parsers for user queries in the
:mod:`whoosh.qparser` module. The default parser is based on `pyparsing
<http://pyparsing.wikispaces.com/>` and implements a query language similar to
the one shipped with Lucene. The parser is quite powerful and how it builds
query trees is fairly customizable.


Using the default parser
========================

To create a :class:`whoosh.qparser.QueryParser` object, pass it the name of the
*default field* to search and the schema of the index you'll be searching.

    from whoosh.qparser import QueryParser

    parser = QueryParser("content", schema=myindex.schema)
    
.. tip::

    You can instantiate a QueryParser object without specifying a schema,
    however the parser will not process the text of the user query (see
    :ref:`querying and indexing <index-query>` below). This is really only
    useful for debugging, when you want to see how QueryParser will build a
    query, but don't want to make up a schema just for testing.

Once you have a QueryParser object, you can call ``parse()`` on it to parse a
query string into a query object::

    >>> parser.parse(u"alpha OR beta gamma")
    Or([Term("content", u"alpha"), Term("content", "beta")])

See the :doc:`query language reference <querylang>` for the features and syntax
of the default parser's query language.


Letting the user search multiple fields
=======================================

The QueryParser object takes terms without explicit fields and assigns them to
the default field you specified when you created the object, so for example if
you created the object with::

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
    
When this MultifieldParser instance parses ``three blind mice``, it treats it as:

.. code-block:: none

    (title:three OR content:three) (title:blind OR content:blind) (title:mice OR content:mice)


.. _index-query:

The relationship between indexing and querying
==============================================

TBD.


Customizing the parser
==============================

QueryParser arguments
---------------------

QueryParser supports two extra keyword arguments:

conjunction
    The query class to use to join sub-queries when the user doesn't explicitly
    specify a boolean operator, such as ``AND`` or ``OR``.
    
    This must be a :class:`whoosh.query.Query` subclass (*not* an instantiated
    object) that accepts a list of subqueries in its ``__init__`` method. The
    default is :class:`whoosh.query.And`.
    
    This is useful if you want to change the default operator to ``OR``, or if
    you've written a custom operator you want the parser to use instead of the
    ones shipped with Whoosh.

termclass
    The query class to use to wrap single terms.
    
    This must be a :class:`whoosh.query.Query` subclass (*not* an instantiated
    object) that accepts a fieldname string and term text unicode string in its
    ``__init__`` method. The default is :class:`whoosh.query.Term`.

    This is useful if you want to chnage the default term class to
    :class:`whoosh.query.Variations`, or if you've written a custom term class
    you want the parser to use instead of the ones shipped with Whoosh.

>>> orparser = QueryParser("content", schema=myschema, conjunction=query.Or)


Writing your own parser
-----------------------

To implement a different query syntax, or for complete control over query
parsing, you can write your own parser.

A parser is simply a class or function that takes input from the user and
generates :class:`whoosh.query.Query` objects from it. For example, you could
write a function that parses queries specified in XML:

.. code-block:: xml

    <and>
        <term field="content">first</term>
        <term field="content">second</term>
        <not>
            <term field="date">20070506</term>
        </not>
    </and>




