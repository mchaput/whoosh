================
``query`` module
================

.. automodule:: whoosh.query

See also :mod:`whoosh.qparser` which contains code for parsing user queries
into query objects.

Base classes
============

The following abstract base classes are subclassed to create the "real"
query operations.

.. autoclass:: Query
    :members:

.. autoclass:: CompoundQuery
.. autoclass:: MultiTerm
.. autoclass:: ExpandingTerm
.. autoclass:: WrappingQuery


Query classes
=============

.. autoclass:: Term
.. autoclass:: Variations
.. autoclass:: FuzzyTerm
.. autoclass:: Phrase
.. autoclass:: And
.. autoclass:: Or
.. autoclass:: DisjunctionMax
.. autoclass:: Not
.. autoclass:: Prefix
.. autoclass:: Wildcard
.. autoclass:: Regex
.. autoclass:: TermRange
.. autoclass:: NumericRange
.. autoclass:: DateRange
.. autoclass:: Every
.. autoclass:: NullQuery


Binary queries
==============

.. autoclass:: Require
.. autoclass:: AndMaybe
.. autoclass:: AndNot
.. autoclass:: Otherwise


Span queries
============

.. autoclass:: Span
    :members:

.. autoclass:: SpanQuery
.. autoclass:: SpanFirst
.. autoclass:: SpanNear
.. autoclass:: SpanNear2
.. autoclass:: SpanNot
.. autoclass:: SpanOr
.. autoclass:: SpanContains
.. autoclass:: SpanBefore
.. autoclass:: SpanCondition


Special queries
===============

.. autoclass:: NestedParent
.. autoclass:: NestedChildren
.. autoclass:: ConstantScoreQuery


Exceptions
==========

.. autoexception:: QueryError
