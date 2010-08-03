================
``query`` module
================

.. automodule:: whoosh.query

See also :mod:`whoosh.qparser` which contains code for parsing user queries into query objects.

Base classes
============

The following abstract base classes are subclassed to create the the "real" query operations.

.. autoclass:: Query
    :members:

.. autoclass:: CompoundQuery

.. autoclass:: MultiTerm


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

.. autoclass:: TermRange

.. autoclass:: Every


Binary operations
=================

.. autoclass:: Require

.. autoclass:: AndMaybe

.. autoclass:: AndNot


Exceptions
==========

.. autoexception:: QueryError
