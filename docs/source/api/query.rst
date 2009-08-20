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

.. autoclass:: Phrase

.. autoclass:: And

.. autoclass:: Or

.. autoclass:: Not

.. autoclass:: Prefix

.. autoclass:: Wildcard

.. autoclass:: TermRange


Binary operations
=================

These binary operators are not generally created by the query parser in :mod:`whoosh.qparser`.
Unless you specifically need these operations, you should use the normal query classes instead.

.. autoclass:: Require

.. autoclass:: AndMaybe

.. autoclass:: AndNot


Exceptions
==========

.. autoexception:: QueryError
