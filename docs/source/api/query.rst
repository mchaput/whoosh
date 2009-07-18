query module
==============

.. automodule:: whoosh.query

Base classes
------------

The following abstract base classes are subclassed to create the the "real" query operations.

.. autoclass:: Query
    :members:

.. autoclass:: SimpleQuery

.. autoclass:: CompoundQuery

.. autoclass:: MultiTerm

.. autoclass:: ExpandingTerm

Query classes
-------------

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
-----------------

These binary operators are not created by the query parser in :mod:`whoosh.qparser`. They are included only for completeness and experimentation. Unless you specifically need these operations, you should use the normal query classes instead.

.. autoclass:: Require

.. autoclass:: AndMaybe

.. autoclass:: AndNot

Exceptions
----------

.. autoclass:: QueryError
