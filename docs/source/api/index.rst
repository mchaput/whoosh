================
``index`` module
================

.. automodule:: whoosh.index

Globals
=======

.. data:: _index_version

    The version number of the index format which this version of Whoosh
    writes.


Functions
=========

.. autofunction:: create_in
.. autofunction:: open_dir
.. autofunction:: exists_in
.. autofunction:: exists
.. autofunction:: version_in
.. autofunction:: version


Index class
===========

.. autoclass:: Index
    :members:


Exceptions
==========

.. autoexception:: EmptyIndexError
.. autoexception:: IndexVersionError
.. autoexception:: OutOfDateError
.. autoexception:: IndexError
