==================
``writing`` module
==================

.. automodule:: whoosh.writing

Globals
=======

.. data:: DOCLENGTH_TYPE

    The data type ("H" or "i") used to store field lengths on disk.
    The default is "H", but if you are indexing very large documents and
    need to be able to store field lengths longer than 65535 tokens, you
    can change this to "i".

.. data:: DOCLENGTH_LIMIT

    The highest possible value representable by ``DOCLENGTH_TYPE``.
    For "H" this is ``2 ** 16 - 1``. Remember to set this if you change
    DOCLENGTH_TYPE.


Writer
======

.. autoclass:: IndexWriter
    :members:


Utility writers
===============

.. autoclass:: AsyncWriter
    :members:
    
.. autoclass:: BatchWriter
    :members:
    
    
Posting writer
==============

.. autoclass:: PostingWriter


Exceptions
==========

.. autoexception:: IndexingError


