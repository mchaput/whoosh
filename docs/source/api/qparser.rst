==================
``qparser`` module
==================

.. automodule:: whoosh.qparser

Parser object
=============

.. autoclass:: QueryParser
    :members:

Pre-made configurations
-----------------------

The following functions return pre-configured QueryParser objects.

.. autofunction:: MultifieldParser

.. autofunction:: SimpleParser

.. autofunction:: DisMaxParser


Plug-ins
========

.. autoclass:: FieldsPlugin
.. autoclass:: CompoundsPlugin
.. autoclass:: NotPlugin
.. autoclass:: WildcardPlugin
.. autoclass:: PrefixPlugin
.. autoclass:: PhrasePlugin
.. autoclass:: RangePlugin
.. autoclass:: SingleQuotesPlugin
.. autoclass:: GroupPlugin
.. autoclass:: BoostPlugin
.. autoclass:: NotPlugin
.. autoclass:: PlusMinusPlugin
.. autoclass:: MultifieldPlugin
.. autoclass:: DisMaxPlugin
.. autoclass:: FieldAliasPlugin
.. autoclass:: CopyFieldPlugin
.. autoclass:: GtLtPlugin


Syntax objects
==============

Groups
------

.. autoclass:: SyntaxObject
.. autoclass:: Group
.. autoclass:: AndGroup
.. autoclass:: OrGroup
.. autoclass:: AndNotGroup
.. autoclass:: AndMaybeGroup
.. autoclass:: DisMaxGroup
.. autoclass:: NotGroup


Tokens
------

.. autoclass:: Token
.. autoclass:: Singleton
.. autoclass:: White
.. autoclass:: BasicSyntax
.. autoclass:: Word

    

