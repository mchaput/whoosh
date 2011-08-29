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

.. autoclass:: Plugin
    :members:

.. autoclass:: SingleQuotePlugin
.. autoclass:: PrefixPlugin
.. autoclass:: WildcardPlugin
.. autoclass:: RegexPlugin
.. autoclass:: BoostPlugin
.. autoclass:: GroupPlugin
.. autoclass:: EveryPlugin
.. autoclass:: FieldsPlugin
.. autoclass:: PhrasePlugin
.. autoclass:: RangePlugin
.. autoclass:: OperatorsPlugin
.. autoclass:: PlusMinusPlugin
.. autoclass:: GtLtPlugin
.. autoclass:: MultifieldPlugin
.. autoclass:: FieldAliasPlugin
.. autoclass:: CopyFieldPlugin


Syntax node objects
===================

Base nodes
----------

.. autoclass:: SyntaxNode
    :members:


Nodes
-----

.. autoclass:: FieldnameNode
.. autoclass:: TextNode
.. autoclass:: WordNode
.. autoclass:: RangeNode
.. autoclass:: MarkerNode


Group nodes
-----------

.. autoclass:: GroupNode
.. autoclass:: BinaryGroup
.. autoclass:: ErrorNode
.. autoclass:: AndGroup
.. autoclass:: OrGroup
.. autoclass:: AndNotGroup
.. autoclass:: AndMaybeGroup
.. autoclass:: DisMaxGroup
.. autoclass:: RequireGroup
.. autoclass:: NotGroup


Operators
---------

.. autoclass:: Operator
.. autoclass:: PrefixOperator
.. autoclass:: PostfixOperator
.. autoclass:: InfixOperator







