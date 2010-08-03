===================
``postings`` module
===================

.. automodule:: whoosh.matching

Matchers
========

.. autoclass:: Matcher
    :members:
    
.. autoclass:: NullMatcher
.. autoclass:: ListMatcher
.. autoclass:: WrappingMatcher
.. autoclass:: MultiMatcher
.. autoclass:: ExcludeMatcher
.. autoclass:: BiMatcher
.. autoclass:: AdditiveBiMatcher
.. autoclass:: UnionMatcher
.. autoclass:: DisjuctionMaxMatcher
.. autoclass:: IntersectionMatcher
.. autoclass:: AndNotMatcher
.. autoclass:: InverseMatcher
.. autoclass:: RequireMatcher
.. autoclass:: AndMaybeMatcher
.. autoclass:: BasePhraseMatcher
.. autoclass:: PostingPhraseMatcher
.. autoclass:: VectorPhraseMatcher


Utility functions
=================

.. autofunction:: make_tree


Exceptions
==========

.. autoexception:: ReadTooFar
.. autoexception:: NoQualityAvailable
