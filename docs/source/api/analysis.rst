===================
``analysis`` module
===================

.. automodule:: whoosh.analysis

Analyzers
=========

.. autoclass:: IDAnalyzer
.. autoclass:: KeywordAnalyzer
.. autoclass:: RegexAnalyzer
.. autoclass:: SimpleAnalyzer
.. autoclass:: StandardAnalyzer
.. autoclass:: StemmingAnalyzer
.. autoclass:: FancyAnalyzer
.. autoclass:: NgramAnalyzer


Tokenizers
==========

.. autoclass:: IDTokenizer
.. autoclass:: RegexTokenizer
.. autoclass:: CharsetTokenizer
.. autoclass:: SpaceSeparatedTokenizer
.. autoclass:: CommaSeparatedTokenizer
.. autoclass:: NgramTokenizer


Filters
=======

.. autoclass:: PassFilter
.. autoclass:: LoggingFilter
.. autoclass:: MultiFilter
.. autoclass:: LowercaseFilter
.. autoclass:: StripFilter
.. autoclass:: StopFilter
.. autoclass:: StemFilter
.. autoclass:: CharsetFilter
.. autoclass:: NgramFilter
.. autoclass:: IntraWordFilter
.. autoclass:: BitWordFilter
.. autoclass:: BoostTextFilter

Token classes and functions
===========================

.. autoclass:: Token
.. autofunction:: unstopped

