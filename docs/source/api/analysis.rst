===================
``analysis`` module
===================

.. automodule:: whoosh.analysis

Analyzers
=========

.. autoclass:: Analyzer
.. autoclass:: IDAnalyzer
.. autoclass:: KeywordAnalyzer
.. autoclass:: RegexAnalyzer
.. autoclass:: SimpleAnalyzer
.. autoclass:: StemmingAnalyzer
.. autoclass:: StandardAnalyzer
.. autoclass:: FancyAnalyzer
.. autoclass:: NgramAnalyzer


Tokenizers
==========

.. autofunction:: IDTokenizer
.. autoclass:: RegexTokenizer
.. autoclass:: CharsetTokenizer
.. autoclass:: SpaceSeparatedTokenizer
.. autoclass:: CommaSeparatedTokenizer
.. autoclass:: NgramTokenizer


Filters
=======

.. autofunction:: PassFilter
.. autofunction:: LowercaseFilter
.. autofunction:: UnderscoreFilter
.. autoclass:: CharsetFilter
.. autoclass:: StopFilter
.. autoclass:: StemFilter
.. autofunction:: CamelFilter
.. autoclass:: NgramFilter


Token classes and functions
===========================

.. autoclass:: Token
.. autofunction:: unstopped

