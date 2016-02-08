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
.. autoclass:: NgramWordAnalyzer
.. autoclass:: LanguageAnalyzer


Tokenizers
==========

.. autoclass:: IDTokenizer
.. autoclass:: RegexTokenizer
.. autoclass:: CharsetTokenizer
.. autoclass:: SpaceSeparatedTokenizer
.. autoclass:: CommaSeparatedTokenizer
.. autoclass:: NgramTokenizer
.. autoclass:: PathTokenizer


Filters
=======

.. autoclass:: PassFilter
.. autoclass:: LoggingFilter
.. autoclass:: MultiFilter
.. autoclass:: TeeFilter
.. autoclass:: ReverseTextFilter
.. autoclass:: LowercaseFilter
.. autoclass:: StripFilter
.. autoclass:: StopFilter
.. autoclass:: StemFilter
.. autoclass:: CharsetFilter
.. autoclass:: NgramFilter
.. autoclass:: IntraWordFilter
.. autoclass:: CompoundWordFilter
.. autoclass:: BiWordFilter
.. autoclass:: ShingleFilter
.. autoclass:: DelimitedAttributeFilter
.. autoclass:: DoubleMetaphoneFilter
.. autoclass:: SubstitutionFilter


Token classes and functions
===========================

.. autoclass:: Token
.. autofunction:: unstopped

