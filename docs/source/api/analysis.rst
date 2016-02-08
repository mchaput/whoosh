===================
``analysis`` module
===================

.. automodule:: whoosh.analysis

Analyzers
=========

.. autofunction:: IDAnalyzer
.. autofunction:: KeywordAnalyzer
.. autofunction:: RegexAnalyzer
.. autofunction:: SimpleAnalyzer
.. autofunction:: StandardAnalyzer
.. autofunction:: StemmingAnalyzer
.. autofunction:: FancyAnalyzer
.. autofunction:: NgramAnalyzer
.. autofunction:: NgramWordAnalyzer
.. autofunction:: LanguageAnalyzer


Tokenizers
==========

.. autoclass:: IDTokenizer
.. autoclass:: RegexTokenizer
.. autoclass:: CharsetTokenizer
.. autofunction:: SpaceSeparatedTokenizer
.. autofunction:: CommaSeparatedTokenizer
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

