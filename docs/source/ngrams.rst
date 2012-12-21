==============================
Indexing and searching N-grams
==============================

Overview
========

N-gram indexing is a powerful method for getting fast, "search as you type"
functionality like iTunes. It is also useful for quick and effective indexing
of languages such as Chinese and Japanese without word breaks.

N-grams refers to groups of N characters... bigrams are groups of two
characters, trigrams are groups of three characters, and so on.

Whoosh includes two methods for analyzing N-gram fields: an N-gram tokenizer,
and a filter that breaks tokens into N-grams.

:class:`whoosh.analysis.NgramTokenizer` tokenizes the entire field into N-grams.
This is more useful for Chinese/Japanese/Korean languages, where it's useful
to index bigrams of characters rather than individual characters. Using this
tokenizer with roman languages leads to spaces in the tokens.

::

    >>> ngt = NgramTokenizer(minsize=2, maxsize=4)
    >>> [token.text for token in ngt(u"hi there")]
    [u'hi', u'hi ', u'hi t',u'i ', u'i t', u'i th', u' t', u' th', u' the', u'th',
    u'the', u'ther', u'he', u'her', u'here', u'er', u'ere', u're']

:class:`whoosh.analysis.NgramFilter` breaks individual tokens into N-grams as
part of an analysis pipeline. This is more useful for languages with word
separation.

::

    >>> my_analyzer = StandardAnalyzer() | NgramFilter(minsize=2, maxsize=4)
    >>> [token.text for token in my_analyzer(u"rendering shaders")]
    [u'ren', u'rend', u'end', u'ende', u'nde', u'nder', u'der', u'deri', u'eri',
    u'erin', u'rin', u'ring', u'ing', u'sha', u'shad', u'had', u'hade', u'ade',
    u'ader', u'der', u'ders', u'ers']

Whoosh includes two pre-configured field types for N-grams:
:class:`whoosh.fields.NGRAM` and :class:`whoosh.fields.NGRAMWORDS`. The only
difference is that ``NGRAM`` runs all text through the N-gram filter, including
whitespace and punctuation, while ``NGRAMWORDS`` extracts words from the text
using a tokenizer, then runs each word through the N-gram filter.

TBD.



