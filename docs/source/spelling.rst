=====================================================
"Did you mean... ?" Correcting errors in user queries
=====================================================

.. note::
    In Whoosh 1.9 the old spelling system based on a separate N-gram index was
    replaced with this significantly more convenient and powerful
    implementation.


Overview
========

Whoosh can quickly suggest replacements for mis-typed words by returning
a list of words from the index (or a dictionary) that are close to the
mis-typed word::

    with ix.searcher() as s:
        corrector = s.corrector("text")
        for mistyped_word in mistyped_words:
            print corrector.suggest(mistyped_word, limit=3)

See the :meth:`whoosh.spelling.Corrector.suggest` method documentation
for information on the arguments.

Currently the suggestion engine is more like a "typo corrector" than a
real "spell checker" since it doesn't do the kind of sophisticated
phonetic matching or semantic/contextual analysis a good spell checker
might. However, it is still very useful.

There are two main strategies for correcting words:

*   Use the terms from an index field.

*   Use words from a word list file.


Pulling suggestions from an indexed field
=========================================

To enable spell checking on the contents of a field, use the
``spelling=True`` keyword argument on the field in the schema
definition::

    schema = Schema(text=TEXT(spelling=True))

(If you have an existing index you want to enable spelling for, you can
alter the schema in-place using the :func:`whoosh.writing.add_spelling`
function to create the missing word graph files.)

.. tip::
    You can get suggestions for fields without the ``spelling`` attribute, but
    calculating the suggestions will be slower.

You can then use the :meth:`whoosh.searching.Searcher.corrector` method
to get a corrector for a field::

    corrector = searcher.corrector("content")

The advantage of using the contents of an index field is that when you
are spell checking queries on that index, the suggestions are tailored
to the contents of the index. The disadvantage is that if the indexed
documents contain spelling errors, then the spelling suggestions will
also be erroneous.


Pulling suggestions from a word list
====================================

There are plenty of word lists available on the internet you can use to
populate the spelling dictionary.

(In the following examples, ``word_list`` can be a list of unicode
strings, or a file object with one word on each line.)

To create a :class:`whoosh.spelling.Corrector` object from a word list::

    from whoosh.spelling import GraphCorrector

    corrector = GraphCorrector.from_word_list(word_list)

Creating a corrector directly from a word list can be slow for large
word lists, so you can save a corrector's graph to a more efficient
on-disk form like this::

    graphfile = myindex.storage.create_file("words.graph")
    # to_file() automatically closes the file when it's finished
    corrector.to_file(graphfile)

To open the graph file again very quickly::

    graphfile = myindex.storage.open_file("words.graph")
    corrector = GraphCorrector.from_graph_file(graphfile)


Merging two or more correctors
==============================

You can combine suggestions from two sources (for example, the contents
of an index field and a word list) using a
:class:`whoosh.spelling.MultiCorrector`::

    c1 = searcher.corrector("content")
    c2 = GraphCorrector.from_graph_file(wordfile)
    corrector = MultiCorrector([c1, c2])


Correcting user queries
=======================

You can spell-check a user query using the
:meth:`whoosh.searching.Searcher.correct_query` method::

    from whoosh import qparser

    # Parse the user query string
    qp = qparser.QueryParser("content", myindex.schema)
    q = qp.parse(qstring)

    # Try correcting the query
    with myindex.searcher() as s:
        corrected = s.correct_query(q, qstring)
        if corrected.query != q:
            print("Did you mean:", corrected.string)

The ``correct_query`` method returns an object with the following
attributes:

``query``
    A corrected :class:`whoosh.query.Query` tree. You can test
    whether this is equal (``==``) to the original parsed query to
    check if the corrector actually changed anything.

``string``
    A corrected version of the user's query string.

``tokens``
    A list of corrected token objects representing the corrected
    terms. You can use this to reformat the user query (see below).


You can use a :class:`whoosh.highlight.Formatter` object to format the
corrected query string. For example, use the
:class:`~whoosh.highlight.HtmlFormatter` to format the corrected string
as HTML::

    from whoosh import highlight

    hf = highlight.HtmlFormatter()
    corrected = s.correct_query(q, qstring, formatter=hf)

See the documentation for
:meth:`whoosh.searching.Searcher.correct_query` for information on the
defaults and arguments.







