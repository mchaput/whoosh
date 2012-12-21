========================================
Stemming, variations, and accent folding
========================================

The problem
===========

The indexed text will often contain words in different form than the one
the user searches for. For example, if the user searches for ``render``, we
would like the search to match not only documents that contain the ``render``,
but also ``renders``, ``rendering``, ``rendered``, etc.

A related problem is one of accents. Names and loan words may contain accents in
the original text but not in the user's query, or vice versa. For example, we
want the user to be able to search for ``cafe`` and find documents containing
``café``.

The default analyzer for the :class:`whoosh.fields.TEXT` field does not do
stemming or accent folding.


Stemming
========

Stemming is a heuristic process of removing suffixes (and sometimes prefixes)
from words to arrive (hopefully, most of the time) at the base word. Whoosh
includes several stemming algorithms such as Porter and Porter2, Paice Husk,
and Lovins.

::

    >>> from whoosh.lang.porter import stem
    >>> stem("rendering")
    'render'

The stemming filter applies the stemming function to the terms it indexes, and
to words in user queries. So in theory all variations of a root word ("render",
"rendered", "renders", "rendering", etc.) are reduced to a single term in the
index, saving space. And all possible variations users might use in a query
are reduced to the root, so stemming enhances "recall".

The :class:`whoosh.analysis.StemFilter` lets you add a stemming filter to an
analyzer chain.

::

    >>> rext = RegexTokenizer()
    >>> stream = rext(u"fundamentally willows")
    >>> stemmer = StemFilter()
    >>> [token.text for token in stemmer(stream)]
    [u"fundament", u"willow"]

The :func:`whoosh.analysis.StemmingAnalyzer` is a pre-packaged analyzer that
combines a tokenizer, lower-case filter, optional stop filter, and stem filter::

    from whoosh import fields
    from whoosh.analysis import StemmingAnalyzer

    stem_ana = StemmingAnalyzer()
    schema = fields.Schema(title=TEXT(analyzer=stem_ana, stored=True),
                           content=TEXT(analyzer=stem_ana))

Stemming has pros and cons.

* It allows the user to find documents without worrying about word forms.

* It reduces the size of the index, since it reduces the number of separate
  terms indexed by "collapsing" multiple word forms into a single base word.

* It's faster than using variations (see below)

* The stemming algorithm can sometimes incorrectly conflate words or change
  the meaning of a word by removing suffixes.

* The stemmed forms are often not proper words, so the terms in the field
  are not useful for things like creating a spelling dictionary.


Variations
==========

Whereas stemming encodes the words in the index in a base form, when you use
variations you instead index words "as is" and *at query time* expand words
in the user query using a heuristic algorithm to generate morphological
variations of the word.

::

    >>> from whoosh.lang.morph_en import variations
    >>> variations("rendered")
    set(['rendered', 'rendernesses', 'render', 'renderless', 'rendering',
    'renderness', 'renderes', 'renderer', 'renderements', 'rendereless',
    'renderenesses', 'rendere', 'renderment', 'renderest', 'renderement',
    'rendereful', 'renderers', 'renderful', 'renderings', 'renders', 'renderly',
    'renderely', 'rendereness', 'renderments'])

Many of the generated variations for a given word will not be valid words, but
it's fairly fast for Whoosh to check which variations are actually in the
index and only search for those.

The :class:`whoosh.query.Variations` query object lets you search for variations
of a word. Whereas the normal :class:`whoosh.query.Term` object only searches
for the given term, the ``Variations`` query acts like an ``Or`` query for the
variations of the given word in the index. For example, the query::

    query.Variations("content", "rendered")

...might act like this (depending on what words are in the index)::

    query.Or([query.Term("content", "render"), query.Term("content", "rendered"),
              query.Term("content", "renders"), query.Term("content", "rendering")])

To have the query parser use :class:`whoosh.query.Variations` instead of
:class:`whoosh.query.Term` for individual terms, use the ``termclass``
keyword argument to the parser initialization method::

    from whoosh import qparser, query

    qp = qparser.QueryParser("content", termclass=query.Variations)

Variations has pros and cons.

* It allows the user to find documents without worrying about word forms.

* The terms in the field are actual words, not stems, so you can use the
  field's contents for other purposes such as spell checking queries.

* It increases the size of the index relative to stemming, because different
  word forms are indexed separately.

* It acts like an ``Or`` search for all the variations, which is slower than
  searching for a single term.


Lemmatization
=============

Whereas stemming is a somewhat "brute force", mechanical attempt at reducing
words to their base form using simple rules, lemmatization usually refers to
more sophisticated methods of finding the base form ("lemma") of a word using
language models, often involving analysis of the surrounding context and
part-of-speech tagging.

Whoosh does not include any lemmatization functions, but if you have separate
lemmatizing code you could write a custom :class:`whoosh.analysis.Filter`
to integrate it into a Whoosh analyzer.


Character folding
=================

You can set up an analyzer to treat, for example, ``á``, ``a``, ``å``, and ``â``
as equivalent to improve recall. This is often very useful, allowing the user
to, for example, type ``cafe`` or ``resume`` and find documents containing
``café`` and ``resumé``.

Character folding is especially useful for unicode characters that may appear
in Asian language texts that should be treated as equivalent to their ASCII
equivalent, such as "half-width" characters.

Character folding is not always a panacea. See this article for caveats on where
accent folding can break down.

http://www.alistapart.com/articles/accent-folding-for-auto-complete/

Whoosh includes several mechanisms for adding character folding to an analyzer.

The :class:`whoosh.analysis.CharsetFilter` applies a character map to token
text. For example, it will filter the tokens ``u'café', u'resumé', ...`` to
``u'cafe', u'resume', ...``. This is usually the method you'll want to use
unless you need to use a charset to tokenize terms::

    from whoosh.analysis import CharsetFilter, StemmingAnalyzer
    from whoosh import fields
    from whoosh.support.charset import accent_map

    # For example, to add an accent-folding filter to a stemming analyzer:
    my_analyzer = StemmingAnalyzer() | CharsetFilter(accent_map)

    # To use this analyzer in your schema:
    my_schema = fields.Schema(content=fields.TEXT(analyzer=my_analyzer))

The :class:`whoosh.analysis.CharsetTokenizer` uses a Sphinx charset table to
both separate terms and perform character folding. This tokenizer is slower
than the :class:`whoosh.analysis.RegexTokenizer` because it loops over each
character in Python. If the language(s) you're indexing can be tokenized using
regular expressions, it will be much faster to use ``RegexTokenizer`` and
``CharsetFilter`` in combination instead of using ``CharsetTokenizer``.

The :mod:`whoosh.support.charset` module contains an accent folding map useful
for most Western languages, as well as a much more extensive Sphinx charset
table and a function to convert Sphinx charset tables into the character maps
required by ``CharsetTokenizer`` and ``CharsetFilter``::

    # To create a filter using an enourmous character map for most languages
    # generated from a Sphinx charset table
    from whoosh.analysis import CharsetFilter
    from whoosh.support.charset import default_charset, charset_table_to_dict
    charmap = charset_table_to_dict(default_charset)
    my_analyzer = StemmingAnalyzer() | CharsetFilter(charmap)

(The Sphinx charset table format is described at
http://www.sphinxsearch.com/docs/current.html#conf-charset-table )














