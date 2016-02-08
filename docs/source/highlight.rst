================================================
How to create highlighted search result excerpts
================================================

Overview
========

The highlighting system works as a pipeline, with four component types.

* **Fragmenters** chop up the original text into __fragments__, based on the
  locations of matched terms in the text.

* **Scorers** assign a score to each fragment, allowing the system to rank the
  best fragments by whatever criterion.

* **Order functions** control in what order the top-scoring fragments are
  presented to the user. For example, you can show the fragments in the order
  they appear in the document (FIRST) or show higher-scoring fragments first
  (SCORE)

* **Formatters** turn the fragment objects into human-readable output, such as
  an HTML string.


Requirements
============

Highlighting requires that you have the text of the indexed document available.
You can keep the text in a stored field, or if the  original text is available
in a file, database column, etc, just reload it on the fly. Note that you might
need to process the text to remove e.g. HTML tags, wiki markup, etc.


How to
======

Get search results::

    results = mysearcher.search(myquery)
    for hit in results:
        print(hit["title"])

You can use the :meth:`~whoosh.searching.Hit.highlights` method on the
:class:`whoosh.searching.Hit` object to get highlighted snippets from the
document containing the search terms.

The first argument is the name of the field to highlight. If the field is
stored, this is the only argument you need to supply::

    results = mysearcher.search(myquery)
    for hit in results:
        print(hit["title"])
        # Assume "content" field is stored
        print(hit.highlights("content"))

If the field is not stored, you need to retrieve the text of the field some
other way. For example, reading it from the original file or a database. Then
you can supply the text to highlight with the ``text`` argument::

    results = mysearcher.search(myquery)
    for hit in results:
        print(hit["title"])

        # Assume the "path" stored field contains a path to the original file
        with open(hit["path"]) as fileobj:
            filecontents = fileobj.read()

        print(hit.highlights("content", text=filecontents))


The character limit
===================

By default, Whoosh only pulls fragments from the first 32K characters of the
text. This prevents very long texts from bogging down the highlighting process
too much, and is usually justified since important/summary information is
usually at the start of a document. However, if you find the highlights are
missing information (for example, very long encyclopedia articles where the
terms appear in a later section), you can increase the fragmenter's character
limit.

You can change the character limit on the results object like this::

    results = mysearcher.search(myquery)
    results.fragmenter.charlimit = 100000

To turn off the character limit::

    results.fragmenter.charlimit = None

If you instantiate a custom fragmenter, you can set the character limit on it
directly::

    sf = highlight.SentenceFragmenter(charlimit=100000)
    results.fragmenter = sf

See below for information on customizing the highlights.

If you increase or disable the character limit to highlight long documents, you
may need to use the tips in the "speeding up highlighting" section below to
make highlighting faster.


Customizing the highlights
==========================

Number of fragments
-------------------

You can use the ``top`` keyword argument to control the number of fragments
returned in each snippet::

    # Show a maximum of 5 fragments from the document
    print hit.highlights("content", top=5)


Fragment size
-------------

The default fragmenter has a ``maxchars`` attribute (default 200) controlling
the maximum length of a fragment, and a ``surround`` attribute (default 20)
controlling the maximum number of characters of context to add at the beginning
and end of a fragment::

    # Allow larger fragments
    results.fragmenter.maxchars = 300

    # Show more context before and after
    results.fragmenter.surround = 50


Fragmenter
----------

A fragmenter controls how to extract excerpts from the original text.

The ``highlight`` module has the following pre-made fragmenters:

:class:`whoosh.highlight.ContextFragmenter` (the default)
    This is a "smart" fragmenter that finds matched terms and then pulls
    in surround text to form fragments. This fragmenter only yields
    fragments that contain matched terms.

:class:`whoosh.highlight.SentenceFragmenter`
    Tries to break the text into fragments based on sentence punctuation
    (".", "!", and "?"). This object works by looking in the original
    text for a sentence end as the next character after each token's
    'endchar'. Can be fooled by e.g. source code, decimals, etc.

:class:`whoosh.highlight.WholeFragmenter`
    Returns the entire text as one "fragment". This can be useful if you
    are highlighting a short bit of text and don't need to fragment it.

The different fragmenters have different options. For example, the default
:class:`~whoosh.highlight.ContextFragmenter` lets you set the maximum
fragment size and the size of the context to add on either side::

    my_cf = highlight.ContextFragmenter(maxchars=100, surround=30)

See the :mod:`whoosh.highlight` docs for more information.

To use a different fragmenter::

    results.fragmenter = my_cf


Scorer
------

A scorer is a callable that takes a :class:`whoosh.highlight.Fragment` object and
returns a sortable value (where higher values represent better fragments).
The default scorer adds up the number of matched terms in the fragment, and
adds a "bonus" for the number of __different__ matched terms. The highlighting
system uses this score to select the best fragments to show to the user.

As an example of a custom scorer, to rank fragments by lowest standard
deviation of the positions of matched terms in the fragment::

    def StandardDeviationScorer(fragment):
        """Gives higher scores to fragments where the matched terms are close
        together.
        """

        # Since lower values are better in this case, we need to negate the
        # value
        return 0 - stddev([t.pos for t in fragment.matched])

To use a different scorer::

    results.scorer = StandardDeviationScorer


Order
-----

The order is a function that takes a fragment and returns a sortable value used
to sort the highest-scoring fragments before presenting them to the user (where
fragments with lower values appear before fragments with higher values).

The ``highlight`` module has the following order functions.

``FIRST`` (the default)
    Show fragments in the order they appear in the document.

``SCORE``
    Show highest scoring fragments first.

The ``highlight`` module also includes ``LONGER`` (longer fragments first) and
``SHORTER`` (shorter fragments first), but they probably aren't as generally
useful.

To use a different order::

    results.order = highlight.SCORE


Formatter
---------

A formatter contols how the highest scoring fragments are turned into a
formatted bit of text for display to the user. It can return anything
(e.g. plain text, HTML, a Genshi event stream, a SAX event generator,
or anything else useful to the calling system).

The ``highlight`` module contains the following pre-made formatters.

:class:`whoosh.highlight.HtmlFormatter`
    Outputs a string containing HTML tags (with a class attribute)
    around the matched terms.

:class:`whoosh.highlight.UppercaseFormatter`
    Converts the matched terms to UPPERCASE.

:class:`whoosh.highlight.GenshiFormatter`
    Outputs a Genshi event stream, with the matched terms wrapped in a
    configurable element.

The easiest way to create a custom formatter is to subclass
``highlight.Formatter`` and override the ``format_token`` method::

    class BracketFormatter(highlight.Formatter):
        """Puts square brackets around the matched terms.
        """

        def format_token(self, text, token, replace=False):
            # Use the get_text function to get the text corresponding to the
            # token
            tokentext = highlight.get_text(text, token)

            # Return the text as you want it to appear in the highlighted
            # string
            return "[%s]" % tokentext

To use a different formatter::

    brf = BracketFormatter()
    results.formatter = brf

If you need more control over the formatting (or want to output something other
than strings), you will need to override other methods. See the documentation
for the :class:`whoosh.highlight.Formatter` class.


Highlighter object
==================

Rather than setting attributes on the results object, you can create a
reusable :class:`whoosh.highlight.Highlighter` object. Keyword arguments let
you change the ``fragmenter``, ``scorer``, ``order``, and/or ``formatter``::

    hi = highlight.Highlighter(fragmenter=my_cf, scorer=sds)

You can then use the :meth:`whoosh.highlight.Highlighter.highlight_hit` method
to get highlights for a ``Hit`` object::

    for hit in results:
        print(hit["title"])
        print(hi.highlight_hit(hit))

(When you assign to a ``Results`` object's ``fragmenter``, ``scorer``, ``order``,
or ``formatter`` attributes, you're actually changing the values on the
results object's default ``Highlighter`` object.)


Speeding up highlighting
========================

Recording which terms matched in which documents during the search may make
highlighting faster, since it will skip documents it knows don't contain any
matching terms in the given field::

    # Record per-document term matches
    results = searcher.search(myquery, terms=True)


PinpointFragmenter
------------------

Usually the highlighting system uses the field's analyzer to re-tokenize the
document's text to find the matching terms in context. If you have long
documents and have increased/disabled the character limit, and/or if the field
has a very complex analyzer, re-tokenizing may be slow.

Instead of retokenizing, Whoosh can look up the character positions of the
matched terms in the index. Looking up the character positions is not
instantaneous, but is usually faster than analyzing large amounts of text.

To use :class:`whoosh.highlight.PinpointFragmenter` and avoid re-tokenizing the
document text, you must do all of the following:

Index the field with character information (this will require re-indexing an
existing index)::

    # Index the start and end chars of each term
    schema = fields.Schema(content=fields.TEXT(stored=True, chars=True))

Record per-document term matches in the results::

    # Record per-document term matches
    results = searcher.search(myquery, terms=True)

Set a :class:`whoosh.highlight.PinpointFragmenter` as the fragmenter::

    results.fragmenter = highlight.PinpointFragmenter()


PinpointFragmenter limitations
------------------------------

When the highlighting system does not re-tokenize the text, it doesn't know
where any other words are in the text except the matched terms it looked up in
the index. Therefore when the fragmenter adds surrounding context, it just adds
or a certain number of characters blindly, and so doesn't distinguish between
content and whitespace, or break on word boundaries, for example::

    >>> hit.highlights("content")
    're when the <b>fragmenter</b>\n       ad'

(This can be embarassing when the word fragments form dirty words!)

One way to avoid this is to not show any surrounding context, but then
fragments containing one matched term will contain ONLY that matched term::

    >>> hit.highlights("content")
    '<b>fragmenter</b>'

Alternatively, you can normalize whitespace in the text before passing it to
the highlighting system::

    >>> text = searcher.stored_
    >>> re.sub("[\t\r\n ]+", " ", text)
    >>> hit.highlights("content", text=text)

...and use the ``autotrim`` option of ``PinpointFragmenter`` to automatically
strip text before the first space and after the last space in the fragments::

    >>> results.fragmenter = highlight.PinpointFragmenter(autotrim=True)
    >>> hit.highlights("content")
    'when the <b>fragmenter</b>'


Using the low-level API
=======================

Usage
-----

The following function lets you retokenize and highlight a piece of text using
an analyzer::

    from whoosh.highlight import highlight

    excerpts = highlight(text, terms, analyzer, fragmenter, formatter, top=3,
                         scorer=BasicFragmentScorer, minscore=1, order=FIRST)

``text``
    The original text of the document.

``terms``
    A sequence or set containing the query words to match, e.g. ("render",
    "shader").

``analyzer``
    The analyzer to use to break the document text into tokens for matching
    against the query terms. This is usually the analyzer for the field the
    query terms are in.

``fragmenter``
    A :class:`whoosh.highlight.Fragmenter` object, see below.

``formatter``
    A :class:`whoosh.highlight.Formatter` object, see below.

``top``
    The number of fragments to include in the output.

``scorer``
    A :class:`whoosh.highlight.FragmentScorer` object. The only scorer currently
    included with Whoosh is :class:`~whoosh.highlight.BasicFragmentScorer`, the
    default.

``minscore``
    The minimum score a fragment must have to be considered for inclusion.

``order``
    An ordering function that determines the order of the "top" fragments in the
    output text.












