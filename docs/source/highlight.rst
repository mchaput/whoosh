================================================
How to create highlighted search result excerpts
================================================

Overview
========

The highlight module requires that you have the text of the indexed 
document available. You can keep the text in a stored field, or if the 
original text is available in a file, database column, etc, just reload 
it on the fly. Note that you might need to process the text to remove 
e.g. HTML tags, wiki markup, etc.

The highlight module works on a pipeline:

#. Run the text through an analyzer to turn it into a token stream [#f1]_.

#. Break the token stream into "fragments" (there are several different styles of fragmentation  available).

#. Score each fragment based on how many matched query terms the fragment contains.

#. Format the highest scoring fragments for display.

.. rubric:: Footnotes

.. [#f1]
    Some search systems, such as Lucene, can use term vectors to highlight text
    without retokenizing it. In my tests I found that using a Position/Character
    term vector didn't give any speed improvement in Whoosh over retokenizing
    the text. This probably needs further investigation.


The easy way
============

The :class:`whoosh.searching.Hit` objects you get from a
:class:`whoosh.searching.Results` object have a
:meth:`~whoosh.searching.Hit.highlights` method which returns highlighted
snippets from the document. The only required argument is the name of the field
to highlight::

    results = searcher.search(myquery)
    for hit in results:
        print hit["title"]
        print hit.highlights("content")
        
This assumes the ``"content"`` field is marked ``stored`` in the schema so it is
available in the stored fields for the document. If you don't store the contents
of the field you want to highlight in the index, but have access to it another
way (for example, reading from a file or a database), you can supply the text as
an optional second argument::

    results = searcher.search(myquery)
    for hit in results:
        print hit["title"]
        
        # Instead of storing the contents in the index, I stored a file path
        # so I could retrieve the contents from the original file
        path = hit["path"]
        text = open(path).read()
        print hit.highlight("content", text)

You can customize the creation of the snippets by setting the ``fragmenter``
and/or ``formatter`` attributes on the :class:`Results` object or using the
``fragmenter`` and/or ``formatter`` keyword arguments to
:meth:`~whoosh.searching.Hit.highlight`. Set the ``Results.fragmenter``
attribute to a :class:`whoosh.highlight.Fragmenter` object (see "Fragmenters"
below) and/or the ``Results.formatter`` attribute to a
:class:`whoosh.highlight.Formatter` object (see "Formatters" below).

For example, to return larger fragments and highlight them by converting to
upper-case instead of with HTML tags::

    from whoosh import highlight

    r = searcher.search(myquery)
    r.fragmenter = highlight.ContextFragmenter(surround=40)
    r.formatter = highlight.UppercaseFormatter()
    
    for hit in r:
        print hit["title"]
        print hit.highlights("content")

Using the keyword argument(s) is useful when you want to alternate highlighting
styles in the same results::

    r = searcher.search(myquery)
    
    # Use this fragmenter for titles, just returns the entire field as a single
    # fragment
    tf = highlight.WholeFragmenter()
    # Use this fragmenter for content
    cf = highlight.SentenceFragmenter()
    
    # Use the same formatter for both
    r.formatter = highlight.HtmlFormatter(tagname="span")
    
    for hit in r:
        # Print the title with matched terms highlighted
        print hit.highlights("title", fragmenter=tf)
        # Print the content snippet
        print hit.highlights("content", fragmenter=cf)

You can use the ``top`` keyword argument to control the number of fragments
returned in each snippet::

    # Show a maximum of 5 fragments from the document
    print hit.highlight("content", top=5)

You can control the order of the fragments in the snippet with the ``order``
keyword argument. The value of the argument should be a sorting function for
fragments. The :mod:`whoosh.highlight` module contains several sorting functions
such as :func:`whoosh.highlight.SCORE`, :func:`whoosh.highlight.FIRST`,
:func:`whoosh.highlight.LONGER`, :func:`whoosh.highlight.SHORTER`. The default
is ``highlight.FIRST``, which is usually best.


Using the low-level API
=======================

Usage
-----

The high-level interface is the highlight function::

    excerpts = highlight(text, terms, analyzer, fragmenter, formatter, top=3,
                         scorer=BasicFragmentScorer, minscore=1, order=FIRST)

text
    The original text of the document.

terms
    A sequence or set containing the query words to match, e.g. ("render",
    "shader").

analyzer
    The analyzer to use to break the document text into tokens for matching
    against the query terms. This is usually the analyzer for the field the
    query terms are in.

fragmenter
    A :class:`whoosh.highlight.Fragmenter` object, see below.

formatter
    A :class:`whoosh.highlight.Formatter` object, see below.

top
    The number of fragments to include in the output.

scorer
    A :class:`whoosh.highlight.FragmentScorer` object. The only scorer currently
    included with Whoosh is :class:`~whoosh.highlight.BasicFragmentScorer`, the
    default.

minscore
    The minimum score a fragment must have to be considered for inclusion.

order
    An ordering function that determines the order of the "top" fragments in the
    output text. This will usually be either SCORE (highest scoring fragments
    first) or FIRST (highest scoring fragments in their original order). (Whoosh
    also includes LONGER (longer fragments first) and SHORTER (shorter fragments
    first) as examples of scoring functions, but they probably aren't as
    generally useful.)


Example
-------

.. code-block:: python

    # Set up the index
    # ----------------

    st = RamStorage()
    schema = fields.Schema(id=fields.ID(stored=True),
                          title=fields.TEXT(stored=True))
    ix = st.create_index(schema)

    w = ix.writer()
    w.add_document(id=u"1", title=u"The man who wasn't there")
    w.add_document(id=u"2", title=u"The dog who barked at midnight")
    w.add_document(id=u"3", title=u"The invisible man")
    w.add_document(id=u"4", title=u"The girl with the dragon tattoo")
    w.add_document(id=u"5", title=u"The woman who disappeared")
    w.commit()

    # Perform a search
    # ----------------

    s = ix.searcher()

    # Parse the user query
    parser = qparser.QueryParser("title", schema=ix.schema)
    q = parser.parse(u"man")

    # Extract the terms the user used in the field we're interested in
    # THIS IS HOW YOU GET THE TERMS ARGUMENT TO highlight()
    terms = [text for fieldname, text in q.all_terms()
            if fieldname == "title"]

    # Get the search results
    r = s.search(q)
    assert len(r) == 2

    # Use the same analyzer as the field uses. To be sure, you can
    # do schema[fieldname].format.analyzer. Be careful not to do this
    # on non-text field types such as DATETIME.
    analyzer = schema["title"].format.analyzer

    # Since we want to highlight the full title, not extract fragments,
    # we'll use WholeFragmenter. See the docs for the highlight module
    # for which fragmenters are available.
    fragmenter = highlight.WholeFragmenter()

    # This object controls what the highlighted output looks like.
    # See the docs for its arguments.
    formatter = highlight.HtmlFormatter()

    for d in r:
       # The text argument to highlight is the stored text of the title
       text = d["title"]

       print highlight.highlight(text, terms, analyzer,
                                 fragmenter, formatter)


Fragmenters
===========

A fragmenter controls the policy of how to extract excerpts from the 
original text. It is a callable that takes the original text, the set of 
terms to match, and the token stream, and returns a sequence of Fragment 
objects.

The available fragmenters are:

WholeFragmenter
    Returns the entire text as one "fragment". This can be useful if you
    are highlighting a short bit of text and don't need to fragment it.

SimpleFragmenter
    Or maybe "DumbFragmenter", this just breaks the token stream into
    equal sized chunks.

SentenceFragmenter
    Tries to break the text into fragments based on sentence punctuation
    (".", "!", and "?"). This object works by looking in the original
    text for a sentence end as the next character after each token's
    'endchar'. Can be fooled by e.g. source code, decimals, etc.

ContextFragmenter
    This is a "smart" fragmenter that finds matched terms and then pulls
    in surround text to form fragments. This fragmenter only yields
    fragments that contain matched terms.

See the :mod:`whoosh.highlight` docs for more information.


Formatters
==========

A formatter contols how the highest scoring fragments are turned into a 
formatted bit of text for display to the user. It can return anything 
(e.g. plain text, HTML, a Genshi event stream, a SAX event generator, 
anything useful to the calling system).

Whoosh currently includes only two formatters, because I wrote this 
module for myself and that's all I needed at the time. Unless you happen 
to be using Genshi also, you'll probably need to implement your own 
formatter. I'll try to add more useful formatters in the future.

UppercaseFormatter
    Converts the matched terms to UPPERCASE.

HtmlFormatter
	Outputs a string containing HTML tags (with a class attribute)
	around the the matched terms.

GenshiFormatter
    Outputs a Genshi event stream, with the matched terms wrapped in a
    configurable element.

See the :mod:`whoosh.highlight` docs for more information.


Writing your own formatter
==========================

A Formatter subclass needs a __call__ method. It is called with the following
arguments::

    formatter(text, fragments)

text
    The original text.

fragments
    An iterable of Fragment objects representing the top scoring
    fragments.

The Fragment object is a simple object that has attributes containing 
basic information about the fragment:

Fragment.startchar
    The index of the first character of the fragment.

Fragment.endchar
    The index of the last character of the fragment.

Fragment.matches
    An ordered list of analysis.Token objects representing the matched
    terms within the fragment.

Fragments.matched_terms
    For convenience: A frozenset of the text of the matched terms within
    the fragment -- i.e. frozenset(t.text for t in self.matches).

The basic work you need to do in the formatter is:

* Take the text of the original document, and pull out the bit between
    Fragment.startchar and Fragment.endchar

* For each Token object in Fragment.matches, highlight the bits of the
   excerpt between Token.startchar and Token.endchar. (Remember that the
   character indices refer to the original text, so you need to adjust
   them for the excerpt.)

The tricky part is that if you're adding text (e.g. inserting HTML tags 
into the output), you have to be careful about keeping the character 
indices straight.



