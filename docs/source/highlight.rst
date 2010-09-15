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


Usage
=====

The high-level interface is the highlight function::

    excerpts = highlight(text, terms, analyzer,
                         fragmenter, formatter, top=3,
                         scorer=BasicFragmentScorer, minscore=1,
                         order=FIRST)

text
    The original text of the document.

terms
    An iterable containing the query words to match, e.g.
    ("render", "shader").

analyzer
    The analyzer to use to break the document text into tokens for
    matching against the query terms. This is usually the analyzer
    for the field the query terms are in.

fragmenter
    A fragmeter callable, see below.

formatter
    A formatter callable, see below.

top
    The number of fragments to include in the output.

scorer
    A scorer callable. The only scorer currently included with Whoosh
    is BasicFragmentScorer, the default.

minscore
    The minimum score a fragment must have to be considered for
    inclusion.

order
    An ordering function that determines the order of the "top"
    fragments in the output text. This will usually be either
    SCORE (highest scoring fragments first) or FIRST (highest
    scoring fragments in their original order). (Whoosh also
    includes LONGER (longer fragments first) and SHORTER (shorter
    fragments first) as examples of scoring functions, but they
    probably aren't as generally useful.)

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
    # we'll use NullFragmenter. See the docs for the highlight module
    # for which fragmenters are available.
    fragmenter = highlight.NullFragmenter

    # This object controls what the highlighted output looks like.
    # See the docs for its arguments.
    formatter = highlight.HtmlFormatter()

    for d in r:
       # The text argument to highlight is the stored text of the title
       text = d["title"]

       print highlight.highlight(text, terms, analyzer,
                                 fragmenter, formatter)


How it works
============

Fragmenters
-----------

A fragmenter controls the policy of how to extract excerpts from the 
original text. It is a callable that takes the original text, the set of 
terms to match, and the token stream, and returns a sequence of Fragment 
objects.

The available fragmenters are:

NullFragmenter
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

(See the docstrings for how to instantiate these)


Formatters
----------

A formatter contols how the highest scoring fragments are turned into a 
formatted bit of text for display to the user. It can return anything 
(e.g. plain text, HTML, a Genshi event stream, a SAX event generater, 
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

(See the docstrings for how to instantiate these)


Writing your own formatter
--------------------------

A formatter must be a callable (a function or an object with a __call__ 
method). It is called with the following arguments::

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
