==============
Whoosh recipes
==============

General
=======

Get the stored fields for a document from the document number
-------------------------------------------------------------
::

    stored_fields = searcher.stored_fields(docnum)


Analysis
========

Eliminate words shorter/longer than N
-------------------------------------

Use a :class:`~whoosh.analysis.StopFilter` and the ``minsize`` and ``maxsize``
keyword arguments. If you just want to filter based on size and not common
words, set the ``stoplist`` to ``None``::

    sf = analysis.StopFilter(stoplist=None, minsize=2, maxsize=40)


Allow optional case-sensitive searches
--------------------------------------

A quick and easy way to do this is to index both the original and lowercased
versions of each word. If the user searches for an all-lowercase word, it acts
as a case-insensitive search, but if they search for a word with any uppercase
characters, it acts as a case-sensitive search::

    class CaseSensitivizer(analysis.Filter):
        def __call__(self, tokens):
            for t in tokens:
                yield t
                if t.mode == "index":
                   low = t.text.lower()
                   if low != t.text:
                       t.text = low
                       yield t

    ana = analysis.RegexTokenizer() | CaseSensitivizer()
    [t.text for t in ana("The new SuperTurbo 5000", mode="index")]
    # ["The", "the", "new", "SuperTurbo", "superturbo", "5000"]


Searching
=========

Find every document
-------------------
::

    myquery = query.Every()


iTunes-style search-as-you-type
-------------------------------

Use the :class:`whoosh.analysis.NgramWordAnalyzer` as the analyzer for the
field you want to search as the user types. You can save space in the index by
turning off positions in the field using ``phrase=False``, since phrase
searching on N-gram fields usually doesn't make much sense::

    # For example, to search the "title" field as the user types
    analyzer = analysis.NgramWordAnalyzer()
    title_field = fields.TEXT(analyzer=analyzer, phrase=False)
    schema = fields.Schema(title=title_field)

See the documentation for the :class:`~whoosh.analysis.NgramWordAnalyzer` class
for information on the available options.


Shortcuts
=========

Look up documents by a field value
----------------------------------
::

    # Single document (unique field value)
    stored_fields = searcher.document(id="bacon")

    # Multiple documents
    for stored_fields in searcher.documents(tag="cake"):
        ...


Sorting and scoring
===================

See :doc:`facets`.


Score results based on the position of the matched term
-------------------------------------------------------

The following scoring function uses the position of the first occurance of a
term in each document to calculate the score, so documents with the given term
earlier in the document will score higher::

    from whoosh import scoring

    def pos_score_fn(searcher, fieldname, text, matcher):
        poses = matcher.value_as("positions")
        return 1.0 / (poses[0] + 1)

    pos_weighting = scoring.FunctionWeighting(pos_score_fn)
    with myindex.searcher(weighting=pos_weighting) as s:
        ...


Results
=======

How many hits were there?
-------------------------

The number of *scored* hits::

    found = results.scored_length()

Depending on the arguments to the search, the exact total number of hits may be
known::

    if results.has_exact_length():
        print("Scored", found, "of exactly", len(results), "documents")

Usually, however, the exact number of documents that match the query is not
known, because the searcher can skip over blocks of documents it knows won't
show up in the "top N" list. If you call ``len(results)`` on a query where the
exact length is unknown, Whoosh will run an unscored version of the original
query to get the exact number. This is faster than the scored search, but may
still be noticeably slow on very large indexes or complex queries.

As an alternative, you might display the *estimated* total hits::

    found = results.scored_length()
    if results.has_exact_length():
        print("Scored", found, "of exactly", len(results), "documents")
    else:
        low = results.estimated_min_length()
        high = results.estimated_length()

        print("Scored", found, "of between", low, "and", high, "documents")


Which terms matched in each hit?
--------------------------------
::

    # Use terms=True to record term matches for each hit
    results = searcher.search(myquery, terms=True)

    for hit in results:
        # Which terms matched in this hit?
        print("Matched:", hit.matched_terms())

        # Which terms from the query didn't match in this hit?
        print("Didn't match:", myquery.all_terms() - hit.matched_terms())


Global information
==================

How many documents are in the index?
------------------------------------
::

    # Including documents that are deleted but not yet optimized away
    numdocs = searcher.doc_count_all()

    # Not including deleted documents
    numdocs = searcher.doc_count()


What fields are in the index?
-----------------------------
::

    return myindex.schema.names()


Is term X in the index?
-----------------------
::

    return ("content", "wobble") in searcher


How many times does term X occur in the index?
----------------------------------------------
::

    # Number of times content:wobble appears in all documents
    freq = searcher.frequency("content", "wobble")

    # Number of documents containing content:wobble
    docfreq = searcher.doc_frequency("content", "wobble")


Is term X in document Y?
------------------------
::

    # Check if the "content" field of document 500 contains the term "wobble"

    # Without term vectors, skipping through list...
    postings = searcher.postings("content", "wobble")
    postings.skip_to(500)
    return postings.id() == 500

    # ...or the slower but easier way
    docset = set(searcher.postings("content", "wobble").all_ids())
    return 500 in docset

    # If field has term vectors, skipping through list...
    vector = searcher.vector(500, "content")
    vector.skip_to("wobble")
    return vector.id() == "wobble"

    # ...or the slower but easier way
    wordset = set(searcher.vector(500, "content").all_ids())
    return "wobble" in wordset

