=============
How to search
=============

Once you've created an index and added documents to it, you can search for those
documents.

The Searcher object
===================

To get a :class:`whoosh.searching.Searcher` object, call ``searcher()`` on your
Index object::

    searcher = myindex.searcher()

You'll usually want to open the searcher using a ``with`` statement so the
searcher is automatically closed when you're done with it (searcher objects
represent a number of open files, so if you don't explicitly close them and the
system is slow to collect them, you can run out of file handles)::

    with ix.searcher() as searcher:
        ...

This is of course equivalent to::

    try:
        searcher = ix.searcher()
        ...
    finally:
        searcher.close()

The Searcher object is the main high-level interface for reading the index. It
has lots of useful methods for getting information about the index, such as
``lexicon(fieldname)``.

>>> list(searcher.lexicon("content"))
[u"document", u"index", u"whoosh]

However, the most important method on the Searcher object is
:meth:`~whoosh.searching.Searcher.search`, which takes a
:class:`whoosh.query.Query` object and returns a
:class:`~whoosh.searching.Results` object::

    from whoosh.qparser import QueryParser

    qp = QueryParser("content", schema=myindex.schema)
    q = qp.parse(u"hello world")

    with myindex.searcher() as s:
        results = s.search(q)

By default the results contains at most the first 10 matching documents. To get
more results, use the ``limit`` keyword::

    results = s.search(q, limit=20)

If you want all results, use ``limit=None``. However, setting the limit
whenever possible makes searches faster because Whoosh doesn't need to examine
and score every document.

Since displaying a page of results at a time is a common pattern, the
``search_page`` method lets you conveniently retrieve only the results on a
given page::

    results = s.search_page(q, 1)

The default page length is 10 hits. You can use the ``pagelen`` keyword argument
to set a different page length::

    results = s.search_page(q, 5, pagelen=20)


Results object
==============

The :class:`~whoosh.searching.Results` object acts like a list of the matched
documents. You can use it to access the stored fields of each hit document, to
display to the user.

>>> # Show the best hit's stored fields
>>> results[0]
{"title": u"Hello World in Python", "path": u"/a/b/c"}
>>> results[0:2]
[{"title": u"Hello World in Python", "path": u"/a/b/c"}, {"title": u"Foo", "path": u"/bar"}]

By default, ``Searcher.search(myquery)`` limits the number of hits to 20, So
the number of scored hits in the ``Results`` object may be less than the number
of matching documents in the index.

>>> # How many documents in the entire index would have matched?
>>> len(results)
27
>>> # How many scored and sorted documents in this Results object?
>>> # This will often be less than len() if the number of hits was limited
>>> # (the default).
>>> results.scored_length()
10

Calling ``len(Results)`` runs a fast (unscored) version of the query again to
figure out the total number of matching documents. This is usually very fast
but for large indexes it can cause a noticeable delay. If you want to avoid
this dalay on very large indexes, you can use the
:meth:`~whoosh.searching.Results.has_exact_length`,
:meth:`~whoosh.searching.Results.estimated_length`,
and :meth:`~whoosh.searching.Results.estimated_min_length` methods to estimate
the number of matching documents without calling ``len()``::

    found = results.scored_length()
    if results.has_exact_length():
        print("Scored", found, "of exactly", len(results), "documents")
    else:
        low = results.estimated_min_length()
        high = results.estimated_length()

        print("Scored", found, "of between", low, "and", "high", "documents")


Scoring and sorting
===================

Scoring
-------

Normally the list of result documents is sorted by *score*. The
:mod:`whoosh.scoring` module contains implementations of various scoring
algorithms. The default is :class:`~whoosh.scoring.BM25F`.

You can set the scoring object to use when you create the searcher using the
``weighting`` keyword argument::

    with myindex.searcher(weighting=whoosh.scoring.Cosine()) as s:
        ...

A scoring object is an object with a :meth:`~whoosh.scoring.Weighting.score`
method that takes information about the term to score and returns a score as a
floating point number.

Sorting
-------

See :doc:`facets`.


Highlighting snippets and More Like This
========================================

See :doc:`highlight` and :doc:`keywords` for information on these topics.


Convenience functions
=====================

The :meth:`~whoosh.searching.Searcher.document` and
:meth:`~whoosh.searching.Searcher.documents` methods on the Searcher object let
you retrieve the stored fields of documents matching terms you pass in keyword
arguments.

This is especially useful for fields such as dates/times, identifiers, paths,
and so on.

>>> list(searcher.documents(indexeddate=u"20051225"))
[{"title": u"Christmas presents"}, {"title": u"Turkey dinner report"}]
>>> print searcher.document(path=u"/a/b/c")
{"title": "Document C"}

These convenience functions have some limitations:

* The results are not scored.
* Multiple keywords are always AND-ed together.
* The entire value of each keyword argument is considered a single term; you
  can't search for multiple terms in the same field.


Combining Results objects
=========================

It is sometimes useful to use the results of another query to influence the
order of a :class:`whoosh.searching.Results` object.

For example, you might have a "best bet" field. This field contains hand-picked
keywords for documents. When the user searches for those keywords, you want
those documents to be placed at the top of the results list. You could try to do
this by boosting the "bestbet" field tremendously, but that can have
unpredictable effects on scoring. It's much easier to simply run the query twice
and combine the results::

    # Parse the user query
    userquery = queryparser.parse(querystring)

    # Get the terms searched for
    termset = set()
    userquery.existing_terms(termset)

    # Formulate a "best bet" query for the terms the user
    # searched for in the "content" field
    bbq = Or([Term("bestbet", text) for fieldname, text
              in termset if fieldname == "content"])

    # Find documents matching the searched for terms
    results = s.search(bbq, limit=5)

    # Find documents that match the original query
    allresults = s.search(userquery, limit=10)

    # Add the user query results on to the end of the "best bet"
    # results. If documents appear in both result sets, push them
    # to the top of the combined results.
    results.upgrade_and_extend(allresults)

The Results object supports the following methods:

``Results.extend(results)``
    Adds the documents in 'results' on to the end of the list of result
    documents.

``Results.filter(results)``
    Removes the documents in 'results' from the list of result documents.

``Results.upgrade(results)``
    Any result documents that also appear in 'results' are moved to the top of
    the list of result documents.

``Results.upgrade_and_extend(results)``
    Any result documents that also appear in 'results' are moved to the top of
    the list of result documents. Then any other documents in 'results' are
    added on to the list of result documents.






