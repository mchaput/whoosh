=============
How to search
=============

Once you've created an index and added documents to it, you can search for those
documents.

The ``Searcher`` object
=======================

To get a :class:`whoosh.searching.Searcher` object, call ``searcher()`` on your
``Index`` object::

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

The ``Searcher`` object is the main high-level interface for reading the index. It
has lots of useful methods for getting information about the index, such as
``lexicon(fieldname)``.

::

    >>> list(searcher.lexicon("content"))
    [u"document", u"index", u"whoosh"]

However, the most important method on the ``Searcher`` object is
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

If you want all results, use ``limit=None``. However, setting the limit whenever
possible makes searches faster because Whoosh doesn't need to examine and score
every document.

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

::

    >>> # Show the best hit's stored fields
    >>> results[0]
    {"title": u"Hello World in Python", "path": u"/a/b/c"}
    >>> results[0:2]
    [{"title": u"Hello World in Python", "path": u"/a/b/c"},
    {"title": u"Foo", "path": u"/bar"}]

By default, ``Searcher.search(myquery)`` limits the number of hits to 20, So the
number of scored hits in the ``Results`` object may be less than the number of
matching documents in the index.

::

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
this delay on very large indexes, you can use the
:meth:`~whoosh.searching.Results.has_exact_length`,
:meth:`~whoosh.searching.Results.estimated_length`, and
:meth:`~whoosh.searching.Results.estimated_min_length` methods to estimate the
number of matching documents without calling ``len()``::

    found = results.scored_length()
    if results.has_exact_length():
        print("Scored", found, "of exactly", len(results), "documents")
    else:
        low = results.estimated_min_length()
        high = results.estimated_length()

        print("Scored", found, "of between", low, "and", high, "documents")


Scoring and sorting
===================

Scoring
-------

Normally the list of result documents is sorted by *score*. The
:mod:`whoosh.scoring` module contains implementations of various scoring
algorithms. The default is :class:`~whoosh.scoring.BM25F`.

You can set the scoring object to use when you create the searcher using the
``weighting`` keyword argument::

    from whoosh import scoring

    with myindex.searcher(weighting=scoring.TF_IDF()) as s:
        ...

A weighting model is a :class:`~whoosh.scoring.WeightingModel` subclass with a
``scorer()`` method that produces a "scorer" instance. This instance has a
method that takes the current matcher and returns a floating point score.

Sorting
-------

See :doc:`facets`.


Highlighting snippets and More Like This
========================================

See :doc:`highlight` and :doc:`keywords` for information on these topics.


Filtering results
=================

You can use the ``filter`` keyword argument to ``search()`` to specify a set of
documents to permit in the results. The argument can be a
:class:`whoosh.query.Query` object, a :class:`whoosh.searching.Results` object,
or a set-like object containing document numbers. The searcher caches filters
so if for example you use the same query filter with a searcher multiple times,
the additional searches will be faster because the searcher will cache the
results of running the filter query

You can also specify a ``mask`` keyword argument to specify a set of documents
that are not permitted in the results.

::

    with myindex.searcher() as s:
        qp = qparser.QueryParser("content", myindex.schema)
        user_q = qp.parse(query_string)

        # Only show documents in the "rendering" chapter
        allow_q = query.Term("chapter", "rendering")
        # Don't show any documents where the "tag" field contains "todo"
        restrict_q = query.Term("tag", "todo")

        results = s.search(user_q, filter=allow_q, mask=restrict_q)

(If you specify both a ``filter`` and a ``mask``, and a matching document
appears in both, the ``mask`` "wins" and the document is not permitted.)

To find out how many results were filtered out of the results, use
``results.filtered_count`` (or ``resultspage.results.filtered_count``)::

    with myindex.searcher() as s:
        qp = qparser.QueryParser("content", myindex.schema)
        user_q = qp.parse(query_string)

        # Filter documents older than 7 days
        old_q = query.DateRange("created", None, datetime.now() - timedelta(days=7))
        results = s.search(user_q, mask=old_q)

        print("Filtered out %d older documents" % results.filtered_count)


Which terms from my query matched?
==================================

You can use the ``terms=True`` keyword argument to ``search()`` to have the
search record which terms in the query matched which documents::

    with myindex.searcher() as s:
        results = s.seach(myquery, terms=True)

You can then get information about which terms matched from the
:class:`whoosh.searching.Results` and :class:`whoosh.searching.Hit` objects::

    # Was this results object created with terms=True?
    if results.has_matched_terms():
        # What terms matched in the results?
        print(results.matched_terms())

        # What terms matched in each hit?
        for hit in results:
            print(hit.matched_terms())


.. _collapsing:

Collapsing results
==================

Whoosh lets you eliminate all but the top N documents with the same facet key
from the results. This can be useful in a few situations:

* Eliminating duplicates at search time.

* Restricting the number of matches per source. For example, in a web search
  application, you might want to show at most three matches from any website.

Whether a document should be collapsed is determined by the value of a "collapse
facet". If a document has an empty collapse key, it will never be collapsed,
but otherwise only the top N documents with the same collapse key will appear
in the results.

See :doc:`/facets` for information on facets.

::

    with myindex.searcher() as s:
        # Set the facet to collapse on and the maximum number of documents per
        # facet value (default is 1)
        results = s.collector(collapse="hostname", collapse_limit=3)

        # Dictionary mapping collapse keys to the number of documents that
        # were filtered out by collapsing on that key
        print(results.collapsed_counts)

Collapsing works with both scored and sorted results. You can use any of the
facet types available in the :mod:`whoosh.sorting` module.

By default, Whoosh uses the results order (score or sort key) to determine the
documents to collapse. For example, in scored results, the best scoring
documents would be kept. You can optionally specify a ``collapse_order`` facet
to control which documents to keep when collapsing.

For example, in a product search you could display results sorted by decreasing
price, and eliminate all but the highest rated item of each product type::

    from whoosh import sorting

    with myindex.searcher() as s:
        price_facet = sorting.FieldFacet("price", reverse=True)
        type_facet = sorting.FieldFacet("type")
        rating_facet = sorting.FieldFacet("rating", reverse=True)

        results = s.collector(sortedby=price_facet,  # Sort by reverse price
                              collapse=type_facet,  # Collapse on product type
                              collapse_order=rating_facet  # Collapse to highest rated
                              )

The collapsing happens during the search, so it is usually more efficient than
finding everything and post-processing the results. However, if the collapsing
eliminates a large number of documents, collapsed search can take longer
because the search has to consider more documents and remove many
already-collected documents.

Since this collector must sometimes go back and remove already-collected
documents, if you use it in combination with
:class:`~whoosh.collectors.TermsCollector` and/or
:class:`~whoosh.collectors.FacetCollector`, those collectors may contain
information about documents that were filtered out of the final results by
collapsing.


Time limited searches
=====================

To limit the amount of time a search can take::

    from whoosh.collectors import TimeLimitCollector, TimeLimit

    with myindex.searcher() as s:
        # Get a collector object
        c = s.collector(limit=None, sortedby="title_exact")
        # Wrap it in a TimeLimitedCollector and set the time limit to 10 seconds
        tlc = TimeLimitedCollector(c, timelimit=10.0)

        # Try searching
        try:
            s.search_with_collector(myquery, tlc)
        except TimeLimit:
            print("Search took too long, aborting!")

        # You can still get partial results from the collector
        results = tlc.results()


Convenience methods
===================

The :meth:`~whoosh.searching.Searcher.document` and
:meth:`~whoosh.searching.Searcher.documents` methods on the ``Searcher`` object let
you retrieve the stored fields of documents matching terms you pass in keyword
arguments.

This is especially useful for fields such as dates/times, identifiers, paths,
and so on.

::

    >>> list(searcher.documents(indexeddate=u"20051225"))
    [{"title": u"Christmas presents"}, {"title": u"Turkey dinner report"}]
    >>> print searcher.document(path=u"/a/b/c")
    {"title": "Document C"}

These methods have some limitations:

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
those documents to be placed at the top of the results list. You could try to
do this by boosting the "bestbet" field tremendously, but that can have
unpredictable effects on scoring. It's much easier to simply run the query
twice and combine the results::

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

The ``Results`` object supports the following methods:

``Results.extend(results)``
    Adds the documents in 'results' on to the end of the list of result
    documents.

``Results.filter(results)``
    Removes the documents in 'results' from the list of result documents.

``Results.upgrade(results)``
    Any result documents that also appear in 'results' are moved to the top
    of the list of result documents.

``Results.upgrade_and_extend(results)``
    Any result documents that also appear in 'results' are moved to the top
    of the list of result documents. Then any other documents in 'results' are
    added on to the list of result documents.






