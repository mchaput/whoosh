=============
How to search
=============

Once you've created an index and added documents to it, you can search for those
documents.

The Searcher object
===================

To get a :class:`whoosh.searching.Searcher` object, call ``searcher()`` on your
Index objdct::

    searcher = myindex.searcher()

The Searcher object is the main high-level interface for reading the index. It
has lots of useful methods for getting information about the index, such as
``most_frequent_terms()``.

>>> list(searcher.most_frequent_terms("content", 3))
[(u"whoosh", 32), (u"index", 24), (u"document", 18)]

However, the most important method on the Searcher object is
:meth:`~whoosh.searching.Searcher.search`, which takes a
:class:`whoosh.query.Query` object and returns a
:class:`~whoosh.searching.Results` object::

    from whoosh.qparser import QueryParser
    
    qp = QueryParser("content", schema=myindex.schema)
    q = queryparser.parse(u"hello world")
    s = myindex.searcher()
    results = s.search(q)

By default the results contains at most the first 10 matching documents. To get
more results, use the ``limit`` keyword::

    results = s.search(q, limit=20)

If you want all results, use ``limit=None``. However, setting the limit
whenever possible makes searches faster because Whoosh doesn't need to examine
and score every document.

Since display a page of results at a time is a common pattern, the
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

>>> # How many documents matched?
>>> len(results)
27
>>> # How many scored and sorted documents in this Results object?
>>> # This will be less than len() if you used the limit keyword argument.
>>> results.scored_length()
10
>>> # Show the best hit's stored fields
>>> results[0]
{"title": u"Hello World in Python", "path": u"/a/b/c"}
>>> results[0:2]
[{"title": u"Hello World in Python", "path": u"/a/b/c"}, {"title": u"Foo", "path": u"/bar"}]


Scoring and sorting
===================

Scoring
-------

Normally the list of result documents is sorted by *score*. The
:mod:`whoosh.scoring` module contains implementations of various scoring
algorithms. The default is :class:`~whoosh.scoring.BM25F`.

You can set the scoring object to use when you create the searcher using the
``weighting`` keyword argument::

    s = myindex.searcher(weighting=whoosh.scoring.Cosine())

A scoring object is an object with a :meth:`~whoosh.scoring.Weighting.score`
method that takes information about the term to score and returns a score as a
floating point number.

Sorting
-------

Instead of sorting the matched documents by a score, you can sort them by the
contents of one or more indexed field(s). These should be fields for which each
document stores one term (i.e. an ID field type), for example "path", "id",
"date", etc.

To sort by the contents of the "path" field::

    results = s.search(myquery, sortedby="path")
    
To sort by the contents of the "date" field, and within that the "id" field::

    results = s.search(myquery, sortedby=["path", "date"])
    
To reverse the sort order::

    results = s.search(myquery, sortedby="path", reverse=True)

Sorting relies on field caches. See :doc:`fieldcaches` for information about
field caches.


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






