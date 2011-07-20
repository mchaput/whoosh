==============
Whoosh recipes
==============

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


Queries
=======



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

    if results.has_exact_length():
        print("Scored", found, "of exactly", len(results), "documents")
    else:
        low = results.estimated_min_length()
        high = results.estimated_length()
    
        print("Scored", found, "of between", low, "and", "high", "documents")


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

What fields are in the index?
-----------------------------
::

    return myindex.schema.names()

Is term X in the index?
-----------------------
::

    return ("content", "wobble") in searcher
    
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
    
    
