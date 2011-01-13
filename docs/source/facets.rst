=====================================
How to present faceted search results
=====================================

.. note::
    The API for sorting and faceting changed in Whoosh 1.5.

Overview
========

It is often very useful to present "faceted" search results to the user.
Faceting is dynamic clustering of search results into categories. The
categories let users view a slice of the total results based on the categories
they're interested in.

For example, if you are programming a shopping website, you might want to
display categories with the search results such as the manufacturers and price
ranges.

==================== =================
Manufacturer         Price
-------------------- -----------------
Apple (5)            $0 - $100 (2)
Sanyo (1)            $101 - $500 (10)          
Sony (2)             $501 - $1000 (1)
Toshiba (5)
==================== =================

You can let your users click the different facet values to only show results
in the given categories.

Another useful UI pattern is to show, say, the top 5 results for different
types of found documents, and let the user click to see more results from a
category they're interested in, similarly to how the Spotlight quick results
work on Mac OS X.

.. tip::
    Whoosh currently only supports **non-overlapping** categories. A document
    cannot belong to facets at the same time. (It is not an error if the facets
    overlap; each document will simply be sorted into one category arbitrarily.)

Faceting relies on field caches. See :doc:`fieldcaches` for information about
field caches.


Categorizing search results by field
====================================

When you use the :meth:`Searcher.search` method, add the `groups` keyword
argument to specify how to categorize the results::

    # Group by the value of the "tag" field
    results = searcher.search(my_query, groupedby=["tag"])
    
    # Retrieve the groups from the results object
    groups = results.groups("tag")

The ``groups()`` method simply returns a dictionary mapping category names
to lists of **document IDs**. The document IDs will be in their relative
order from the original results.

    {"small": [5, 1, 4, 8, 2], "medium": [3, 0, 6], "large": [9, 7]}

The last thing you need to know is how to translate document numbers into
something you can display. The ``Searcher`` object's ``stored_fields()``
method takes a document number and returns the document's stored fields as a
dictionary::

    for category_name in categories:
        print "Top 5 documents in the %s category" % category_name
        doclist = categories[category_name]
        for docnum, score in doclist[:5]:
            print "  ", searcher.stored_fields(docnum)
        if len(doclist) > 5:
            print "  (%s more)" % (len(doclist) - 5)

You can use the categories dictionary and ``stored_fields()`` to display the
categories and results any way you want in your application.


Getting multiple categorizations of results
===========================================

To generate multiple groupings, you can name multiple fields in the list you
pass to the `groups` keyword::

    # Generate separate groupings for the "tag" and "size" fields
    results = searcher.search(my_query, groupedby=["tag", "size"])
    
    # Get the groupings by "tag"
    tag_groups = results.groups("tag")
    
    # Get the groupings by "size"
    size_groups = results.groups("size")


Categorizing by multiple fields
===============================

To group results by the *combined values of multiple fields*, use a tuple of
field names instead of a field name. For example, if you have two fields named
``tag`` and ``size``, you could group the results by all combinations of the
``tag`` and ``size`` field, such as ``('tag1', 'small')``,
``('tag2', 'small')``, ``('tag1', 'medium')``, and so on::

    # Generate a grouping from the combination of the "tag" and "size" fields
    results = searcher.search(my_query, groupedby=[("tag", "size")])
    
    groups = results.groups(("tag", "size"))


Categorizing based on custom queries
====================================

If you need more complex categories, you can set up categories defined by
queries. For example, you can create price categories using range queries::

    # Use queries to define each category
    # (Here I'll assume "price" is a NUMERIC field, so I'll use
    # NumericRange)
    categories = {}
    category["$0 - $100"] = query.NumericRange("price", 0, 100)
    category["$101 - $500"] = query.NumericRange("price", 101, 500)
    category["$501 - $1000"] = query.NumericRange("price", 501, 1000)
    
    # Define the facets on the searcher. If save=True, the cached
    # facets will be saved to disk for future use. Use save=False to
    # avoid this for one-off queries.
    searcher.define_facets("pricerange", categories, save=False)

Now you can use ``pricerange`` as if it was the name of a field for the
purposes of grouping and sorting::

    r = searcher.search(my_query, groupedby=["princerange"])










