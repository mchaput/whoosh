=====================================
How to present faceted search results
=====================================


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

You should use a separate ``Facets`` object for each different type of
categorization. For example, in the shopping example, "manufacturer" and
"price" would be two separate ``Facets`` objects.


Setting up categories
=====================

The :class:`whoosh.searching.Facets` object lets you categorize search results
based on custom criteria::

    from whoosh import index
    from whoosh.searching import Facets
    
    ix = index.open_dir("myindex")
    searcher = ix.searcher()

Categorizing based on the value of an indexed field
---------------------------------------------------

The most common way to break down results is by the value of an indexed field,
usually an ``ID`` field, such as ``manufacturer``. The ``Facets.from_field()``
method sets up the facets based on the terms in a field automatically::

    manuf_facets = Facets.from_field(searcher, "manufacturer")

Categorizing based on custom queries
------------------------------------

If you need more complex categories, you can set up categories defined by
queries. For example, you can create price categories using range queries::

    # Assuming "price" is a NUMERIC field, we'll need to convert
    # numeric values to text before we can search for them, using
    # the field's to_text() method
    tt = searcher.schema["price"].to_text

    price_facets = Facets(searcher)
    price_facets.add_facet("$0 - $100",
                           query.Range("price", tt(0), tt(100)))
    price_facets.add_facet("$101 - $500",
                           query.Range("price", tt(101), tt(500)))
    price_facets.add_facet("$501 - $1000",
                           query.Range("price", tt(501), tt(1000)))
    price_facets.add_facet("$1001+",
                           query.Range("price", tt(1001), None))

Note that the facets object currently only supports **non-overlapping**
categories. A document cannot belong to two categories in the same Facets
object. It is not an error if the facets overlap; each document will simply be
sorted into one category arbitrarily.

If it's convenient, you can also instantiate a Facets object with keyword
arguments mapping names to queries::

    my_facets = Facets(searcher,
                       small=Or([Term("size", "s"), Term("size", "xs")]),
                       medium=Term("size", "m"),
                       large=Or([Term("size", "l"), Term("size", "xl")]))


Categorizing search results
===========================

First, get the search results.

(Normally, the searcher uses a bunch of optimizations to avoid working having
to look at every search result. However, if you want to preserve the scored
order of documents in each category, Whoosh needs to score every matching
document, so use ``limit=None``. If you only want to use ``counts()`` or don't
care about the relative order of documents within categories, you don't need
to use ``limit=None``)::

    results = searcher.search(my_query, limit=None)

Now you can use your Facets object(s) to sort the results into categories::

    categories = my_facets.categorize(results)

The ``categorize()`` method simply returns a dictionary mapping category names to
lists of **document numbers** and **scores**. The document numbers will be in
their relative order from the original results.

>>> print categories
{"small": [(5, 2.0), (1, 1.8), (4, 1.5), (8, 1.3), (2, 0.8)],
 "medium": [(3, 2.5), (0, 1.32), (6, 0.28)],
 "large": [(9, 2.3), (7, 1.4)]}

(If there were documents in the results that didn't match any of the categories
in the ``Facets`` object, they will be grouped under a ``None`` key. If you
didn't score all documents by using ``limit=None``, the score will be None for
all documents.)

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

