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

    price_facets = Facets()
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

If you set up the ``Facets`` object this way, you need to call the ``study()``
method on the Facets object with a searcher to set up the categories::

    price_facets.study(searcher)

If it's convenient, you can also instantiate a Facets object with keyword
arguments mapping names to queries::

    my_facets = Facets(small=Or([Term("size", "s"), Term("size", "xs")]),
                       medium=Term("size", "m"),
                       large=Or([Term("size", "l"), Term("size", "xl")]))
    my_facets.study(searcher)


The ``study()`` method
======================

A ``Facets`` object stores lists of documents numbers corresponding to
categories. For performance you can re-use Facet objects between searches, but
any time the index changes or you add or remove a facet, you need to call
``study()`` with a searcher to update the Facets object::

    my_facets.study(searcher)
    
(If you create a Facets object using the ``Facets.from_field()`` class
method, you don't need to call ``study()`` on the resulting object, the
class method takes care of that.)


Categorizing search results
===========================

First, get the search results. Normally, the searcher uses a bunch of
optimizations to avoid working having to look at every search result. However,
since we want to know the order of documents in each category, we have to
score every matching document, so use ``limit=None`` to tell the searcher not
to limit the number of results::

    results = searcher.search(my_query, limit=None)

Now you can use your Facets object(s) to sort the results into categories::

    categories = my_facets.categorize(results)

The ``categorize()`` method simply returns a dictionary mapping category names to
lists of **document numbers**. The document numbers will be in their relative order
from the original results.

>>> print categories
{"small": [5, 1, 4, 8, 2], "medium": [3, 0, 6], "large": [9, 7]}

(If there were documents in the results that didn't match any of the categories
in the ``Facets`` object, they will be grouped under a ``None`` key.)

The last thing you need to know is how to translate document numbers into
something you can display. The ``Searcher`` object's ``stored_fields()``
method takes a document number and returns the document's stored fields as a
dictionary::

    for category_name in categories:
        print "Top 5 documents in the %s category" % category_name
        doclist = categories[category_name]
        for docnum in doclist[:5]:
            print "  ", searcher.stored_fields(docnum)
        if len(doclist) > 5:
            print "  (%s more)" % (len(doclist) - 5)

You can use the categories dictionary and ``stored_fields()`` to display the
categories and results any way you want in your application.

