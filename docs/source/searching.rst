How to search
=============

Once you've created an index and added documents to it, you can search for those documents.

Query objects
-------------

Before you can find documents in the index, you need a way to tell Whoosh what you're looking for. The objects in the query module define ways of searching for documents.

The simplest query is one that matches a single term::

    from whoosh.query import *

    # Matches any documents containing the term "render"
    # in the field "content"
    q = Term("content", u"render")

Parsing user queries
--------------------

Running a search
----------------

Scoring and sorting
-------------------

Results objects
---------------

Advanced searching topics
=========================

Manipulating queries
--------------------

