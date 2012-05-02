===========================================
Indexing and searching document hierarchies
===========================================

Overview
========

Whoosh's full-text index is essentially a flat database of documents. However,
Whoosh supports two techniques for simulating the indexing and querying of
hierarchical documents, that is, sets of documents that form a parent-child
hierarchy, such as "Chapter - Section - Paragraph" or
"Module - Class - Method".

You can specify parent-child relationships *at indexing time*, by grouping
documents in the same hierarchy, and then use the
:class:`whoosh.query.NestedParent` and/or :class:`whoosh.query.NestedChildren`
to find parents based on their children or vice-versa.

Alternatively, you can use *query time joins*, essentially like external key
joins in a database, where you perform one search to find a relevant document,
then use a stored value on that document (for example, a ``parent`` field) to
look up another document.

Both methods have pros and cons.


Using nested document indexing
==============================

Indexing
--------

This method works by indexing a "parent" document and all its "child" documents
*as a "group"* so they are guaranteed to end up in the same segment. You can
use the context manager returned by ``IndexWriter.group()`` to group
documents::

    with ix.writer() as w:
        with w.group():
            w.add_document(kind="class", name="Index")
            w.add_document(kind="method", name="add document")
            w.add_document(kind="method", name="add reader")
            w.add_document(kind="method", name="close")
        with w.group():
            w.add_document(kind="class", name="Accumulator")
            w.add_document(kind="method", name="add")
            w.add_document(kind="method", name="get result")
        with w.group():
            w.add_document(kind="class", name="Calculator")
            w.add_document(kind="method", name="add")
            w.add_document(kind="method", name="add all")
            w.add_document(kind="method", name="add some")
            w.add_document(kind="method", name="multiply")
            w.add_document(kind="method", name="close")
        with w.group():
            w.add_document(kind="class", name="Deleter")
            w.add_document(kind="method", name="add")
            w.add_document(kind="method", name="delete")

Alternatively you can use the ``start_group()`` and ``end_group()`` methods::

    with ix.writer() as w:
        w.start_group()
        w.add_document(kind="class", name="Index")
        w.add_document(kind="method", name="add document")
        w.add_document(kind="method", name="add reader")
        w.add_document(kind="method", name="close")
        w.end_group()

Each level of the hierarchy should have a query that distinguishes it from
other levels (for example, in the above index, you can use ``kind:class`` or
``kind:method`` to match different levels of the hierarchy).

Once you've indexed the hierarchy of documents, you can use two query types to
find parents based on children or vice-versa.

(There is currently no support in the default query parser for nested queries.)


NestedParent query
------------------

The :class:`whoosh.query.NestedParent` query type lets you specify a query for
child documents, but have the query return an "ancestor" document from higher
in the hierarchy::

    # First, we need a query that matches all the documents in the "parent"
    # level we want of the hierarchy
    all_parents = query.Term("kind", "class")

    # Then, we need a query that matches the children we want to find
    wanted_kids = query.Term("name", "close")

    # Now we can make a query that will match documents where "name" is
    # "close", but the query will return the "parent" documents of the matching
    # children
    q = query.NestedParent(all_parents, wanted_kids)
    # results = Index, Calculator

Note that in a hierarchy with more than two levels, you can specify a "parents"
query that matches any level of the hierarchy, so you can return the top-level
ancestors of the matching children, or the second level, third level, etc.

The query works by first building a bit vector representing which documents are
"parents"::

     Index
     |      Calculator
     |      |
     1000100100000100
         |        |
         |        Deleter
         Accumulator

Then for each match of the "child" query, it calculates the previous parent
from the bit vector and returns it as a match (it only returns each parent once
no matter how many children match). This parent lookup is very efficient::

     1000100100000100
        |
     |<-+ close


NestedChildren query
--------------------

The opposite of ``NestedParent`` is :class:`whoosh.query.NestedChildren`. This
query lets you match parents but return their children. This is useful, for
example, to search for an album title and return the songs in the album::

    # Query that matches all documents in the "parent" level we want to match
    # at
    all_parents = query.Term("kind", "album")

    # Parent documents we want to match
    wanted_parents = query.Term("album_title", "heaven")

    # Now we can make a query that will match parent documents where "album_title"
    # contains "heaven", but the query will return the "child" documents of the
    # matching parents
    q1 = query.NestedChildren(all_parents, wanted_parents)

You can then combine that query with an ``AND`` clause, for example to find
songs with "hell" in the song title that occur on albums with "heaven" in the
album title::

    q2 = query.And([q1, query.Term("song_title", "hell")])


Deleting and updating hierarchical documents
--------------------------------------------

The drawback of the index-time method is *updating and deleting*. Because the
implementation of the queries depends on the parent and child documents being
contiguous in the segment, you can't update/delete just one child document.
You can only update/delete an entire top-level document at once (for example,
if your hierarchy is "Chapter - Section - Paragraph", you can only update or
delete entire chapters, not a section or paragraph). If the top-level of the
hierarchy represents very large blocks of text, this can involve a lot of
deleting and reindexing.

Currently ``Writer.update_document()`` does not automatically work with nested
documents. You must manually delete and re-add document groups to update them.

To delete nested document groups, use the ``Writer.delete_by_query()``
method with a ``NestedParent`` query::

    # Delete the "Accumulator" class
    all_parents = query.Term("kind", "class")
    to_delete = query.Term("name", "Accumulator")
    q = query.NestedParent(all_parents, to_delete)
    with myindex.writer() as w:
        w.delete_by_query(q)


Using query-time joins
======================

A second technique for simulating hierarchical documents in Whoosh involves
using a stored field on each document to point to its parent, and then using
the value of that field at query time to find parents and children.

For example, if we index a hierarchy of classes and methods using pointers
to parents instead of nesting::

    # Store a pointer to the parent on each "method" document
    with ix.writer() as w:
        w.add_document(kind="class", c_name="Index", docstring="...")
        w.add_document(kind="method", m_name="add document", parent="Index")
        w.add_document(kind="method", m_name="add reader", parent="Index")
        w.add_document(kind="method", m_name="close", parent="Index")

        w.add_document(kind="class", c_name="Accumulator", docstring="...")
        w.add_document(kind="method", m_name="add", parent="Accumulator")
        w.add_document(kind="method", m_name="get result", parent="Accumulator")

        w.add_document(kind="class", c_name="Calculator", docstring="...")
        w.add_document(kind="method", m_name="add", parent="Calculator")
        w.add_document(kind="method", m_name="add all", parent="Calculator")
        w.add_document(kind="method", m_name="add some", parent="Calculator")
        w.add_document(kind="method", m_name="multiply", parent="Calculator")
        w.add_document(kind="method", m_name="close", parent="Calculator")

        w.add_document(kind="class", c_name="Deleter", docstring="...")
        w.add_document(kind="method", m_name="add", parent="Deleter")
        w.add_document(kind="method", m_name="delete", parent="Deleter")

    # Now do manual joins at query time
    with ix.searcher() as s:
        # Tip: Searcher.document() and Searcher.documents() let you look up
        # documents by field values more easily than using Searcher.search()

        # Children to parents:
        # Print the docstrings of classes on which "close" methods occur
        for child_doc in s.documents(m_name="close"):
            # Use the stored value of the "parent" field to look up the parent
            # document
            parent_doc = s.document(c_name=child_doc["parent"])
            # Print the parent document's stored docstring field
            print(parent_doc["docstring"])

        # Parents to children:
        # Find classes with "big" in the docstring and print their methods
        q = query.Term("kind", "class") & query.Term("docstring", "big")
        for hit in s.search(q, limit=None):
            print("Class name=", hit["c_name"], "methods:")
            for child_doc in s.documents(parent=hit["c_name"]):
                print("  Method name=", child_doc["m_name"])

This technique is more flexible than index-time nesting in that you can
delete/update individual documents in the hierarchy piece by piece, although it
doesn't support finding different parent levels as easily. It is also slower
than index-time nesting (potentially much slower), since you must perform
additional searches for each found document.

Future versions of Whoosh may include "join" queries to make this process more
efficient (or at least more automatic).

