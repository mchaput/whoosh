===========
Quick start
===========

Whoosh is a library of classes and functions for indexing text and then searching the index. It allows you to develop custom search engines for your content. For example, if you were creating blogging software, you could use Whoosh to add a search function to allow users to search blog entries.

Background concepts
===================

To understand Whoosh, there are a few important terms and concepts:

Documents
    The individual pieces of content you want to make searchable. The word "documents" might imply files, but the data source could really be anything -- articles in a content management system, blog posts in a blogging system, chunks of a very large file, rows returned from an SQL query, individual email messages from a mailbox file, or whatever. When you get search results from Whoosh, the results are a list of documents, whatever "documents" means in your search engine.
    
Fields
    Each document contains a set of fields. Typical fields might be "title", "content", "url", "keywords", "status", "date", etc. Fields can be indexed (so they're searchable) and/or stored with the document. Storing the field makes it available in search results. For example, you typically want to store the "title" field so your search results can display it.
    
Corpus
    The set of documents you are indexing.
    
Indexing
    This is the heart of how a search engine works. Whoosh creates a reverse index, which is basically a table listing every word in the corpus, and for each word, the list of documents in which it appears. It can be more complicated (the index can also list how many times the word appears in each document, the positions at which it appears, etc.) but that's how it basically works.
    
Analysis
    The process of breaking the text of a field into individual words (called "terms") to be indexed. This consists of tokenizing the text into terms, and then optionally filtering the tokenized terms (for example, lowercasing and removing stop words). Whoosh includes several different analyzers.
    
Pluggable components
    Like its ancestor Lucene, Whoosh is not really a search engine, it's a toolkit for creating a search engine. Practically no important behavior of Whoosh is hard-coded. Indexing of text, the level of information stored for each term in each field, parsing of search queries, the types of queries allowed, scoring algorithms, etc. are all customizable, replaceable, and extensible. In this quick start we'll generally use defaults and predefined object, but keep in mind that these are just defaults. Everything is flexible.

All indexed text is Unicode
    Anything to do with indexed text in Whoosh must be unicode.

Setting up
==========

To start using Whoosh, you must create an index.

At a low level, there are basically steps involved:

* Create a Storage object for the index. In the common case of storing the index in a directory, there a convenience functions to do this for you.

* Create a Schema object defining the fields that are indexed for each document. You only need to do this once when you create the index. The schema is pickled and stored with the index.

* Create an Index object representing the index, using the store and the schema.

A Storage object represents that medium in which the index will be stored. Usually this will be ``FileStorage``, which stores the index as a set of files in a directory. There is also ``RamStorage``, which simply keeps the index in memory. Future versions of Whoosh may include other storage options. ::

    import os, os.path
    from whoosh.filedb.filestore import FileStorage

    if not os.path.exists("index"):
        os.mkdir("index")

    storage = FileStorage("index")

The Schema is a very important object that defines the fields which will be stored and/or indexed. When you create the Schema object, you use keyword arguments to map field names to field types. The list of fields and their types defines what you are indexing and what's searchable. Whoosh comes with some very useful predefined field types, and you can easily create your own.

:class:`whoosh.fields.ID`
    This type simply indexes (and optionally stores) the entire value of the field as a single unit (that is, it doesn't break it up into individual words). This is useful for fields such as a file path, URL, date, category, etc.
    
:class:`whoosh.fields.STORED`
    This field is stored with the document, but not indexed. This field type is not indexed and not searchable. This is useful for document information you want to display to the user in the search results.
    
:class:`whoosh.fields.KEYWORD`
    This type is designed for space- or comma-separated keywords. This type is indexed and searchable (and optionally stored). To save space, it does not support phrase searching.
    
:class:`whoosh.fields.TEXT`
    This type is for body text. It indexes (and optionally stores) the text and stores term positions to allow phrase searching.
    
:class:`whoosh.fields.NGRAM`
    TODO

(As a shortcut, if you don't need to pass any arguments to the field type, you can just give the class name and Whoosh will instantiate the object for you.) ::

    from whoosh.fields import Schema, STORED, ID, KEYWORD, TEXT

    schema = Schema(title=TEXT(stored=True), content=TEXT,
                    path=ID(stored=True), tags=KEYWORD, icon=STORED)

Once you have the Storage and Schema objects, you can create the Index object::

    ix = storage.create_index(schema)
    
To open an existing index::

	ix = storage.open_index()

Since you'll usually be using the FileStorage class for the index storage, there are couple of convenience functions that let you skip some of the above steps:

``index.create_in`` creates an index in a given directory using a supplied schema::

    import whoosh.index as index

    ix = index.create_in("index_dir1", schema)
    
``index.open_dir`` takes a directory name as an argument and returns an Index object::

    import whoosh.index as index

    ix = index.open_dir("index_dir1")


Indexing documents
==================

OK, so we've got an Index object, now we can start adding documents. The writer() method of the Index object returns an ``IndexWriter`` object that lets you add documents to the index. The IndexWriter's ``add_document(**kwargs)`` method accepts keyword arguments where the field name is mapped to a value::

    writer = ix.writer()
    writer.add_document(title=u"My document", content=u"This is my document!",
                        path=u"/a", tags=u"first short", icon=u"/icons/star.png")
    writer.add_document(title=u"Second try", content=u"This is the second example.",
                        path=u"/b", tags=u"second short", icon=u"/icons/sheep.png")
    writer.add_document(title=u"Third time's the charm", content=u"Examples are many.",
                        path=u"/c", tags=u"short", icon=u"/icons/book.png")
    writer.commit()

Two important notes:

* You don't have to fill in a value for every field. Whoosh doesn't care if you leave out a field from a document.

* Indexed fields must be passed a unicode value. Fields that are stored but not indexed (STORED field type) can be passed any pickle-able object.

If you have a field that is both indexed and stored, you can even index a unicode value but store a different object if necessary (it's usually not, but sometimes this is really useful) using this trick::

    writer.add_document(title=u"Title to be indexed", _stored_title=u"Stored title")

Calling commit() on the ``IndexWriter`` saves the added documents to the index. Once your documents are in the index, you can search for them.


Searching
=========

First, we'll show how to load an existing index from disk. In this case, we have an index in a directory called index. We can create a Storage object manually, and use it to create an Index object. The Schema object is pickled and saved with the index; we don't need to recreate it to load the index::

    from whoosh.filedb.filestore import FileStorage

    storage = FileStorage("index")
    ix = storage.open_index()

Since you'll usually be loading the index from disk, you can use the ``open_dir()`` function from the index module to avoid having to create the storage object. It takes a path to the index directory and returns an Index object::

    from whoosh import index

    ix = open_dir("index")

So, let's say a user has typed a search into a search box and you want to run that search on you index.

To begin searching the index, we'll need a Searcher object::

    searcher = ix.searcher()

Now you'll need to parse a query string into Query objects. (You can also create your own tree of Query objects programmatically, which is very powerful, and even lets you use a few query types that aren't available in the query string syntax)::

    from whoosh.qparser import QueryParser

    parser = QueryParser("content", schema = ix.schema)

The first argument, ``"content"``, specifies the "default" field to use when the user doesn't specify a field for a word/phrase/clause. This is usually the "body text" field. Specifying the schema lets the parser know which analyzers to use for which fields. If you don't have a schema (usually when you're testing the parser), you can omit the schema. In that case, the parser won't filter the query terms (for example, it won't lower-case them).

The default ``QueryParser`` implements a query language very similar to Lucene's. It lets you connect terms with AND or OR, eleminate terms with NOT, group terms together into clauses with parentheses, and specify different fields to search. By default it joins clauses together with AND (so by default, all terms you specify must be in the document for the document to match)::

    >>> print(parser.parse(u"render shade animate"))
    And([Term("content", "render"), Term("content", "shade"), Term("content", "animate")])

    >>> print(parser.parse(u"render OR (title:shade keyword:animate)"))
    Or([Term("content", "render"), And([Term("title", "shade"), Term("keyword", "animate")])])

    >>> print(parser.parse(u"rend*"))
    Prefix("content", "rend")
    
We'll create a query object we can use to find a document in the index we created above::

    query = parser.parse(u"second")

Now you can use the searcher to find documents that match the query::

    results = searcher.search(query)

``Searcher.search()`` finds the matching documents, puts them in order based on their score (by default Whoosh uses the BM25F scoring algorithm, but you can choose a different one or write your own), and returns a ``Results`` object.

The Results object acts more or less like a list of dictionaries, where each dictionary contains the stored fields of the document. The first document in the list is the most relevant based on the scoring algorithm::

    >>> print(len(results))
    1
    >>> print(results[0])
    {"title": "Second try", "path": "/b", "icon": "/icons/sheep.png"}

Whoosh includes extra features for dealing with search results, such as highlighting the search terms in excerpts from the original documents, expanding the query terms based on the top few documents found, and paginating the results (e.g. "Showing results 1-20, page 1 of 4"), but these are beyond the scope of this quick start.


