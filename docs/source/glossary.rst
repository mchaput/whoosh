.. _glossary:

========
Glossary
========

.. glossary::

    Analysis
        The process of breaking the text of a field into individual *terms*
        to be indexed. This consists of tokenizing the text into terms, and then optionally
        filtering the tokenized terms (for example, lowercasing and removing *stop words*).
        Whoosh includes several different analyzers.

    Corpus
        The set of documents you are indexing.

    Documents
        The individual pieces of content you want to make searchable.
        The word "documents" might imply files, but the data source could really be
        anything -- articles in a content management system, blog posts in a blogging
        system, chunks of a very large file, rows returned from an SQL query, individual
        email messages from a mailbox file, or whatever. When you get search results
        from Whoosh, the results are a list of documents, whatever "documents" means in
        your search engine.

    Fields
        Each document contains a set of fields. Typical fields might be "title", "content",
        "url", "keywords", "status", "date", etc. Fields can be indexed (so they're
        searchable) and/or stored with the document. Storing the field makes it available
        in search results. For example, you typically want to store the "title" field so
        your search results can display it.

    Forward index
        A table listing every document and the words that appear in the document.
        Whoosh lets you store *term vectors* that are a kind of forward index.

    Indexing
        The process of examining documents in the corpus and adding them to the
        *reverse index*.

    Postings
        The *reverse index* lists every word in the corpus, and for each word, a list
        of documents in which that word appears, along with some optional information
        (such as the number of times the word appears in that document). These items
        in the list, containing a document number and any extra information, are
        called *postings*. In Whoosh the information stored in postings is customizable
        for each *field*.

    Reverse index
        Basically a table listing every word in the corpus, and for each word, the
        list of documents in which it appears. It can be more complicated (the index can
        also list how many times the word appears in each document, the positions at which
        it appears, etc.) but that's how it basically works.

    Schema
        Whoosh requires that you specify the *fields* of the index before you begin
        indexing. The Schema associates field names with metadata about the field, such
        as the format of the *postings* and whether the contents of the field are stored
        in the index.

    Term vector
        A *forward index* for a certain field in a certain document. You can specify
        in the Schema that a given field should store term vectors.

