==============================
Using the Whoosh spell checker
==============================


Overview
========

Whoosh includes pure-Python spell-checking library functions that use the Whoosh
search engine for back-end storage.

To create a :class:`~whoosh.spelling.SpellChecker` object::

    from whoosh.filedb.filestore import FileStorage
    from whoosh.spelling import SpellChecker
    
    # FileStorage object based on the directory "spelldict"
    st = FileStorage("spelldict")
    
    # SpellChecker object needs a Storage object in which to put its index.
    speller = SpellChecker(st)

If you have a Whoosh ``Index`` object and you want to open the spelling
dictionary in the same directory as the index, you can re-use the ``Index``
object's ``Storage``::

    from whoosh import index
    
    # Open the "main" index
    ix = index.open_dir("index")

    # Start/open a spelling dictionary in the same directory
    speller = SpellChecer(ix.storage)

Whoosh lets you keep multiple indexes in the same directory by assigning the
indexes different names. The default name for a regular index is ``_MAIN``. The
default name for the index created by the SpellChecker object is ``SPELL`` (so
you can keep your main index and a spelling index in the same directory by
default). You can pass an ``indexname`` argument to the SpellChecker constructor
to choose a different index name (for example, if you want to keep multiple
spelling dictionaries in the same directory)::

    speller = SpellChecker(st, indexname="COMMON_WORDS")


Creating the spelling dictionary
================================

You need to populate the spell-checking dictionary with (properly spelled) words
to check against. There are a few strategies for doing this:

*   Add all the words that appear in a certain field in a Whoosh index.
 
    For example, if you've created an index for a collection of documents with
    the contents indexed in a field named ``content``, you can automatically add
    all the words from that field::
    
        from whoosh import index
    
        # Open the main index
        ix = index.open_dir("index")
        
        # Populate the spelling dictionary with the words from the
        # main index's 'content' field.
        speller.add_field(ix, "content")
        
    The advantage of using the contents of an index field is that when you are
    spell checking queries on that index, the suggestions are tailored to the
    contents of the index. The disadvantage is that if the indexed documents
    contain spelling errors, then the spelling suggestions will also be
    erroneous.
 
*   Use a preset list of words. The ``add_words`` method lets you add words from any iterable.
 
    There are plenty of word lists available on the internet you can use to
    populate the spelling dictionary. ::
    
        speller.add_words(["custom", "word", "list"])
    
        # Assume this is file contains a list of words, one on each line
        wordfile = open("words.txt")
        
        # add_words() takes an iterable, so we can pass it the file object
        # directly
        speller.add_words(wordfile)
        
*   Use a combination of word lists and index field contents. For example, you
    could add words from a field, but only if they appear in the word list::
 
        # Open the list of words (one on each line) and load it into a set
        wordfile = open("words.txt")
        wordset = set(wordfile)
        
        # Open the main index
        ix = index.open_dir("index")
        reader = ix.reader()
        
        # Add words from the main index's 'content' field only if they
        # appear in the word list
        speller.add_words(word for word in reader.lexicon("content")
                          if word in wordset)

Note that adding words to the dictionary should be done all at once. Each call
to ``add_field()``, ``add_words()``, or ``add_scored_words()`` (see below)
creates a writer, adds to the underlying index, and the closes the writer, just
like adding documents to a regular Whoosh index. **DO NOT** do anything like
this::

    # This would be very slow
    for word in my_list_of_words:
        speller.add_words([word])
        
**Be careful** not to add the same word to the spelling dictionary more than
once. The ``SpellChecker`` code *does not* currently guard against this
automatically.


Gettings suggestions
====================

Once you have words in the spelling dictionary, you can use the ``suggest()``
method to check words::

    >>> st = store.FileStorage("spelldict")
    >>> speller = SpellChecker(st)
    >>> speller.suggest("woosh")
    ["whoosh"]
    
The ``number`` keyword argument sets the maximum number of suggestions to return
(default is 3). ::

    >>> # Get the top 5 suggested replacements for this word
    >>> speller.suggest("rundering", number=5)
    
    >>> # Get only the top suggested replacement for this word
    >>> speller.suggest("woosh", number=1)


Word scores
===========

Each word in the dictionary can have a "score" associated with it. When two or
more suggestions have the same "edit distance" (number of differences) from the
checked word, the score is used to order them in the suggestion list.

By default the list of suggestions is only ordered by the number of differences
between the suggestion and the original word. To make the ``suggest()`` method
use word scores, use the ``usescores=True`` keyword argument. ::

    speller.suggest("woosh", usescores=True)

The main use for this is to use the word's frequency in the index as its score,
so common words are suggested before obscure words. **Note** The ``add_field()``
method does this by default.

If you want to add a list of words with scores manually, you can use the
``add_scored_words()`` method::

    # Takes an iterable of ("word", score) tuples
    speller.add_scored_words([("whoosh", 2.0), ("search", 1.0), ("find", 0.5)])

For example, if you wanted to reverse the default behavior of ``add_field()`` so
that *obscure* words would be suggested before common words, you could do this::

    # Open the main index
    ix = index.open_dir("index")
    reader = ix.reader()
    
    # IndexReader.iter_field() yields (term_text, doc_freq, index_freq) tuples
    # for each term in the given field.
    
    # We pull out the term text and the index frequency of each term, and
    # then invert the frequency so terms with lower frequencies get higher
    # scores in the spelling dictionary
    speller.add_scored_words((termtext, 1 / index_freq)
                             for termtext, doc_freq, index_freq
                             in reader.iter_field("content"))


Spell checking Whoosh queries
=============================

If you want to spell check a user query, first parse the user's query into a
``whoosh.query.Query`` object tree, using the default parser or your own custom
parser. For example::

    from whoosh.qparser import QueryParser
    parser = QueryParser("content", schema=my_schema)
    user_query = parser.parse(user_query_string)
    
Then you can use the ``all_terms()`` or ``existing_terms()`` methods of the
``Query`` object to extract the set of terms used in the query. The two methods
work in a slightly unusual way: instead of returning a list, you pass them a
set, and they populate the set with the query terms::

    termset = set()
    user_query.all_terms(termset)
    
The ``all_terms()`` method simply adds all the terms found in the query. The
``existing_terms()`` method takes an IndexReader object and only adds terms from
the query *that exist* in the reader's underlying index. ::

    reader = my_index.reader()
    termset = set()
    user_query.existing_terms(reader, termset)
    
Of course, it's more useful to spell check the terms that are *missing* from the
index, not the ones that exist. The ``reverse=True`` keyword argument to
``existing_terms()`` lets us find the missing terms

    missing = set()
    user_query.existing_terms(reader, missing, reverse=True)
    
So now you have a set of ``("fieldname", "termtext")`` tuples. Now you can check
them against the spelling dictionary::

    # Load the main index
    ix = index.open_dir("index")
    reader = ix.reader()
    
    # Load a spelling dictionary stored in the same directory
    # as the main index
    speller = SpellChecker(ix.storage)

    # Extract missing terms from the user query
    missing = set()
    user_query.existing_terms(reader, missing, reverse=True)
    
    # Print a list of suggestions for each missing word
    for fieldname, termtext in missing:
        # Only spell check terms in the "content" field
        if fieldname == "content":
            suggestions = speller.suggest(termtext)
            if suggestions:
                print "%s not found. Might I suggest %r?" % (termtext, suggestions)


Updating the spelling dictionary
================================

The spell checker is mainly intended to be "write-once, read-many". You can
continually add words to the dictionary, but it is not possible to remove words
or dynamically update the dictionary.

Currently the best strategy available for keeping a spelling dictionary
up-to-date with changing content is simply to **delete and re-create** the
spelling dictionary periodically.

Note, to clear the spelling dictionary so you can start re-adding words, do
this::

    speller = SpellChecker(storage_object)
    speller.index(create=True)

