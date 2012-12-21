===============
About analyzers
===============

Overview
========

An analyzer is a function or callable class (a class with a ``__call__`` method)
that takes a unicode string and returns a generator of tokens. Usually a "token"
is a word, for example the string "Mary had a little lamb" might yield the
tokens "Mary", "had", "a", "little", and "lamb". However, tokens do not
necessarily correspond to words. For example, you might tokenize Chinese text
into individual characters or bi-grams. Tokens are the units of indexing, that
is, they are what you are able to look up in the index.

An analyzer is basically just a wrapper for a tokenizer and zero or more
filters. The analyzer's ``__call__`` method will pass its parameters to a
tokenizer, and the tokenizer will usually be wrapped in a few filters.

A tokenizer is a callable that takes a unicode string and yields a series of
``analysis.Token`` objects.

For example, the provided :class:`whoosh.analysis.RegexTokenizer` class
implements a customizable, regular-expression-based tokenizer that extracts
words and ignores whitespace and punctuation.

::

    >>> from whoosh.analysis import RegexTokenizer
    >>> tokenizer = RegexTokenizer()
    >>> for token in tokenizer(u"Hello there my friend!"):
    ...   print repr(token.text)
    u'Hello'
    u'there'
    u'my'
    u'friend'

A filter is a callable that takes a generator of Tokens (either a tokenizer or
another filter) and in turn yields a series of Tokens.

For example, the provided :meth:`whoosh.analysis.LowercaseFilter` filters tokens
by converting their text to lowercase. The implementation is very simple::

    def LowercaseFilter(tokens):
        """Uses lower() to lowercase token text. For example, tokens
        "This","is","a","TEST" become "this","is","a","test".
        """

        for t in tokens:
            t.text = t.text.lower()
            yield t

You can wrap the filter around a tokenizer to see it in operation::

    >>> from whoosh.analysis import LowercaseFilter
    >>> for token in LowercaseFilter(tokenizer(u"These ARE the things I want!")):
    ...   print repr(token.text)
    u'these'
    u'are'
    u'the'
    u'things'
    u'i'
    u'want'

An analyzer is just a means of combining a tokenizer and some filters into a
single package.

You can implement an analyzer as a custom class or function, or compose
tokenizers and filters together using the ``|`` character::

    my_analyzer = RegexTokenizer() | LowercaseFilter() | StopFilter()

The first item must be a tokenizer and the rest must be filters (you can't put a
filter first or a tokenizer after the first item). Note that this only works if at
least the tokenizer is a subclass of ``whoosh.analysis.Composable``, as all the
tokenizers and filters that ship with Whoosh are.

See the :mod:`whoosh.analysis` module for information on the available analyzers,
tokenizers, and filters shipped with Whoosh.


Using analyzers
===============

When you create a field in a schema, you can specify your analyzer as a keyword
argument to the field object::

    schema = Schema(content=TEXT(analyzer=StemmingAnalyzer()))


Advanced Analysis
=================

Token objects
-------------

The ``Token`` class has no methods. It is merely a place to record certain
attributes. A ``Token`` object actually has two kinds of attributes: *settings*
that record what kind of information the ``Token`` object does or should contain,
and *information* about the current token.


Token setting attributes
------------------------

A ``Token`` object should always have the following attributes. A tokenizer or
filter can check these attributes to see what kind of information is available
and/or what kind of information they should be setting on the ``Token`` object.

These attributes are set by the tokenizer when it creates the Token(s), based on
the parameters passed to it from the Analyzer.

Filters **should not** change the values of these attributes.

====== ================ =================================================== =========
Type   Attribute name   Description                                         Default
====== ================ =================================================== =========
str    mode             The mode in which the analyzer is being called,     ''
                        e.g. 'index' during indexing or 'query' during
                        query parsing
bool   positions        Whether term positions are recorded in the token    False
bool   chars            Whether term start and end character indices are    False
                        recorded in the token
bool   boosts           Whether per-term boosts are recorded in the token   False
bool   removestops      Whether stop-words should be removed from the       True
                        token stream
====== ================ =================================================== =========


Token information attributes
----------------------------

A ``Token`` object may have any of the following attributes. The ``text`` attribute
should always be present. The original attribute may be set by a tokenizer. All
other attributes should only be accessed or set based on the values of the
"settings" attributes above.

======== ========== =================================================================
Type     Name       Description
======== ========== =================================================================
unicode  text       The text of the token (this should always be present)
unicode  original   The original (pre-filtered) text of the token. The tokenizer may
                    record this, and filters are expected not to modify it.
int      pos        The position of the token in the stream, starting at 0
                    (only set if positions is True)
int      startchar  The character index of the start of the token in the original
                    string (only set if chars is True)
int      endchar    The character index of the end of the token in the original
                    string (only set if chars is True)
float    boost      The boost for this token (only set if boosts is True)
bool     stopped    Whether this token is a "stop" word
                    (only set if removestops is False)
======== ========== =================================================================

So why are most of the information attributes optional? Different field formats
require different levels of information about each token. For example, the
``Frequency`` format only needs the token text. The ``Positions`` format records term
positions, so it needs them on the ``Token``. The ``Characters`` format records term
positions and the start and end character indices of each term, so it needs them
on the token, and so on.

The ``Format`` object that represents the format of each field calls the analyzer
for the field, and passes it parameters corresponding to the types of
information it needs, e.g.::

    analyzer(unicode_string, positions=True)

The analyzer can then pass that information to a tokenizer so the tokenizer
initializes the required attributes on the ``Token`` object(s) it produces.


Performing different analysis for indexing and query parsing
------------------------------------------------------------

Whoosh sets the ``mode`` setting attribute to indicate whether the analyzer is
being called by the indexer (``mode='index'``) or the query parser
(``mode='query'``). This is useful if there's a transformation that you only
want to apply at indexing or query parsing::

    class MyFilter(Filter):
        def __call__(self, tokens):
            for t in tokens:
                if t.mode == 'query':
                    ...
                else:
                    ...

The :class:`whoosh.analysis.MultiFilter` filter class lets you specify different
filters to use based on the mode setting::

    intraword = MultiFilter(index=IntraWordFilter(mergewords=True, mergenums=True),
                            query=IntraWordFilter(mergewords=False, mergenums=False))


Stop words
----------

"Stop" words are words that are so common it's often counter-productive to index
them, such as "and", "or", "if", etc. The provided ``analysis.StopFilter`` lets you
filter out stop words, and includes a default list of common stop words.

::

    >>> from whoosh.analysis import StopFilter
    >>> stopper = StopFilter()
    >>> for token in stopper(LowercaseFilter(tokenizer(u"These ARE the things I want!"))):
    ...   print repr(token.text)
    u'these'
    u'things'
    u'want'

However, this seemingly simple filter idea raises a couple of minor but slightly
thorny issues: renumbering term positions and keeping or removing stopped words.


Renumbering term positions
--------------------------

Remember that analyzers are sometimes asked to record the position of each token
in the token stream:

============= ========== ========== ========== ==========
Token.text    u'Mary'    u'had'     u'a'       u'lamb'
Token.pos     0          1          2          3
============= ========== ========== ========== ==========

So what happens to the ``pos`` attribute of the tokens if ``StopFilter`` removes
the words ``had`` and ``a`` from the stream? Should it renumber the positions to
pretend the "stopped" words never existed? I.e.:

============= ========== ==========
Token.text    u'Mary'    u'lamb'
Token.pos     0          1
============= ========== ==========

or should it preserve the original positions of the words? I.e:

============= ========== ==========
Token.text    u'Mary'    u'lamb'
Token.pos     0          3
============= ========== ==========

It turns out that different situations call for different solutions, so the
provided ``StopFilter`` class supports both of the above behaviors. Renumbering
is the default, since that is usually the most useful and is necessary to
support phrase searching. However, you can set a parameter in StopFilter's
constructor to tell it not to renumber positions::

    stopper = StopFilter(renumber=False)


Removing or leaving stop words
------------------------------

The point of using ``StopFilter`` is to remove stop words, right? Well, there
are actually some situations where you might want to mark tokens as "stopped"
but not remove them from the token stream.

For example, if you were writing your own query parser, you could run the user's
query through a field's analyzer to break it into tokens. In that case, you
might want to know which words were "stopped" so you can provide helpful
feedback to the end user (e.g. "The following words are too common to search
for:").

In other cases, you might want to leave stopped words in the stream for certain
filtering steps (for example, you might have a step that looks at previous
tokens, and want the stopped tokens to be part of the process), but then remove
them later.

The ``analysis`` module provides a couple of tools for keeping and removing
stop-words in the stream.

The ``removestops`` parameter passed to the analyzer's ``__call__`` method (and
copied to the ``Token`` object as an attribute) specifies whether stop words should
be removed from the stream or left in.

::

    >>> from whoosh.analysis import StandardAnalyzer
    >>> analyzer = StandardAnalyzer()
    >>> [(t.text, t.stopped) for t in analyzer(u"This is a test")]
    [(u'test', False)]
    >>> [(t.text, t.stopped) for t in analyzer(u"This is a test", removestops=False)]
    [(u'this', True), (u'is', True), (u'a', True), (u'test', False)]

The ``analysis.unstopped()`` filter function takes a token generator and yields
only the tokens whose ``stopped`` attribute is ``False``.

.. note::
    Even if you leave stopped words in the stream in an analyzer you use for
    indexing, the indexer will ignore any tokens where the ``stopped``
    attribute is ``True``.


Implementation notes
--------------------

Because object creation is slow in Python, the stock tokenizers do not create a
new ``analysis.Token`` object for each token. Instead, they create one ``Token`` object
and yield it over and over. This is a nice performance shortcut but can lead to
strange behavior if your code tries to remember tokens between loops of the
generator.

Because the analyzer only has one ``Token`` object, of which it keeps changing the
attributes, if you keep a copy of the Token you get from a loop of the
generator, it will be changed from under you. For example::

    >>> list(tokenizer(u"Hello there my friend"))
    [Token(u"friend"), Token(u"friend"), Token(u"friend"), Token(u"friend")]

Instead, do this::

    >>> [t.text for t in tokenizer(u"Hello there my friend")]

That is, save the attributes, not the token object itself.

If you implement your own tokenizer, filter, or analyzer as a class, you should
implement an ``__eq__`` method. This is important to allow comparison of ``Schema``
objects.

The mixing of persistent "setting" and transient "information" attributes on the
``Token`` object is not especially elegant. If I ever have a better idea I might
change it. ;) Nothing requires that an Analyzer be implemented by calling a
tokenizer and filters. Tokenizers and filters are simply a convenient way to
structure the code. You're free to write an analyzer any way you want, as long
as it implements ``__call__``.



