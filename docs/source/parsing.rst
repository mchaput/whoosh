====================
Parsing user queries
====================

Overview
========

The job of a query parser is to convert a *query string* submitted by a user
into *query objects* (objects from the :mod:`whoosh.query` module).

For example, the user query:

.. code-block:: none

    rendering shading

might be parsed into query objects like this::

    And([Term("content", u"rendering"), Term("content", u"shading")])

Whoosh includes a powerful, modular parser for user queries in the
:mod:`whoosh.qparser` module. The default parser implements a query language
similar to the one that ships with Lucene. However, by changing plugins or using
functions such as :func:`whoosh.qparser.MultifieldParser`,
:func:`whoosh.qparser.SimpleParser` or :func:`whoosh.qparser.DisMaxParser`, you
can change how the parser works, get a simpler parser or change the query
language syntax.

(In previous versions of Whoosh, the query parser was based on ``pyparsing``.
The new hand-written parser is less brittle and more flexible.)

.. note::

    Remember that you can directly create query objects programmatically using
    the objects in the :mod:`whoosh.query` module. If you are not processing
    actual user queries, this is preferable to building a query string just to
    parse it.


Using the default parser
========================

To create a :class:`whoosh.qparser.QueryParser` object, pass it the name of the
*default field* to search and the schema of the index you'll be searching.

::

    from whoosh.qparser import QueryParser

    parser = QueryParser("content", schema=myindex.schema)

.. tip::

    You can instantiate a ``QueryParser`` object without specifying a schema,
    however the parser will not process the text of the user query. This is
    useful for debugging, when you want to see how QueryParser will build a
    query, but don't want to make up a schema just for testing.

Once you have a ``QueryParser`` object, you can call ``parse()`` on it to parse a
query string into a query object::

    >>> parser.parse(u"alpha OR beta gamma")
    Or([Term("content", u"alpha"), Term("content", "beta")])

See the :doc:`query language reference <querylang>` for the features and syntax
of the default parser's query language.


Common customizations
=====================

Searching for any terms instead of all terms by default
-------------------------------------------------------

If the user doesn't explicitly specify ``AND`` or ``OR`` clauses::

    physically based rendering

...by default, the parser treats the words as if they were connected by ``AND``,
meaning all the terms must be present for a document to match::

    physically AND based AND rendering

To change the parser to use ``OR`` instead, so that any of the terms may be
present for a document to match, i.e.::

    physically OR based OR rendering

...configure the QueryParser using the ``group`` keyword argument like this::

    from whoosh import qparser

    parser = qparser.QueryParser(fieldname, schema=myindex.schema,
                                 group=qparser.OrGroup)

The Or query lets you specify that documents that contain more of the query
terms score higher. For example, if the user searches for ``foo bar``, a
document with four occurances of ``foo`` would normally outscore a document
that contained one occurance each of ``foo`` and ``bar``. However, users
usually expect documents that contain more of the words they searched for
to score higher. To configure the parser to produce Or groups with this
behavior, use the ``factory()`` class method of ``OrGroup``::

	og = qparser.OrGroup.factory(0.9)
	parser = qparser.QueryParser(fieldname, schema, group=og)

where the argument to ``factory()`` is a scaling factor on the bonus
(between 0 and 1).


Letting the user search multiple fields by default
--------------------------------------------------

The default QueryParser configuration takes terms without explicit fields and
assigns them to the default field you specified when you created the object, so
for example if you created the object with::

    parser = QueryParser("content", schema=myschema)

And the user entered the query:

.. code-block:: none

    three blind mice

The parser would treat it as:

.. code-block:: none

    content:three content:blind content:mice

However, you might want to let the user search *multiple* fields by default. For
example, you might want "unfielded" terms to search both the ``title`` and
``content`` fields.

In that case, you can use a :class:`whoosh.qparser.MultifieldParser`. This is
just like the normal QueryParser, but instead of a default field name string, it
takes a *sequence* of field names::

    from whoosh.qparser import MultifieldParser

    mparser = MultifieldParser(["title", "content"], schema=myschema)

When this MultifieldParser instance parses ``three blind mice``, it treats it
as:

.. code-block:: none

    (title:three OR content:three) (title:blind OR content:blind) (title:mice OR content:mice)


Simplifying the query language
------------------------------

Once you have a parser::

    parser = qparser.QueryParser("content", schema=myschema)

you can remove features from it using the
:meth:`~whoosh.qparser.QueryParser.remove_plugin_class` method.

For example, to remove the ability of the user to specify fields to search::

    parser.remove_plugin_class(qparser.FieldsPlugin)

To remove the ability to search for wildcards, which can be harmful to query
performance::

    parser.remove_plugin_class(qparser.WildcardPlugin)

See :doc:`/api/qparser` for information about the plugins included with
Whoosh's query parser.


Changing the AND, OR, ANDNOT, ANDMAYBE, and NOT syntax
------------------------------------------------------

The default parser uses English keywords for the AND, OR, ANDNOT, ANDMAYBE,
and NOT functions::

    parser = qparser.QueryParser("content", schema=myschema)

You can replace the default ``CompoundsPlugin`` and ``NotPlugin`` objects to
replace the default English tokens with your own regular expressions.

The :class:`whoosh.qparser.CompoundsPlugin` implements the ability to use AND,
OR, ANDNOT, and ANDMAYBE clauses in queries. You can instantiate a new
``CompoundsPlugin`` and use the ``And``, ``Or``, ``AndNot``, and ``AndMaybe``
keyword arguments to change the token patterns::

    # Use Spanish equivalents instead of AND and OR
    cp = qparser.CompoundsPlugin(And=" Y ", Or=" O ")
    parser.replace_plugin(cp)

The :class:`whoosh.qparser.NotPlugin` implements the ability to logically NOT
subqueries. You can instantiate a new ``NotPlugin`` object with a different
token::

    np = qparser.NotPlugin("NO ")
    parser.replace_plugin(np)

The arguments can be pattern strings or precompiled regular expression objects.

For example, to change the default parser to use typographic symbols instead of
words for the AND, OR, ANDNOT, ANDMAYBE, and NOT functions::

    parser = qparser.QueryParser("content", schema=myschema)
    # These are regular expressions, so we have to escape the vertical bar
    cp = qparser.CompoundsPlugin(And="&", Or="\\|", AndNot="&!", AndMaybe="&~")
    parser.replace_plugin(cp)
    parser.replace_plugin(qparser.NotPlugin("!"))


Adding less-than, greater-than, etc.
------------------------------------

Normally, the way you match all terms in a field greater than "apple" is with
an open ended range::

    field:{apple to]

The :class:`whoosh.qparser.GtLtPlugin` lets you specify the same search like
this::

    field:>apple

The plugin lets you use ``>``, ``<``, ``>=``, ``<=``, ``=>``, or ``=<`` after
a field specifier, and translates the expression into the equivalent range::

    date:>='31 march 2001'

    date:[31 march 2001 to]


Adding fuzzy term queries
-------------------------

Fuzzy queries are good for catching misspellings and similar words.
The :class:`whoosh.qparser.FuzzyTermPlugin` lets you search for "fuzzy" terms,
that is, terms that don't have to match exactly. The fuzzy term will match any
similar term within a certain number of "edits" (character insertions,
deletions, and/or transpositions -- this is called the "Damerau-Levenshtein
edit distance").

To add the fuzzy plugin::

    parser = qparser.QueryParser("fieldname", my_index.schema)
    parser.add_plugin(qparser.FuzzyTermPlugin())

Once you add the fuzzy plugin to the parser, you can specify a fuzzy term by
adding a ``~`` followed by an optional maximum edit distance. If you don't
specify an edit distance, the default is ``1``.

For example, the following "fuzzy" term query::

    cat~

would match ``cat`` and all terms in the index within one "edit" of cat,
for example ``cast`` (insert ``s``), ``at`` (delete ``c``), and ``act``
(transpose ``c`` and ``a``).

If you wanted ``cat`` to match ``bat``, it requires two edits (delete ``c`` and
insert ``b``) so you would need to set the maximum edit distance to ``2``::

    cat~2

Because each additional edit you allow increases the number of possibilities
that must be checked, edit distances greater than ``2`` can be very slow.

It is often useful to require that the first few characters of a fuzzy term
match exactly. This is called a prefix. You can set the length of the prefix
by adding a slash and a number after the edit distance. For example, to use
a maximum edit distance of ``2`` and a prefix length of ``3``::

    johannson~2/3

You can specify a prefix without specifying an edit distance::

    johannson~/3

The default prefix distance is ``0``.


Allowing complex phrase queries
-------------------------------

The default parser setup allows phrase (proximity) queries such as::

    "whoosh search library"

The default phrase query tokenizes the text between the quotes and creates a
search for those terms in proximity.

If you want to do more complex proximity searches, you can replace the phrase
plugin with the :class:`whoosh.qparser.SequencePlugin`, which allows any query
between the quotes. For example::

    "(john OR jon OR jonathan~) peters*"

The sequence syntax lets you add a "slop" factor just like the regular phrase::

    "(john OR jon OR jonathan~) peters*"~2

To replace the default phrase plugin with the sequence plugin::

    parser = qparser.QueryParser("fieldname", my_index.schema)
    parser.remove_plugin_class(qparser.PhrasePlugin)
    parser.add_plugin(qparser.SequencePlugin())

Alternatively, you could keep the default phrase plugin and give the sequence
plugin different syntax by specifying a regular expression for the start/end
marker when you create the sequence plugin. The regular expression should have
a named group ``slop`` for the slop factor. For example::

    parser = qparser.QueryParser("fieldname", my_index.schema)
    parser.add_plugin(qparser.SequencePlugin("!(~(?P<slop>[1-9][0-9]*))?"))

This would allow you to use regular phrase queries and sequence queries at the
same time::

    "regular phrase" AND !sequence query~2!


Advanced customization
======================

QueryParser arguments
---------------------

QueryParser supports two extra keyword arguments:

``group``
    The query class to use to join sub-queries when the user doesn't explicitly
    specify a boolean operator, such as ``AND`` or ``OR``. This lets you change
    the default operator from ``AND`` to ``OR``.

    This will be the :class:`whoosh.qparser.AndGroup` or
    :class:`whoosh.qparser.OrGroup` class (*not* an instantiated object) unless
    you've written your own custom grouping syntax you want to use.

``termclass``
    The query class to use to wrap single terms.

    This must be a :class:`whoosh.query.Query` subclass (*not* an instantiated
    object) that accepts a fieldname string and term text unicode string in its
    ``__init__`` method. The default is :class:`whoosh.query.Term`.

    This is useful if you want to change the default term class to
    :class:`whoosh.query.Variations`, or if you've written a custom term class
    you want the parser to use instead of the ones shipped with Whoosh.

::

    >>> from whoosh.qparser import QueryParser, OrGroup
    >>> orparser = QueryParser("content", schema=myschema, group=OrGroup)


Configuring plugins
-------------------

The query parser's functionality is provided by a set of plugins. You can
remove plugins to remove functionality, add plugins to add functionality, or
replace default plugins with re-configured or rewritten versions.

The :meth:`whoosh.qparser.QueryParser.add_plugin`,
:meth:`whoosh.qparser.QueryParser.remove_plugin_class`, and
:meth:`whoosh.qparser.QueryParser.replace_plugin` methods let you manipulate
the plugins in a ``QueryParser`` object.

See :doc:`/api/qparser` for information about the available plugins.


.. _custom-op:

Creating custom operators
-------------------------

* Decide whether you want a ``PrefixOperator``, ``PostfixOperator``, or ``InfixOperator``.

* Create a new :class:`whoosh.qparser.syntax.GroupNode` subclass to hold
  nodes affected by your operator. This object is responsible for generating
  a :class:`whoosh.query.Query` object corresponding to the syntax.

* Create a regular expression pattern for the operator's query syntax.

* Create an ``OperatorsPlugin.OpTagger`` object from the above information.

* Create a new ``OperatorsPlugin`` instance configured with your custom
  operator(s).

* Replace the default ``OperatorsPlugin`` in your parser with your new instance.

For example, if you were creating a ``BEFORE`` operator::

    from whoosh import qparser, query

    optype = qparser.InfixOperator
    pattern = " BEFORE "

    class BeforeGroup(qparser.GroupNode):
        merging = True
        qclass = query.Ordered

Create an OpTagger for your operator::

    btagger = qparser.OperatorPlugin.OpTagger(pattern, BeforeGroup,
                                              qparser.InfixOperator)

By default, infix operators are left-associative. To make a right-associative
infix operator, do this::

    btagger = qparser.OperatorPlugin.OpTagger(pattern, BeforeGroup,
                                              qparser.InfixOperator,
                                              leftassoc=False)

Create an :class:`~whoosh.qparser.plugins.OperatorsPlugin` instance with your
new operator, and replace the default operators plugin in your query parser::

    qp = qparser.QueryParser("text", myschema)
    my_op_plugin = qparser.OperatorsPlugin([(btagger, 0)])
    qp.replace_plugin(my_op_plugin)

Note that the list of operators you specify with the first argument is IN
ADDITION TO the default operators (AND, OR, etc.). To turn off one of the
default operators, you can pass None to the corresponding keyword argument::

    cp = qparser.OperatorsPlugin([(optagger, 0)], And=None)

If you want ONLY your list of operators and none of the default operators,
use the ``clean`` keyword argument::

    cp = qparser.OperatorsPlugin([(optagger, 0)], clean=True)

Operators earlier in the list bind more closely than operators later in the
list.





