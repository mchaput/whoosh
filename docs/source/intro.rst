======================
Introduction to Whoosh
======================

About Whoosh
------------

Whoosh was created by `Matt Chaput <mailto:matt@whoosh.ca>`_. It started as a quick and dirty
search server for the online documentation of the `Houdini <http://www.sidefx.com/>`_
3D animation software package. Side Effects Software generously allowed Matt to open source
the code in case it might be useful to anyone else who needs a very flexible or pure-Python
search engine (or both!).

* Whoosh is fast, but uses only pure Python, so it will run anywhere Python runs,
  without requiring a compiler.

* By default, Whoosh uses the `Okapi BM25F <http://en.wikipedia.com/wiki/BM25>`_ ranking
  function, but like most things the ranking function can be easily customized.

* Whoosh creates fairly small indexes compared to many other search libraries.

* All indexed text in Whoosh must be *unicode*.

* Whoosh lets you store arbitrary Python objects with indexed documents.


What is Whoosh?
---------------

Whoosh is a fast, pure Python search engine library.

The primary design impetus of Whoosh is that it is pure Python. You should be able to
use Whoosh anywhere you can use Python, no compiler or Java required.

Like one of its ancestors, Lucene, Whoosh is not really a search engine, it's a programmer
library for creating a search engine [1]_.

Practically no important behavior of Whoosh is hard-coded. Indexing
of text, the level of information stored for each term in each field, parsing of search queries,
the types of queries allowed, scoring algorithms, etc. are all customizable, replaceable, and
extensible.


.. [1] It would of course be possible to build a turnkey search engine on top of Whoosh,
       like Nutch and Solr use Lucene.


What can Whoosh do for you?
---------------------------

Whoosh lets you index free-form or structured text and then quickly find matching
documents based on simple or complex search criteria.


Getting help with Whoosh
------------------------

You can view outstanding issues on the
`Whoosh Bitbucket page <http://bitbucket.org/mchaput/whoosh>`_
and get help on the `Whoosh mailing list <http://groups.google.com/group/whoosh>`_.
