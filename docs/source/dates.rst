================================
Indexing and parsing dates/times
================================

Indexing dates
==============

Whoosh lets you index and search dates/times using the
:class:`whoosh.fields.DATETIME` field type. Instead of passing text for the
field in ``add_document()``, you use a Python ``datetime.datetime`` object::

    from datetime import datetime, timedelta
    from whoosh import fields, index

    schema = fields.Schema(title=fields.TEXT, content=fields.TEXT,
                           date=fields.DATETIME)
    ix = index.create_in("indexdir", schema)

    w = ix.writer()
    w.add_document(title="Document 1", content="Rendering images from the command line",
                   date=datetime.utcnow())
    w.add_document(title="Document 2", content="Creating shaders using a node network",
                   date=datetime.utcnow() + timedelta(days=1))
    w.commit()


Parsing date queries
====================

Once you've have an indexed ``DATETIME`` field, you can search it using a rich
date parser contained in the :class:`whoosh.qparser.dateparse.DateParserPlugin`::

    from whoosh import index
    from whoosh.qparser import QueryParser
    from whoosh.qparser.dateparse import DateParserPlugin

    ix = index.open_dir("indexdir")

    # Instatiate a query parser
    qp = QueryParser("content", ix.schema)

    # Add the DateParserPlugin to the parser
    qp.add_plugin(DateParserPlugin())

With the ``DateParserPlugin``, users can use date queries such as::

    20050912
    2005 sept 12th
    june 23 1978
    23 mar 2005
    july 1985
    sep 12
    today
    yesterday
    tomorrow
    now
    next friday
    last tuesday
    5am
    10:25:54
    23:12
    8 PM
    4:46 am oct 31 2010
    last tuesday to today
    today to next friday
    jan 2005 to feb 2008
    -1 week to now
    now to +2h
    -1y6mo to +2 yrs 23d

Normally, as with other types of queries containing spaces, the users need
to quote date queries containing spaces using single quotes::

    render date:'last tuesday' command
    date:['last tuesday' to 'next friday']

If you use the ``free`` argument to the ``DateParserPlugin``, the plugin will
try to parse dates from unquoted text following a date field prefix::

    qp.add_plugin(DateParserPlugin(free=True))

This allows the user to type a date query with spaces and special characters
following the name of date field and a colon. The date query can be mixed
with other types of queries without quotes::

    date:last tuesday
    render date:oct 15th 2001 5:20am command

If you don't use the ``DateParserPlugin``, users can still search DATETIME
fields using a simple numeric form ``YYYY[MM[DD[hh[mm[ss]]]]]`` that is built
into the ``DATETIME`` field::

    from whoosh import index
    from whoosh.qparser import QueryParser

    ix = index.open_dir("indexdir")
    qp = QueryParser("content", schema=ix.schema)

    # Find all datetimes in 2005
    q = qp.parse(u"date:2005")

    # Find all datetimes on June 24, 2005
    q = qp.parse(u"date:20050624")

    # Find all datetimes from 1am-2am on June 24, 2005
    q = qp.parse(u"date:2005062401")

    # Find all datetimes from Jan 1, 2005 to June 2, 2010
    q = qp.parse(u"date:[20050101 to 20100602]")


About time zones and basetime
=============================

The best way to deal with time zones is to always index ``datetime``\ s in native
UTC form. Any ``tzinfo`` attribute on the ``datetime`` object is *ignored*
by the indexer. If you are working with local datetimes, you should convert them
to native UTC datetimes before indexing.


Date parser notes
=================

Please note that the date parser is still somewhat experimental.


Setting the base datetime
-------------------------

When you create the ``DateParserPlugin`` you can pass a ``datetime`` object to
the ``basedate`` argument to set the datetime against which relative queries
(such as ``last tuesday`` and ``-2 hours``) are measured. By default, the
basedate is ``datetime.utcnow()`` at the moment the plugin is instantiated::

    qp.add_plugin(DateParserPlugin(basedate=my_datetime))


Registering an error callback
-----------------------------

To avoid user queries causing exceptions in your application, the date parser
attempts to fail silently when it can't parse a date query. However, you can
register a callback function to be notified of parsing failures so you can
display feedback to the user. The argument to the callback function is the
date text that could not be parsed (this is an experimental feature and may
change in future versions)::

    errors = []
    def add_error(msg):
        errors.append(msg)
    qp.add_plugin(DateParserPlug(callback=add_error))

    q = qp.parse(u"date:blarg")
    # errors == [u"blarg"]


Using free parsing
------------------

While the ``free`` option is easier for users, it may result in ambiguities.
As one example, if you want to find documents containing reference to a march
and the number 2 in documents from the year 2005, you might type::

    date:2005 march 2

This query would be interpreted correctly as a date query and two term queries
when ``free=False``, but as a single date query when ``free=True``. In this
case the user could limit the scope of the date parser with single quotes::

    date:'2005' march 2


Parsable formats
----------------

The date parser supports a wide array of date and time formats, however it is
not my intention to try to support *all* types of human-readable dates (for
example ``ten to five the friday after next``). The best idea might be to pick
a date format that works and try to train users on it, and if they use one of
the other formats that also works consider it a happy accident.


Limitations
===========

* Since it's based on Python's ``datetime.datetime`` object, the ``DATETIME``
  field shares all the limitations of that class, such as no support for
  dates before year 1 on the proleptic Gregorian calendar. The ``DATETIME``
  field supports practically unlimited dates, so if the ``datetime`` object
  is every improved it could support it. An alternative possibility might
  be to add support for ``mxDateTime`` objects someday.

* The ``DateParserPlugin`` currently only has support for English dates.
  The architecture supports creation of parsers for other languages, and I
  hope to add examples for other languages soon.

* ``DATETIME`` fields do not currently support open-ended ranges. You can
  simulate an open ended range by using an endpoint far in the past or future.




