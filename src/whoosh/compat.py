import sys

if sys.version_info[0] < 3:
    PY3 = False

    def b(s):
        return s

    import cStringIO as StringIO
    StringIO = BytesIO = StringIO.StringIO
    callable = callable
    integer_types = (int, long)
    iteritems = lambda o: o.iteritems()
    itervalues = lambda o: o.itervalues()
    iterkeys = lambda o: o.iterkeys()
    from itertools import izip
    long_type = long
    next = lambda o: o.next()
    import cPickle as pickle
    from cPickle import dumps, loads, dump, load
    string_type = basestring
    text_type = unicode
    unichr = unichr
    from urllib import urlretrieve

    def u(s):
        return unicode(s, "unicode_escape")

    def with_metaclass(meta, base=object):
        class _WhooshBase(base):
            __metaclass__ = meta
        return _WhooshBase

    xrange = xrange
    zip_ = zip
else:
    PY3 = True
    import collections

    def b(s):
        return s.encode("latin-1")

    import io
    BytesIO = io.BytesIO
    callable = lambda o: isinstance(o, collections.Callable)
    exec_ = eval("exec")
    integer_types = (int,)
    iteritems = lambda o: o.items()
    itervalues = lambda o: o.values()
    iterkeys = lambda o: iter(o.keys())
    izip = zip
    long_type = int
    next = next
    import pickle
    from pickle import dumps, loads, dump, load
    StringIO = io.StringIO
    string_type = str
    text_type = str
    unichr = chr
    from urllib.request import urlretrieve

    def u(s):
        return s

    def with_metaclass(meta, base=object):
        ns = dict(base=base, meta=meta)
        exec_("""class _WhooshBase(base, metaclass=meta):
    pass""", ns)
        return ns["_WhooshBase"]

    xrange = range
    zip_ = lambda * args: list(zip(*args))
