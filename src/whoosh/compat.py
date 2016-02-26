import array
import sys


if sys.version_info[0] < 3:
    from itertools import izip_longest as zip_longest

    def b(s):
        return s

    integer_types = (int, long)
    long_type = long
    string_type = basestring
    text_type = unicode
    bytes_type = str

    def byte(num):
        return chr(num)

    def with_metaclass(meta, base=object):
        class _WhooshBase(base):
            __metaclass__ = meta
        return _WhooshBase

else:
    from itertools import zip_longest

    integer_types = (int,)
    long_type = int
    string_type = str
    text_type = str
    bytes_type = bytes

    def byte(num):
        return bytes((num,))

    def with_metaclass(meta, base=object):
        ns = dict(base=base, meta=meta)
        exec_("""class _WhooshBase(base, metaclass=meta):
    pass""", ns)
        return ns["_WhooshBase"]


if hasattr(array.array, "tobytes"):
    def array_tobytes(arry):
        return arry.tobytes()

    def array_frombytes(arry, bs):
        return arry.frombytes(bs)
else:
    def array_tobytes(arry):
        return arry.tostring()

    def array_frombytes(arry, bs):
        return arry.fromstring(bs)
