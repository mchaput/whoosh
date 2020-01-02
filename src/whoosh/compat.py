import sys
from struct import pack


if sys.version_info[0] < 3:
    def b(s):
        return s

    integer_types = (int, long)
    long_type = long
    string_type = basestring
    text_type = unicode
    bytes_type = bytes

else:
    integer_types = (int,)
    long_type = int
    string_type = str
    text_type = str
    bytes_type = bytes


def byte(num):
    return pack("B", num)

