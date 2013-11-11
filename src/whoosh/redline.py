import os.path
import re
import struct
from bisect import bisect_left, bisect_right, insort
from itertools import chain

from whoosh.compat import next, xrange, iteritems
from whoosh.compat import load, dump


itemheader = struct.Struct("<Hi")
ushort_struct = struct.Struct("<H")
int_struct = struct.Struct("<i")
uint_struct = struct.Struct("<I")
long_struct = struct.Struct("<q")
pack_ushort, unpack_ushort = ushort_struct.pack, ushort_struct.unpack
pack_int, unpack_int = int_struct.pack, int_struct.unpack
pack_uint, unpack_uint = uint_struct.pack, uint_struct.unpack
pack_long, unpack_long = long_struct.pack, long_struct.unpack


class Region(object):
    def __init__(self, start, end, minkey, maxkey, length):
        self.start = start
        self.end = end
        self.minkey = minkey
        self.maxkey = maxkey
        self.length = length

    def __repr__(self):
        return "<%s %r-%r>" % (self.__class__.__name__,
                               self.minkey, self.maxkey)


class RegionReader(object):
    def __init__(self, dbfile, mm, region):
        self._dbfile = dbfile
        self._mm = mm
        self._region = region

        self._start = region.start
        self._end = region.end
        self.minkey = region.minkey
        self.maxkey = region.maxkey
        self._length = region.length
        self.loaded = False

        self._poses = None
        self._index = None

    def load(self):
        f = self._dbfile
        _read = f.read
        _unpack = itemheader.unpack
        _headersize = itemheader.size

        pos = self._start
        f.seek(pos)
        for i in xrange(self._length):
            keylen, vlen = _unpack(_read(_headersize))
            pos += _headersize
            key = _read(keylen)
            pos += keylen

            self._poses[key] = pos
            pos += vlen

        assert f.tell() == pos == self._end
        self.loaded = True

    def __getitem__(self, key):
        pos = self._poses[key]
        return self._mm[pos]


def write_regions(dbfile, items, maxsize):
    _write = dbfile.write
    _pack = itemheader.pack
    _headersize = itemheader.size

    start = dbfile.tell()
    minkey = None
    size = 0
    length = 0

    key = None
    for key, value in items:
        if minkey is None:
            minkey = key

        _write(_pack(len(key), len(value)) + key + value)
        size += _headersize + len(key) + len(value)
        length += 1

        if size >= maxsize:
            end = dbfile.tell()
            reg = Region(start, end, minkey, key, length)
            yield reg

            size = 0
            length = 0
            minkey = None
            start = end

    if length:
        assert minkey is not None and key is not None
        reg = Region(start, dbfile.tell(), minkey, key, length)
        yield reg


def read_region(dbfile, region, start=None):
    _read = dbfile.read
    _unpack = itemheader.unpack
    _headersize = itemheader.size

    start = start if start is not None else region.start
    dbfile.seek(start)

    first = True
    for i in xrange(region.length):
        keylen, vlen = _unpack(_read(_headersize))
        key = _read(keylen)
        val = _read(vlen)

        if first:
            assert key == region.minkey
            first = False

        yield key, val

    assert dbfile.tell() == region.end


def bisect_regions(regions, key):
    # Find the index of the region that would contain the given key

    lo = 0
    hi = len(regions)
    while lo < hi:
        mid = (lo + hi) // 2
        region = regions[mid]

        if region.minkey <= key <= region.maxkey:
            return mid
        elif region.maxkey < key:
            lo = mid + 1
        else:
            hi = mid

    return lo


def segment_keys(regions, keys):
    if not keys:
        return

    k1 = keys[0]
    kn = keys[-1]

    if not regions or k1 > regions[-1].maxkey or kn < regions[0].minkey:
        return [(keys, None)]

    new = []
    left = 0
    r = bisect_regions(regions, k1)

    while left < len(keys) and r < len(regions):
        leftkey = keys[left]
        region = regions[r]

        if leftkey > region.maxkey:
            r += 1
        elif leftkey < region.minkey:
            right = bisect_left(keys, region.minkey, left)
            new.append((keys[left:right], None))
            left = right
        else:
            right = bisect_right(keys, region.maxkey, left)
            new.append((keys[left:right], region))
            left = right
            regions.pop(r)

    if left < len(keys):
        new.append((keys[left:], None))

    return new


def merge_items(olditems, newitems):
    i = 0
    _len = len(newitems)
    for item in olditems:
        key = item[0]

        # Yield any items in newitems that come before the current key
        # in the iterator
        while i < _len and newitems[i][0] < key:
            yield newitems[i]
            i += 1

        # newitems override olditems
        if i < _len and newitems[i][0] == key:
            item = newitems[i]
            i += 1

            # If the value is a tombstone, swallow the item
            if item[1] is None:
                continue

        yield item

    if i < _len:
        for item in newitems[i:]:
            yield item





