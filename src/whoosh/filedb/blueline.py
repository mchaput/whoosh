# Copyright 2015 Matt Chaput. All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#    1. Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#
#    2. Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY MATT CHAPUT ``AS IS'' AND ANY EXPRESS OR
# IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
# EVENT SHALL MATT CHAPUT OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
# OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
# EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
# The views and conclusions contained in the software and documentation are
# those of the authors and should not be interpreted as representing official
# policies, either expressed or implied, of Matt Chaput.

import logging
import struct
from abc import abstractmethod
from array import array
from collections import deque
from genericpath import commonprefix
from struct import Struct
from typing import Iterable, List, Optional, Sequence, Tuple, Union

from whoosh.filedb.datafile import Data, OutputFile
from whoosh.metadata import MetaData
from whoosh.system import IS_LITTLE
from whoosh.util.numlists import min_array_code


logger = logging.getLogger(__name__)


# Constants

MAX_KEY_LENGTH = 32767 * 2 + 1


# Exceptions

class KeyLengthError(Exception):
    pass


class RegionNotFound(Exception):
    pass


class InvalidCursor(Exception):
    pass


# Region reference

class Ref(object):
    __slots__ = ("offset", "count", "minkey", "maxkey", "end_offset")

    # I - offset
    # H - count (number of items in the region)
    # i - minkey length
    # i - maxkey length
    header = Struct("<IHii")

    def __init__(self, offset: int, count: int, minkey: bytes, maxkey: bytes,
                 end_offset: int=None):
        self.offset = offset
        self.count = count
        self.minkey = minkey
        self.maxkey = maxkey
        self.end_offset = end_offset

    def __eq__(self, other):
        return (type(self) == type(other) and
                self.offset == other.offset and
                self.count == other.count and
                self.minkey == other.minkey and
                self.maxkey == other.maxkey)

    def __ne__(self, other):
        return not self == other

    def __repr__(self):
        return "<Ref @%d %d %r-%r>" % (self.offset, self.count, self.minkey,
                                       self.maxkey)

    def __len__(self):
        return self.count

    def to_bytes(self) -> bytes:
        return (self.header.pack(self.offset, self.count,
                                 len(self.minkey), len(self.maxkey)) +
                self.minkey +
                self.maxkey
                )

    @classmethod
    def from_bytes(cls, bs: Union[Data, bytes], offset: int) -> 'Ref':
        """
        Reads a frozen Ref and returns a tuple of the Ref and its end offset so
        you can read the next ref in the file.

        :param bs: the source bytes.
        :param offset: the offset at which to read.
        """

        h = cls.header
        end = offset + h.size
        offset, count, minklen, maxklen = h.unpack(bs[offset:end])
        minkey = bytes(bs[end:end + minklen])
        last = end + minklen + maxklen
        maxkey = bytes(bs[end + minklen:last])
        return cls(offset, count, minkey, maxkey, last)


# Region header

class RegionHeader(MetaData):
    magic_bytes = b"Regn"
    version_number = 0
    flags = "was_little klen_fixed vlen_fixed"
    field_order = ("count size poscode klencode vlencode prefixlen prefixbytes "
                   "fixedklen fixedvlen")

    was_little = False  # was created on a little-endian machine
    klen_fixed = False  # all keys have the same length
    vlen_fixed = False  # all values have the same length

    count = "i"  # number of items in this region
    size = "i"  # length of item data in bytes
    poscode = "c"  # typecode for positions array
    klencode = "c"  # typecode for key lengths array
    vlencode = "c"  # typecode for value lengths array
    prefixlen = "B"  # length of common prefix (0-16)
    prefixbytes = "16s"  # common prefix bytes
    fixedklen = "i"  # fixed length of all keys (if klen_fixed)
    fixedvlen = "i"  # fixed length of all values (if vlen_fixed)


# Region IO functions

def write_regions(output: OutputFile, items: Iterable[Tuple[bytes, bytes]],
                  maxsize: int=128) -> Iterable[Ref]:
    """
    Writes key/value pairs to the given file. Yields a Ref object for each
    region as it is written.

    :param output: the file to write to.
    :param items: a sequence of ``(b'key', b'value')`` tuples.
    :param maxsize: the maximum number of items in a region.
    """

    buff = []
    for item in items:
        buff.append(item)
        if len(buff) >= maxsize:
            yield write_region(output, buff)
            buff = []

    if buff:
        yield write_region(output, buff)


def write_region(output: OutputFile, items: List[Tuple[bytes, bytes]]
                 ) -> Ref:
    """
    Writes a list of key/value pairs to the given file, and returns a Ref object
    containing information about the on-disk region.

    :param output: the file to write to.
    :param items: a list of ```(b'key', b'value')`` tuples.
    """

    # Writes a single region to a file
    assert isinstance(items, list)
    assert items

    # Remember the current file offset for the Ref object
    offset = output.tell()

    # Separate the items into key and value lists
    keys = []
    values = []
    for key, value in items:
        if keys and key <= keys[-1]:
            raise Exception("Keys out of order")

        keys.append(key)
        values.append(value)
    minkey = keys[0]
    maxkey = keys[-1]

    # Find any common key prefix and remove it from the keys
    if len(keys) > 1:
        pre = commonprefix(keys)
        prefixlen = 0
        prefixbytes = b''
        if pre:
            prelen = len(pre)
            if prelen > 16:
                pre = pre[:16]
                prelen = 16
            keys = [k[prelen:] for k in keys]
            prefixlen = prelen
            prefixbytes = pre
    else:
        prefixlen = 0
        prefixbytes = b''

    # Create a list of key lengths
    klen_is_fixed = False
    klens = array("i", (len(k) for k in keys))
    firstklen = klens[0]
    fixedklen = 0
    # If all the keys have the same length, store the fixed length in the header
    if all(kl == firstklen for kl in klens):
        klen_is_fixed = True
        fixedklen = firstklen
        klencode = "-"
    else:
        # Minimize the key length array
        klencode = min_array_code(max(klens))
        if klencode != "i":
            klens = array(klencode, klens)

    # Create a list of key lengths
    vlen_is_fixed = False
    vlens = array("i", (len(v) for v in values))
    firstvlen = vlens[0]
    fixedvlen = 0
    # If all the keys have the same length, store the fixed length in the header
    if all(vl == firstvlen for vl in vlens):
        vlen_is_fixed = True
        fixedvlen = firstvlen
        vlencode = "-"
    else:
        # Minimize the value length array
        vlencode = min_array_code(max(vlens))
        if vlencode != "i":
            vlens = array(vlencode, vlens)

    # If the key and value are not both fixed lengths, create a array of the
    # starting position of each key/value pair. We could derive this from the
    # key/value lengths, but we trade space for speed and pre-compute it.
    base = 0
    if klen_is_fixed and vlen_is_fixed:
        poses = None
        poscode = "-"
    else:
        poses = array("i")
        for klen, vlen in zip(klens, vlens):
            poses.append(base)
            base += klen + vlen
        poscode = min_array_code(max(poses))
        if poscode != "i":
            poses = array(poscode, poses)

    # Calculate the size of the arrays + data
    content_size = base
    if not (klen_is_fixed and vlen_is_fixed):
        # Add size of position array
        content_size += len(poses) * poses.itemsize
    if not klen_is_fixed:
        # Add size of key length array
        content_size += len(klens) * klens.itemsize
    if not vlen_is_fixed:
        # Add size of value length array
        content_size += len(vlens) * vlens.itemsize
    # Add length of data
    content_size += sum(klens) + sum(vlens)

    # Write the header
    output.write(RegionHeader(
        was_little=IS_LITTLE, count=len(items),
        prefixlen=prefixlen, prefixbytes=prefixbytes,
        klencode=klencode, klen_fixed=klen_is_fixed, fixedklen=fixedklen,
        vlencode=vlencode, vlen_fixed=vlen_is_fixed, fixedvlen=fixedvlen,
        poscode=poscode, size=content_size,
    ).encode())

    # Write the positions, key lengths, and value lengths if necessary
    if not (klen_is_fixed and vlen_is_fixed):
        poses.tofile(output)
    if not klen_is_fixed:
        klens.tofile(output)
    if not vlen_is_fixed:
        vlens.tofile(output)

    # Join the key/value pairs and write them to disk in one go
    bio = bytearray()
    for key, value in zip(keys, values):
        bio += key + value
    output.write(bio)

    # Return a Ref object pointing to the new region
    return Ref(offset, len(keys), minkey, maxkey, output.tell())


# Region readers

class KeyValueReader(object):
    def close(self):
        pass

    @abstractmethod
    def __getitem__(self, key: bytes) -> bytes:
        raise NotImplementedError

    @abstractmethod
    def __len__(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def __iter__(self) -> Iterable[bytes]:
        raise NotImplementedError

    @abstractmethod
    def key_range(self, start: bytes, end: Optional[bytes]) -> Iterable[bytes]:
        """
        Yields the keys between the given start and end keys.

        :param start: the key to start yielding from.
        :param end: the key to yield up to. If this is None, yield all keys to
            the end.
        """

        raise NotImplementedError

    @abstractmethod
    def items(self) -> Iterable[Tuple[bytes, bytes]]:
        """
        Yields all ``(b'key', b'value')`` pairs in the region.
        """

        raise NotImplementedError(self.__class__.__name__)

    @abstractmethod
    def item_range(self, start: bytes, end: bytes
                   ) -> Iterable[Tuple[bytes, bytes]]:
        """
        Yields all ``(b'key', b'value')`` pairs starting from a certain key.

        :param start: the key to start yielding from.
        :param end: the key to yield up to.
        """

        raise NotImplementedError

    @abstractmethod
    def min_key(self):
        """
        Returns the smallest key in the region.
        """

        raise NotImplementedError

    @abstractmethod
    def max_key(self):
        """
        Returns the largest key in the region.
        """

        raise NotImplementedError

    @abstractmethod
    def cursor(self) -> 'Cursor':
        """
        Returns a cursor view of the items.
        """

        raise NotImplementedError


class Region(KeyValueReader):
    """
    Represents a block of key/value pairs on disk.
    """

    def __init__(self, data: Data, content_start: int, count: int,
                 poses: Sequence[int],
                 klens: Sequence[int], vlens: Sequence[int],
                 prefix: bytes, fixedklen: int, fixedvlen: int,
                 minkey: bytes=None, maxkey: bytes=None,
                 preread_keys: bool=False):
        """
        :param data: the mmap to read from.
        :param content_start: the start position of the items.
        :param count: the number of items.
        :param poses: a list of offsets of the items.
        :param klens: a list of key lengths for the items.
        :param vlens: a list of value lengths for the items.
        :param prefix: a common prefix for all keys.
        :param fixedklen: a common length of all keys, or -1.
        :param fixedvlen: a common length of all values, or -1.
        :param minkey: the smallest key in the region, if already known.
        :param maxkey: the smallest key in the region, if already known.
        :param preread_keys: load all keys into memory. This is faster when
            you know you will access the region linearly, but takes more memory.
        """

        self._data = data
        self._content_start = content_start
        self._count = count
        self._poses = poses
        self._klens = klens
        self._vlens = vlens

        self._prefix = prefix
        self._prefixlen = len(prefix)

        self._fixedklen = fixedklen
        self._fixedvlen = fixedvlen
        self._klen_is_fixed = fixedklen != -1
        self._vlen_is_fixed = fixedvlen != -1
        if self._klen_is_fixed and self._vlen_is_fixed:
            self._fixeditemsize = self._fixedklen + self._fixedvlen
        else:
            self._fixeditemsize = -1

        self._minkey = minkey
        self._maxkey = maxkey

        self._keylist = None
        self._lookup = None
        if preread_keys:
            self.enable_preread()

    def __repr__(self):
        return "<%s %r-%r>" % (type(self).__name__, self._minkey, self._maxkey)

    @classmethod
    def from_ref(cls, data: Data, ref: Ref, load_arrays: bool=False,
                 preread_keys: bool=False) -> 'Region':
        return cls.load(data, ref.offset, minkey=ref.minkey, maxkey=ref.maxkey,
                        load_arrays=load_arrays, preread_keys=preread_keys)

    @classmethod
    def load(cls, data: Data, offset: int=0, minkey: bytes=None,
             maxkey: bytes=None, load_arrays: bool=False,
             preread_keys: bool=False) -> 'Region':
        # Reads the region info from a mmap at the given offset

        # Read and unpack the header struct
        head = RegionHeader.decode(data, offset)
        count = head.count
        assert head.version_number == 0

        # True if this machine matches the endianness of the region
        native = head.was_little == IS_LITTLE

        # Calculate the starts and ends of the arrays
        klen_is_fixed = head.klen_fixed
        vlen_is_fixed = head.vlen_fixed

        poses_size = klens_size = vlens_size = 0
        if not (klen_is_fixed and vlen_is_fixed):
            poses_size = struct.calcsize(head.poscode) * count
        if not klen_is_fixed:
            klens_size = struct.calcsize(head.klencode) * count
        if not vlen_is_fixed:
            vlens_size = struct.calcsize(head.vlencode) * count

        poses_start = offset + RegionHeader.get_size()
        klens_start = poses_start + poses_size
        vlens_start = klens_start + klens_size
        content_start = vlens_start + vlens_size

        poses = klens = vlens = None
        # Get array-like objects for the positions, key lengths, and value
        # lengths, if necessary
        if not (klen_is_fixed and vlen_is_fixed):
            poses = data.map_array(head.poscode, poses_start, count,
                                   load=load_arrays, native=native)
        if not klen_is_fixed:
            klens = data.map_array(head.klencode, klens_start, count,
                                   load=load_arrays, native=native)
        if not vlen_is_fixed:
            vlens = data.map_array(head.vlencode, vlens_start, count,
                                   load=load_arrays, native=native)

        prefix = head.prefixbytes[:head.prefixlen]
        fixedklen = head.fixedklen if head.klen_fixed else -1
        fixedvlen = head.fixedvlen if head.vlen_fixed else -1

        return cls(data, content_start, count, poses, klens, vlens,
                   prefix, fixedklen, fixedvlen, minkey, maxkey,
                   preread_keys=preread_keys)

    def enable_preread(self):
        # logger.debug("Enabling preread on region %r", self)
        self._lookup = dict((key, i) for i, key in enumerate(self._keys()))

    def close(self):
        # If we have memoryviews, release them
        for arry in (self._poses, self._klens, self._vlens):
            if hasattr(arry, "release"):
                arry.release()

    def __len__(self) -> int:
        return self._count

    def __iter__(self) -> Iterable[bytes]:
        return self._keys()

    def __contains__(self, key: bytes) -> bool:
        try:
            suffix = self._unprefix(key)
        except KeyError:
            return False

        if self._lookup and suffix in self._lookup:
            return True

        i = self._suffix_index(suffix)
        if i < self._count:
            x = self._suffix_at(i)
            if x == suffix:
                # self._lookup[suffix] = i
                return True

        return False

    def __getitem__(self, key: bytes):
        if self._lookup is not None:
            i = self._lookup[key]
            return self.value_at(i)

        suffix = self._unprefix(key)
        i = self._suffix_index(suffix)
        if i >= self._count:
            raise KeyError

        if self._suffix_at(i) == suffix:
            # self._lookup[suffix] = i
            return self.value_at(i)
        else:
            raise KeyError(key)

    def _unprefix(self, key: bytes) -> bytes:
        prefixlen = self._prefixlen
        if prefixlen:
            if key.startswith(self._prefix):
                return key[prefixlen:]
            else:
                raise KeyError(key)
        return key

    def _reprefix(self, suffix: bytes) -> bytes:
        if self._prefixlen:
            return self._prefix + suffix
        else:
            return suffix

    def _side(self, key: bytes) -> int:
        # For a key that can't be in this region (because it starts with a
        # different prefix), returns ``0`` if it would be before the keys in
        # this region, or the region length if it would be after the keys in
        # this region
        prefix = key[:self._prefixlen]
        if prefix < self._prefix:
            return 0
        else:
            return self._count

    def _position_at(self, i):
        # Look up the position for the item at the given index
        if self._fixeditemsize >= 0:
            return i * self._fixeditemsize
        else:
            return self._poses[i]

    def _klen_at(self, i):
        # Look up the key length for the item at the given index
        return self._fixedklen if self._klen_is_fixed else self._klens[i]

    def _vlen_at(self, i):
        # Look up the value length for the item at the given index
        return self._fixedvlen if self._vlen_is_fixed else self._vlens[i]

    def _keys(self, lo: int=0, hi: int=None,
              reprefix: bool=True) -> Iterable[bytes]:
        # Yields the keys in the region, starting at index ``lo`` and ending
        # before index ``hi`` (or at the end if ``hi`` is not given).
        _position_at = self._position_at
        _klen_at = self._klen_at
        data = self._data
        contentstart = self._content_start
        prefixlen = self._prefixlen
        prefixbytes = self._prefix

        hi = hi if hi is not None else self._count
        for i in range(lo, hi):
            pos = contentstart + _position_at(i)
            keybytes = bytes(data[pos:pos + _klen_at(i)])

            if reprefix and prefixlen:
                yield prefixbytes + keybytes
            else:
                yield keybytes

    def _items(self, lo: int=0, hi: int=None,
              reprefix: bool=True) -> Iterable[Tuple[bytes, bytes]]:
        # Yields the items in the region, starting at index ``lo`` and ending
        # before index ``hi`` (or at the end if ``hi`` is not given).
        _position_at = self._position_at
        _klen_at = self._klen_at
        _vlen_at = self._vlen_at
        data = self._data
        contentstart = self._content_start
        prefixlen = self._prefixlen
        prefixbytes = self._prefix

        hi = hi if hi is not None else self._count
        for i in range(lo, hi):
            pos = contentstart + _position_at(i)
            klen =  _klen_at(i)
            keybytes = bytes(data[pos:pos + klen])
            valbytes = data[pos + klen:pos + klen + _vlen_at(i)]

            if reprefix and prefixlen:
                yield prefixbytes + keybytes, valbytes
            else:
                yield keybytes, valbytes

    def _ranges(self) -> Iterable[Tuple[int, int, int]]:
        # Yields tuples of (position, key_length, value_length) for each item
        # in the reigon
        datastart = self._content_start
        _position_at = self._position_at
        _klen_at = self._klen_at
        _vlen_at = self._vlen_at

        for i in range(self._count):
            yield datastart + _position_at(i), _klen_at(i), _vlen_at(i)

    def key_index(self, key: bytes, lo: int=0) -> int:
        try:
            key = self._unprefix(key)
        except KeyError:
            # This key can't be in this region because it doesn't share the
            # prefix
            return self._side(key)
        else:
            return self._suffix_index(key, lo)

    def _suffix_index(self, suffix: bytes, lo: int=0) -> int:
        # Returns the index of first item <= the given key, after removing the
        # shared prefix

        data = self._data
        content_start = self._content_start
        _position_at = self._position_at
        _klen_at = self._klen_at

        # Do a binary search of the on-disk keys
        hi = self._count
        while lo < hi:
            mid = (lo + hi) // 2
            pos = content_start + _position_at(mid)
            if bytes(data[pos:pos + _klen_at(mid)]) < suffix:
                lo = mid + 1
            else:
                hi = mid

        return lo

    def key_range(self, start: bytes, end: Optional[bytes]) -> Iterable[bytes]:
        # Get the index of the start key
        try:
            start = self._unprefix(start)
        except KeyError:
            left = self._side(start)
        else:
            left = self._suffix_index(start)

        # Get the index of the end key
        if end is None:
            right = len(self)
        else:
            try:
                end = self._unprefix(end)
            except KeyError:
                right = self._side(end)
            else:
                right = self._suffix_index(end, left)

        return self._keys(left, right)

    def items(self) -> Iterable[Tuple[bytes, bytes]]:
        data = self._data
        reprefix = self._reprefix
        for pos, klen, vlen in self._ranges():
            yield (reprefix(bytes(data[pos:pos + klen])),
                   data[pos + klen:pos + klen + vlen])

    def item_range(self, start: bytes, end: bytes
                   ) -> Iterable[Tuple[bytes, bytes]]:
        try:
            start = self._unprefix(start)
        except KeyError:
            left = self._side(start)
        else:
            left = self._suffix_index(start)

        # Get the index of the end key
        try:
            end = self._unprefix(end)
        except KeyError:
            right = self._side(end)
        else:
            right = self._suffix_index(end, left)

        return self._items(left, right)

    def key_at(self, i: int) -> bytes:
        return bytes(self._reprefix(self._suffix_at(i)))

    def _suffix_at(self, i: int) -> bytes:
        # Returns the key at the given index, not including the shared prefix
        if i >= self._count:
            raise IndexError(i)
        pos = self._content_start + self._position_at(i)
        return self._data[pos:pos + self._klen_at(i)]

    def value_at(self, i: int) -> bytes:
        vpos = self._content_start + self._position_at(i) + self._klen_at(i)
        return self._data[vpos:vpos + self._vlen_at(i)]

    def min_key(self) -> bytes:
        mk = self._minkey
        if mk is None:
            mk = self._minkey = self.key_at(0)
        return mk

    def max_key(self) -> bytes:
        mk = self._maxkey
        if mk is None:
            mk = self._maxkey = self.key_at(self._count - 1)
        return mk

    def cursor(self) -> 'RegionCursor':
        return RegionCursor(self)


class MultiRegion(KeyValueReader):
    """
    Amalgamates the data in multiple regions.
    """

    def __init__(self, data: Data, reflist: List[Ref], cachesize: int=128,
                 load_arrays: bool=False, preread_keys: bool=False):
        """
        :param data: the mmap to read from.
        :param reflist: a list of Ref objects representing the regions.
        :param cachesize: the maximum number of Region objects to keep in
            memory.
        :param preread_keys: load keys into memory, useful when merging to trade
            memory for speed.
        """

        self._data = data
        self._refs = reflist
        self._cachesize = cachesize
        self._load_arrays = load_arrays
        self._preread_keys = preread_keys

        self._queue = deque()
        self._regions = {}
        self._keycache = {}
        self._keycache_counter = 0
        self.misses = 0

    def __len__(self) -> int:
        return sum(len(r) for r in self._refs)

    def __iter__(self) -> Iterable[bytes]:
        for ref in self._refs:
            region = self._region_for_ref(ref)
            for key in region:
                yield key

    def __contains__(self, key: bytes) -> bool:
        ref = self._ref_for_key(key)
        if not ref.minkey <= key <= ref.maxkey:
            return False

        region = self._region_for_ref(ref)
        return key in region

    def __getitem__(self, key: bytes) -> bytes:
        reflist = self._refs
        i = self.ref_index(key)
        if i < len(reflist):
            ref = reflist[i]
            if ref.minkey <= key <= ref.maxkey:
                region = self._region_for_ref(ref)
                return region[key]
        raise KeyError(key)

    def enable_preread(self):
        if not self._preread_keys:
            for region in self._regions.values():
                region.enable_preread()
        self._preread_keys = True

    # KeyValueReader interface

    def key_range(self, start: bytes, end: Optional[bytes]) -> Iterable[bytes]:
        reflist = self._refs
        left = self.ref_index(start)

        if end is None:
            right = len(reflist)
        else:
            right = self.ref_index(end) + 1

        for i in range(left, right):
            region = self._region_for_ref(reflist[i])

            # Tricky: yield partials from the start and end regions, but use
            # full key_range method on regions in the middle for speed
            if i == left or i == right - 1:
                # First/last region: use key_range to do partial
                for key in region.key_range(start, end):
                    yield key
            else:
                # Middle regions: use __iter__ to yield all keys
                for key in region:
                    yield key

    def items(self) -> Iterable[Tuple[bytes, bytes]]:
        for ref in self._refs:
            region = self._region_for_ref(ref)
            for item in region.items():
                yield item

    def close(self):
        for region in self._regions.values():
            region.close()

    #

    def min_key(self) -> bytes:
        region = self._region_for_ref(self._refs[0])
        return region.min_key()

    def max_key(self) -> bytes:
        region = self._region_for_ref(self._refs[-1])
        return region.max_key()

    def region_count(self) -> int:
        return len(self._refs)

    def region_at(self, i) -> Region:
        ref = self._refs[i]
        return self._region_for_ref(ref)

    def ref_index(self, key: bytes) -> int:
        # Do a binary search of the on-disk keys
        reflist = self._refs
        lo = 0
        hi = len(reflist)
        while lo < hi:
            mid = (lo + hi) // 2
            ref = reflist[mid]
            if ref.minkey <= key <= ref.maxkey:
                return mid

            if key > ref.maxkey:
                lo = mid + 1
            else:
                hi = mid

        return lo

    def _region_for_ref(self, ref: Ref) -> Region:
        # Look for the given region in the cache, and if it's not there, load
        # it from disk
        try:
            r = self._regions[ref.offset]
        except KeyError:
            self.misses += 1
            r = self._realize(ref)
        return r

    def _region_for_key(self, key: bytes) -> Region:
        # Get the Region containing the given key
        ref = self._ref_for_key(key)
        return self._region_for_ref(ref)

    def _ref_for_key(self, key: bytes) -> Ref:
        # Find the reference for the region that would contain the given key
        reflist = self._refs
        i = self.ref_index(key)
        if i >= len(reflist):
            i = len(reflist) - 1
        return reflist[i]

    def _realize(self, ref: Ref) -> Region:
        # Load the referenced region from disk
        region = Region.from_ref(self._data, ref, load_arrays=self._load_arrays,
                                 preread_keys=self._preread_keys)

        # Add the region to the cache, keyed by its offset
        offset = ref.offset
        if offset not in self._regions:
            queue = self._queue
            queue.append(offset)
            if len(queue) > self._cachesize:
                deloffset = queue.popleft()
                del self._regions[deloffset]
            self._regions[offset] = region

        return region

    def cursor(self) -> 'MultiRegionCursor':
        return MultiRegionCursor(self)


# Cursors

class Cursor(object):
    def __iter__(self) -> Iterable[bytes]:
        while self.is_valid():
            yield self.key()
            self.next()

    @abstractmethod
    def first(self):
        """
        Positions the cursor at the first item.
        """

        raise NotImplementedError

    @abstractmethod
    def next(self):
        """
        Moves the cursor to the next item.
        """

        raise NotImplementedError

    @abstractmethod
    def seek(self, key: bytes):
        """
        Moves the cursor to the first item <= the given key.

        :param key: the key to move to.
        """

        raise NotImplementedError

    @abstractmethod
    def key(self) -> bytes:
        """
        Returns the key at the current position.
        """

        raise NotImplementedError

    @abstractmethod
    def value(self) -> bytes:
        """
        Returns the value at the current position.
        """

        raise NotImplementedError

    @abstractmethod
    def is_valid(self) -> bool:
        """
        Returns True if the cursor is positioned at an item, not at the end.
        """

        raise NotImplementedError


class RegionCursor(Cursor):
    def __init__(self, region: Region):
        self._region = region
        self._i = 0
        self._length = len(region)

    def first(self):
        self._i = 0

    def next(self):
        if self._i < self._length:
            self._i += 1
        else:
            raise InvalidCursor

    def seek(self, key):
        self._i = self._region.key_index(key)

    def key(self) -> bytes:
        if self._i < self._length:
            return self._region.key_at(self._i)
        else:
            raise InvalidCursor

    def value(self) -> bytes:
        if self._i < self._length:
            return self._region.value_at(self._i)
        else:
            raise InvalidCursor

    def is_valid(self) -> bool:
        return self._i < self._length


class MultiRegionCursor(Cursor):
    def __init__(self, multiregion: MultiRegion):
        self._multi = multiregion
        self._i = 0
        self._cursor = self._multi.region_at(0).cursor()
        self._count = self._multi.region_count()

    def first(self):
        self._i = 0
        self._cursor = self._multi.region_at(0).cursor()

    def next(self):
        if self._i >= self._count:
            raise InvalidCursor

        self._cursor.next()
        if not self._cursor.is_valid():
            self._i += 1
            if self._i < self._multi.region_count():
                self._cursor = self._multi.region_at(self._i).cursor()

    def seek(self, key: bytes):
        self._i = self._multi.ref_index(key)
        if self._i == self._count:
            return

        self._cursor = self._multi.region_at(self._i).cursor()
        self._cursor.seek(key)
        if not self._cursor.is_valid():
            self.next()

    def key(self) -> bytes:
        if self._i >= self._multi.region_count():
            raise InvalidCursor
        return self._cursor.key()

    def value(self) -> bytes:
        if self._i >= self._multi.region_count():
            raise InvalidCursor
        return self._cursor.value()

    def is_valid(self) -> bool:
        return self._i < self._count and self._cursor.is_valid()


class SuffixCursor(Cursor):
    """
    Presents a cursor view of a subset of the keys in another cursor that start
    with a certain prefix.
    """

    def __init__(self, cur: Cursor, prefix: bytes):
        self._cur = cur
        self._prefix = prefix
        self._valid = True

        self._cur.seek(prefix)
        self._check()

    def _check(self):
        self._valid = (self._cur.is_valid() and
                       self._cur.key().startswith(self._prefix))

    def first(self):
        self._cur.seek(self._prefix)
        self._check()

    def next(self):
        if not self._valid:
            raise InvalidCursor
        self._cur.next()
        self._check()

    def seek(self, key: bytes):
        self._cur.seek(self._prefix + key)
        self._check()

    def key(self) -> bytes:
        if not self._valid:
            raise InvalidCursor
        return self._cur.key()[len(self._prefix):]

    def value(self) -> bytes:
        if not self._valid:
            raise InvalidCursor
        return self._cur.value()

    def is_valid(self) -> bool:
        return self._valid and self._cur.is_valid()

