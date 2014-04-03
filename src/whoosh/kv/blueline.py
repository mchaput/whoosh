# Copyright 2014 Matt Chaput. All rights reserved.
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

import errno
import mmap
import os.path
import struct
import sys
import time
from array import array
from base64 import b32encode, b32decode
from bisect import bisect_left, bisect_right
from collections import deque

from whoosh.compat import array_frombytes
from whoosh.compat import iteritems, izip, xrange
from whoosh.compat import pickle
from whoosh.compat import bytes_type, string_type
from whoosh.kv.db import Database, DBReader, DBWriter, Cursor
from whoosh.kv.db import EmptyCursor, MergeCursor
from whoosh.kv.db import EmptyDatabaseError, OverrunError, ReadOnlyError
from whoosh.kv.db import LockError
from whoosh.kv.memory import MemoryCursor
from whoosh.system import IS_LITTLE, emptybytes
from whoosh.util import now, random_bytes
from whoosh.util.numlists import min_array_code


# Only try to use memoryview.cast() on Python 3.2 or above
has_memview = sys.version_info > (2, 6)
has_memcast = sys.version_info > (3, 1)


# Exceptions

class RegionNotFound(Exception):
    pass


# API objects

class Blueline(Database):
    def __init__(self, path, name="main", use_mmap=True):
        self.path = path
        self.name = name
        self.use_mmap = use_mmap

    def create(self):
        try:
            os.makedirs(self.path)
        except FileExistsError:
            pass

        while True:
            try:
                self.read_toc()
            except EmptyDatabaseError:
                lock = self.lock()
                toc = Toc(self.name + ".dat", [])
                self.create_file(toc.filename).close()
                if lock.acquire():
                    self.write_toc(toc)
                    lock.release()
                    break
                else:
                    time.sleep(0.05)
            else:
                break
        return self

    def destroy(self):
        # Remove all files
        try:
            for name in self.list_files():
                os.remove(self._filepath(name))
            # Try to remove the directory
            if not self.list_files():
                os.rmdir(self.path)
        except FileNotFoundError:
            pass

    def optimize(self):
        with self.open(write=True) as w:
            w.optimize()

    def open(self, write=False, create=False, cachesize=256,
             blocksize=8192, buffersize=32 * 2**20, use_mmap=True):
        if write:
            if create:
                self.create()

            lock = self.lock()
            if not lock.acquire():
                raise LockError
            return BluelineWriter(self, lock, cachesize=cachesize,
                                  blocksize=blocksize, buffersize=buffersize,
                                  use_mmap=use_mmap)
        else:
            return BluelineReader(self, cachesize=cachesize, use_mmap=use_mmap)

    @staticmethod
    def new_tag(tagset):
        while True:
            tag = b32encode(random_bytes(5)).decode("ascii").lower()
            if tag not in tagset:
                return tag

    def _filepath(self, name):
        return os.path.join(self.path, name)

    def write_toc(self, obj):
        with self.create_file(self.name + ".toc.new") as f:
            pickle.dump(obj, f, 2)
        count = 0
        done = False
        while not done:
            try:
                os.rename(self._filepath(self.name + ".toc.new"),
                          self._filepath(self.name + ".toc"))
            except IOError:
                count += 1
                if count >= 5:
                    raise
                else:
                    time.sleep(0.05)
            else:
                done = True

    def read_toc(self):
        try:
            with self.open_file(self.name + ".toc") as f:
                return pickle.load(f)
        except FileNotFoundError:
            raise EmptyDatabaseError

    def file_exists(self, name):
        return os.path.exists(self._filepath(name))

    def create_file(self, name, mode="w+b"):
        return open(self._filepath(name), mode)

    def open_file(self, name, mode="r+b"):
        return open(self._filepath(name), mode)
        # except IOError:
        #     e = sys.exc_info()[1]
        #     if e.errno == errno.EMFILE:
        #         raise TooManyOpenFiles

    def map_from_file(self, f, use_mmap=True):
        if self.use_mmap and use_mmap:
            try:
                return mmap.mmap(f.fileno(), 0)
            except (mmap.error, OSError):
                e = sys.exc_info()[1]
                # If we got an error because there wasn't enough memory to
                # open the map, ignore it and fall through, we'll just use the
                # (slower) "sub-file" implementation
                if e.errno != errno.ENOMEM:
                    raise
            except ValueError:
                pass
        return FileMap(f)

    def open_map(self, name, use_mmap=True):
        f = self.open_file(name)
        return self.map_from_file(f, use_mmap=use_mmap)

    def delete_file(self, name):
        os.remove(self._filepath(name))

    def file_size(self, name):
        return os.path.getsize(self._filepath(name))

    def last_modified(self, name):
        return os.path.getmtime(self._filepath(name))

    def list_files(self):
        return os.listdir(self.path)

    def lock(self, name=""):
        name = "%s_%s.lock" % (self.name, name)
        return FileLock(self._filepath(name))


class BluelineReader(DBReader):
    def __init__(self, db, cachesize=256, use_mmap=True):
        self._db = db
        self._toc = db.read_toc()
        self._mm = db.open_map(self._toc.filename, use_mmap=use_mmap)
        self._cache = BlockCache(self.load, None, None,
                                 self._leaving, cachesize)
        self._cursorpool = []
        self._cursorpoolsize = 8
        self.closed = False

    def __len__(self):
        cache = self._cache
        return sum(len(cache.block_or_ref(ref)) for ref in self._toc)

    def __contains__(self, key):
        if not self._toc:
            return False
        _, ref = self._ref_for_key(key)
        if ref.minkey <= key <= ref.maxkey:
            block = self._cache.get(ref)
            return key in block

    def __getitem__(self, key):
        if self._toc:
            i, ref = self._ref_for_key(key)
            if ref.minkey <= key <= ref.maxkey:
                block = self._cache.get(ref)
                return block[key]
        raise KeyError(key)

    def _check_in(self, cursor):
        if len(self._cursorpool) < self._cursorpoolsize:
            self._cursorpool.append(cursor)

    def cursor(self):
        # pool = self._cursorpool
        # if pool:
        #     c = pool.pop()
        #     c.first()
        #     return c

        cache = self._cache
        toc = self._toc
        if len(toc) == 1:
            block = cache.get(toc[0])
            if block:
                c = BlockCursor(block, parent=self)
            else:
                c = EmptyCursor()
        else:
            c = SerialCursor(cache, toc, parent=self)

        # if len(pool) < self._cursorpoolsize:
        #     pool.append(c)
        return c

    def keys(self):
        cache = self._cache
        for ref in self._toc:
            block = cache.get(ref)
            for key in block:
                yield key

    def key_range(self, start, end):
        cache = self._cache
        first = True
        for ref in self._ref_range(start, end):
            block = cache.get(ref)
            if first:
                for key in block.key_range(start, end):
                    yield key
                first = False
            else:
                for key in block:
                    if key >= end:
                        return
                    yield key

    def items(self):
        for block in self._all_blocks():
            for key in block:
                yield key, block[key]

    #

    def load(self, ref):
        mm = self._mm
        return DiskBlock(ref.tag, mm, ref)

    def _ref_range(self, start, end):
        toc = self._toc
        # Return an iterator of regions touched by the given key range
        i = bisect_refs(toc, start)
        while i < len(toc):
            ref = toc[i]
            if ref.minkey > end:
                break
            yield ref
            i += 1

    def _ref_for_key(self, key):
        toc = self._toc
        i = bisect_refs(toc, key)
        if i == len(toc):
            i -= 1
        return i, toc[i]

    def _all_blocks(self):
        cache = self._cache
        for ref in self._toc:
            yield cache.get(ref)

    def _block_for_key(self, key):
        i, ref = self._ref_for_key(key)
        block = self._cache.get(ref)
        return i, block

    def _leaving(self, block):
        pass

    def might_contain(self, key):
        toc = self._toc
        ref = toc[bisect_refs(toc, key)]
        return ref.minkey <= key <= ref.maxkey


class BluelineWriter(BluelineReader, DBWriter):
    def __init__(self, db, lock, cachesize, blocksize, buffersize,
                 use_mmap=True):
        self._db = db
        self._toc = db.read_toc()
        self._datafile = db.create_file(self._toc.filename, mode="r+b")
        self._mm = db.map_from_file(self._datafile, use_mmap)
        self._datafile.seek(0, 2)
        self._startlength = self._datafile.tell()
        self._cachesize = cachesize
        self._cache = BlockCache(self.load, self.save, self.new_tag,
                                 self._leaving, cachesize)
        self._cursorpool = []
        self._cursorpoolsize = 8

        self._lock = lock
        self._blocksize = blocksize
        self._buffer = {}
        self._buffersize = buffersize
        self._bufferkeys = None
        self._buffered = 0
        self.closed = False

    def __len__(self):
        return BluelineReader.__len__(self) + len(self._buffer)

    def __getitem__(self, key):
        try:
            return self._buffer[key]
        except KeyError:
            return BluelineReader.__getitem__(self, key)

    def __setitem__(self, key, value):
        assert isinstance(key, bytes_type) and isinstance(value, bytes_type)
        self._buffer[key] = value
        self._bufferkeys = None
        self._buffered += len(key) + len(value)
        if self._buffered > self._buffersize:
            self._buffered = self._calc_buffer_size()
            if self._buffered > self._buffersize:
                self.flush()

        # i, block = self._block_for_key(key, write=True)
        # block[key] = value
        # if len(block) > 2 and len(block) > self._blocksize:
        #     self._split(i, block)

    def __delitem__(self, key):
        if key in self._buffer:
            del self._buffer[key]
            self._bufferkeys = None

        if self._toc:
            i, block = self._block_for_key(key, write=True)
            del block[key]
            assert block.dirty
            if not block:
                self._cache.remove(block.tag)
                del self._toc[i]

    def clear(self):
        self._toc = Toc(self._toc.filename, [])
        self._datafile = self._db.create_file(self._toc.filename, mode="r+b")
        self._cache = BlockCache(self.load, self.save, self.new_tag,
                                 self._leaving, self._cachesize)
        self._cursorpool = []
        self._buffer = {}
        self._bufferkeys = None
        self._buffered = 0

    def new_tag(self):
        return self._db.new_tag(self._toc.tagset())

    def delete_by_prefix(self, prefix):
        buff = self._buffer
        toc = self._toc
        cache = self._cache

        if buff:
            sbks = self._sorted_buffer_keys()
            start = end = bisect_left(sbks, prefix)
            while end < len(sbks) and sbks[end].startswith(prefix):
                end += 1
            if end > start:
                for i in xrange(start, end):
                    del buff[sbks[i]]
                del sbks[start:end]

        i = bisect_refs(toc, prefix)
        while i < len(toc):
            ref = toc[i]
            if ref.minkey <= prefix <= ref.maxkey:
                block = cache.get(ref, write=True)
                block.delete_by_prefix(prefix)
            else:
                break
            i += 1

    def keys(self):
        gen = BluelineReader.keys(self)
        if self._buffer:
            gen = dedup_merge(gen, self._sorted_buffer_keys())
        return gen

    def key_range(self, start, end):
        gen = BluelineReader.key_range(self, start, end)
        if self._buffer:
            bkeys = self._sorted_buffer_keys()
            left = bisect_left(bkeys, start)
            right = bisect_left(bkeys, end)
            bkeys = bkeys[left:right]
            gen = dedup_merge(gen, bkeys)
        return gen

    def items(self):
        gen = BluelineReader.items(self)
        if self._buffer:
            gen = dedup_merge_items(gen, sorted(self._buffer.items()))
        return gen

    def cursor(self):
        if self._toc and self._buffer:
            a = self._buffer_cursor()
            b = BluelineReader.cursor(self)
            return MergeCursor(a, b)
        elif self._toc:
            return BluelineReader.cursor(self)
        elif self._buffer:
            return self._buffer_cursor()
        else:
            return EmptyCursor()

    def _buffer_cursor(self):
        return MemoryCursor(self._sorted_buffer_keys(), self._buffer)

    def _calc_buffer_size(self):
        buff = self._buffer
        return sum(len(k) + len(v) for k, v in iteritems(buff))

    def _sorted_buffer_keys(self):
        if self._bufferkeys is None:
            self._bufferkeys = sorted(self._buffer)
        return self._bufferkeys

    @staticmethod
    def _overlap_blocks(refs, keys):
        # The index of the key we're looking at
        left = 0
        # Iterate through the blocks
        for ref in refs:
            # Yield any keys before the current block
            if left < len(keys) and keys[left] < ref.minkey:
                right = bisect_left(keys, ref.minkey, left)
                yield ([], keys[left:right], right - left)
                left = right

            # If the remaining keys are all after the current block, this block
            # doesn't need to be rewritten
            if left >= len(keys) or keys[left] > ref.maxkey:
                yield ([ref], [], ref.length)
            else:
                # Handle overlapping keys
                right = bisect_right(keys, ref.maxkey, left)
                yield ([ref], keys[left:right], ref.length + (right - left))
                left = right

        # Yield any keys after the last block
        if left < len(keys):
            yield ([], keys[left:], len(keys) - left)

    def flush(self):
        toc = self._toc
        cache = self._cache
        buff = self._buffer
        datafile = self._datafile
        blocksize = self._blocksize
        allkeys = self._sorted_buffer_keys()

        spec = list(self._overlap_blocks(toc, allkeys))
        if len(spec) > 1:
            # Merge small blocks
            i = 0
            while i < len(spec):
                refs, keys, length = spec[i]
                j = i + 1
                if length < blocksize:
                    merged = False
                    # Look left
                    if i > 0:
                        leftrefs, leftkeys, leftlen = spec[i - 1]
                        if length + leftlen <= blocksize:
                            i -= 1
                            refs = leftrefs + refs
                            keys = leftkeys + keys
                            length += leftlen
                            merged = True
                    # Look right
                    if j < len(spec):
                        rightrefs, rightkeys, rightlen = spec[j]
                        if length + rightlen <= blocksize:
                            refs = refs + rightrefs
                            keys = keys + rightkeys
                            length += rightlen
                            j += 1
                            merged = True
                    if merged:
                        spec[i:j] = [(refs, keys, length)]
                        j = i + 1
                i = j

        # Rewrite overlapping blocks
        rewrites = 0
        newtoc = Toc(toc.filename, [])
        tagset = toc.tagset()
        for refs, keys, _ in spec:
            if len(refs) == 1 and not keys:
                newtoc.append(refs[0])
                continue

            items = [(key, buff[key]) for key in keys]
            if refs:
                rewrites += len(refs)
                d = {}
                for ref in refs:
                    block = cache.pop(ref)
                    d.update(dict(block.items()))
                d.update(dict(items))
                items = sorted(iteritems(d))

            for i in xrange(0, len(items), blocksize):
                tag = self._db.new_tag(tagset)
                datafile.seek(0, 2)
                newtoc.append(write_region(tag, datafile, items[i:i + blocksize]))
                tagset.add(tag)

        newtoc.check_in_order()
        self._toc = newtoc
        self._buffer = {}
        self._bufferkeys = None
        self._buffered = 0

    def save(self, block):
        toc = self._toc
        tag = block.tag
        for i in xrange(len(toc)):
            if toc[i].tag == tag:
                break
        else:
            raise Exception

        datafile = self._datafile
        datafile.seek(0, 2)
        toc[i] = write_region(tag, datafile, list(block.items()))

    def _block_for_key(self, key, write=False):
        i, ref = self._ref_for_key(key)
        block = self._cache.get(ref, write=write)
        return i, block

    def _leaving(self, block):
        for ref in self._toc:
            if ref.tag == block.tag:
                ref.from_block(block)
                break

    def cancel(self):
        self._datafile.truncate(self._startlength)
        self._datafile.close()
        # self._db.clean()
        self._lock.release()
        self.closed = True

    def commit(self):
        if self._buffer:
            self.flush()
        self._cache.close()
        self._datafile.close()
        self._db.write_toc(self._toc)
        # self._db.clean(self._toc.tagset())
        self._lock.release()
        self.closed = True


# Cursor objects

class BlockCursor(Cursor):
    def __init__(self, block, parent=None):
        self.block = block
        self._i = 0
        self._parent = parent

    def __del__(self):
        if hasattr(self, "_parent") and self._parent:
            self._parent._check_in(self)

    def __repr__(self):
        return "<%s %r %d/%d>" % (self.__class__.__name__, self.block.tag,
                                  self._i, len(self.block))

    def is_active(self):
        return self._i < len(self.block)

    def first(self):
        self._i = 0

    def next(self):
        if self._i >= len(self.block):
            raise OverrunError
        self._i += 1

    def find(self, key, fromfirst=True):
        i = 0 if fromfirst else self._i
        self._i = self.block.key_index(key, i)

    def key(self):
        try:
            return self.block.key_at(self._i)
        except IndexError:
            return None

    def value(self):
        key = self.key()
        if key is None:
            return None
        return self.block[key]

    def keys(self):
        return iter(self.block)


class SerialCursor(Cursor):
    def __init__(self, cache, toc, parent=None):
        self._cache = cache
        self._toc = toc
        self._i = 0
        if self.is_active():
            self._cursor = self._make_cursor()
        self._parent = parent

    def __del__(self):
        if hasattr(self, "_parent") and self._parent:
            self._parent._check_in(self)

    def _make_cursor(self):
        ref = self._toc[self._i]
        block = self._cache.get(ref)
        return BlockCursor(block)

    def first(self):
        self._i = 0
        self._cursor = self._make_cursor()

    def is_active(self):
        return self._i < len(self._toc)

    def next(self):
        self._cursor.next()
        self._check()

    def find(self, key, fromfirst=True):
        toc = self._toc
        i = 0 if fromfirst else self._i
        self._i = bisect_refs(toc, key, i)
        while self._i < len(toc):
            cursor = self._make_cursor()
            cursor.find(key)
            if cursor.is_active():
                self._cursor = cursor
                break
            self._i += 1

    def _check(self):
        if self._i < len(self._toc) and not self._cursor.is_active():
            self._i += 1
            if self._i < len(self._toc):
                self._cursor = self._make_cursor()

    def key(self):
        if self._i < len(self._toc):
            return self._cursor.key()

    def value(self):
        if self._i < len(self._toc):
            return self._cursor.value()

    def keys(self):
        toc = self._toc
        for i in xrange(len(toc)):
            self._i = i
            self._cursor = cursor = self._make_cursor()
            for key in cursor:
                yield key


# TOC objects

class Toc(object):
    def __init__(self, datatfilename, blockrefs):
        self.filename = datatfilename
        self.blockrefs = blockrefs

    def __repr__(self):
        return "<%s %r %r>" % (self.__class__.__name__, self.filename,
                               self.blockrefs)

    def __eq__(self, other):
        return (
            type(self) is type(other)
            and self.filename == other.filename
            and self.blockrefs == other.blockrefs
        )

    def __len__(self):
        return len(self.blockrefs)

    def __getitem__(self, item):
        return self.blockrefs.__getitem__(item)

    def __setitem__(self, key, value):
        return self.blockrefs.__setitem__(key, value)

    def __delitem__(self, key):
        return self.blockrefs.__delitem__(key)

    def __bool__(self):
        return bool(self.blockrefs)

    def __nonzero__(self):
        return self.__bool__()

    def append(self, obj):
        self.blockrefs.append(obj)

    def extend(self, ls):
        self.blockrefs.extend(ls)

    def remove(self, obj):
        return self.blockrefs.remove(obj)

    def pop(self, item):
        return self.blockrefs.pop(item)

    def check_in_order(self):
        """
        Sanity check the refs in this TOC.
        """

        refs = self.blockrefs
        for i in xrange(1, len(refs)):
            if refs[i].minkey <= refs[i - 1].maxkey:
                raise Exception

    def tagset(self):
        return set(ref.tag for ref in self.blockrefs)


class BlockRef(object):
    def __init__(self, tag=None, minkey=None, maxkey=None, length=0, offset=0):
        self.tag = tag
        self.minkey = minkey
        self.maxkey = maxkey
        self.length = length
        self.offset = offset

    def __repr__(self):
        return "<%s %r %d>" % (self.__class__.__name__, self.tag, self.length)

    def __eq__(self, other):
        return type(self) is type(other) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not self.__eq__(other)

    def __len__(self):
        return self.length

    def min_key(self):
        return self.minkey

    def max_key(self):
        return self.maxkey

    def from_block(self, block):
        self.tag = block.tag
        self.minkey = block.min_key()
        self.maxkey = block.max_key()
        self.length = len(block)
        return self


# Block reading/writing objects

class BlockCache(object):
    def __init__(self, load, save, newtag, callback, size):
        self.load = load
        self.save = save
        self.newtag = newtag
        self.callback = callback if callback else None
        self.size = size
        self.queue = deque()
        self.blocks = {}
        self.refs = {}

    def __len__(self):
        return len(self.blocks)

    def loaded(self, tag):
        return tag in self.blocks

    def get(self, ref, write=False):
        tag = ref.tag
        try:
            block = self.blocks[tag]
        except KeyError:
            block = self._realize(ref)
            # ref.from_block(block)

        # If we need a writable block, replace the DiskBlock with a BufferBlock
        if write and not block.dirty:
            block = self._cow(ref, block)

        return block

    def _cow(self, ref, block):
        # Converts a read-only block to a buffer block with a new tag

        # Remove the on-disk block from the cache
        self.remove(block.tag)
        # Convert to a buffer block
        block = BufferBlock(self.newtag(), list(block.items()), dirty=True)
        # Change the index reference to the block to reflect the new tag
        ref.from_block(block)
        # Add the buffer block back into the cache
        self.add(block)
        return block

    def pop(self, ref):
        tag = ref.tag
        try:
            block = self.blocks[tag]
        except KeyError:
            block = self.load(ref)
        else:
            self.remove(tag)
        return block

    def block_or_ref(self, ref):
        try:
            return self.blocks[ref.tag]
        except KeyError:
            return ref

    def add(self, block):
        tag = block.tag
        if tag not in self.blocks:
            queue = self.queue
            queue.append(tag)
            if len(queue) > self.size:
                self._flush(queue.popleft())
        self.blocks[tag] = block

    def remove(self, tag):
        del self.blocks[tag]
        self.queue.remove(tag)

    def _realize(self, ref):
        block = self.load(ref)
        self.add(block)
        return block

    def _flush(self, tag):
        block = self.blocks.pop(tag)
        if block.dirty:
            assert isinstance(block, BufferBlock)
            if block:
                if self.callback:
                    self.callback(block)
                self.save(block)

    def close(self):
        for tag in list(self.blocks):
            self._flush(tag)


class BufferBlock(object):
    write = True

    def __init__(self, tag, items, dirty=False, sorted=True):
        self.tag = tag
        self.keymap = dict(items)
        self.keylist = [k for k, _ in items]
        self.dirty = dirty
        self.sorted = sorted
        self.minkey = None
        self.maxkey = None

    def __repr__(self):
        return "<%r, %r>" % (self.keymap, self.keylist)

    def __bool__(self):
        return bool(self.keylist)

    def __nonzero__(self):
        return self.__bool__()

    def __len__(self):
        return len(self.keylist)

    def __contains__(self, key):
        return key in self.keymap

    def __setitem__(self, key, value):
        if key not in self.keymap:
            keylist = self.keylist
            self.sorted = not keylist or (self.sorted and key > keylist[-1])
            keylist.append(key)
        self.keymap[key] = value

    def __getitem__(self, key):
        return self.keymap[key]

    def __delitem__(self, key):
        keymap = self.keymap
        if key in keymap:
            del keymap[key]
            self.keylist.remove(key)

    def __iter__(self):
        self._sort()
        return iter(self.keylist)

    def _sort(self):
        if not self.sorted:
            self.keylist.sort()
            self.sorted = True

    def _calc_size(self):
        return sum(len(k) + len(v) for k, v in iteritems(self.keymap))

    def delete_by_prefix(self, prefix):
        self._sort()
        keylist = self.keylist
        keymap = self.keymap
        start = end = bisect_left(keylist, prefix)
        while end < len(keylist) and keylist[end].startswith(prefix):
            end += 1
        if end > start:
            for i in xrange(start, end):
                del keymap[keylist[i]]
            del keylist[start:end]

    def key_index(self, key, lo=0):
        self._sort()
        return bisect_left(self.keylist, key, lo)

    def key_at(self, i):
        self._sort()
        return self.keylist[i]

    def key_range(self, start, end):
        self._sort()
        keylist = self.keylist
        left = bisect_left(keylist, start)
        right = bisect_left(keylist, end)
        for i in xrange(left, right):
            yield keylist[i]

    def iter_from(self, key):
        self._sort()
        keys = self.keylist
        pos = bisect_left(keys, key)
        for i in xrange(pos, len(keys)):
            yield keys[i]

    def items(self):
        self._sort()
        keymap = self.keymap
        return ((key, keymap[key]) for key in self.keylist)

    def min_key(self):
        self._sort()
        if self.keylist:
            return self.keylist[0]
        else:
            return emptybytes

    def max_key(self):
        self._sort()
        if self.keylist:
            return self.keylist[-1]
        else:
            return emptybytes


class DiskBlock(object):
    load_arrays = True
    write = False
    dirty = False

    def __init__(self, tag, mm, blockref):
        self.tag = tag
        self._mm = mm
        self._blockref = blockref
        self._offset = blockref.offset
        self._length = blockref.length
        self._minkey = blockref.minkey
        self._maxkey = blockref.maxkey

        info = read_region(mm, self._offset, self.load_arrays)
        self._header, self._poses, self._klens, self._vlens, self._datastart = info
        assert self.tag == self._header.tag, "%r != %r" % (self.tag, self._header.tag)
        assert self._length == self._header.length
        self._datasize = self._header.datasize

        self._lookup = dict((key, i) for i, key in enumerate(self._keys()))

    def __bool__(self):
        return True

    def __nonzero__(self):
        return True

    def __len__(self):
        return self._length

    def __iter__(self):
        return self._keys()

    def __contains__(self, key):
        return key in self._lookup
        # i = self.key_index(key)
        # return self.key_at(i) == key

    def __getitem__(self, key):
        i = self.key_index(key)
        if self.key_at(i) == key:
            pos = self._datastart + self._poses[i] + len(key)
            return self._mm[pos:pos + self._vlens[i]]
        raise KeyError(key)

    def _keys(self, lo=0, hi=None):
        mm = self._mm
        datastart = self._datastart
        poses = self._poses
        klens = self._klens

        hi = hi if hi is not None else self._length
        for i in xrange(lo, hi):
            pos = datastart + poses[i]
            yield mm[pos:pos + klens[i]]

    def _ranges(self):
        datastart = self._datastart
        poses = self._poses
        klens = self._klens
        vlens = self._vlens

        for i in xrange(self._length):
            yield datastart + poses[i], klens[i], vlens[i]

    def key_index(self, key, lo=0):
        try:
            return self._lookup[key]
        except KeyError:
            pass

        mm = self._mm
        datastart = self._datastart
        poses = self._poses
        klens = self._klens

        # Do a binary search of the on-disk keys
        hi = self._length
        while lo < hi:
            mid = (lo + hi) // 2
            pos = datastart + poses[mid]
            midkey = mm[pos:pos + klens[mid]]
            if midkey < key:
                lo = mid + 1
            else:
                hi = mid
        return lo

    def key_range(self, start, end):
        left = self.key_index(start)
        right = self.key_index(end, left)
        return self._keys(left, right)

    def items(self):
        mm = self._mm
        for pos, klen, vlen in self._ranges():
            yield mm[pos:pos + klen], mm[pos + klen:pos + klen + vlen]

    def key_at(self, i):
        pos = self._datastart + self._poses[i]
        return self._mm[pos:pos + self._klens[i]]

    def value_at(self, i):
        vpos = self._datastart + self._poses[i] + self._klens[i]
        return self._mm[vpos:vpos + self._vlens[i]]

    def min_key(self):
        return self._minkey

    def max_key(self):
        return self._maxkey


# Fallback objects

class FileMap(object):
    """
    Implements an object with a similar interface to a ``mmap``, but fakes
    atomic access to the file using ``seek()`` and ``read()``, for platforms
    or circumstances where using memory-mapping is not possible or desirable.
    """

    def __init__(self, fileobj, offset=0):
        self._file = fileobj
        self._offset = offset

    def __getitem__(self, item):
        _file = self._file
        if isinstance(item, slice):
            start = item.start
            end = item.stop
        else:
            start = item
            end = item + 1
        _file.seek(start + self._offset)
        return _file.read(end - start)


class MmapArray(object):
    """
    Implements an array-like interface similar to a ``cast()``-ed ``memorymap``,
    but fakes item access using ``Struct.unpack()``, for Python versions that
    do not support ``memorymap.cast()``.
    """

    def __init__(self, mm, fmt, offset, length):
        """
        :param mm: a ``mmap`` or ``FileMap`` object.
        :param fmt: the ``struct`` format string to use to access items.
        :param offset: the offset of the beginning of the array in the file.
        :param length: the number of items in the array.
        """
        self._mm = mm
        self._struct = struct.Struct(fmt)
        self._offset = offset
        self._length = length

    def __len__(self):
        return self._length

    def __iter__(self):
        _mm = self._mm
        size = self._struct.size
        unpack = self._struct.unpack
        for i in xrange(self._length):
            pos = self._offset + i * size
            yield unpack(_mm[pos:pos + size])[0]

    def __getitem__(self, n):
        _mm = self._mm
        _struct = self._struct
        _offset = self._offset
        _unpack = _struct.unpack
        _size = _struct.size

        if isinstance(n, slice):
            out = []
            start, stop, step = n.indices(self._length)
            for i in xrange(start, stop, step):
                pos = _offset + i * _size
                out.append(_unpack(_mm[pos:pos + _size])[0])
            return out
        else:
            pos = _offset + n * _struct.size
            return _unpack(_mm[pos:pos + _size])[0]


# File lock object

class FileLock(object):
    """
    Implements a process-level file lock using ``msvcrt`` on Windows and
    ``fcntl`` on UNIX.
    """

    def __init__(self, path):
        """
        :param path: the filesystem path of the lock file.
        """
        self.path = path
        self.file = None
        self.locked = False

    def __enter__(self):
        if not self.acquire():
            raise LockError

    def __exit__(self, *_):
        if self.locked:
            self.release()

    def __del__(self):
        try:
            if self.locked:
                self.release()
        except AttributeError:
            pass

    def acquire(self, blocking=False):
        """
        Acquire the lock. Returns True if the lock was acquired.

        :param blocking: if True, call blocks until the lock is acquired.
            This may not be available on all platforms. On Windows, this is
            actually just a delay of 10 seconds, rechecking every second.
        """

        self.file = open(self.path, "w+b")
        fd = self.file.fileno()

        if os.name == "nt":
            import msvcrt
            mode = msvcrt.LK_NBLCK
            if blocking:
                mode = msvcrt.LK_LOCK
            fn = lambda: msvcrt.locking(fd, mode, 1)
        else:
            import fcntl
            mode = fcntl.LOCK_EX
            if not blocking:
                mode |= fcntl.LOCK_NB
            fn = lambda: fcntl.flock(fd, mode)

        try:
            fn()
        except IOError:
            e = sys.exc_info()[1]
            if e.errno not in (errno.EAGAIN, errno.EACCES, errno.EDEADLK):
                raise
            return False

        self.locked = True
        return True

    def release(self):
        if not self.locked:
            raise Exception("Lock was not acquired")

        if os.name == "nt":
            import msvcrt
            msvcrt.locking(self.file.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.flock(self.file.fileno(), fcntl.LOCK_UN)
        self.file.close()
        self.locked = False


class Header(object):
    # The header struct
    # B   = version
    # B   = flag bits
    #       Bit 1 = crated on little-endian machine
    # 5s = the ID for this region
    # i   = the number of items in the region
    # i   = the total length in bytes of this region file
    # c   = the typecode of the positions array
    # c   = the typecode of the key lengths array
    # c   = the typecode of the value lengths array
    # XXx = unused bytes for future expansion
    header = struct.Struct("<BB5siiccc17x")
    headersize = header.size
    # assert headersize % 8 == 0

    def __init__(self, version=None, flags=0, tag=None, length=None,
                 datasize=None, poscode=None, klencode=None, vlencode=None):
        self.version = version
        self.flags = flags
        self.tag = tag
        self.length = length
        self.datasize = datasize
        self.poscode = poscode
        self.klencode = klencode
        self.vlencode = vlencode

    @classmethod
    def from_bytes(cls, bytestring):
        header = cls(*cls.header.unpack(bytestring))
        # Python 2/3 is stupid
        if not isinstance(header.poscode, string_type):
            header.poscode = header.poscode.decode("ascii")
            header.klencode = header.klencode.decode("ascii")
            header.vlencode = header.vlencode.decode("ascii")
        header.tag = b32encode(header.tag).lower().decode("ascii")
        return header

    @classmethod
    def from_map(cls, mm, offset):
        return cls.from_bytes(mm[offset:offset + cls.headersize])

    def to_bytes(self):
        assert self.version and self.version < 256
        assert isinstance(self.flags, int) and self.flags >= 0
        assert isinstance(self.tag, bytes_type)
        assert self.length
        assert self.datasize
        assert isinstance(self.poscode, string_type) and len(self.poscode) == 1
        assert isinstance(self.klencode, string_type) and len(self.klencode) == 1
        assert isinstance(self.vlencode, string_type) and len(self.vlencode) == 1

        pc = self.poscode.encode("ascii")  # type code of positions array
        klc = self.klencode.encode("ascii")  # type code of lengths array
        vlc = self.vlencode.encode("ascii")  # type code of lengths array
        return self.header.pack(self.version, self.flags, self.tag,
                                self.length, self.datasize, pc, klc, vlc)

    def is_little(self):
        return self.flags & 1


# Region IO functions

def write_region(tag, regionfile, items):
    assert isinstance(items, list)
    assert items
    # Writes a single region to a file in the store

    offset = regionfile.tell()
    header = Header()
    header.version = 1
    header.flags = int(IS_LITTLE)
    header.length = len(items)
    header.tag = b32decode(tag, casefold=True)

    keys = []
    values = []
    for key, value in items:
        keys.append(key)
        values.append(value)

    klens = array("i", (len(k) for k in keys))
    klencode = min_array_code(max(klens))
    if klencode != "i":
        klens = array(klencode, klens)
    header.klencode = klencode

    vlens = array("i", (len(v) for v in values))
    vlencode = min_array_code(max(vlens))
    if vlencode != "i":
        vlens = array(vlencode, vlens)
    header.vlencode = vlencode

    base = 0
    poses = array("i")
    header.poscode = "i"
    for klen, vlen in izip(klens, vlens):
        poses.append(base)
        base += klen + vlen

    datasize = (
        len(poses) * poses.itemsize +
        len(klens) * klens.itemsize +
        len(vlens) * vlens.itemsize +
        sum(klens) + sum(vlens)
    )
    header.datasize = datasize

    write = regionfile.write
    write(header.to_bytes())
    poses.tofile(regionfile)
    klens.tofile(regionfile)
    vlens.tofile(regionfile)
    for item in items:
        write(emptybytes.join(item))

    # print("Wrote block", tag, "at", offset)
    return BlockRef(tag, keys[0], keys[-1], len(items), offset)


def read_region(mm, offset=0, load_arrays=False):
    # Reads the region info from a mmap at the given offset

    # Read and unpack the header struct
    header = Header.from_map(mm, offset)
    length = header.length

    assert header.version == 1
    # True if the region was written on a little-endian machine
    was_little = header.is_little()
    # True if this machine matches the endianness of the region
    native = was_little == IS_LITTLE

    # Calculate the starts and ends of the arrays
    possize = struct.calcsize(header.poscode) * length
    klensize = struct.calcsize(header.klencode) * length
    vlensize = struct.calcsize(header.vlencode) * length

    posbase = offset + header.headersize
    klenbase = posbase + possize
    vlenbase = klenbase + klensize
    datastart = vlenbase + vlensize

    if native and isinstance(mm, mmap.mmap) and has_memcast:
        # If the endianness matches, and this is a real mmap, and
        # memoryview.cast() is available, then use a memoryview
        mv = memoryview(mm)
        poses = mv[posbase:klenbase].cast(header.poscode)
        klens = mv[klenbase:vlenbase].cast(header.klencode)
        vlens = mv[vlenbase:datastart].cast(header.vlencode)

    elif load_arrays:
        poses = array(header.poscode)
        array_frombytes(poses, mm[posbase:klenbase])
        klens = array(header.klencode)
        array_frombytes(klens, mm[klenbase:vlenbase])
        vlens = array(header.vlencode)
        array_frombytes(vlens, mm[vlenbase:datastart])
        if not native:
            poses.byteswap()
            klens.byteswap()
            vlens.byteswap()

    else:
        # Otherwise, fake memoryview.cast() using MmapArray
        endian = "<" if was_little else ">"
        poses = MmapArray(mm, endian + header.poscode, posbase, length)
        klens = MmapArray(mm, endian + header.klencode, klenbase, length)
        vlens = MmapArray(mm, endian + header.vlencode, vlenbase, length)

    # print("Read block", header.tag, "from", offset)
    return header, poses, klens, vlens, datastart


# Support functions

def bisect_refs(blockrefs, key, lo=0, hi=None):
        hi = hi or len(blockrefs)
        while lo < hi:
            mid = (lo + hi) // 2
            bref = blockrefs[mid]

            if bref.minkey <= key <= bref.maxkey:
                return mid
            elif bref.maxkey < key:
                lo = mid + 1
            else:
                hi = mid

        return lo


def dedup_merge(keyiter, keylist):
    i = 0
    _listlen = len(keylist)
    for key in keyiter:
        k = None
        while i < _listlen and keylist[i] <= key:
            k = keylist[i]
            yield k
            i += 1

        if k != key:
            yield key

    while i < _listlen:
        yield keylist[i]
        i += 1


def dedup_merge_items(ititer, itlist):
    i = 0
    _listlen = len(itlist)
    for item in ititer:
        k = None
        while i < _listlen and itlist[i][0] <= item[0]:
            k = itlist[i][0]
            yield k
            i += 1

        if k != item[0]:
            yield item[0]

    while i < _listlen:
        yield itlist[i]
        i += 1


