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

from bisect import bisect_left

from whoosh.kv.db import Database, DBReader, DBWriter, Cursor
from whoosh.kv.db import OverrunError, ReadOnlyError


class MemoryDB(Database):
    def __init__(self, path=None, name="main", from_map=None):
        self._path = path
        self._name = name
        self._map = {} if from_map is None else from_map

    def create(self):
        return self

    def open(self, write=False, create=False):
        _map = self._map
        if write:
            _map = _map.copy()
        return MemoryView(_map, self, write=write)


class MemoryView(DBWriter):
    def __init__(self, from_map=None, from_db=None, write=False):
        self._map = from_map or {}
        self._db = from_db
        self._keys = None
        self.minkey = min(self._map) if self._map else None
        self.maxkey = max(self._map) if self._map else None
        self._write = write
        self.closed = False

    def __len__(self):
        return len(self._map)

    def __getitem__(self, key):
        return self._map[key]

    def __contains__(self, key):
        return key in self._map

    def __bool__(self):
        return bool(self._map)

    def __repr__(self):
        return "<%s %r-%r>" % (self.__class__.__name__,
                               self.minkey, self.maxkey)

    def __setitem__(self, key, value):
        if not self._write:
            raise ReadOnlyError
        self._map[key] = value
        self._keys = None

    def __delitem__(self, key):
        if not self._write:
            raise ReadOnlyError
        try:
            del self._map[key]
        except KeyError:
            pass
        else:
            self._keys = None

    def writable(self):
        return self._write

    def cursor(self):
        self._sort()
        return MemoryCursor(self._keys, self._map)

    def load(self):
        return self

    def update(self, other):
        if not self._write:
            raise ReadOnlyError
        self._map.update(other)
        self._keys = None

    def find(self, key):
        self._sort()
        _keys = self._keys
        i = bisect_left(_keys, key)
        if i >= len(_keys):
            return None
        return _keys[i]

    def key_range(self, start, end):
        self._sort()
        _keys = self._keys
        left = bisect_left(_keys, start)
        right = bisect_left(_keys, end)
        for i in range(left, right):
            yield _keys[i]

    def _sort(self):
        if self._keys is None:
            self._keys = sorted(self._map)

    def keys(self):
        self._sort()
        return iter(self._keys)

    def items(self):
        _map = self._map
        for k in self.keys():
            yield k, _map[k]

    def sorted_keys(self):
        self._sort()
        return self._keys

    def dict(self):
        return self._map

    def cancel(self):
        del self._map
        self.closed = True

    def commit(self):
        if self._db is not None:
            self._db._map = self._map
        self.closed = True


class MemoryCursor(Cursor):
    def __init__(self, keylist, keymap):
        self._keys = keylist
        self._map = keymap
        self._i = 0

    def is_active(self):
        return self._i < len(self._keys)

    def first(self):
        self._i = 0

    def next(self):
        if self._i >= len(self._keys):
            raise OverrunError
        self._i += 1

    def find(self, key, fromfirst=True):
        i = 0 if fromfirst else self._i
        self._i = bisect_left(self._keys, key, i)

    def key(self):
        return self._keys[self._i] if self._i < len(self._keys) else None

    def value(self):
        key = self.key()
        return None if key is None else self._map[key]



