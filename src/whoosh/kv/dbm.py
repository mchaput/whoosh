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
import os.path
import shutil

try:
    import dbm
except ImportError:
    import anydbm as dbm

from whoosh.compat import xrange
from whoosh.kv.db import Database, DBReader, DBWriter, Cursor
from whoosh.kv.db import OverrunError, ReadOnlyError


class DBM(Database):
    def __init__(self, path):
        self.path = path

    def create(self):
        w = self.open(write=True, create=True)
        w.close()
        return self

    def destroy(self):
        if os.path.exists(self.path):
            shutil.rmtree(self.path)

    def optimize(self):
        with self.open(wriwriteue) as w:
            w.optimize()

    def open(self, write=False, create=False):
        if create:
            flags = "c"
        elif write:
            flags = "w"
        else:
            flags = "r"
        db = dbm.open(self.path, flags)
        if write:
            return DBMWriter(db)
        else:
            return DBMReader(db)

    def close(self):
        pass


class DBMReader(DBReader):
    def __init__(self, db):
        self._db = db
        self._sortedkeys = None

    def __len__(self):
        return len(self._db)

    def __contains__(self, key):
        return key in self._db

    def __getitem__(self, key):
        return self._db[key]

    def _get_sorted_keys(self):
        sk = self._sortedkeys
        if sk is None:
            sk = self._sortedkeys = sorted(self._db.keys())
        return sk

    def get(self, key, default=None):
        return self._db.get(key, default)

    def update(self, other):
        self._map.update(other)
        self._sortedkeys = None

    def find(self, key):
        keys = self._get_sorted_keys()
        i = bisect_left(keys, key)
        if i >= len(keys):
            return None
        return keys[i]

    def key_range(self, start, end):
        keys = self._get_sorted_keys()
        left = bisect_left(keys, start)
        right = bisect_left(keys, end)
        for i in xrange(left, right):
            yield keys[i]

    def keys(self):
        return iter(self._get_sorted_keys())

    def values(self):
        db = self._db
        for key in self._get_sorted_keys():
            yield db[key]

    def items(self):
        db = self._db
        for key in self._get_sorted_keys():
            yield key, db[key]

    def cursor(self):
        return GDBMCursor(self._db, self._get_sorted_keys())

    def close(self):
        self._sortedkeys = None
        self._db.close()


class GDBMCursor(Cursor):
    def __init__(self, db, keylist):
        self._db = db
        self._keys = keylist
        self._i = 0

    def is_active(self):
        return self._i < len(self._keys)

    def first(self):
        self._i = 0

    def next(self):
        keys = self._keys
        if self._i >= len(keys):
            raise OverrunError
        self._i += 1

    def find(self, key, fromfirst=True):
        i = 0 if fromfirst else self._i
        self._i = bisect_left(self._keys, key, i)

    def key(self):
        try:
            return self._keys[self._i]
        except IndexError:
            return None

    def value(self):
        try:
            key = self._keys[self._i]
        except IndexError:
            return None
        else:
            return self._db[key]

    def close(self):
        self._db = self._keys = None


class DBMWriter(DBMReader, DBWriter):
    def __setitem__(self, key, value):
        self._db[key] = value

    def __delitem__(self, key):
        try:
            del self._db[key]
        except KeyError:
            pass

    def optimize(self):
        if hasattr(self._db, "reorganize"):
            self._db.reorganize()

    def cancel(self):
        self._db.close()

    def close(self):
        self._db.close()



