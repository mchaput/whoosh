# Copyright 2013 Matt Chaput. All rights reserved.
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

from __future__ import absolute_import
import os.path
import shutil

import lmdb

from whoosh.kv.db import Database, DBReader, DBWriter, Cursor
from whoosh.kv.db import OverrunError, ReadOnlyError


MAX_MAP_SIZE = 4 * 1024 * 1024 * 1024
MAX_NAMED_DBS = 16


class LMDB(Database):
    def __init__(self, path, name="main", map_size=MAX_MAP_SIZE,
                 sync=False, max_dbs=16, **kwargs):
        self.path = path
        self.name = name
        self.map_size = map_size
        self.sync = sync
        self.max_dbs = max_dbs
        self.kwargs = kwargs

    def create(self):
        try:
            os.mkdir(self.path)
        except FileExistsError:
            pass

    def destroy(self):
        env = self._env()
        txn = env.begin(write=True)
        try:
            txn.delete(self.name)
        except KeyError:
            pass
        topkeys = list(txn.cursor())
        txn.commit()

        if not topkeys:
            try:
                shutil.rmtree(self.path)
            except IOError:
                pass

    def open(self, write=False, create=True, buffers=False):
        if create:
            self.create()

        env = self._env()
        subdb = env.open_db(self.name)
        txn = lmdb.Transaction(env, subdb, write=write, buffers=buffers)
        if write:
            return LMDBWriter(txn)
        else:
            return LMDBReader(txn)

    def _env(self):
        return lmdb.open(self.path, map_size=self.map_size, sync=self.sync,
                         max_dbs=self.max_dbs, **self.kwargs)


class LMDBReader(DBReader):
    def __init__(self, txn):
        self._txn = txn
        self.closed = False

    def __len__(self):
        return self._txn.stat()["entries"]

    def __contains__(self, key):
        return self._txn.get(key) is not None

    def __getitem__(self, key):
        v = self._txn.get(key)
        if v is None:
            raise KeyError(key)
        return v

    def get(self, key, default=None):
        return self._txn.get(key, default)

    def cursor(self):
        return LMDBCursor(self._txn.cursor())

    def keys(self):
        cur = self._txn.cursor()
        return cur.iternext(keys=True, values=False)

    def key_range(self, start, end):
        cur = self._txn.cursor()
        cur.set_range(start)
        for key in cur.iternext(keys=True, values=False):
            if key >= end:
                break
            yield key

    def values(self):
        cur = self._txn.cursor()
        return cur.iternext(keys=False, values=True)

    def items(self):
        cur = self._txn.cursor()
        return cur.iternext(keys=True, values=True)

    def close(self):
        self._txn.commit()
        self.closed = True


class LMDBWriter(LMDBReader, DBWriter):
    def __setitem__(self, key, value):
        self._txn.put(key, value)

    def __delitem__(self, key):
        self._txn.delete(key)
        # if not self._txn.delete(key):
        #     raise KeyError(key)

    def delete_by_prefix(self, prefix):
        cur = self.cursor()
        cur.find(prefix)
        while cur.is_active() and cur.key().startswith(prefix):
            cur.delete()
        cur.close()

    def optimize(self):
        pass

    def cancel(self):
        self._txn.abort()
        self.closed = True

    def commit(self):
        self._txn.commit()
        self.closed = True


class LMDBCursor(Cursor):
    def __init__(self, cur):
        self._cur = cur
        self._active = cur.first()

    def is_active(self):
        return self._active

    def first(self):
        self._cur.first()

    def find(self, key, fromfirst=True):
        self._active = self._cur.set_range(key)

    def next(self):
        if not self._active:
            raise OverrunError
        self._active = self._cur.next()

    def key(self):
        if self._active:
            return self._cur.key()
        else:
            return None

    def value(self):
        if self._active:
            return self._cur.value()
        else:
            return None

    def item(self):
        if self._active:
            return self._cur.item()
        else:
            return None

    def delete(self):
        self._cur.delete()




