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

import os.path
import shutil

import BTrees
import ZODB
import transaction

from whoosh.compat import next
from whoosh.kv.db import Database, DBReader, DBWriter, Cursor
from whoosh.kv.db import OverrunError, ReadOnlyError


class Zodb(Database):
    def __init__(self, path, name="main"):
        self.path = path
        self.name = name
        self.filepath = os.path.join(self.path, self.name)
        self._db = None

    def create(self):
        try:
            os.mkdir(self.path)
        except FileExistsError:
            pass

        self._make_db()
        self._create_btree()
        return self

    def destroy(self):
        try:
            for fname in os.listdir(self.path):
                if fname == self.name or fname.startswith(self.name + "."):
                    os.remove(os.path.join(self.path, fname))
            if not os.listdir(self.path):
                os.rmdir(self.path)
        except FileNotFoundError:
            pass

    def open(self, write=False, create=False):
        self._make_db()
        if write and create:
            self._create_btree()

        conn = self._db.open()
        if write:
            transaction.begin()
            return ZodbWriter(conn)
        else:
            return ZodbReader(conn)

    def _create_btree(self):
        conn = self._db.open()
        transaction.begin()
        root = conn.root
        if not hasattr(root, "data"):
            conn.root.data = BTrees.OOBTree.OOBTree()
        transaction.commit()
        conn.close()

    def _make_db(self):
        if self._db is None:
            self._db = ZODB.DB(self.filepath)


class ZodbReader(DBReader):
    def __init__(self, conn):
        self._conn = conn
        self._root = conn.root
        self._data = self._root.data
        self.closed = False

    def __contains__(self, key):
        return key in self._data

    def __len__(self):
        return len(self._data)

    def __iter__(self):
        return self._data.iterkeys()

    def __getitem__(self, key):
        return self._data.__getitem__(key)

    def get(self, key, default=None):
        return self._data.get(key, default)

    def cursor(self):
        return ZodbCursor(self._data)

    def key_range(self, start, end):
        return self._data.iterkeys(min=start, max=end,
                                   excludemin=False, excludemax=True)

    def find(self, key):
        for key in self._data.keys(min=key):
            return key
        return None

    def keys(self):
        return self._data.keys()

    def items(self):
        return self._data.items()

    def close(self):
        self._conn.close()
        self.closed = True


class ZodbWriter(ZodbReader, DBWriter):
    def __init__(self, conn):
        self._conn = conn
        self._root = conn.root
        self._data = self._root.data
        self.closed = False

    def __setitem__(self, key, value):
        self._data.__setitem__(key, value)

    def __delitem__(self, key):
        try:
            self._data.__delitem__(key)
        except KeyError:
            pass

    def delete_by_prefix(self, prefix):
        data = self._data
        keys = list(self.cursor().expand_prefix(prefix))
        for key in keys:
            del data[key]

    def update(self, d):
        self._data.update(d)

    def cancel(self):
        transaction.abort()
        self.closed = True

    def close(self):
        self.commit()

    def commit(self):
        transaction.commit()
        self.closed = True


class ZodbCursor(Cursor):
    # Since ZODB Btrees don't have an available cursor object, we'll simulate
    # it by keeping a reference to an iterator
    def __init__(self, data):
        self._data = data
        self._key, self._value = None, None
        self._active = True
        self._it = self._data.iteritems()
        self.next()

    def is_active(self):
        return self._active

    def first(self):
        self._it = self._data.iteritems()
        self.next()

    def next(self):
        if not self._active:
            raise OverrunError
        try:
            self._key, self._value = next(self._it)
        except StopIteration:
            self._active = False
            self._key = None
            self._value = None

    def find(self, key, fromfirst=True):
        self._it = self._data.iteritems(min=key, excludemin=False)
        self.next()

    def key(self):
        return self._key

    def value(self):
        return self._value





