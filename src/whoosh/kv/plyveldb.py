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

import plyvel

from whoosh.index import LockError
from whoosh.kv.db import Database, DBReader, DBWriter, Cursor
from whoosh.kv.db import MergeCursor, HideCursor
from whoosh.kv.db import OverrunError, ReadOnlyError


class Plyvel(Database):
    def __init__(self, path, name="main"):
        self.path = path
        self.name = name
        self.filepath = os.path.join(self.path, self.name)

    def create(self):
        try:
            os.mkdir(self.path)
        except FileExistsError:
            pass
        try:
            os.mkdir(self.filepath)
        except FileExistsError:
            pass

    def destroy(self, *args, **kwargs):
        try:
            shutil.rmtree(self.filepath)
            if not os.listdir(self.path):
                os.rmdir(self.path)
        except IOError:
            pass

    def optimize(self):
        pass

    def open(self, write=False, create=False):
        olddb = plyvel.DB(self.filepath, create_if_missing=True)
        if write:
            newpath = self.filepath + ".transaction"
            try:
                os.mkdir(newpath)
            except FileExistsError:
                raise LockError("Database is locked")
            newdb = plyvel.DB(newpath, create_if_missing=True)
            return PlyvelWriter(olddb, newpath, newdb)
        else:
            snapshot = olddb.snapshot()
            return PlyvelReader(olddb, snapshot)

    def close(self):
        pass


class PlyvelReader(DBReader):
    def __init__(self, rawdb, snapshot):
        self._rawdb = rawdb
        self._db = snapshot
        self.closed = False

    def __contains__(self, key):
        # LevelDB has no efficient way to check if a key exists!
        v = self._db.get(key)
        return not v is None

    def __iter__(self):
        return self.keys()

    def __len__(self):
        #  LevelDB has no efficient way to get the number of keys in the db
        count = 0
        for _ in self:
            count += 1
        return count

    def __getitem__(self, key):
        v = self._db.get(key)
        if v is None:
            raise KeyError(key)
        return v

    def cursor(self):
        return PlyvelCursor(self._db)

    def get(self, key, default=None):
        return self._db.get(key, default=default)

    def keys(self):
        return self._db.iterator(include_key=True, include_value=False)

    def key_range(self, start, end):
        return self._db.iterator(include_key=True, include_value=False,
                                     start=start, stop=end)

    def values(self):
        return self._db.iterator(include_key=False, include_value=True)

    def items(self):
        return self._db.iterator(include_key=True, include_value=True)

    def close(self):
        self._db.close()
        self._rawdb.close()
        self.closed = True


class PlyvelWriter(PlyvelReader, DBWriter):
    # Because LevelDB lacks transactions (among so many other features that a
    # well engineered key-value database should have), we simulate a transaction
    # using a second database

    def __init__(self, db, newpath, newdb):
        self._db = db
        self._newpath = newpath
        self._newdb = newdb
        self._deleted = set()
        self.closed = False

    def __contains__(self, key):
        if key in self._deleted:
            return False
        return PlyvelReader.__contains__(self, key)

    def __getitem__(self, key):
        v = self._get(key)
        if v is None:
            raise KeyError(key)
        return v

    def __setitem__(self, key, value):
        self._newdb.put(key, value)
        self._deleted.discard(key)

    def __delitem__(self, key):
        self._newdb.delete(key)
        self._deleted.add(key)

    def _get(self, key):
        v = self._newdb.get(key)
        if v is None:
            if key in self._deleted:
                return None
            v = self._db.get(key)
        return v

    def get(self, key, default=None):
        v = self._get(key)
        v = default if v is None else v
        return v

    def cursor(self):
        a = PlyvelCursor(self._db)
        b = PlyvelCursor(self._newdb)
        cursor = MergeCursor(a, b)
        if self._deleted:
            cursor = HideCursor(cursor, self._deleted)
        return cursor

    def writable(self):
        return True

    def update_items(self, items):
        batch = self._newdb.write_batch()
        for key, value in items:
            batch.put(key, value)
        batch.write()

    def key_range(self, start, end):
        return DBReader.key_range(self, start, end)

    def keys(self):
        return self.cursor().keys()

    def items(self):
        return self.cursor().items()

    def _delete_new_dir(self):
        shutil.rmtree(self._newpath)

    def commit(self):
        # Batch copy the items from the "new" database back to the main database
        batch = self._db.write_batch()
        for key, value in self._newdb.iterator(include_key=True,
                                               include_value=True):
            batch.put(key, value)
        for key in self._deleted:
            batch.delete(key)
        batch.write()
        self._db.close()

        # Close the "new" database and delete it
        self._newdb.close()
        self._delete_new_dir()
        self.closed = True

    def cancel(self):
        self._db.close()

        # Close the "new" database and delete it
        self._newdb.close()
        self._delete_new_dir()
        self.closed = True


class PlyvelCursor(Cursor):
    def __init__(self, db):
        self._it = db.raw_iterator()
        self._it.seek_to_first()
        self.close = self._it.close

    def is_active(self):
        return self._it.valid()

    def first(self):
        self._it.seek_to_first()

    def find(self, key, fromfirst=True):
        self._it.seek(key)

    def key(self):
        return self._it.key() if self._it.valid() else None

    def value(self):
        return self._it.value() if self._it.valid() else None

    def next(self):
        if not self._it.valid():
            raise OverrunError
        self._it.next()

