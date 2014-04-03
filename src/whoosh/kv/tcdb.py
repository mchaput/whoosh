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

from tc import BDB, BDBOWRITER, BDBOCREAT

from whoosh.kv.db import Database, DBReader, DBWriter, Cursor
from whoosh.kv.db import OverrunError, ReadOnlyError


class TC(Database):
    def __init__(self, path):
        self.path = path

    def create(self):
        w = self.open(write=True, create=True)
        w.close()
        return self

    def optimize(self):
        pass

    def open(self, write=False, create=False):
        flags = 0
        if write:
            flags |= BDBOWRITER
        if create:
            flags |= BDBOCREAT

        db = BDB()
        db.open(self.path, flags)
        if write:
            return TokyoWriter(db)
        else:
            return TokyoReader(db)


class TokyoReader(DBReader):
    def __init__(self, db):
        self._db = db
        self.closed = False

    def __len__(self):
        return len(self._db)

    def __contains__(self, key):
        return key in self._db

    def __getitem__(self, key):
        return self._db[key]

    def get(self, key, default=None):
        try:
            return self._db.get(key)
        except KeyError:
            return default

    def cursor(self):
        return TCCursor(self._db.curnew())

    def keys(self):
        try:
            return self._db.iterkeys()
        except KeyError:
            return []

    def values(self):
        try:
            return self._db.itervalues()
        except KeyError:
            return []

    def items(self):
        # For some reason, iteritems() returns unicode instead of bytes (!)
        # so do it manually :(
        db = self._db
        for key in self.keys():
            yield key, db[key]

    def key_range(self, start, end):
        cur = self._db.curnew()
        try:
            cur.jump(start)
            while True:
                key = cur.key()
                if key >= end:
                    return
                yield key
                cur.next()
        except KeyError:
            return

    def close(self):
        self._db.close()
        self.closed = True


class TokyoWriter(TokyoReader, DBWriter):
    def __init__(self, db):
        self._db = db
        self._db.tranbegin()
        self.closed = False

    def __setitem__(self, key, value):
        self._db[key] = value

    def __delitem__(self, key):
        try:
            self._db.out(key)
        except KeyError:
            pass

    def delete_by_prefix(self, prefix):
        cur = self.cursor()
        cur.find(prefix)
        while cur.is_active() and cur.key().startswith(prefix):
            cur.delete()
        cur.close()

    def cancel(self):
        self._db.tranabort()
        self._db.close()
        self.closed = True

    def commit(self):
        self._db.trancommit()
        self._db.close()
        self.closed = True


class TCCursor(Cursor):
    def __init__(self, cur):
        self._cur = cur
        self._active = False
        self.first()

    def is_active(self):
        return self._active

    def first(self):
        try:
            self._cur.first()
        except KeyError:
            self._active = False
        else:
            self._active = True

    def find(self, key, fromfirst=True):
        try:
            self._cur.jump(key)
        except KeyError:
            self._active = False

    def next(self):
        if not self._active:
            raise OverrunError
        try:
            self._cur.next()
        except KeyError:
            self._active = False

    def key(self):
        if self._active:
            return self._cur.key()
        else:
            return None

    def value(self):
        if self._active:
            return self._cur.val()
        else:
            return None

    def delete(self):
        self._cur.out()
