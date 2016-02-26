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

"""
This module contains a simplistic key-value storage mechanism on top of Google's
AppEngine DataStore API. The underlying API does not provide transactions as a
first-class object, so there is **NO WAY** to make data access consistent. If
you read and write at the same time, strange things will happen.

It uses a memcache CAS operation as a write lock, so a failed operation can leave
the index write-locked until the key's timeout expires, and on the other hand
the lock might expire while you're indexing if it's not long enough.

I don't use Google App Engine (and don't encourage anyone else to), so this code
is pretty much untested. If you have to store your index in GAE, then this might
be helpful. Patches are welcome.
"""

import time

from google.appengine.api import memcache
from google.appengine.ext import db

from whoosh.kv.db import Database, DBReader, DBWriter, Cursor
from whoosh.kv.db import OverrunError, LockError


class KVPair(db.Model):
    key = db.Key
    value = db.Blob


class DataStoreDB(Database):
    def __init__(self, path, name="main"):
        self.name = name
        self._mutex = Mutex(name)

    def open(self, write=False, create=False, force=False):
        if write:
            if self._mutex.acquire() or force:
                return DataStoreWriter(self._mutex)
            else:
                raise LockError("DataStore index is locked")
        else:
            return DataStoreReader()


class DataStoreReader(DBReader):
    def __init__(self):
        self.closed = False

    def __len__(self):
        raise NotImplementedError

    def __getitem__(self, key):
        v = self.get(key)
        if v is None:
            raise KeyError(key)
        return v

    def get(self, key, default=None):
        ukey = key.decode("iso-8859-1")
        cached = memcache.get(ukey)
        if cached is not None:
            return cached

        # GAE uses unicode string keys, so "decode" the key to unicode using
        # a straight byte-to-char encoding
        pair = KVPair.get(ukey)
        if pair:
            memcache.add(ukey, pair.value)
            return pair.value
        else:
            return default

    def cursor(self):
        return DataStoreCursor()

    def key_range(self, start, end):
        q = KVPair.all(keys_only=True)
        q.filter("__key__ >=", start.decode("iso-8859-1"))
        q.filter("__key__ <", end.decode("iso-8859-1"))
        q.order("+__key__")
        for key in q.run():
            yield key.encode("iso-8859-1")

    def find(self, key):
        q = KVPair.all(keys_only=True)
        q.filter("__key__ >=", key.decode("iso-8859-1"))
        q.order("+__key__")
        for key in q.run(limit=1):
            return key.encode("iso-8859-1")
        else:
            return None

    def keys(self):
        q = KVPair.all(keys_only=True)
        for key in q.run():
            return key.encode("iso-8859-1")

    def items(self):
        q = KVPair.all()
        for pair in q.run():
            return pair.key.encode("iso-8859-1"), pair.value

    def close(self):
        self.closed = True


class DataStoreWriter(DataStoreReader, DBWriter):
    def __init__(self, mutex):
        self._mutex = mutex
        self.closed = False

    def __setitem__(self, key, value):
        ukey = key.decode("iso-8859-1")
        q = KVPair(key=ukey, value=value)
        q.put()
        memcache.add(ukey, value)

    def __delitem__(self, key):
        ukey = key.decode("iso-8859-1")
        db.delete(ukey)
        memcache.delete(ukey)

    def cancel(self):
        # No transaction support -- can't cancel
        self._mutex.release()
        self.closed = True

    def commit(self):
        # No transaction support -- can't commit
        self._mutex.release()
        self.closed = True


class DataStoreCursor(Cursor):
    def __init__(self):
        self._active = True
        self._q = None
        self._key = None
        self._value = None
        self.first()

    def is_active(self):
        return self._active

    def first(self):
        self._q = KVPair.all()
        self._q.order("+__key__")
        self.next()

    def find(self, key, fromfirst=True):
        self._q = KVPair.all()
        self._q.filter("__key__ >=", key.decode("iso-8859-1"))
        self._q.order("+__key__")
        self.next()

    def next(self):
        if not self._active:
            raise OverrunError
        try:
            pair = next(self._q)
        except StopIteration:
            self._active = False
            self._key = None
            self._value = None
        else:
            self._key = pair.key.encode("iso-8859-1")
            self._value = pair.value

    def key(self):
        return self._key

    def value(self):
        return self._value


class Mutex(object):
    def __init__(self, name):
        self.name = name

    def acquire(self):
        return memcache.add(self.name, 'dummy')

    def release(self):
        memcache.delete(self.name)
