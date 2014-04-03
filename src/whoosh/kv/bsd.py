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

import errno
import os.path
import sys

from bsddb3 import db

from whoosh.kv.db import Database, DBReader, DBWriter, Cursor
from whoosh.kv.db import OverrunError, ReadOnlyError


class BSD(Database):
    def __init__(self, path, name="main"):
        self.path = path
        self.name = name

    def _create_dir(self):
        path = self.path
        if not os.path.exists(path):
            try:
                os.makedirs(path)
            except OSError:
                e = sys.exc_info()[0]
                if e.errno != errno.EEXIST:
                    raise

    def _env(self, create):
        flags = db.DB_INIT_MPOOL | db.DB_INIT_LOCK | db.DB_INIT_LOG | db.DB_INIT_TXN
        if create:
            self._create_dir()
            flags |= db.DB_CREATE
        e = db.DBEnv()
        e.open(self.path, flags)
        return e

    def create(self):
        e = self._env(True)
        e.close()

    def destroy(self):
        env = db.DBEnv()
        env.dbremove(self.path, self.name)
        env.remove(self.path)
        env.close()
        # import shutil
        # try:
        #     shutil.rmtree(self.path)
        # except FileNotFoundError:
        #     pass

    def _open_btree(self, write=False, create=False):
        e = self._env(create)
        txn = e.txn_begin(flags=db.DB_TXN_SNAPSHOT)
        bt = db.DB(e)
        flags = 0
        if write:
            flags |= db.DB_MULTIVERSION | db.DB_CREATE
        else:
            flags |= db.DB_RDONLY
        bt.open(self.name, db.DB_BTREE, flags=flags, txn=txn)

        return e, bt, txn

    def optimize(self):
        return
        # env, bt, txn = self._open_btree(write=True)
        # bt.compact()
        # bt.close()
        # env.close()

    def open(self, write=False, create=False):
        env, bt, txn = self._open_btree(write=write, create=create)
        if write:
            return BSDWriter(env, bt, txn)
        else:
            return BSDReader(env, bt, txn)

    def close(self):
        pass


class BSDReader(DBReader):
    def __init__(self, env, bt, txn):
        self._env = env
        self._bt = bt
        self._txn = txn
        self.closed = False

    def __len__(self):
        return self._bt.stat(txn=self._txn)["nkeys"]

    def __contains__(self, key):
        return self._bt.has_key(key, txn=self._txn)

    def __getitem__(self, key):
        v = self._bt.get(key, txn=self._txn)
        if v is None:
            raise KeyError
        return v

    def get(self, key, default=None):
        return self._bt.get(key, default=default, txn=self._txn)

    def cursor(self):
        return BSDCursor(self._bt.cursor(txn=self._txn))

    def close(self):
        self._bt.close()
        self._env.close()
        self.closed = True


class BSDWriter(BSDReader, DBWriter):
    def __init__(self, env, bt, txn):
        self._env = env
        self._bt = bt
        self._txn = txn
        self.closed = False

    def __setitem__(self, key, value):
        self._bt.put(key, value, txn=self._txn)

    def __delitem__(self, key):
        try:
            self._bt.delete(key, txn=self._txn)
        except db.DBNotFoundError:
            pass

    def delete_by_prefix(self, prefix):
        cur = self.cursor()
        cur.find(prefix)
        while cur.is_active() and cur.key().startswith(prefix):
            cur.delete()
            cur.next()
        cur.close()

    def optimize(self):
        return
        # self._bt.compact()

    def cancel(self):
        self._txn.abort()
        self._bt.close()
        self._env.close()
        self.closed = True

    def commit(self):
        self._txn.commit()
        self._bt.close()
        self._env.close()
        self.closed = True


class BSDCursor(Cursor):
    # BSDDB does not allow the cursor to be "at the end" of the data; it can
    # only be moved to the last pair. We'll emulate the ability to move just
    # past the last pair by setting _atend.

    def __init__(self, cur):
        self._cur = cur
        self._key = self._value = None
        self._atend = False

        # A BSDDB cursor begins in an unitialized state, call first() to start
        # it at the first pair.
        self.first()

    def is_active(self):
        return not self._atend

    def first(self):
        item = self._cur.first()
        if item is None:
            self._atend = True
        else:
            self._key, self._value = item

    def next(self):
        if self._atend:
            raise OverrunError
        item = self._cur.next()
        if item is None:
            self._atend = True
        else:
            self._key, self._value = item

    def find(self, key, fromfirst=True):
        item = self._cur.set_range(key)
        if item is None:
            self._atend = True
        else:
            self._key, self._value = item

    def key(self):
        return None if self._atend else self._key

    def value(self):
        return None if self._atend else self._value

    def item(self):
        return (None, None) if self._atend else self._key, self._value

    def delete(self):
        self._cur.delete()

    def close(self):
        self._cur.close()
