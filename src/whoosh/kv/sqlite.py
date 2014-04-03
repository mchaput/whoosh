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

import os.path
import sqlite3 as sqlite
from sqlite3 import Binary

from whoosh.compat import b
from whoosh.kv.db import Database, DBReader, DBWriter, Cursor
from whoosh.kv.db import OverrunError, ReadOnlyError


class Sqlite(Database):
    default_table = "kv"
    drop = True

    def __init__(self, path, name="main"):
        self.path = path
        self.name = name
        self.filepath = os.path.join(self.path, self.name + ".sqlite")

    def _connection(self):
        c = sqlite.connect(self.filepath)
        # c.execute("PRAGMA read_uncommitted = 1;")
        return c

    def create(self, table=None):
        table = table or self.default_table
        if self.path:
            try:
                os.mkdir(self.path)
            except FileExistsError:
                pass

        c = self._connection()
        cur = c.cursor()
        cur.execute(
            "create table if not exists "
            + table
            + " (k blob primary key, v blob);"
        )
        c.commit()
        return self

    def destroy(self):
        if self.path:
            try:
                os.remove(self.filepath)
                if not os.listdir(self.path):
                    os.rmdir(self.path)
            except FileNotFoundError:
                pass

    def optimize(self):
        self._connection(False).execute("vacuum")

    def open(self, write=False, create=False):
        c = self._connection()
        return self.open_connection(c, write=write, create=create)

    def open_connection(self, conn, table=None, write=False, create=False,
                        closing=True):
        table = table or self.default_table
        if write:
            if create:
                self.create()
            return SqliteWriter(conn, table, closing=closing)
        else:
            return SqliteReader(conn, table, closing=closing)


class SqliteReader(DBReader):
    def __init__(self, connection, table, closing=True):
        self._c = connection
        self._table = table
        self._closing = closing
        self.closed = False

    def __len__(self):
        cur = self._c.execute("SELECT Count(*) FROM " + self._table)
        return cur.fetchone()[0]

    def __contains__(self, key):
        row = self._c.execute(
            "select 1 from "
            + self._table
            + " where k=? limit 1;", (Binary(key),)
        ).fetchone()
        return row is not None

    def __getitem__(self, key):
        row = self._c.execute(
            "select v from "
            + self._table
            + " where k=? limit 1;", (Binary(key),)
        ).fetchone()
        if row is None:
            raise KeyError(key)
        return bytes(row[0])

    def cursor(self):
        return SqliteCursor(self._c.cursor(), self._table)

    def keys(self):
        cur = self._c.execute("select k from " + self._table + " order by k;")
        for row in cur:
            yield bytes(row[0])

    def key_range(self, start, end):
        cur = self._c.execute(
            "select k from " + self._table + " where k >= ? and k < ?",
            (Binary(start), Binary(end))
        )
        for row in cur:
            yield bytes(row[0])

    def update_items(self, items):
        c = self._c
        for key, value in items:
            c.execute("insert or replace into "
                      + self._table
                      + " values (?, ?)",
                      (Binary(key), Binary(value)))

    def close(self):
        if self._closing:
            self._c.close()
        self.closed = True


class SqliteWriter(SqliteReader, DBWriter):
    def __init__(self, connection, table, closing=True):
        self._c = connection
        self._table = table
        self._closing = closing
        self._c.execute("begin immediate transaction")
        self.closed = False

    def __setitem__(self, key, value):
        self._c.execute(
            "insert or replace into " + self._table + " values (?, ?)",
            (Binary(key), Binary(value))
        )

    def __delitem__(self, key):
        self._c.execute(
            "delete from " + self._table + " where k=?", (Binary(key),)
        )

    def delete_by_prefix(self, prefix):
        self._c.execute(
            "delete from " + self._table + " where k like ?",
            (Binary(prefix + b"%"), )
        )

    def close(self):
        self.commit()

    def cancel(self):
        self._c.rollback()
        if self._closing:
            self._c.close()
        self.closed = True

    def commit(self):
        self._c.commit()
        if self._closing:
            self._c.close()
        self.closed = True


class SqliteCursor(Cursor):
    def __init__(self, cur, table):
        self._cur = cur
        self._table = table
        self._row = None
        self.first()

    def is_active(self):
        return self._row is not None

    def first(self):
        self._cur.execute("select k, v from " + self._table + " order by k;")
        self._row = self._cur.fetchone()

    def next(self):
        if self._row is None:
            raise OverrunError
        else:
            self._row = self._cur.fetchone()

    def find(self, key, fromfirst=True):
        self._cur.execute("select k, v from "
                          + self._table
                          + " where k >= ? order by k;", (key,))
        self._row = self._cur.fetchone()

    def key(self):
        if self._row is None:
            return None
        else:
            return self._row[0]

    def value(self):
        if self._row is None:
            return None
        else:
            return self._row[1]

    def item(self):
        return self._row

