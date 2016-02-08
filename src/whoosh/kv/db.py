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
import tempfile
from abc import ABCMeta, abstractmethod
from bisect import bisect_left, bisect_right

from whoosh.compat import xrange, bytes_type
from whoosh.util import random_name


class ReadOnlyError(Exception):
    pass


class EmptyDatabaseError(Exception):
    pass


class LockError(Exception):
    """
    Raised if the database cannot be locked.
    """

    pass


class OverrunError(Exception):
    """
    Raised when a cursor or database method tries to read past the end of
    available data.
    """

    pass


class Database(object):
    """
    Base class for key-value database implementations.
    """

    __metaclass__ = ABCMeta

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def create(self):
        with self.open(create=True):
            return self

    def destroy(self):
        pass

    @abstractmethod
    def open(self, write=False, create=False):
        raise NotImplementedError

    def optimize(self):
        pass

    def close(self):
        pass

    @classmethod
    def temp(cls):
        """
        This class method exists to make testing easier. It simply instantiates
        an instance of the database class set up to write/read temporary data.

        Note that this method does **not** create the underlying directory,
        files, or whatever, to make the database valid to access. To do that,
        you must first call ``.create()`` on the returned database object.

        The default implementation simply creates a randomly named path in the
        temp directory and passes it as the only argument to the initializer.
        If a database doesn't use the filesystem, or requires extra arguments,
        it must override this method.

        :rtype: :class:`Database`
        """

        name = "%s_%s" % (cls.__name__, random_name(10))
        temppath = os.path.join(tempfile.gettempdir(), name)
        return cls(temppath)


class DBReader(object):
    __metaclass__ = ABCMeta

    def __init__(self):
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not exc_type:
            self.close()

    @abstractmethod
    def __len__(self):
        raise NotImplementedError

    def __iter__(self):
        return self.keys()

    @abstractmethod
    def __getitem__(self, item):
        raise NotImplementedError

    def __setitem__(self, key, value):
        raise ReadOnlyError

    def __delitem__(self, key):
        raise ReadOnlyError

    def update(self, d):
        raise ReadOnlyError

    def update_items(self, items):
        raise ReadOnlyError

    @abstractmethod
    def cursor(self):
        raise NotImplementedError(self.__class__)

    def writable(self):
        return False

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default

    def key_range(self, start, end):
        cur = self.cursor()
        cur.find(start)
        key = cur.key()
        while cur.is_active() and key < end:
            yield key
            cur.next()
            key = cur.key()

    def expand_prefix(self, prefix):
        return self.cursor().expand_prefix(prefix)

    def find(self, key):
        cur = self.cursor()
        cur.find(key)
        key = cur.key() if cur.is_active() else None
        cur.close()
        return key

    def keys(self):
        return iter(self)

    def items(self):
        return self.cursor().items()

    def close(self):
        self.closed = True


class DBWriter(DBReader):
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not self.closed:
            if exc_type:
                self.cancel()
            else:
                self.commit()

    @abstractmethod
    def __setitem__(self, key, value):
        raise NotImplementedError

    @abstractmethod
    def __delitem__(self, key):
        raise NotImplementedError

    def clear(self):
        for key in self:
            del self[key]

    def delete_by_prefix(self, prefix):
        # Hopefully you can override this implementation in your subclass
        keys = list(self.expand_prefix(prefix))
        for key in keys:
            del self[key]

    def writable(self):
        return True

    def update(self, d):
        self.update_items((key, d[key]) for key in d)

    def update_items(self, items):
        for key, value in items:
            self[key] = value

    def optimize(self):
        pass

    @abstractmethod
    def cancel(self):
        raise NotImplementedError

    @abstractmethod
    def commit(self):
        raise NotImplementedError

    def close(self):
        # This must be here because a DBWriter implements the DBReader interface
        # Just call commit()
        self.commit()


# Cursor

class Cursor(object):
    __metaclass__ = ABCMeta

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.close()

    def __iter__(self):
        return self.keys()

    def expand_prefix(self, prefix):
        key = self.key
        next_ = self.next

        self.find(prefix)
        while self.is_active():
            k = key()
            if not k.startswith(prefix):
                break
            yield k
            next_()

    @abstractmethod
    def is_active(self):
        raise NotImplementedError

    @abstractmethod
    def first(self):
        raise NotImplementedError

    @abstractmethod
    def next(self):
        raise NotImplementedError

    def find(self, key, fromfirst=True):
        # Be sure to replace this crappy default implementation when you
        # subclass!
        if fromfirst:
            self.first()
        while self.is_active() and self.key() < key:
            self.next()

    @abstractmethod
    def key(self):
        raise NotImplementedError

    @abstractmethod
    def value(self):
        raise NotImplementedError

    def item(self):
        return self.key(), self.value()

    def keys(self):
        while self.is_active():
            yield self.key()
            self.next()

    def items(self):
        while self.is_active():
            yield self.item()
            self.next()

    def close(self):
        pass


# Utility cursor objects

class EmptyCursor(Cursor):
    def is_active(self):
        return False

    def first(self):
        pass

    def next(self):
        raise Exception

    def find(self, key, fromfirst=True):
        pass

    def key(self):
        return None

    def value(self):
        return None


class MergeCursor(Cursor):
    def __init__(self, a, b):
        # B overrides A
        self._a = a
        self._b = b

    def __repr__(self):
        return "%s(%r, %r)" % (type(self).__name__, self._a, self._b)

    def is_active(self):
        return self._a.is_active() or self._b.is_active()

    def first(self):
        self._a.first()
        self._b.first()

    def next(self):
        if not self._a.is_active():
            return self._b.next()
        if not self._b.is_active():
            return self._a.next()

        ak = self._a.key()
        bk = self._b.key()
        if ak <= bk:
            self._a.next()
        if bk <= ak:
            self._b.next()

    def find(self, key, fromfirst=True):
        self._a.find(key, fromfirst)
        self._b.find(key, fromfirst)

    def key(self):
        if not self._a.is_active():
            return self._b.key()
        elif not self._b.is_active():
            return self._a.key()

        ak = self._a.key()
        bk = self._b.key()
        if ak <= bk:
            return ak
        else:
            return bk

    def value(self):
        if not self._a.is_active():
            return self._b.value()
        elif not self._b.is_active():
            return self._a.value()

        ak = self._a.key()
        bk = self._b.key()
        if ak <= bk:
            return self._a.value()
        else:
            return self._b.value()


class HideCursor(Cursor):
    def __init__(self, cursor, removeset):
        self._cur = cursor
        self._remove = removeset
        self.is_active = self._cur.is_active
        self.key = self._cur.key
        self.value = self._cur.value
        self.item = self._cur.item
        self._find_next()

    def _find_next(self):
        cur = self._cur
        remove = self._remove
        while cur.is_active() and cur.key() in remove:
            cur.next()

    def expand_prefix(self, prefix):
        remove = self._remove
        for key in self._cur.expand_prefix():
            if key not in remove:
                yield key

    def first(self):
        self._cur.first()
        self._find_next()

    def next(self):
        self._cur.next()
        self._find_next()

    def find(self, key, fromfirst=True):
        self._cur.find(key, fromfirst=fromfirst)
        self._find_next()

    def is_active(self):
        return self._cur.is_active()

    def key(self):
        return self._cur.key()

    def value(self):
        return self._cur.value()

    def item(self):
        return self._cur.item()
