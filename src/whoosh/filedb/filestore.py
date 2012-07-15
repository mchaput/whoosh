# Copyright 2009 Matt Chaput. All rights reserved.
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

import os, random
from threading import Lock

from whoosh.compat import BytesIO, memoryview_
from whoosh.filedb.structfile import BufferFile, StructFile
from whoosh.index import _DEF_INDEX_NAME, EmptyIndexError
from whoosh.util.filelock import FileLock


# Exceptions

class ReadOnlyError(Exception):
    pass


# Base class

class Storage(object):
    """Abstract base class for storage objects.
    """

    readonly = False
    supports_mmap = False

    def __iter__(self):
        return iter(self.list())

    def create_index(self, schema, indexname=_DEF_INDEX_NAME, indexclass=None):
        if self.readonly:
            raise ReadOnlyError
        if indexclass is None:
            import whoosh.index
            indexclass = whoosh.index.FileIndex
        return indexclass.create(self, schema, indexname)

    def open_index(self, indexname=_DEF_INDEX_NAME, schema=None, indexclass=None):
        if indexclass is None:
            import whoosh.index
            indexclass = whoosh.index.FileIndex
        return indexclass(self, schema=schema, indexname=indexname)

    def index_exists(self, indexname=None):
        if indexname is None:
            indexname = _DEF_INDEX_NAME
        try:
            ix = self.open_index(indexname)
            gen = ix.latest_generation()
            ix.close()
            return gen > -1
        except EmptyIndexError:
            pass
        return False

    def create_file(self, name):
        raise NotImplementedError

    def create_temp(self):
        name = hex(random.getrandbits(128))[2:] + ".tmp"
        return name, self.create_file(name)

    def open_file(self, name, *args, **kwargs):
        raise NotImplementedError

    def list(self):
        raise NotImplementedError

    def file_exists(self, name):
        raise NotImplementedError

    def file_modified(self, name):
        raise NotImplementedError

    def file_length(self, name):
        raise NotImplementedError

    def delete_file(self, name):
        raise NotImplementedError

    def rename_file(self, frm, to, safe=False):
        raise NotImplementedError

    def lock(self, name):
        raise NotImplementedError

    def close(self):
        pass

    def optimize(self):
        pass


class OverlayStorage(Storage):
    """Overlays two storage objects. Reads are processed from the first if it
    has the named file, otherwise the second. Writes always go to the second.
    """

    def __init__(self, a, b):
        self.a = a
        self.b = b

    def create_index(self, *args, **kwargs):
        self.b.create_index(*args, **kwargs)

    def open_index(self, *args, **kwargs):
        self.a.open_index(*args, **kwargs)

    def create_file(self, *args, **kwargs):
        return self.b.create_file(*args, **kwargs)

    def create_temp(self, *args, **kwargs):
        return self.b.create_temp(*args, **kwargs)

    def open_file(self, name, *args, **kwargs):
        if self.a.file_exists(name):
            return self.a.open_file(name, *args, **kwargs)
        else:
            return self.b.open_file(name, *args, **kwargs)

    def list(self):
        return list(set(self.a.list()) | set(self.b.list()))

    def file_exists(self, name):
        return self.a.file_exists(name) or self.b.file_exists(name)

    def file_modified(self, name):
        if self.a.file_exists(name):
            return self.a.file_modified(name)
        else:
            return self.b.file_modified(name)

    def file_length(self, name):
        if self.a.file_exists(name):
            return self.a.file_length(name)
        else:
            return self.b.file_length(name)

    def delete_file(self, name):
        return self.b.delete_file(name)

    def rename_file(self, *args, **kwargs):
        raise NotImplementedError

    def lock(self, name):
        return self.b.lock(name)

    def close(self):
        self.a.close()
        self.b.close()

    def optimize(self):
        self.a.optimize()
        self.b.optimize()


class FileStorage(Storage):
    """Storage object that stores the index as files in a directory on disk.
    """

    supports_mmap = True

    def __init__(self, path, supports_mmap=True, readonly=False, debug=False):
        self.folder = path
        self.supports_mmap = supports_mmap
        self.readonly = readonly
        self._debug = debug
        self.locks = {}

        if not os.path.exists(path):
            raise IOError("Directory %s does not exist" % path)

    def create_file(self, name, excl=False, mode="wb", **kwargs):
        if self.readonly:
            raise ReadOnlyError

        path = self._fpath(name)
        if excl:
            flags = os.O_CREAT | os.O_EXCL | os.O_RDWR
            if hasattr(os, "O_BINARY"):
                flags |= os.O_BINARY
            fd = os.open(path, flags)
            fileobj = os.fdopen(fd, mode)
        else:
            fileobj = open(path, mode)

        f = StructFile(fileobj, name=name, **kwargs)
        return f

    def open_file(self, name, *args, **kwargs):
        f = StructFile(open(self._fpath(name), "rb"), name=name, *args,
                       **kwargs)
        return f

    def _fpath(self, fname):
        return os.path.abspath(os.path.join(self.folder, fname))

    def clean(self):
        if self.readonly:
            raise ReadOnlyError

        path = self.folder
        if not os.path.exists(path):
            os.mkdir(path)

        files = self.list()
        for fname in files:
            os.remove(os.path.join(path, fname))

    def list(self):
        try:
            files = os.listdir(self.folder)
        except IOError:
            files = []

        return files

    def file_exists(self, name):
        return os.path.exists(self._fpath(name))

    def file_modified(self, name):
        return os.path.getmtime(self._fpath(name))

    def file_length(self, name):
        return os.path.getsize(self._fpath(name))

    def delete_file(self, name):
        if self.readonly:
            raise ReadOnlyError

        os.remove(self._fpath(name))

    def rename_file(self, oldname, newname, safe=False):
        if self.readonly:
            raise ReadOnlyError

        if os.path.exists(self._fpath(newname)):
            if safe:
                raise NameError("File %r exists" % newname)
            else:
                os.remove(self._fpath(newname))
        os.rename(self._fpath(oldname), self._fpath(newname))

    def lock(self, name):
        return FileLock(self._fpath(name))

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, repr(self.folder))


class RamStorage(Storage):
    """Storage object that keeps the index in memory.
    """

    supports_mmap = False

    def __init__(self):
        self.files = {}
        self.locks = {}
        self.folder = ''

    def list(self):
        return list(self.files.keys())

    def clean(self):
        self.files = {}

    def total_size(self):
        return sum(self.file_length(f) for f in self.list())

    def file_exists(self, name):
        return name in self.files

    def file_length(self, name):
        if name not in self.files:
            raise NameError(name)
        return len(self.files[name])

    def file_modified(self, name):
        return -1

    def delete_file(self, name):
        if name not in self.files:
            raise NameError(name)
        del self.files[name]

    def rename_file(self, name, newname, safe=False):
        if name not in self.files:
            raise NameError(name)
        if safe and newname in self.files:
            raise NameError("File %r exists" % newname)

        content = self.files[name]
        del self.files[name]
        self.files[newname] = content

    def create_file(self, name, **kwargs):
        def onclose_fn(sfile):
            self.files[name] = sfile.file.getvalue()
        f = StructFile(BytesIO(), name=name, onclose=onclose_fn)
        return f

    def open_file(self, name, **kwargs):
        if name not in self.files:
            raise NameError(name)
        buf = memoryview_(self.files[name])
        return BufferFile(buf, name=name, **kwargs)

    def lock(self, name):
        if name not in self.locks:
            self.locks[name] = Lock()
        return self.locks[name]


def copy_to_ram(storage):
    """Copies the given FileStorage object into a new RamStorage object.
    
    :rtype: :class:`RamStorage`
    """

    import shutil
    ram = RamStorage()
    for name in storage.list():
        f = storage.open_file(name)
        r = ram.create_file(name)
        shutil.copyfileobj(f.file, r.file)
        f.close()
        r.close()
    return ram
