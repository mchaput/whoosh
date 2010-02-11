#===============================================================================
# Copyright 2009 Matt Chaput
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#===============================================================================

import os
from cStringIO import StringIO
from threading import Lock

from whoosh.index import _DEF_INDEX_NAME
from whoosh.store import Storage
from whoosh.support.filelock import FileLock
from whoosh.filedb.structfile import StructFile


class FileStorage(Storage):
    """Storage object that stores the index as files in a directory on disk.
    """

    def __init__(self, path, mapped=True):
        self.folder = path
        self.mapped = mapped
        self.locks = {}

        if not os.path.exists(path):
            raise IOError("Directory %s does not exist" % path)

    def __iter__(self):
        return iter(self.list())

    def create_index(self, schema, indexname=_DEF_INDEX_NAME):
        from whoosh.filedb.fileindex import FileIndex
        return FileIndex(self, schema=schema, create=True, indexname=indexname)

    def open_index(self, indexname=_DEF_INDEX_NAME, schema=None):
        from whoosh.filedb.fileindex import FileIndex
        return FileIndex(self, schema=schema, indexname=indexname)

    def create_file(self, name):
        f = StructFile(open(self._fpath(name), "wb"), name=name,
                       mapped=self.mapped)
        return f

    def open_file(self, name, *args, **kwargs):
        f = StructFile(open(self._fpath(name), "rb"), *args, **kwargs)
        f._name = name
        return f

    def _fpath(self, fname):
        return os.path.join(self.folder, fname)

    def clean(self):
        path = self.folder
        if not os.path.exists(path):
            os.mkdir(path)

        files = self.list()
        for file in files:
            os.remove(os.path.join(path, file))

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
        os.remove(self._fpath(name))

    def rename_file(self, frm, to, safe=False):
        if os.path.exists(self._fpath(to)):
            if safe:
                raise NameError("File %r exists" % to)
            else:
                os.remove(self._fpath(to))
        os.rename(self._fpath(frm), self._fpath(to))

    def _getlock(self, name):
        return FileLock(self._fpath(name))

    def lock(self, name):
        return FileLock(self._fpath(name))

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, repr(self.folder))


class RamStorage(FileStorage):
    """Storage object that keeps the index in memory.
    """

    def __init__(self):
        self.files = {}
        self.locks = {}
        self.folder = ''

    def __iter__(self):
        return iter(self.list())

    def list(self):
        return self.files.keys()

    def clean(self):
        self.files = {}

    def total_size(self):
        return sum(self.file_length(f) for f in self.list())

    def file_exists(self, name):
        return name in self.files

    def file_length(self, name):
        if name not in self.files:
            raise NameError
        return len(self.files[name])

    def delete_file(self, name):
        if name not in self.files:
            raise NameError
        del self.files[name]

    def rename_file(self, name, newname, safe=False):
        if name not in self.files:
            raise NameError("File %r does not exist" % name)
        if safe and newname in self.files:
            raise NameError("File %r exists" % newname)

        content = self.files[name]
        del self.files[name]
        self.files[newname] = content

    def create_file(self, name):
        def onclose_fn(sfile):
            self.files[name] = sfile.file.getvalue()
        f = StructFile(StringIO(), name=name, onclose=onclose_fn)
        return f

    def open_file(self, name, *args, **kwargs):
        if name not in self.files:
            raise NameError
        return StructFile(StringIO(self.files[name]), *args, **kwargs)

    def lock(self, name):
        if name not in self.locks:
            self.locks[name] = Lock()
        return self.locks[name]


def copy_to_ram(storage):
    """Copies the given FileStorage object into a new RamStorage object.
    
    :rtype: :class:`RamStorage`
    """

    import shutil #, time
    #t = time.time()
    ram = RamStorage()
    for name in storage.list():
        f = storage.open_file(name)
        r = ram.create_file(name)
        shutil.copyfileobj(f.file, r.file)
        f.close()
        r.close()
    #print time.time() - t, "to load index into ram"
    return ram









