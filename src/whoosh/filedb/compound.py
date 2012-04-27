# Copyright 2011 Matt Chaput. All rights reserved.
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

import errno, mmap, sys
from threading import Lock
from shutil import copyfileobj

from whoosh.compat import BytesIO, memoryview_
from whoosh.filedb.structfile import StructFile
from whoosh.filedb.filestore import FileStorage
from whoosh.system import emptybytes


class CompoundStorage(FileStorage):
    readonly = True

    def __init__(self, store, name, use_mmap=True, basepos=0):
        self.name = name
        self.file = store.open_file(name)
        self.file.seek(basepos)

        self.diroffset = self.file.read_long()
        self.dirlength = self.file.read_int()
        self.file.seek(self.diroffset)
        self.dir = self.file.read_pickle()
        self.options = self.file.read_pickle()
        self.locks = {}
        self.source = None

        if use_mmap and store.supports_mmap and hasattr(self.file, "fileno"):
            # Try to open the entire segment as a memory-mapped object
            try:
                fileno = self.file.fileno()
                self.source = mmap.mmap(fileno, 0, access=mmap.ACCESS_READ)
                # If that worked, we can close the file handle we were given
                self.file.close()
                self.file = None
            except OSError:
                e = sys.exc_info()[1]
                # If we got an error because there wasn't enough memory to
                # open the map, ignore it and fall through, we'll just use the
                # (slower) "sub-file" implementation
                if e.errno == errno.ENOMEM:
                    pass

    def __repr__(self):
        return "<%s (%s)>" % (self.__class__.__name__, self.name)

    def close(self):
        if self.source:
            self.source.close()
        if self.file:
            self.file.close()

    def open_file(self, name, *args, **kwargs):
        info = self.dir[name]
        offset = info["offset"]
        length = info["length"]

        if self.source:
            # Create a memoryview/buffer from the mmap
            buf = memoryview_(self.source, offset, length)
            f = BytesIO(buf)
        else:
            # If mmap is not available, use the slower sub-file implementation
            f = SubFile(self.file, offset, length)
        return StructFile(f, name=name)

    def list(self):
        return list(self.dir.keys())

    def file_exists(self, name):
        return name in self.dir

    def file_length(self, name):
        info = self.dir[name]
        return info["length"]

    def file_modified(self, name):
        info = self.dir[name]
        return info["modified"]

    def lock(self, name):
        if name not in self.locks:
            self.locks[name] = Lock()
        return self.locks[name]

    @staticmethod
    def assemble(dbfile, store, names, **options):
        assert names, names

        directory = {}
        basepos = dbfile.tell()
        dbfile.write_long(0)  # Directory position
        dbfile.write_int(0)  # Directory length

        # Copy the files into the compound file
        for name in names:
            if name.endswith(".toc") or name.endswith(".seg"):
                raise Exception(name)

        for name in names:
            offset = dbfile.tell()
            length = store.file_length(name)
            modified = store.file_modified(name)
            directory[name] = {"offset": offset, "length": length,
                               "modified": modified}
            f = store.open_file(name)
            copyfileobj(f, dbfile)
            f.close()

        dirpos = dbfile.tell()  # Remember the start of the directory
        dbfile.write_pickle(directory)  # Write the directory
        dbfile.write_pickle(options)
        endpos = dbfile.tell()  # Remember the end of the directory
        dbfile.flush()
        dbfile.seek(basepos)  # Seek back to the start
        dbfile.write_long(dirpos)  # Directory position
        dbfile.write_int(endpos - dirpos)  # Directory length

        dbfile.close()


class SubFile(object):
    def __init__(self, parentfile, offset, length, name=None):
        self._file = parentfile
        self._offset = offset
        self._length = length
        self._pos = 0

        self.name = name
        self.closed = False

    def close(self):
        self.closed = True

    def read(self, size=None):
        if size is None:
            size = self._length - self._pos
        else:
            size = min(size, self._length - self._pos)
        if size < 0:
            size = 0

        if size > 0:
            self._file.seek(self._offset + self._pos)
            self._pos += size
            return self._file.read(size)
        else:
            return emptybytes

    def readline(self):
        maxsize = self._length - self._pos
        self._file.seek(self._offset + self._pos)
        data = self._file.readline()
        if len(data) > maxsize:
            data = data[:maxsize]
        self._pos += len(data)
        return data

    def seek(self, where, whence=0):
        if whence == 0:  # Absolute
            pos = where
        elif whence == 1:  # Relative
            pos = self._pos + where
        elif whence == 2:  # From end
            pos = self._length - where

        self._pos = pos

    def tell(self):
        return self._pos



