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

import mmap
from threading import Lock
from shutil import copyfileobj

from whoosh.compat import BytesIO, PY3
from whoosh.filedb.structfile import StructFile
from whoosh.filedb.filestore import FileStorage


class CompoundStorage(FileStorage):
    readonly = True

    def __init__(self, store, name, basepos=0):
        self.name = name
        f = store.open_file(name)
        f.seek(basepos)

        self.diroffset = f.read_long()
        self.dirlength = f.read_int()
        f.seek(self.diroffset)
        self.dir = f.read_pickle()
        self.options = f.read_pickle()

        if store.supports_mmap:
            self.source = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ)
        else:
            # Can't mmap files in this storage object, so we'll have to take
            # the hit and read the whole file as a string :(
            f.seek(basepos)
            self.source = f.read(self.diroffset)
        f.close()
        self.locks = {}

    def __repr__(self):
        return "<%s (%s)>" % (self.__class__.__name__, self.name)

    def open_file(self, name, *args, **kwargs):
        info = self.dir[name]
        offset = info["offset"]
        length = info["length"]
        if PY3:
            buf = memoryview(self.source)[offset:offset + length]
        else:
            buf = buffer(self.source, offset, length)
        f = StructFile(BytesIO(buf), name=name)
        return f

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
    def assemble(out, store, names, **options):
        assert names, names

        dir = {}
        basepos = out.tell()
        out.write_long(0)  # Directory position
        out.write_int(0)  # Directory length

        # Copy the files into the compound file
        for name in names:
            if name.endswith(".toc") or name.endswith(".seg"):
                raise Exception(name)

        for name in names:
            offset = out.tell()
            length = store.file_length(name)
            modified = store.file_modified(name)
            dir[name] = {"offset": offset, "length": length,
                         "modified": modified}
            f = store.open_file(name)
            copyfileobj(f, out)
            f.close()

        dirpos = out.tell()  # Remember the start of the directory
        out.write_pickle(dir)  # Write the directory
        out.write_pickle(options)
        endpos = out.tell()  # Remember the end of the directory
        out.flush()
        out.seek(basepos)  # Seek back to the start
        out.write_long(dirpos)  # Directory position
        out.write_int(endpos - dirpos)  # Directory length

        out.close()





