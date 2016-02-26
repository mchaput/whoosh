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

from __future__ import with_statement
import errno
import io
import mmap
import os
import sys
import tempfile
from abc import abstractmethod
from binascii import crc32
from io import BytesIO
from threading import Lock
from typing import Any, Dict, Iterable, List

import furl

from whoosh import fields, index
from whoosh.ifaces import storage
from whoosh.filedb import datafile
from whoosh.metadata import MetaData
from whoosh.system import IS_LITTLE
from whoosh.util import random_name
from whoosh.util.filelock import FileLock


# Type aliases

if sys.version_info[0] >= 3:
    File = io.IOBase
else:
    File = file


# TOC header

class TocHeader(MetaData):
    magic_bytes = b"Wtoc"
    flags = "was_little"
    field_order = "length checksum"

    length = "i"
    checksum = "I"


# Session class

class FileSession(storage.Session):
    def __init__(self, store: 'BaseFileStorage', indexname: str,
                 writable: bool):
        super(FileSession, self).__init__(store, indexname, writable)
        if writable:
            self._lock = store.lock(indexname + "_LOCK")
            if not self._lock.acquire():
                raise Exception("Could not lock writable session")
        else:
            self._lock = None

    def close(self):
        if self._lock:
            self._lock.release()


# Base class

class BaseFileStorage(storage.Storage):
    """
    Abstract base class for storage objects.

    A storage object is a virtual flat filesystem, allowing the creation and
    retrieval of file-like objects
    (:class:`~whoosh.filedb.structfile.StructFile` objects). The default
    implementation (:class:`FileStorage`) uses actual files in a directory.

    All access to files in Whoosh goes through this object. This allows more
    different forms of storage (for example, in RAM, in a database, in a single
    file) to be used transparently.

    For example, to create a :class:`FileStorage` object::

        # Create a storage object
        st = FileStorage("indexdir")
        # Create the directory if it doesn't already exist
        st.create()

    The :meth:`Storage.create` method makes it slightly easier to swap storage
    implementations. The ``create()`` method handles set-up of the storage
    object. For example, ``FileStorage.create()`` creates the directory. A
    database implementation might create tables. This is designed to let you
    avoid putting implementation-specific setup code in your application.
    """

    readonly = False
    supports_mmap = False

    def __iter__(self) -> Iterable[str]:
        return iter(self.list())

    def __enter__(self):
        self.create()
        return self

    def __exit__(self, *_):
        self.close()

    # Implement index methods using files

    def open(self, indexname: str=None, writable: bool=False) -> FileSession:
        indexname = indexname or index.DEFAULT_INDEX_NAME
        return FileSession(self, indexname, writable)

    def save_toc(self, session: 'storage.Session', toc: 'index.Toc'):
        # This backend has no concept of a session, we just need the indexname
        indexname = session.indexname

        # Write the file with a temporary name so other processes don't notice
        # it until it's done
        real_filename = toc.make_filename(indexname, toc.generation)
        temp_filename = toc.make_filename(indexname, toc.generation, ".tmp")

        tocbytes = toc.to_bytes()
        toclen = len(tocbytes)
        check = crc32(tocbytes)
        headbytes = TocHeader(was_little=IS_LITTLE, length=toclen,
                              checksum=check).encode()

        with self.create_file(temp_filename) as f:
            f.write(headbytes)
            f.write(tocbytes)
            f.write_uint_le(check)

        # Rename the file into place
        self.rename_file(temp_filename, real_filename, safe=True)

    def latest_generation(self, session: 'storage.Session'):
        indexname = session.indexname
        regex = index.Toc.toc_regex(indexname)

        mx = -2
        for filename in self:
            m = regex.match(filename)
            if m:
                mx = max(int(m.group(1)), mx)

        if mx == -2:
            raise storage.TocNotFound(indexname)

        return mx

    def load_toc(self, session: 'storage.Session', generation: int=None,
                 schema: 'fields.Schema'=None):
        # This backend has no concept of a session, all we need from the object
        # is the indexname
        indexname = session.indexname

        if generation is None:
            generation = self.latest_generation(session)

        filename = index.Toc.make_filename(indexname, generation)
        try:
            with self.map_file(filename) as data:
                # Read the header at the beginning of the file
                head = TocHeader.decode(data)
                start = TocHeader.get_size()
                end = start + head.length

                # Read the encoded TOC
                tocbytes = bytes(data[start:end])
                if len(tocbytes) != head.length:
                    raise index.WhooshIndexError("Partial TOC error")

                # Compare the checksums
                check = crc32(tocbytes)
                if check != head.checksum:
                    raise index.WhooshIndexError("TOC checksum error")
                if data.get_uint_le(end) != check:
                    raise index.WhooshIndexError("TOC checksum error")

                toc = index.Toc.from_bytes(tocbytes)
                assert toc.generation == generation
                toc.filename = filename
                return toc
        except FileNotFoundError:
            raise storage.TocNotFound("Index %s generation %s not found" %
                                      (indexname, generation))

    # Specify more abstract methods for working with files

    @abstractmethod
    def create_file(self, name: str) -> datafile.OutputFile:
        """
        Creates a file with the given name in this storage.

        :param name: the name for the new file.
        :return: a :class:`whoosh.filedb.structfile.StructFile` instance.
        """

        raise NotImplementedError

    @abstractmethod
    def open_file(self, name: str) -> File:
        """
        Opens a file with the given name in this storage.

        :param name: the name for the new file.
        :return: a :class:`whoosh.filedb.structfile.StructFile` instance.
        """

        raise NotImplementedError

    @abstractmethod
    def map_file(self, name, offset=0, length=0) -> datafile.Data:
        """
        Opens a file as a memory map (or an fallback substitute) and returns a
        bytes-like object.

        :param name: the name of the file to open.
        :param offset: the starting offset of the region to return.
        :param length: the length of the region to return
        :return:
        """

        raise NotImplementedError

    @abstractmethod
    def list(self) -> List[str]:
        """
        Returns a list of file names in this storage.
        """

        raise NotImplementedError

    @abstractmethod
    def file_exists(self, name: str) -> bool:
        """
        Returns True if the given file exists in this storage.

        :param name: the name to check.
        """

        raise NotImplementedError

    @abstractmethod
    def file_modified(self, name: str) -> float:
        """
        Returns the last-modified time of the given file in this storage (as
        a "ctime" UNIX timestamp).

        :param name: the name to check.
        """

        raise NotImplementedError

    @abstractmethod
    def file_length(self, name: str) -> int:
        """
        Returns the size (in bytes) of the given file in this storage.

        :param name: the name to check.
        """

        raise NotImplementedError

    @abstractmethod
    def delete_file(self, name: str):
        """
        Removes the given file from this storage.

        :param name: the name to delete.
        """

        raise NotImplementedError

    @abstractmethod
    def rename_file(self, frm: str, to: str, safe: bool=False):
        """
        Renames a file in this storage.

        :param frm: The current name of the file.
        :param to: The new name for the file.
        :param safe: if True, raise an exception if a file with the new name
            already exists.
        """

        raise NotImplementedError

    def close(self):
        """
        Closes any resources opened by this storage object. For some storage
        implementations this will be a no-op, but for others it is necessary
        to release locks and/or prevent leaks, so it's a good idea to call it
        when you're done with a storage object.
        """

        pass

    def optimize(self):
        """
        Optimizes the storage object. The meaning and cost of "optimizing"
        will vary by implementation. For example, a database implementation
        might run a garbage collection procedure on the underlying database.
        """

        pass


class OverlayStorage(BaseFileStorage):
    """
    Overlays two storage objects. Reads are processed from the first if it
    has the named file, otherwise the second. Writes always go to the second.
    """

    def __init__(self, a: BaseFileStorage, b: BaseFileStorage):
        self.a = a
        self.b = b

    def create_index(self, *args, **kwargs):
        self.b.create_index(*args, **kwargs)

    def open_index(self, *args, **kwargs):
        self.a.open_index(*args, **kwargs)

    def create_file(self, name: str) -> datafile.OutputFile:
        return self.b.create_file(name)

    def open_file(self, name: str) -> File:
        if self.a.file_exists(name):
            return self.a.open_file(name)
        else:
            return self.b.open_file(name)

    def map_file(self, name, offset=0, length=0) -> datafile.Data:
        if self.a.file_exists(name):
            return self.a.map_file(name, offset=offset, length=length)
        else:
            return self.b.map_file(name, offset=offset, length=length)

    def list(self) -> List[str]:
        return list(set(self.a.list()) | set(self.b.list()))

    def file_exists(self, name: str) -> bool:
        return self.a.file_exists(name) or self.b.file_exists(name)

    def file_modified(self, name: str) -> float:
        if self.a.file_exists(name):
            return self.a.file_modified(name)
        else:
            return self.b.file_modified(name)

    def file_length(self, name: str) -> int:
        if self.a.file_exists(name):
            return self.a.file_length(name)
        else:
            return self.b.file_length(name)

    def delete_file(self, name: str):
        return self.b.delete_file(name)

    def rename_file(self, frm: str, to: str, safe: bool=False):
        raise Exception("Can't rename files in an overlay storage")

    def lock(self, name: str) -> Any:
        return self.b.lock(name)

    def temp_storage(self, name: str=None) -> BaseFileStorage:
        return self.b.temp_storage(name=name)

    def close(self):
        self.a.close()
        self.b.close()

    def optimize(self):
        self.a.optimize()
        self.b.optimize()


@storage.url_handler
class FileStorage(BaseFileStorage):
    """
    Storage object that stores the index as files in a directory on disk.
    """

    url_scheme = "file"
    supports_mmap = True

    def __init__(self, path: str, supports_mmap: bool=True,
                 readonly: bool=False):
        """
        :param path: a path to a directory.
        :param supports_mmap: if True (the default), use the ``mmap`` module to
            open memory mapped files. You can open the storage object with
            ``supports_mmap=False`` to force Whoosh to open files normally
            instead of with ``mmap``.
        :param readonly: If ``True``, the object will raise an exception if you
            attempt to create or rename a file.
        """

        self.folder = path
        self.supports_mmap = supports_mmap
        self.readonly = readonly
        self.locks = {}

    def __repr__(self):
        return "%s(%r)" % (self.__class__.__name__, self.folder)

    @classmethod
    def from_url(cls, url: str) -> 'FileStorage':
        url = furl.furl(url)
        assert url.scheme == cls.url_scheme

        path = str(url.path)
        supports_mmap = url.args.get("mmap") == "true"
        readonly = url.args.get("readonly") == "true"
        return cls(path, supports_mmap=supports_mmap, readonly=readonly)

    def as_url(self) -> str:
        args = {"mmap": self.supports_mmap, "readonly": self.readonly}
        return furl.furl().set(scheme="file", path=self.folder, args=args).url

    def create(self):
        """
        Creates this storage object's directory path using ``os.makedirs`` if
        it doesn't already exist.

        >>> from whoosh.filedb.filestore import FileStorage
        >>> st = FileStorage("indexdir")
        >>> st.create()

        This method returns ``self``, you can say::

            st = FileStorage("indexdir").create()

        Note that you can simply create handle the creation of the directory
        yourself and open the storage object using the initializer::

            dirname = "indexdir"
            os.mkdir(dirname)
            st = FileStorage(dirname)

        However, using the ``create()`` method allows you to potentially swap in
        other storage implementations more easily.

        :return: a :class:`Storage` instance.
        """

        dirpath = os.path.abspath(self.folder)
        # If the given directory does not already exist, try to create it
        try:
            os.makedirs(dirpath)
        except OSError:
            # This is necessary for compatibility between Py2 and Py3
            e = sys.exc_info()[1]
            # If we get an error because the path already exists, ignore it
            if e.errno != errno.EEXIST:
                raise

        # Raise an exception if the given path is not a directory
        if not os.path.isdir(dirpath):
            e = IOError("%r is not a directory" % dirpath)
            e.errno = errno.ENOTDIR
            raise e

        return self

    def destroy(self):
        """Removes any files in this storage object and then removes the
        storage object's directory. What happens if any of the files or the
        directory are in use depends on the underlying platform.
        """

        # Remove all files
        self.clean()
        # Try to remove the directory
        os.rmdir(self.folder)

    def _fpath(self, fname):
        return os.path.abspath(os.path.join(self.folder, fname))

    def create_file(self, name: str, excl: bool=False, mode: str="wb",
                    **kwargs) -> datafile.OutputFile:
        """Creates a file with the given name in this storage.

        :param name: the name for the new file.
        :param excl: if True, try to open the file in "exclusive" mode.
        :param mode: the mode flags with which to open the file. The default is
            ``"wb"``.
        :param kwargs: additional keyword arguments are passed to
            ``OutputFile``.
        """

        if self.readonly:
            raise storage.ReadOnlyError

        path = self._fpath(name)
        if excl:
            flags = os.O_CREAT | os.O_EXCL | os.O_RDWR
            if hasattr(os, "O_BINARY"):
                flags |= os.O_BINARY
            fd = os.open(path, flags)
            fileobj = os.fdopen(fd, mode)
        else:
            fileobj = open(path, mode)

        f = datafile.OutputFile(fileobj, name=name, **kwargs)
        return f

    def open_file(self, name) -> File:
        return open(self._fpath(name), "rb")

    def map_file(self, name, offset=0, length=0) -> datafile.Data:
        """
        Opens an existing file in this storage.

        :param name: the name of the file to open.
        :param offset: the offset of the region to open.
        :param length: the length of the region to open.
        """

        filesize = self.file_length(name)
        f = open(self._fpath(name), "rb")
        dataobj = None

        # Can we use mmap?
        use_mmap = (
            mmap and self.supports_mmap and hasattr(f, "fileno") and
            filesize < sys.maxsize
        )
        # Check if this file is real. In some versions of Python, non-real
        # files don't have a fileno method, but in others it exists but raises
        # an exception.
        fileno = -1
        if use_mmap and hasattr(f, "fileno"):
            try:
                fileno = f.fileno()
            except io.UnsupportedOperation:
                use_mmap = False

        if use_mmap:
            # Try to open the entire segment as a memory-map object
            try:
                mm = mmap.mmap(fileno, 0, access=mmap.ACCESS_READ)
                dataobj = datafile.MemData(mm)
            except (mmap.error, OSError):
                e = sys.exc_info()[1]
                # If we got an error because there wasn't enough memory to
                # open the map, ignore it, we'll just use the (slower)
                # "sub-file" implementation
                if e.errno == errno.ENOMEM:
                    pass
                else:
                    raise
            else:
                # If that worked, we can close the file handle we were given
                f.close()

        if dataobj is None:
            # mmap isn't available, so fake it with the FileMap
            dataobj = datafile.FileData(f, name, offset=offset, length=length)
        return dataobj

    def clean(self, ignore: bool=False):
        if self.readonly:
            raise storage.ReadOnlyError

        path = self.folder
        files = self.list()
        for fname in files:
            try:
                os.remove(os.path.join(path, fname))
            except OSError:
                if not ignore:
                    raise

    def list(self) -> List[str]:
        try:
            files = os.listdir(self.folder)
        except IOError:
            files = []

        return files

    def file_exists(self, name: str) -> bool:
        return os.path.exists(self._fpath(name))

    def file_modified(self, name: str) -> float:
        return os.path.getmtime(self._fpath(name))

    def file_length(self, name: str) -> int:
        return os.path.getsize(self._fpath(name))

    def delete_file(self, name: str):
        if self.readonly:
            raise storage.ReadOnlyError

        os.remove(self._fpath(name))

    def rename_file(self, oldname: str, newname: str, safe: bool=False):
        if self.readonly:
            raise storage.ReadOnlyError

        if os.path.exists(self._fpath(newname)):
            if safe:
                raise NameError("File %r exists" % newname)
            else:
                os.remove(self._fpath(newname))
        os.rename(self._fpath(oldname), self._fpath(newname))

    def lock(self, name: str) -> FileLock:
        # TODO: lock name should include indexname to be unique across indexes
        return FileLock(self._fpath(name))

    def temp_storage(self, name=None) -> BaseFileStorage:
        name = name or "%s.tmp" % random_name()
        path = os.path.join(self.folder, name)
        tempstore = FileStorage(path)
        return tempstore.create()


class RamStorage(BaseFileStorage):
    """
    Storage object that keeps the index in memory.
    """

    supports_mmap = False

    def __init__(self):
        self.files = {}  # type: Dict[str, bytes]
        self.locks = {}  # type: Dict[str, Lock]

    def destroy(self):
        del self.files
        del self.locks

    def list(self) -> List[str]:
        return list(self.files.keys())

    def clean(self):
        self.files = {}

    def total_size(self) -> int:
        return sum(self.file_length(f) for f in self.list())

    def file_exists(self, name: str) -> bool:
        return name in self.files

    def file_length(self, name: str) -> int:
        if name not in self.files:
            raise NameError(name)
        return len(self.files[name])

    def file_modified(self, name: str) -> float:
        return -1

    def delete_file(self, name: str):
        if name not in self.files:
            raise NameError(name)
        del self.files[name]

    def rename_file(self, name: str, newname: str, safe: bool=False):
        if name not in self.files:
            raise NameError(name)
        if safe and newname in self.files:
            raise NameError("File %r exists" % newname)

        content = self.files[name]
        del self.files[name]
        self.files[newname] = content

    def create_file(self, name: str, **kwargs) -> datafile.OutputFile:
        def _onclose(sfile):
            self.files[name] = sfile._file.getvalue()
        f = datafile.OutputFile(BytesIO(), name=name, onclose=_onclose)
        return f

    def open_file(self, name: str, **kwargs) -> File:
        if name not in self.files:
            raise NameError(name)
        return BytesIO(memoryview(self.files[name]))

    def map_file(self, name: str, offset: int=0, length: int=0
                 ) -> datafile.Data:
        content = memoryview(self.files[name])
        if offset or length:
            content = content[offset:offset + length]
        return datafile.MemData(content, name=name)

    def lock(self, name: str) -> Lock:
        if name not in self.locks:
            self.locks[name] = Lock()
        return self.locks[name]

    def temp_storage(self, name: str=None) -> BaseFileStorage:
        tdir = tempfile.gettempdir()
        name = name or "%s.tmp" % random_name()
        path = os.path.join(tdir, name)
        tempstore = FileStorage(path)
        return tempstore.create()


