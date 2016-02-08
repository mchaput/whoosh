import io
import sys
import time
from shutil import copyfileobj
from struct import Struct
from typing import List, Tuple

try:
    import mmap
except ImportError:
    mmap = None

from whoosh.compat import xrange
from whoosh.filedb import datafile
from whoosh.filedb import filestore
from whoosh.metadata import MetaData
from whoosh.system import IS_LITTLE
from whoosh.util import unclosed


# Type aliases

if sys.version_info[0] >= 3:
    File = io.IOBase
else:
    File = file


class CompoundFooter(MetaData):
    magic_bytes = b"Cmpd"
    version_number = 0
    flags = "was_little"
    field_order = "dir_offset dir_count"

    was_little = False

    dir_offset = "q"
    dir_count = "i"


# Directory entry header
# H - length of file name
# q - offset
# q - file length
# f - file modified time
dir_entry = Struct("<Hqqf")


# Assembler

class AssemblingStorage(filestore.BaseFileStorage):
    """
    Concatenates multiple files into a single "compound" file with a directory
    stuck on the end, to address individual file ranges within the file.
    """

    def __init__(self, store: filestore.BaseFileStorage, name: str):
        self._store = store
        self._name = name
        self._file = store.create_file(name)
        # Maps ingested file names to (offset, size, modtime) tuples
        self._directory = {}  # type: Dict[str, Tuple[int, int, float]]

        # Write a magic number to identify this format
        self._file.write(CompoundFooter.magic_bytes)

    def _add_file(self, name):
        offset = self._file.tell()
        with self._store.open_file(name) as f:
            copyfileobj(f, self._file)
        size = self._file.tell() - offset
        self._directory[name] = offset, size, time.time()

    def create_file(self, name: str, excl: bool=False, mode: str="wb",
                    **kwargs) -> datafile.OutputFile:
        if name in self._directory:
            raise Exception("Duplicate file name %r in compound file" % name)
        f = self._store.create_file(name)

        def _afterclose(*args):
            self._add_file(name)
            self._store.delete_file(name)
        f.aftercloses.append(_afterclose)

        return f

    def open_file(self, name: str) -> File:
        if name in self._directory:
            raise Exception("Can't open an assembled file")
        return self._store.open_file(name)

    def map_file(self, name, offset=0, length=0) -> datafile.Data:
        if name in self._directory:
            # We could actually open this file in the assembly...
            raise Exception("Can't open an assembled file")
        return self._store.map_file(name, offset, length)

    def list(self) -> List[str]:
        return self._store.list() + list(self._directory)

    def file_exists(self, name: str):
        return name in self._directory or self._store.file_exists(name)

    def file_modified(self, name: str):
        if name in self._directory:
            return self._directory[name][2]
        else:
            return self._store.file_modified(name)

    def file_length(self, name: str):
        if name in self._directory:
            return self._directory[name][1]
        else:
            return self._store.file_length(name)

    def delete_file(self, name: str):
        if name in self._directory:
            raise Exception("Can't delete %r from compound file" % name)
        self._store.delete_file(name)

    def rename_file(self, frm: str, to: str, safe: bool=False):
        raise Exception("AssemblingStorage doesn't support rename")

    def lock(self, name: int):
        return self._store.lock(name)

    def temp_storage(self, name: str=None):
        return self._store.temp_storage(name)

    def close(self):
        f = self._file

        # Remember the start of the directory
        dir_offset = f.tell()

        # Write the directory entries
        for fname in self._directory:
            foff, fsize, fmod = self._directory[fname]
            nbytes = fname.encode("utf8")
            f.write(dir_entry.pack(len(nbytes), foff, fsize, fmod))
            f.write(nbytes)

        # Write the file footer
        f.write(CompoundFooter(
            was_little=IS_LITTLE, dir_offset=dir_offset,
            dir_count=len(self._directory)
        ).encode())
        self._file.close()


class CompoundStorage(filestore.FileStorage):
    """
    Presents a compound file as a FileStorage object.
    """

    def __init__(self, st: filestore.BaseFileStorage, name: str, offset: int=0,
                 length: int=0):
        self._data = data = st.map_file(name, offset=offset, length=length)
        self.closed = False

        # Read the magic number at the start of the file
        CompoundFooter.check_magic(data, offset)

        # Read the footer at the end of the file
        footer_start = len(data) - CompoundFooter.get_size()
        foot = CompoundFooter.decode(data, footer_start)
        assert foot.version_number == 0

        # Pull information out of the flag bits
        self._native = foot.was_little == IS_LITTLE

        # Read the directory
        de = dir_entry
        entry_start = foot.dir_offset
        self._directory = {}  # type: Dict[str, Tuple[int, int, float]]
        for _ in xrange(foot.dir_count):
            entry_end = entry_start + de.size
            nmlen, off, size, modt = de.unpack(data[entry_start:entry_end])
            name_end = entry_end + nmlen
            name = bytes(data[entry_end:name_end]).decode("utf8")
            self._directory[name] = off, size, modt
            entry_start = name_end
        assert entry_start == footer_start

    @unclosed
    def close(self):
        # Close the underlying map
        self._data.close()
        self.closed = True

    def _entry(self, name: str) -> Tuple[int, int, float]:
        try:
            return self._directory[name]
        except KeyError:
            raise NameError("Unknown file %r" % (name,))

    def range(self, name: str) -> Tuple[int, int]:
        entry = self._entry(name)
        return entry[0], entry[1]

    def create_file(self, name: str, excl: bool=False, mode: str="wb",
                    **kwargs):
        raise filestore.ReadOnlyError

    @unclosed
    def open_file(self, name: str, *args, **kwargs) -> 'SubFile':
        raise Exception
        # offset, length = self.range(name)
        # return SubFile(self._file, offset, length, name=name)

    @unclosed
    def map_file(self, name, offset=0, length=0, use_mmap=True
                 ) -> datafile.Data:
        fileoffset, filelength = self.range(name)
        length = length or filelength

        if not (0 <= offset <= filelength):
            raise ValueError("Map offset %r is outside of file length %s" %
                             (offset, filelength))
        if offset + length > filelength:
            raise ValueError("Map length %r is outside of file length %s" %
                             (length, filelength))

        return self._data.subset(fileoffset + offset, length)

    def clean(self, ignore: bool=False):
        raise filestore.ReadOnlyError

    def list(self) -> List[str]:
        return list(self._directory)

    def file_exists(self, name: str) -> bool:
        return name in self._directory

    def file_length(self, name: str) -> int:
        return self._entry(name)[1]

    def file_modified(self, name: str) -> float:
        return self._entry(name)[2]

    def delete_file(self, name: str):
        raise filestore.ReadOnlyError

    def rename_file(self, oldname: str, newname: str, safe: bool=False):
        raise filestore.ReadOnlyError

    def lock(self, name: str):
        raise filestore.ReadOnlyError

    def temp_storage(self, name=None):
        raise filestore.ReadOnlyError


class SubFile(object):
    """
    Presents a subset of a real file as a file-like object.
    """

    def __init__(self, parentfile, offset, length, name=None):
        self._file = parentfile
        self._offset = offset
        self._length = length
        self._end = offset + length
        self._pos = 0

        self.name = name
        self.closed = False

    def close(self):
        self.closed = True

    def subset(self, position, length, name=None):
        start = self._offset + position
        end = start + length
        name = name or self.name
        assert self._offset >= start >= self._end
        assert self._offset >= end >= self._end
        return SubFile(self._file, self._offset + position, length, name=name)

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
            return b''

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
        else:
            raise ValueError

        self._pos = pos

    def tell(self):
        return self._pos


#

# class Sinks(object):
#     """
#     This object lets you write to multiple file-like objects, which are really
#     going into one file (to save file handles) in separate tracked blocks. The
#     object can then reassemble each virtual file's blocks to read the entire
#     file out again.
#
#     The writes to the different files must be serial. This does not support
#     threading, parallel writing, etc. This is just a way to organize writes.
#
#     What you get back from this object's create_file method does not implement
#     every aspect of a file-like object, just tell() and write().
#     """
#
#     def __init__(self, f, buffersize: int=32 * 1024):
#         self._file = f
#         self._buffersize = buffersize
#         self._streams = {}
#
#     def __iter__(self) -> Iterable[str]:
#         return iter(self._streams)
#
#     def create_file(self, name):
#         ss = self.SubStream(self._file, self._buffersize)
#         self._streams[name] = ss
#         return structfile.StructFile(ss)
#
#     def blocks(self, name: str) -> Iterable[bytes]:
#         return self._streams[name].blocks()
#
#     def save_to_files(self, store: filestore.BaseFileStorage,
#                       namefn: Callable[[str], str]=None):
#         for name in self:
#             fname = namefn(name) if namefn else name
#             with store.create_file(fname) as f:
#                 for block in self.blocks(name):
#                     f.write(block)
#
#     class SubStream(object):
#         def __init__(self, f, buffersize: int):
#             self._file = f
#             self._buffersize = buffersize
#             self._buffer = BytesIO()
#             # List of (offset, length) tuples
#             self._blocks = []  # type: List[Tuple[int, int]]
#
#         def tell(self) -> int:
#             return sum(b[1] for b in self._blocks) + self._buffer.tell()
#
#         @unclosed
#         def write(self, inbytes: bytes):
#             bio = self._buffer
#             buflen = bio.tell()
#             length = buflen + len(inbytes)
#             if length >= self._buffersize:
#                 offset = self._dbfile.tell()
#                 self._file.write(bio.getvalue()[:buflen])
#                 self._file.write(inbytes)
#
#                 self._blocks.append((offset, length))
#                 self._buffer.seek(0)
#             else:
#                 bio.write(inbytes)
#
#         def blocks(self) -> Iterable[bytes]:
#             f = self._file
#             bio = self._buffer
#             for offset, length in self._blocks:
#                 f.seek(offset)
#                 yield f.read(length)
#
#             buflen = bio.tell()
#             if buflen:
#                 yield bio.getvalue()[:buflen]
#
#
#
