import io
import logging
import sys
import time
from shutil import copyfileobj
from struct import Struct
from typing import Dict, Iterable, List, Sequence, Tuple

try:
    import mmap
except ImportError:
    mmap = None

from whoosh.ifaces import codecs, storage
from whoosh.filedb import datafile, filestore
from whoosh.metadata import MetaData
from whoosh.system import IS_LITTLE
from whoosh.util import unclosed


logger = logging.getLogger(__name__)


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

def assemble_files(from_store: 'filestore.FileStorage',
                   filenames: Iterable[str],
                   to_store: 'filestore.FileStorage', compoundname: str,
                   delete: bool=False):
    directory = {}
    with to_store.create_file(compoundname) as outfile:
        # Write the magic bytes at the start of the file
        outfile.write(CompoundFooter.magic_bytes)

        for name in filenames:
            offset = outfile.tell()
            with from_store.open_file(name) as f:
                copyfileobj(f, outfile)

            if delete:
                from_store.delete_file(name)

            size = outfile.tell() - offset
            directory[name] = offset, size, time.time()

        # Remember the start of the directory
        dir_offset = outfile.tell()

        # Write the directory entries
        for fname in directory:
            foff, fsize, fmod = directory[fname]
            nbytes = fname.encode("utf8")
            outfile.write(dir_entry.pack(len(nbytes), foff, fsize, fmod))
            outfile.write(nbytes)

        # Write the file footer
        outfile.write(CompoundFooter(
            was_little=IS_LITTLE, dir_offset=dir_offset,
            dir_count=len(directory),
        ).encode())


def assemble_segment(from_store: 'filestore.FileStorage',
                     to_store: 'filestore.FileStorage',
                     segment: 'codecs.FileSegment',
                     segment_filename: str, delete: bool=False):
    names = list(segment.file_names(from_store))
    if not names:
        raise ValueError("No files match this segment")

    assemble_files(from_store, names, to_store, segment_filename, delete=delete)


class CompoundStorage(filestore.FileStorage):
    """
    Presents a compound file as a FileStorage object.
    """

    def __init__(self, store: filestore.BaseFileStorage, name: str,
                 offset: int=0, length: int=0):
        logger.debug("Opening compound segment %r in storage %r", name, store)
        self._storage = store
        self._name = name
        self._data = data = store.map_file(name, offset=offset, length=length)
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
        for _ in range(foot.dir_count):
            entry_end = entry_start + de.size
            nmlen, off, size, modt = de.unpack(data[entry_start:entry_end])
            name_end = entry_end + nmlen
            name = bytes(data[entry_end:name_end]).decode("utf8")
            self._directory[name] = off, size, modt
            entry_start = name_end
        assert entry_start == footer_start

    def __repr__(self):
        return "<%s %r %s>" % (type(self).__name__, self._storage, self._name)

    def open(self, indexname: str=None, writable: bool=False
             ) -> filestore.FileSession:
        if writable:
            raise ValueError("Can't open a compound storage writable")

        session = self._storage.open(indexname, writable=False)
        session.store = self
        return session

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
        raise storage.ReadOnlyError

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
        raise storage.ReadOnlyError

    def list(self) -> List[str]:
        return list(self._directory)

    def file_exists(self, name: str) -> bool:
        return name in self._directory

    def file_length(self, name: str) -> int:
        return self._entry(name)[1]

    def file_modified(self, name: str) -> float:
        return self._entry(name)[2]

    def delete_file(self, name: str):
        raise storage.ReadOnlyError

    def rename_file(self, oldname: str, newname: str, safe: bool=False):
        raise storage.ReadOnlyError

    def lock(self, name: str):
        raise storage.ReadOnlyError

    def temp_storage(self, name=None):
        raise storage.ReadOnlyError


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
