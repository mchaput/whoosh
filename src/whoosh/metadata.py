import struct
from typing import List, Tuple, Union

from whoosh.filedb.datafile import Data


class FileHeaderError(Exception):
    pass


class MetaData(object):
    flags = ""
    field_order = ""
    version_number = 0
    magic_bytes = b""

    def __init__(self, **kwargs):
        flagnames, fieldnames, _ = self._types()

        for key in kwargs:
            if key.startswith("_"):
                continue
            if (key != "version_number" and
                        key not in flagnames and
                        key not in fieldnames):
                raise KeyError("Unknown argument %r" % key)

        self.__dict__.update(kwargs)

    def __repr__(self):
        items = ", ".join("%s=%r" % (key, value) for key, value
                          in sorted(self.__dict__.items()))
        return "%s(%s)" % (type(self).__name__, items)

    @classmethod
    def get_size(cls):
        _, _, fmt = cls._types()
        return len(cls.magic_bytes) + struct.calcsize(fmt)

    @classmethod
    def _types(cls) -> Tuple[List[str], List[str], str]:
        flagnames = cls.flags.split()
        order = cls.field_order.split()

        fieldnames = []
        # Start format with spaces for version and flags
        fmt = "<BB"
        for name in order:
            if not hasattr(cls, name):
                raise KeyError(name)
            vtype = getattr(cls, name)

            if not isinstance(vtype, str):
                raise ValueError("Unknown type %r for name %s" % (vtype, name))

            if ((vtype.endswith("x") and not name.startswith("_")) or
                    (name.startswith("_") and not vtype.endswith("x"))):
                raise Exception("%s is not a padding field" % name)

            if not name.startswith("_"):
                fieldnames.append(name)
            fmt += vtype

        return flagnames, fieldnames, fmt

    def encode(self) -> bytes:
        # Look for class variables
        flagnames, fieldnames, fmt = self._types()

        flags = 0
        for i, name in enumerate(flagnames):
            if getattr(self, name):
                flags |= 1 << i

        values = [self.version_number, flags]
        for name in fieldnames:
            val = getattr(self, name)
            if getattr(type(self), name) == "c":
                val = val.encode("ascii")
            values.append(val)

        return self.magic_bytes + struct.pack(fmt, *values)

    @classmethod
    def check_magic(cls, bs: Union[Data, bytes], offset: int=0) -> bool:
        magic = cls.magic_bytes
        magicbytes = bytes(bs[offset:offset + len(magic)])
        if magicbytes != magic:
            raise FileHeaderError("Magic number %r doesn't match %r" %
                                  (magicbytes, magic))

    @classmethod
    def decode(cls, bs: Union[Data, bytes], offset: int=0):
        cls.check_magic(bs, offset)
        flagnames, fieldnames, fmt = cls._types()

        start = offset + len(cls.magic_bytes)
        end = start + struct.calcsize(fmt)
        vals = struct.unpack(fmt, bs[start:end])

        flags = vals[1]
        obj = cls(version_number=vals[0], _flags=flags)

        # Pull flag values out of flags byte
        for i, flagname in enumerate(flagnames):
            setattr(obj, flagname, bool(flags & (1 << i)))

        # Pull field values out of struct
        for i, fieldname in enumerate(fieldnames):
            if fieldname.startswith("_"):
                continue

            val = vals[i + 2]

            if getattr(cls, fieldname) == "c" and isinstance(val, bytes):
                val = val.decode("ascii")
            setattr(obj, fieldname, val)

        return obj
