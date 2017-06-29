# Copyright 2012 Matt Chaput. All rights reserved.
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

# from __future__ import with_statement
# from bisect import bisect_left
# from threading import Lock, RLock
#
# from whoosh.ifaces import codecs
# from whoosh.matching import ListMatcher
# from whoosh.reading import SegmentReader, TermInfo, TermNotFound
# from whoosh.writing import SegmentWriter

import threading
from bisect import bisect_left
from io import BytesIO
from collections import defaultdict
from typing import Any, Dict, Iterable, Optional, Sequence, Set, Tuple

from whoosh import columns, fields, index
from whoosh.codec import null
from whoosh.ifaces import codecs, storage, readers
from whoosh.postings import basic, postform, postings, ptuples


class CapabilityError(Exception):
    pass


# Helper functions

# Functions to generate fake field names for internal columns
def _vecfield(fieldname: str) -> str:
    return "_%s_vec" % fieldname


# Column type to store pointers to encoded vectors
VECTOR_COLUMN = columns.CompressedBytesColumn()


# Implementations

class MemoryStorage(storage.Storage):
    def __init__(self, ):
        self._generation = -1
        self._id_counter = 0
        self._toc = None

    def open(self, indexname: str=None, writable: bool=False
             ) -> storage.Session:
        return storage.Session(self, indexname, writable, self._id_counter)

    def save_toc(self, session: storage.Session, toc: 'index.Toc'):
        self._toc = toc
        self._generation += 1

    def load_toc(self, session: storage.Session, generation: int=None
                 ) -> 'index.Toc':
        if generation is not None:
            assert generation == self._generation
        return self._toc

    def latest_generation(self, session: storage.Session) -> int:
        return self._generation

    def lock(self, name: str) -> storage.Lock:
        return threading.Lock()

    def temp_storage(self, name: str = None) -> 'storage.Storage':
        from whoosh.filedb.filestore import RamStorage
        return RamStorage()


class MemorySegment(codecs.Segment):
    def __init__(self, indexname: str, store_per_doc: bool=True):
        from whoosh.filedb.filestore import RamStorage

        self._indexname = indexname
        self._storeperdoc = store_per_doc
        self.terms = {}
        self.terminfos = {}
        self.docs = []
        self.colstore = RamStorage() if store_per_doc else None

        self.docfieldlens = defaultdict(list)

        self._size = 0
        self._deleted = set()
        self._fieldlength = defaultdict(int)

        self._indoc = False
        self._docdata = None
        self._coldata = BytesIO()
        self._sorted = {}

    @property
    def docnum(self) -> int:
        return len(self.docs)

    def segment_id(self) -> str:
        return str(id(self))

    def sorted_terms(self, fieldname: str) -> Sequence[bytes]:
        try:
            return self._sorted[fieldname]
        except KeyError:
            srtd = sorted(self.terms[fieldname])
            self._sorted[fieldname] = srtd
            return srtd

    def term_info(self, fieldname: str, termbytes: bytes):
        return self.terminfos[(fieldname, termbytes)]

    def codec(self) -> codecs.Codec:
        return MemoryCodec(store_per_doc=self._storeperdoc)

    def size(self) -> int:
        return self._size

    def doc_count_all(self) -> int:
        return len(self.docs)

    def set_doc_count(self, doccount: int):
        raise CapabilityError("Can't set doc count on MemorySegment")

    def field_length(self, fieldname: str, default: int=0) -> int:
        return self._fieldlength.get(fieldname, default)

    def deleted_count(self) -> int:
        return len(self._deleted)

    def deleted_docs(self) -> Set:
        return self._deleted

    def delete_document(self, docnum: int):
        self._deleted.add(docnum)

    def is_deleted(self, docnum: int) -> bool:
        return docnum in self._deleted

    def should_rewrite(self) -> bool:
        return self._deleted > (len(self.docs) // 2)


@codecs.register("memory")
class MemoryCodec(codecs.Codec):
    def __init__(self, store_per_doc: bool=True):
        self._storeperdoc = store_per_doc

    def name(self) -> str:
        return "whoosh.codecs.memory"

    def postings_io(self) -> 'postings.PostingsIO':
        return basic.BasicIO()

    def per_document_writer(self, session: 'storage.Session',
                            segment: MemorySegment
                            ) -> 'codecs.PerDocumentWriter':
        if self._storeperdoc:
            return MemPerDocWriter(segment)
        else:
            return null.NullPerDocWriter()

    def field_writer(self, session: 'storage.Session',
                     segment: MemorySegment) -> 'MemFieldWriter':
        return MemFieldWriter(segment)

    def per_document_reader(self, session: 'storage.Session',
                            segment: MemorySegment
                            ) -> 'codecs.PerDocumentReader':
        if self._storeperdoc:
            return MemPerDocReader(segment)
        else:
            return null.NullPerDocReader()

    def terms_reader(self, session: 'storage.Session',
                     segment: MemorySegment) -> 'MemTermsReader':
        return MemTermsReader(segment)

    def new_segment(self, session: 'storage.Session'):
        return MemorySegment(session.indexname, store_per_doc=self._storeperdoc)

    def segment_from_bytes(self, bs: bytes) -> codecs.Segment:
        return MemorySegment.from_bytes(bs)


class MemPerDocWriter(codecs.PerDocumentWriter):
    def __init__(self, segment: MemorySegment):
        self._segment = segment
        self._io = basic.BasicIO()
        self._storedfields = {}  # type: dict
        self._colwriters = {}
        self._docnum = 0

    def postings_io(self) -> 'postings.PostingsIO':
        return self._io

    def start_doc(self, docnum: int):
        self._storedfields = {}
        self._docnum = docnum

    def add_field(self, fieldname: str, fieldobj: 'fields.FieldType',
                  value: Any, length: int):
        if fieldobj.stored and value is not None:
            self._storedfields[fieldname] = value

        self._segment.docfieldlens[fieldname].append(length)

    def add_column_value(self, fieldname: str, columnobj: 'columns.Column',
                         value: Any):
        if fieldname in self._colwriters:
            _, cwriter = self._colwriters[fieldname]
        else:
            colstore = self._segment.colstore
            cfile = colstore.create_file(fieldname)
            cwriter = columnobj.writer(cfile)
            self._colwriters[fieldname] = cfile, cwriter

        cwriter.add(self._docnum, value)

    def add_vector_postings(self, fieldname: str, fieldobj: 'fields.FieldType',
                            posts: 'Sequence[postings.PostTuple]'):
        data = self._io.vector_to_bytes(fieldobj.vector, posts)
        self.add_raw_vector(fieldname, data)

    def add_raw_vector(self, fieldname: str, data: bytes):
        self.add_column_value(_vecfield(fieldname), VECTOR_COLUMN, data)

    def finish_doc(self):
        self._segment.docs.append(self._storedfields)
        self._docnum += 1

    def finish_field(self):
        pass

    def close(self):
        for fieldname, (cfile, cwriter) in self._colwriters.items():
            cwriter.finish(self._docnum)
            cfile.close()


class MemFieldWriter(codecs.FieldWriter):
    def __init__(self, segment: MemorySegment):
        self._segment = segment
        self._io = basic.BasicIO()
        self._fieldname = ''
        self._fieldobj = None  # type: fields.FieldType
        self._fielddict = {}
        self._termbytes = b''
        self._posts = []
        self._terminfo = None  # type: readers.TermInfo

    def postings_io(self) -> 'postings.PostingsIO':
        return self._io

    def start_field(self, fieldname: str, fieldobj: 'fields.FieldType'):
        self._fieldname = fieldname
        self._fieldobj = fieldobj
        self._fielddict = self._segment.terms.setdefault(fieldname, {})

    def start_term(self, termbytes: bytes):
        self._termbytes = termbytes
        self._posts = []
        self._terminfo = readers.TermInfo()

    def add_posting(self, post: 'ptuples.PostTuple'):
        self._posts.append(self._io.condition_post(post))

    def add_raw_post(self, rawpost: 'ptuples.RawPost'):
        self._posts.append(rawpost)

    def finish_term(self):
        s = self._segment
        self._terminfo.add_posting_list_stats(self._posts)
        s.terminfos[(self._fieldname, self._termbytes)] = self._terminfo

        encoded_block = self._io.doclist_to_bytes(self._fieldobj.format,
                                                  self._posts)
        s.terms[self._fieldname][self._termbytes] = encoded_block


class MemPerDocReader(codecs.PerDocumentReader):
    def __init__(self, segment: MemorySegment):
        self._segment = segment
        self._vector_readers = {}
        self._io = segment.codec().postings_io()

    def doc_count(self) -> int:
        return self._segment.doc_count()

    def doc_count_all(self) -> int:
        return self._segment.doc_count_all()

    def has_deletions(self) -> bool:
        return self._segment.has_deletions()

    def is_deleted(self, docnum) -> bool:
        return self._segment.is_deleted(docnum)

    def deleted_docs(self) -> Set[int]:
        return self._segment.deleted_docs()

    def all_doc_ids(self) -> Iterable[int]:
        s = self._segment
        for docnum in range(self._segment.doc_count_all()):
            if not s.is_deleted(docnum):
                yield docnum

    def supports_columns(self) -> bool:
        return True

    def has_column(self, fieldname: str) -> bool:
        return fieldname in self._segment.colstore

    def column_reader(self, fieldname: str, column: 'columns.Column',
                      reverse: bool=False) -> 'columns.ColumnReader':
        colstore = self._segment.colstore
        cfile = colstore.map_file(fieldname)
        return column.reader(cfile, 0, length=colstore.file_length(fieldname),
                             doccount=self.doc_count_all(), native=True,
                             reverse=reverse)

    # Vectors

    def _vector_bytes(self, docnum: int, fieldname: str) -> Optional[bytes]:
        if fieldname in self._vector_readers:
            vreader = self._vector_readers[fieldname]
        else:
            vecfield = _vecfield(fieldname)
            vreader = self.column_reader(vecfield, VECTOR_COLUMN)
            self._vector_readers[fieldname] = vreader
        return vreader[docnum]

    def has_vector(self, docnum: int, fieldname: str):
        return bool(self._vector_bytes(docnum, fieldname))

    def vector(self, docnum: int, fieldname: str):
        vbytes = self._vector_bytes(docnum, fieldname)
        if not vbytes:
            return postings.EmptyVectorReader()
            # raise readers.NoVectorError("This document has no stored vector")
        return self._io.vector_reader(vbytes)

    # Stored fields

    def stored_fields(self, docnum: int) -> Dict:
        return self._segment.docs[docnum]

    # Lengths

    def field_length(self, fieldname: str) -> int:
        return self._segment.field_length(fieldname)

    def doc_field_length(self, docnum: int, fieldname: str, default: int = 0
                         ) -> int:
        return self._segment.docfieldlens[fieldname][docnum]

    def min_field_length(self, fieldname: str) -> int:
        return min(self._segment.docfieldlens[fieldname])

    def max_field_length(self, fieldname: str) -> int:
        return max(self._segment.docfieldlens[fieldname])


class MemTermsReader(codecs.TermsReader):
    def __init__(self, segment: MemorySegment):
        self._segment = segment
        self._io = basic.BasicIO()

    def __contains__(self, term: codecs.TermTuple) -> bool:
        fieldname, termbytes = term
        try:
            fdict = self._segment.terms[fieldname]
        except KeyError:
            return False
        return termbytes in fdict

    def cursor(self, fieldname: str, fieldobj: 'fields.FieldType'
               ) -> 'MemCursor':
        return MemCursor(fieldname, fieldobj, self._segment)

    def terms(self) -> Iterable[codecs.TermTuple]:
        for fieldname in sorted(self._segment.terms):
            for termbytes in self._segment.sorted_terms(fieldname):
                yield fieldname, termbytes

    def items(self) -> 'Iterable[Tuple[codecs.TermTuple, readers.TermInfo]]':
        for fieldname in sorted(self._segment.terms):
            for termbytes in self._segment.sorted_terms(fieldname):
                yield ((fieldname, termbytes),
                       self.term_info(fieldname, termbytes))

    def term_info(self, fieldname: str, termbytes: bytes) -> readers.TermInfo:
        return self._segment.term_info(fieldname, termbytes)

    def indexed_field_names(self) -> Sequence[str]:
        return sorted(self._segment.terms)

    def matcher(self, fieldname: str, termbytes: bytes, fmt: 'postform.Format',
                scorer=None):
        from whoosh.matching import PostReaderMatcher

        postbytes = self._segment.terms[fieldname][termbytes]
        dlr = self._io.doclist_reader(postbytes)
        tinfo = self._segment.term_info(fieldname, termbytes)
        return PostReaderMatcher(dlr, fieldname, termbytes, tinfo, self._io,
                                 scorer=scorer)


class MemCursor(codecs.TermCursor):
    def __init__(self, fieldname: str, fieldobj: 'fields.FieldType',
                 segment: MemorySegment):
        self.fieldname = fieldname
        self.field = fieldobj
        self._segment = segment
        self._termlist = segment.sorted_terms(fieldname)
        self._i = 0

    def first(self):
        self._i = 0

    def is_valid(self):
        return self._i < len(self._termlist)

    def seek(self, termbytes: bytes):
        self._i = bisect_left(self._termlist, termbytes)

    def next(self):
        self._i += 1

    def termbytes(self) -> bytes:
        return self._termlist[self._i]

    def term_info(self) -> 'readers.TermInfo':
        return self._segment.term_info(self.fieldname, self.termbytes())


