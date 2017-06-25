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

"""
This module contains base classes/interfaces for "codec" objects.
"""

import pickle
from abc import abstractmethod
from bisect import bisect_right
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

from whoosh import columns, fields
from whoosh.automata import lev
from whoosh.automata.fsa import DFA
from whoosh.compat import text_type
from whoosh.ifaces import readers, storage
from whoosh.postings import postform, postings, ptuples
from whoosh.system import IS_LITTLE
from whoosh.util.loading import find_object


# Exceptions

class UnknownCodecError(Exception):
    pass


class UnsupportedFeature(Exception):
    pass


class OutOfOrderError(Exception):
    pass


class InvalidCursor(Exception):
    pass


# Typing aliases

# (fieldname, termbytes)
TermTuple = Tuple[str, bytes]


# Registry
codec_registry = {}


def register(name: str):
    def _fn(cls):
        register_codec(name, cls)
        return cls
    return _fn


def register_codec(name: str, cls: type):
    codec_registry[name] = cls


def codec_by_name(name: str) -> 'Codec':
    try:
        return codec_registry[name]
    except KeyError:
        pass

    return find_object(name)
    # raise UnknownCodecError(name)


# Filename

# Base classes

class Segment:
    """
    Do not instantiate this object directly. It is used by the Index object
    to hold information about a segment. A list of objects of this class are
    pickled as part of the TOC file.

    The TOC file stores a minimal amount of information -- mostly a list of
    Segment objects. Segments are the real reverse indexes. Having multiple
    segments allows quick incremental indexing: just create a new segment for
    the new documents, and have the index overlay the new segment over previous
    ones for purposes of reading/search. "Optimizing" the index combines the
    contents of existing segments into one (removing any deleted documents
    along the way).
    """

    def __init__(self, indexname: str, was_little: bool=IS_LITTLE):
        from whoosh import index

        self._indexname = indexname
        self.was_little = was_little
        self._segid = index.make_segment_id()

    def json_info(self) -> dict:
        return {
            "indexname": self.index_name(),
            "codec_name": self.codec_name(),
            "was_little": self.was_little,
            "id": self.segment_id(),
            "size": self.size(),
            "doc_count": self.doc_count(),
            "deleted_count": self.deleted_count(),
        }

    @classmethod
    def from_bytes(cls, bs: bytes) -> 'Segment':
        return pickle.loads(bytes(bs))

    def to_bytes(self) -> bytes:
        return pickle.dumps(self)

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, self.segment_id())

    def index_name(self) -> str:
        return self._indexname

    def segment_id(self) -> str:
        return self._segid

    def codec_name(self) -> str:
        return self.codec().name()

    # Interface

    @abstractmethod
    def codec(self) -> 'Codec':
        raise NotImplementedError

    @abstractmethod
    def size(self) -> int:
        """
        Returns the size of the segment in bytes. This doesn't need to be
        extremely accurate.
        """

        raise NotImplementedError

    @abstractmethod
    def doc_count_all(self) -> int:
        """
        Returns the total number of documents, DELETED OR UNDELETED, in this
        segment.
        """

        raise NotImplementedError

    @abstractmethod
    def set_doc_count(self, doccount: int):
        raise NotImplementedError

    @abstractmethod
    def field_length(self, fieldname: str, default: int=0) -> int:
        raise NotImplementedError

    @abstractmethod
    def deleted_count(self) -> int:
        """
        Returns the total number of deleted documents in this segment.
        """

        raise NotImplementedError

    @abstractmethod
    def deleted_docs(self) -> Set:
        raise NotImplementedError

    @abstractmethod
    def delete_document(self, docnum: int):
        """Deletes the given document number. The document is not actually
        removed from the index until it is optimized.

        :param docnum: The document number to delete.
        """

        raise NotImplementedError

    @abstractmethod
    def is_deleted(self, docnum: int) -> bool:
        """
        Returns True if the given document number is deleted.

        :param docnum: The document number to delete.
        """

        raise NotImplementedError

    # Override-able default implementation

    def should_rewrite(self) -> bool:
        """
        Returns True if this segment has enough cruft (as defined arbitrarily
        by the implementation) that it would improve efficiency (by some
        implementation-specific measure) to rewrite it.
        """

        return False

    # Derived methods

    def doc_count(self) -> int:
        """
        Returns the number of (undeleted) documents in this segment.
        """

        return self.doc_count_all() - self.deleted_count()

    def delete_documents(self, docnums: Iterable[int]):
        deldoc = self.delete_document
        for docnum in docnums:
            deldoc(docnum)

    def has_deletions(self) -> bool:
        """
        Returns True if any documents in this segment are deleted.
        """

        return self.deleted_count() > 0


class FileSegment(Segment):
    def file_names(self, store) -> Iterable[str]:
        from whoosh import index
        myid = self.segment_id()
        regex = index.segment_regex(self.index_name())
        for filename in store:
            match = regex.match(filename)
            if match and match.group("id") == myid:
                yield filename


# Base codec class

class Codec:
    length_stats = True

    # Self

    @abstractmethod
    def name(self) -> str:
        """
        Returns a string uniquely identifying this codec class. The convention
        is to use the fully qualified name (e.g. package.module.Class).
        """

        raise NotImplementedError

    def short_name(self) -> str:
        """
        Returns a short name for this codec, for use in filenames.
        """
        return self.name().split(".")[-1]

    # Per document value writer

    @abstractmethod
    def per_document_writer(self, session: 'storage.Session',
                            segment: Segment) -> 'PerDocumentWriter':
        raise NotImplementedError

    # Inverted index writer

    @abstractmethod
    def field_writer(self, session: 'storage.Session',
                     segment: Segment) -> 'FieldWriter':
        raise NotImplementedError

    # Index readers

    def automata(self, session: 'storage.Session',
                 segment: Segment) -> 'Automata':
        return Automata()

    @abstractmethod
    def per_document_reader(self, session: 'storage.Session',
                            segment: Segment) -> 'PerDocumentReader':
        raise NotImplementedError

    @abstractmethod
    def terms_reader(self, session: 'storage.Session',
                     segment: Segment) -> 'TermsReader':
        raise NotImplementedError

    # Segments

    @abstractmethod
    def new_segment(self, session: 'storage.Session') -> Segment:
        raise NotImplementedError

    def finish_segment(self, session: 'storage.Session', segment: Segment):
        pass

    def segment_storage(self, store: 'storage.Storage', segment: Segment
                        ) -> 'storage.Storage':
        return store

    @abstractmethod
    def segment_from_bytes(self, bs: bytes) -> Segment:
        raise NotImplementedError

    # On-disk posting block format

    @abstractmethod
    def postings_io(self) -> 'postings.PostingsIO':
        raise NotImplementedError


class WrappingCodec(Codec):
    def name(self) -> str:
        return "%s(%s)" % (type(self).__name__, self._child.name())

    def __init__(self, child: Codec):
        self._child = child

    def per_document_writer(self, session: 'storage.Session',
                            segment: Segment) -> 'PerDocumentWriter':
        return self._child.per_document_writer(session, segment)

    def field_writer(self, session: 'storage.Session', segment: Segment
                     ) -> 'FieldWriter':
        return self._child.field_writer(session, segment)

    def automata(self, session: 'storage.Session', segment: Segment
                 ) -> 'Automata':
        return self._child.automata(session, segment)

    def terms_reader(self, session: 'storage.Session', segment: Segment
                     ) -> 'TermsReader':
        return self._child.terms_reader(session, segment)

    def per_document_reader(self, session: 'storage.Session',
                            segment: Segment) -> 'PerDocumentReader':
        return self._child.per_document_reader(session, segment)

    def new_segment(self, session: 'storage.Session') -> Segment:
        return self._child.new_segment(session)

    def finish_segment(self, session: 'storage.Session', segment: Segment):
        self._child.finish_segment(session, segment)

    def segment_storage(self, store: 'storage.Storage', segment: Segment
                        ) -> 'storage.Storage':
        return self._child.segment_storage(store, segment)

    def segment_from_bytes(self, bs: bytes) -> Codec:
        return self._child.segment_from_bytes(bs)

    def postings_io(self) -> 'postings.PostingsIO':
        return self._child.postings_io()


# Writer classes

class PerDocumentWriter:
    @abstractmethod
    def postings_io(self) -> 'postings.PostingsIO':
        raise NotImplementedError

    @abstractmethod
    def start_doc(self, docnum: int):
        raise NotImplementedError

    @abstractmethod
    def add_field(self, fieldname: str, fieldobj: 'fields.FieldType',
                  value: Any, length: int):
        raise NotImplementedError

    @abstractmethod
    def add_column_value(self, fieldname: str, columnobj: 'columns.Column',
                         value: Any):
        raise NotImplementedError("Codec does not implement writing columns")

    @abstractmethod
    def add_vector_postings(self, fieldname: str, fieldobj: 'fields.FieldType',
                            posts: 'Sequence[ptuples.PostTuple]'):
        raise NotImplementedError

    @abstractmethod
    def add_raw_vector(self, fieldname: str, data: bytes):
        raise NotImplementedError

    def finish_doc(self):
        pass

    def close(self):
        pass


class FieldWriter:
    @abstractmethod
    def postings_io(self) -> 'postings.PostingsIO':
        raise NotImplementedError

    @abstractmethod
    def start_field(self, fieldname: str, fieldobj: 'fields.FieldType'):
        raise NotImplementedError

    @abstractmethod
    def start_term(self, termbytes: bytes):
        raise NotImplementedError

    @abstractmethod
    def add_posting(self, post: 'ptuples.PostTuple'):
        raise NotImplementedError

    @abstractmethod
    def add_raw_post(self, rawpost: 'ptuples.RawPost'):
        raise NotImplementedError

    def add_elements(self, *args, **kwargs):
        self.add_posting(ptuples.posting(*args, **kwargs))

    def add_posting_list(self, posts: 'Iterable[ptuples.PostTuple]'):
        add = self.add_posting
        for post in posts:
            add(post)

    def finish_term(self):
        pass

    def finish_field(self):
        pass

    def close(self):
        pass

    def add_dict_of_dicts(
        self, schema,
        fielddict: 'Dict[str, Dict[bytes, Sequence[ptuples.PostTuple]]]'
    ):
        for fieldname in sorted(fielddict):
            self.start_field(fieldname, schema[fieldname])
            termdict = fielddict[fieldname]
            for termbytes in sorted(termdict):
                self.start_term(termbytes)
                for post in termdict[termbytes]:
                    self.add_posting(post)
                self.finish_term()
            self.finish_field()


# Cursor classes

class TermCursor:
    def __init__(self, fieldname: str, fieldobj: 'fields.FieldType'):
        self.fieldname = fieldname
        self.field = fieldobj

    def __iter__(self):
        while self.is_valid():
            yield self.termbytes()
            self.next()

    @abstractmethod
    def first(self):
        raise NotImplementedError

    @abstractmethod
    def is_valid(self) -> bool:
        return False

    @abstractmethod
    def seek(self, termbytes: bytes):
        raise NotImplementedError

    @abstractmethod
    def next(self):
        raise NotImplementedError

    @abstractmethod
    def termbytes(self) -> bytes:
        raise NotImplementedError

    def text(self) -> text_type:
        return self.field.from_bytes(self.termbytes())

    @abstractmethod
    def term_info(self) -> 'readers.TermInfo':
        raise NotImplementedError


class EmptyCursor(TermCursor):
    def __init__(self, *args, **kwargs):
        pass

    def first(self):
        pass

    def is_valid(self):
        return False

    def seek(self, term):
        pass

    def next(self):
        raise InvalidCursor

    def termbytes(self):
        raise InvalidCursor

    def text(self):
        raise InvalidCursor

    def term_info(self):
        raise InvalidCursor


class MultiCursor(TermCursor):
    def __init__(self, cursors: Sequence[TermCursor]):
        self._cursors = [c for c in cursors if c.is_valid()]
        self._low = []
        self._tbytes = None
        self.next()

    def _find_low(self) -> bytes:
        # Finds the cursor(s) that is/are on the lowest term
        low = []
        lowterm = None

        for c in self._cursors:
            if c.is_valid():
                cterm = c.term()
                if low and cterm == lowterm:
                    low.append(c)
                elif low and cterm < lowterm:
                    low = [c]
                    lowterm = cterm

        self._low = low
        self._tbytes = lowterm
        return lowterm

    def first(self):
        for c in self._cursors:
            c.first()
        return self._find_low()

    def is_valid(self) -> bool:
        return any(c.is_valid() for c in self._cursors)

    def seek(self, termbytes: bytes):
        for c in self._cursors:
            c.seek(termbytes)
        return self._find_low()

    def next(self):
        for c in self._cursors:
            c.next()
        return self._find_low()

    def termbytes(self) -> bytes:
        return self._tbytes

    def text(self):
        low = self._low

        text = None
        for c in low:
            if text is None:
                text = c.text()
            elif text != c.text():
                raise Exception("Error: subcursors have different texts: %r" %
                                [c.text() for c in low])
        return text

    def term_info(self) -> 'readers.TermInfo':
        tis = [c.term_info() for c in self._low]
        return readers.TermInfo.combine(tis) if tis else None


# Reader classes

class TermsReader:
    @abstractmethod
    def __contains__(self, term: TermTuple) -> bool:
        raise NotImplementedError

    def set_merging_hint(self):
        pass

    @abstractmethod
    def cursor(self, fieldname: str, fieldobj: 'Optional[fields.FieldType]'
               ) -> TermCursor:
        raise NotImplementedError

    @abstractmethod
    def terms(self) -> Iterable[TermTuple]:
        raise NotImplementedError

    def term_range(self, fieldname: str, start: bytes, end: Optional[bytes]
                   ) -> Iterable[TermTuple]:
        cur = self.cursor(fieldname, None)
        cur.seek(start)
        while cur.is_valid() and ((end is None) or cur.termbytes() < end):
            yield cur.termbytes()
            cur.next()

    @abstractmethod
    def items(self) -> 'Iterable[Tuple[TermTuple, readers.TermInfo]]':
        raise NotImplementedError

    # @abstractmethod
    # def items_from(self, fieldname: str,
    #                prefix: bytes) -> Iterable[Tuple[TermTuple, TermInfo]]:
    #     raise NotImplementedError

    @abstractmethod
    def term_info(self, fieldname: str, termbytes: bytes) -> 'readers.TermInfo':
        raise NotImplementedError

    def weight(self, fieldname: str, termbytes: bytes) -> float:
        return self.term_info(fieldname, termbytes).weight()

    def doc_frequency(self, fieldname: str, termbytes: bytes) -> int:
        return self.term_info(fieldname, termbytes).doc_frequency()

    @abstractmethod
    def matcher(self, fieldname: str, termbytes: bytes,
                fmt: 'postform.Format', scorer=None):
        raise NotImplementedError

    @abstractmethod
    def indexed_field_names(self) -> Sequence[str]:
        raise NotImplementedError

    def close(self):
        pass


class Automata:
    @staticmethod
    def levenshtein_dfa(uterm: text_type, maxdist: int, prefix: int=0):
        return lev.levenshtein_automaton(uterm, maxdist, prefix).to_dfa()

    @staticmethod
    def find_matches(dfa: DFA, cur: TermCursor) -> Iterable[text_type]:
        unull = chr(0)

        if not cur.is_valid():
            return
        term = cur.text()

        match = dfa.next_valid_string(term)
        while match:
            cur.seek(match)
            if not cur.is_valid():
                return

            term = cur.text()
            if match == term:
                yield match
                term += unull
            match = dfa.next_valid_string(term)

    def terms_within(self, fieldcur: TermCursor, uterm: text_type,
                     maxdist: int, prefix=0) -> Iterable[text_type]:
        dfa = self.levenshtein_dfa(uterm, maxdist, prefix)
        return self.find_matches(dfa, fieldcur)


# Per-doc value reader

class PerDocumentReader:
    def set_merging_hint(self):
        pass

    def close(self):
        pass

    @abstractmethod
    def doc_count(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def doc_count_all(self) -> int:
        raise NotImplementedError

    # Deletions

    @abstractmethod
    def has_deletions(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def is_deleted(self, docnum) -> bool:
        raise NotImplementedError

    @abstractmethod
    def deleted_docs(self) -> Set[int]:
        raise NotImplementedError

    def all_doc_ids(self) -> Iterable[int]:
        """
        Returns an iterator of all document IDs in the reader.
        """

        is_deleted = self.is_deleted
        return (docnum for docnum in range(self.doc_count_all())
                if not is_deleted(docnum))

    def iter_docs(self) -> Iterable[Tuple[int, Dict]]:
        for docnum in self.all_doc_ids():
            yield docnum, self.stored_fields(docnum)

    # Columns

    def supports_columns(self) -> bool:
        return False

    def has_column(self, fieldname: str) -> bool:
        return False

    # Don't need to override this if supports_columns() returns False
    def column_reader(self, fieldname: str, column: 'columns.Column',
                      reverse: bool=False) -> 'columns.ColumnReader':
        raise UnsupportedFeature

    # Lengths

    @abstractmethod
    def doc_field_length(self, docnum: int, fieldname: str,
                         default: int=0) -> int:
        raise NotImplementedError

    @abstractmethod
    def field_length(self, fieldname: str) -> int:
        raise NotImplementedError

    @abstractmethod
    def min_field_length(self, fieldname: str) -> int:
        raise NotImplementedError

    @abstractmethod
    def max_field_length(self, fieldname: str) -> int:
        raise NotImplementedError

    # Vectors

    def has_vector(self, docnum: int, fieldname: str) -> bool:
        return False

    # Don't need to override this if has_vector() always returns False
    def vector(self, docnum: int, fieldname: str) -> 'postings.VectorReader':
        raise UnsupportedFeature

    # Stored

    @abstractmethod
    def stored_fields(self, docnum: int) -> Dict:
        raise NotImplementedError

    def all_stored_fields(self) -> Iterable[Dict]:
        for docnum in self.all_doc_ids():
            yield self.stored_fields(docnum)


# # Wrapping Segment
#
# class WrappingSegment(Segment):
#     def __init__(self, child):
#         self._child = child
#
#     def codec(self):
#         return self._child.codec()
#
#     def index_name(self):
#         return self._child.index_name()
#
#     def segment_id(self):
#         return self._child.segment_id()
#
#     def is_compound(self):
#         return self._child.is_compound()
#
#     def should_assemble(self):
#         return self._child.should_assemble()
#
#     def make_filename(self, ext):
#         return self._child.make_filename(ext)
#
#     def list_files(self, storage):
#         return self._child.list_files(storage)
#
#     def create_file(self, storage, ext, **kwargs):
#         return self._child.create_file(storage, ext, **kwargs)
#
#     def open_file(self, storage, ext, **kwargs):
#         return self._child.open_file(storage, ext, **kwargs)
#
#     def create_compound_file(self, storage):
#         return self._child.create_compound_file(storage)
#
#     def open_compound_file(self, storage):
#         return self._child.open_compound_file(storage)
#
#     def delete_document(self, docnum, delete=True):
#         return self._child.delete_document(docnum, delete=delete)
#
#     def has_deletions(self):
#         return self._child.has_deletions()
#
#     def deleted_count(self):
#         return self._child.deleted_count()
#
#     def deleted_docs(self):
#         return self._child.deleted_docs()
#
#     def is_deleted(self, docnum):
#         return self._child.is_deleted(docnum)
#
#     def set_doc_count(self, doccount):
#         self._child.set_doc_count(doccount)
#
#     def doc_count(self):
#         return self._child.doc_count()
#
#     def doc_count_all(self):
#         return self._child.doc_count_all()


# Multi per doc reader

class MultiPerDocumentReader(PerDocumentReader):
    def __init__(self, readers: List[PerDocumentReader]):
        self._readers = readers

        self._doc_offsets = []
        self._doccount = 0
        for pdr in readers:
            self._doc_offsets.append(self._doccount)
            self._doccount += pdr.doc_count_all()

        self.is_closed = False

    def set_merging_hint(self):
        for pdr in self._readers:
            pdr.set_merging_hint()

    def close(self):
        for r in self._readers:
            r.close()
        self.is_closed = True

    def doc_count_all(self) -> int:
        return self._doccount

    def doc_count(self) -> int:
        total = 0
        for r in self._readers:
            total += r.doc_count()
        return total

    def _document_reader(self, docnum: int) -> int:
        return max(0, bisect_right(self._doc_offsets, docnum) - 1)

    def _reader_and_docnum(self, docnum: int) -> Tuple[int, int]:
        rnum = self._document_reader(docnum)
        offset = self._doc_offsets[rnum]
        return rnum, docnum - offset

    def stored_fields(self, docnum: int) -> Dict:
        reader_i, sub_docnum = self._reader_and_docnum(docnum)
        reader = self._readers[reader_i]
        return reader.stored_fields(sub_docnum)

    # Deletions

    def has_deletions(self) -> bool:
        return any(r.has_deletions() for r in self._readers)

    def is_deleted(self, docnum: int) -> bool:
        x, y = self._reader_and_docnum(docnum)
        return self._readers[x].is_deleted(y)

    def deleted_docs(self) -> Set:
        docset = set()
        for r, offset in zip(self._readers, self._doc_offsets):
            docset.update((docnum + offset for docnum in r.deleted_docs()))
        return docset

    def all_doc_ids(self) -> Iterable[int]:
        for r, offset in zip(self._readers, self._doc_offsets):
            for docnum in r.all_doc_ids():
                yield docnum + offset

    # Columns

    def has_column(self, fieldname: str) -> bool:
        return any(r.has_column(fieldname) for r in self._readers)

    def column_reader(self, fieldname: str, column: 'columns.Column',
                      reverse: bool=False) -> 'columns.ColumnReader':
        if not self.has_column(fieldname):
            raise ValueError("No column %r" % (fieldname,))

        default = column.default_value()
        colreaders = []
        for r in self._readers:
            if r.has_column(fieldname):
                cr = r.column_reader(fieldname, column, reverse)
            else:
                cr = columns.EmptyColumnReader(default, r.doc_count_all())
            colreaders.append(cr)

        if len(colreaders) == 1:
            return colreaders[0]
        else:
            return columns.MultiColumnReader(colreaders)

    # Lengths

    def doc_field_length(self, docnum: int, fieldname: str,
                         default: int=0) -> int:
        x, y = self._reader_and_docnum(docnum)
        return self._readers[x].doc_field_length(y, fieldname, default)

    def field_length(self, fieldname: str) -> int:
        total = 0
        for r in self._readers:
            total += r.field_length(fieldname)
        return total

    def min_field_length(self, fieldname: str) -> int:
        return min(r.min_field_length(fieldname) for r in self._readers)

    def max_field_length(self, fieldname: str) -> int:
        return max(r.max_field_length(fieldname) for r in self._readers)





