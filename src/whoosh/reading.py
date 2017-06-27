# Copyright 2007 Matt Chaput. All rights reserved.
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
This module contains classes that allow reading from an index.
"""

from bisect import bisect_right
from functools import wraps
from heapq import heapify, heapreplace, heappop
from typing import (
    cast, Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union,
)

from whoosh import columns, fields, idsets
from whoosh.ifaces import codecs, readers, storage, weights
from whoosh.compat import text_type
from whoosh.ifaces import matchers
from whoosh.postings import postings
from whoosh.util import unclosed


# Typing aliases

TermTuple = Tuple[str, bytes]
TermText = Union[text_type, bytes]


# Decorators and helpers

# Decorator that raises an exception if the reader is closed or the fieldname
# in the first argument doesn't exist
def field_checked(f):
    @wraps(f)
    def check_field_wrapper(self, fieldname, *args, **kwargs):
        if self.closed:
            raise ValueError("Operation on a closed object")
        if fieldname not in self.schema:
            raise readers.TermNotFound("No field %r" % fieldname)
        if not self.schema[fieldname].indexed:
            raise readers.TermNotFound("Field %r is not indexed" % fieldname)
        return f(self, fieldname, *args, **kwargs)
    return check_field_wrapper


# Segment-based reader

class SegmentReader(readers.IndexReader):
    def __init__(self, storage: 'storage.Storage', schema: 'fields.Schema',
                 segment: 'codecs.Segment', generation: int=None,
                 use_codec: 'codecs.Codec'=None):
        """
        :param storage: the Storage object containing the segment's files.
        :param schema: the Schema object for this segment.
        :param segment: the Segment object to read.
        :param generation: the generation number of the index this object is
            reading.
        :param use_codec: if not None, use this Codec object to read the segment
            instead of the one that originally wrote the segment.
        """

        self.schema = schema
        self.closed = False

        self._segment = segment
        self._segid = self._segment.segment_id()
        self._gen = generation

        # Create a new reading session
        self._codec = use_codec if use_codec else segment.codec()
        self._main_storage = storage

        # Give the codec a chance to give us a specialized storage object
        # (e.g. for compound segments)
        self._storage = self._codec.segment_storage(storage, segment)
        # Open a read-only session
        self._session = self._storage.open(segment.index_name(),
                                           writable=False)

        # Get sub-readers from codec
        self._terms = self._codec.terms_reader(self._session, segment)
        self._perdoc = self._codec.per_document_reader(self._session, segment)

        self.default_idset_type = idsets.BitSet
        self._deleted_set = None

        from whoosh.writing.writing import EOL_FIELDNAME
        if (self._perdoc.supports_columns() and
                self._perdoc.has_column(EOL_FIELDNAME)):
            self._eol = self._eol_docs()
        else:
            self._eol = frozenset(())

    def __repr__(self):
        return "%s(%r, %r)" % (self.__class__.__name__, self._storage,
                               self._segment)

    def _eol_docs(self) -> Set[int]:
        from datetime import datetime
        from whoosh.writing.writing import EOL_FIELDNAME, EOL_COLUMN
        from whoosh.util.times import datetime_to_long

        now = datetime_to_long(datetime.utcnow())
        creader = self._perdoc.column_reader(EOL_FIELDNAME, EOL_COLUMN)
        delset = set()
        for docnum, eol in enumerate(creader):
            if eol and eol <= now:
                delset.add(docnum)
        creader.close()
        return delset

    def segment(self) -> 'codecs.Segment':
        return self._segment

    def json_info(self) -> dict:
        return {
            "schema": self.schema.json_info(),
            "segment_id": self.segment().json_info(),
            "generation": self.generation(),
            "has_deletions": self.has_deletions(),
            "doc_count": self.doc_count(),
            "doc_count_all": self.doc_count_all(),
            "indexed_fields": list(self.indexed_field_names()),
        }

    @unclosed
    def has_deletions(self) -> bool:
        return self._eol or self._perdoc.has_deletions()

    @unclosed
    def doc_count(self) -> int:
        if self._eol:
            return len(self.deleted_set())
        else:
            return self._perdoc.doc_count()

    @unclosed
    def doc_count_all(self) -> int:
        return self._perdoc.doc_count_all()

    @unclosed
    def is_deleted(self, docnum: int):
        return docnum in self._eol or self._perdoc.is_deleted(docnum)

    def generation(self) -> int:
        return self._gen

    @unclosed
    def set_merging_hint(self):
        self._terms.set_merging_hint()
        self._perdoc.set_merging_hint()

    @unclosed
    def indexed_field_names(self) -> Sequence[str]:
        return self._terms.indexed_field_names()

    @unclosed
    def __contains__(self, term: TermTuple) -> bool:
        fieldname, termbytes = term
        if fieldname not in self.schema:
            return False
        termbytes = self._text_to_bytes(fieldname, termbytes)
        return (fieldname, termbytes) in self._terms

    @unclosed
    def close(self):
        self._terms.close()
        self._perdoc.close()
        self.closed = True

    @unclosed
    def stored_fields(self, docnum) -> Dict:
        assert docnum >= 0
        schema = self.schema
        sfs = self._perdoc.stored_fields(docnum)
        # Double-check with schema to filter out removed fields
        return dict(item for item in sfs.items() if item[0] in schema)

    # Delegate doc methods to the per-doc reader

    @unclosed
    def all_doc_ids(self) -> Iterable[int]:
        if self._eol:
            _eol = self._eol
            return (docid for docid in self._perdoc.all_doc_ids()
                    if docid not in _eol)
        else:
            return self._perdoc.all_doc_ids()

    @unclosed
    def iter_docs(self) -> Iterable[Tuple[int, Dict]]:
        if self._eol:
            _eol = self._eol
            return ((docid, self.stored_fields(docid))
                    for docid in self._perdoc.all_doc_ids()
                    if docid not in _eol)
        else:
            return self._perdoc.iter_docs()

    @unclosed
    def field_length(self, fieldname: str) -> int:
        return self._perdoc.field_length(fieldname)

    @unclosed
    def min_field_length(self, fieldname: str) -> int:
        return self._perdoc.min_field_length(fieldname)

    @unclosed
    def max_field_length(self, fieldname: str) -> int:
        return self._perdoc.max_field_length(fieldname)

    @unclosed
    def doc_field_length(self, docnum: int, fieldname: str,
                         default: int=0) -> int:
        assert isinstance(docnum, int)
        return self._perdoc.doc_field_length(docnum, fieldname, default)

    @unclosed
    def has_vector(self, docnum: int, fieldname: str) -> bool:
        return self._perdoc.has_vector(docnum, fieldname)

    #

    @unclosed
    def all_terms(self) -> Iterable[Tuple[str, bytes]]:
        schema = self.schema
        return ((fieldname, text) for fieldname, text in self._terms.terms()
                if fieldname in schema)

    @field_checked
    def term_range(self, fieldname: str, start: TermText,
                   end: Optional[TermText]) -> Iterable[bytes]:
        schema = self.schema
        if fieldname not in schema:
            return iter(())

        if end is not None:
            end = self._text_to_bytes(fieldname, end)

        return self._terms.term_range(fieldname, start, end)

    @field_checked
    def term_info(self, fieldname: str, termbytes: TermText
                  ) -> 'readers.TermInfo':
        termbytes = self._text_to_bytes(fieldname, termbytes)
        try:
            return self._terms.term_info(fieldname, termbytes)
        except KeyError:
            raise readers.TermNotFound("%s:%r" % (fieldname, termbytes))

    @unclosed
    def __iter__(self) -> 'Iterable[Tuple[TermTuple, readers.TermInfo]]':
        schema = self.schema
        return ((term, terminfo) for term, terminfo in self._terms.items()
                if term[0] in schema)

    # @field_checked
    # def iter_from(self, fieldname: str,
    #               termbytes: bytes) -> Iterable[Tuple[TermTuple, TermInfo]]:
    #     schema = self.schema
    #     termbytes = self._text_to_bytes(fieldname, termbytes)
    #     return ((term, terminfo) for term, terminfo
    #             in self._terms.items_from(fieldname, termbytes)
    #             if term[0] in schema)

    @field_checked
    def weight(self, fieldname: str, termbytes: TermText) -> float:
        termbytes = self._text_to_bytes(fieldname, termbytes)
        try:
            return self._terms.weight(fieldname, termbytes)
        except KeyError:
            return 0

    @field_checked
    def doc_frequency(self, fieldname: str, termbytes: TermText) -> int:
        termbytes = self._text_to_bytes(fieldname, termbytes)
        try:
            return self._terms.doc_frequency(fieldname, termbytes)
        except readers.TermNotFound:
            return 0

    def deleted_set(self) -> 'idsets.DocIdSet':
        if self._deleted_set is None:
            deldocs = self._perdoc.deleted_docs()
            self._deleted_set = self.default_idset_type(deldocs)
            if self._eol:
                self._deleted_set.update(self._eol)
        return cast(idsets.DocIdSet, self._deleted_set)

    @field_checked
    def matcher(self, fieldname: str, termbytes: TermText,
                scorer: weights.Scorer=None,
                include: 'Union[idsets.DocIdSet, Set]'=None,
                exclude: 'Union[idsets.DocIdSet, Set]'=None
                ) -> 'matchers.Matcher':
        from whoosh.matching.wrappers import FilterMatcher

        termbytes = self._text_to_bytes(fieldname, termbytes)
        format_ = self.schema[fieldname].format
        matcher = self._terms.matcher(fieldname, termbytes, format_, scorer)

        if self._eol or self._perdoc.has_deletions():
            deleted = self.deleted_set()
            if exclude:
                exclude = idsets.OverlaySet(deleted, exclude)
            else:
                exclude = deleted

        if include is not None:
            matcher = FilterMatcher(matcher, include, exclude=False)
        if exclude:
            matcher = FilterMatcher(matcher, exclude, exclude=True)

        return matcher

    @unclosed
    def vector(self, docnum: int, fieldname: str) -> 'postings.VectorReader':
        if fieldname not in self.schema:
            raise readers.TermNotFound("No %r field" % fieldname)
        return self._perdoc.vector(docnum, fieldname)

    @field_checked
    def cursor(self, fieldname) -> 'codecs.TermCursor':
        fieldobj = self.schema[fieldname]
        return self._terms.cursor(fieldname, fieldobj)

    @field_checked
    def terms_within(self, fieldname: str, text: TermText, maxdist: int,
                     prefix: int=0) -> Iterable[text_type]:
        # Replaces the horribly inefficient base implementation with one based
        # on skipping through the word list efficiently using a DFA

        fieldobj = self.schema[fieldname]
        spellfield = fieldobj.spelling_fieldname(fieldname)
        auto = self._codec.automata(self._session, self._segment)
        fieldcur = self.cursor(spellfield)
        return auto.terms_within(fieldcur, text, maxdist, prefix)

    # Column methods

    def _special_column(self, fieldname: str, columnobj: columns.Column):
        return self._perdoc.column_reader(fieldname, columnobj)

    def has_column(self, fieldname: str) -> bool:
        if fieldname not in self.schema:
            return False

        colobj = self.schema[fieldname].column
        return colobj and self._perdoc.has_column(fieldname)

    @field_checked
    def column_reader(self, fieldname: str, column: columns.Column=None,
                      reverse=False, translate=True) -> columns.ColumnReader:

        try:
            fieldobj = self.schema[fieldname]
        except KeyError:
            raise readers.TermNotFound("No %r field" % fieldname)

        column = column or fieldobj.column
        if not column:
            column = fieldobj.default_column()

        if self._perdoc.has_column(fieldname):
            creader = self._perdoc.column_reader(fieldname, column,
                                                 reverse=reverse)
        else:
            # This segment doesn't have a column file for this field, so create
            # a fake column reader that always returns the default value.
            default = column.default_value(reverse=reverse)
            creader = columns.EmptyColumnReader(default, self.doc_count_all())

        if translate:
            # Wrap the column in a Translator to give the caller
            # nice values instead of sortable representations
            fcv = fieldobj.from_column_value
            creader = columns.TranslatingColumnReader(creader, fcv)

        return creader


# Fake IndexReader class for empty indexes

class EmptyReader(readers.IndexReader):
    def __init__(self, schema=None):
        self.schema = schema or fields.Schema()
        self.closed = False

    def __contains__(self, term: TermTuple) -> bool:
        return False

    def cursor(self, fieldname: str) -> 'codecs.TermCursor':
        return codecs.EmptyCursor()

    def indexed_field_names(self) -> Sequence[str]:
        return iter(())

    def all_terms(self) -> Iterable[TermTuple]:
        return iter(())

    def term_info(self, fieldname: str, termbytes: TermText
                  ) -> 'readers.TermInfo':
        raise readers.TermNotFound((fieldname, termbytes))

    def __iter__(self) -> 'Iterable[Tuple[TermTuple, readers.TermInfo]]':
        return iter(())

    def iter_field(self, fieldname: str, prefix: TermText=b''
                   ) -> 'Iterable[Tuple[bytes, readers.TermInfo]]':
        return iter(())

    def iter_prefix(self, fieldname: str, prefix: TermText=b''
                    )-> 'Iterable[Tuple[bytes, readers.TermInfo]]':
        return iter(())

    def lexicon(self, fieldname: str) -> Iterable[bytes]:
        return iter(())

    def has_deletions(self) -> bool:
        return False

    def is_deleted(self, docnum: int) -> bool:
        return False

    def stored_fields(self, docnum: int) -> Dict:
        raise KeyError("No document number %s" % docnum)

    def iter_docs(self) -> Iterable[Tuple[int, Dict]]:
        return iter(())

    def doc_count_all(self) -> int:
        return 0

    def doc_count(self) -> int:
        return 0

    def weight(self, fieldname: str, text: TermText) -> float:
        return 0

    def doc_frequency(self, fieldname: str, text: TermText) -> int:
        return 0

    def field_length(self, fieldname: str) -> int:
        return 0

    def min_field_length(self, fieldname: str) -> int:
        return 0

    def max_field_length(self, fieldname: str) -> int:
        return 0

    def doc_field_length(self, docnum: int, fieldname: str,
                         default: int=0) -> int:
        return default

    def matcher(self, fieldname: str, termbytes: TermText,
                scorer: weights.Scorer=None,
                include: 'Union[idsets.DocIdSet, Set]'=None,
                exclude: 'Union[idsets.DocIdSet, Set]'=None
                ) -> 'matchers.Matcher':
        raise readers.TermNotFound("%s:%r" % (fieldname, termbytes))

    def has_vector(self, docnum: int, fieldname: str) -> bool:
        return False

    def vector(self, docnum: int, fieldname: str) -> 'postings.VectorReader':
        raise KeyError("No document number %s" % docnum)


# Multisegment reader class

class MultiReader(readers.IndexReader):
    """
    Do not instantiate this object directly. Instead use Index.reader().
    """

    def __init__(self, readers: 'List[readers.IndexReader]',
                 generation: int=None):
        self.readers = readers
        self._gen = generation
        self.closed = False

        self.schema = None
        if readers:
            self.schema = readers[0].schema

        self.doc_offsets = []
        self.base = 0
        for r in self.readers:
            self.doc_offsets.append(self.base)
            self.base += r.doc_count_all()

    def _document_segment(self, docnum: int) -> int:
        return max(0, bisect_right(self.doc_offsets, docnum) - 1)

    def _segment_and_docnum(self, docnum: int) -> Tuple[int, int]:
        segmentnum = self._document_segment(docnum)
        offset = self.doc_offsets[segmentnum]
        return segmentnum, docnum - offset

    def is_atomic(self) -> bool:
        return False

    def leaf_readers(self) -> 'List[Tuple[readers.IndexReader, int]]':
        return list(zip(self.readers, self.doc_offsets))

    def add_reader(self, reader: 'readers.IndexReader'):
        self.readers.append(reader)
        self.doc_offsets.append(self.base)
        self.base += reader.doc_count_all()

    def set_merging_hint(self):
        for r in self.readers:
            r.set_merging_hint()

    def close(self):
        for d in self.readers:
            d.close()
        self.closed = True

    def generation(self) -> int:
        return self._gen

    # Term methods

    def __contains__(self, term: TermTuple) -> bool:
        return any((term in r) for r in self.readers)

    @staticmethod
    def _merge_iters(iterlist: List[Iterable[Any]]
                     ) -> Iterable[Any]:
        # Merge-sorts terms coming from a list of term iterators.

        # Create a map so we can look up each iterator by its id() value
        itermap = {}
        for it in iterlist:
            itermap[id(it)] = it

        # Fill in the list with the head term from each iterator.
        current = []
        for it in iterlist:
            try:
                term = next(it)
            except StopIteration:
                continue
            current.append((term, id(it)))
        # Number of active iterators
        active = len(current)

        # If only one iterator is active, just yield from it and return
        if active == 1:
            term, itid = current[0]
            it = itermap[itid]
            yield term
            for term in it:
                yield term
            return

        # Otherwise, do a streaming heap sort of the terms from the iterators
        heapify(current)
        while active:
            # Peek at the first term in the sorted list
            term = current[0][0]

            # Re-iterate on all items in the list that have that term
            while active and current[0][0] == term:
                it = itermap[current[0][1]]
                try:
                    nextterm = next(it)
                    heapreplace(current, (nextterm, id(it)))
                except StopIteration:
                    heappop(current)
                    active -= 1

            # Yield the term
            yield term

    def indexed_field_names(self) -> Sequence[str]:
        names = set()
        for r in self.readers:
            names.update(r.indexed_field_names())
        return sorted(names)

    def all_terms(self) -> Iterable[TermTuple]:
        return self._merge_iters([r.all_terms() for r in self.readers])

    def term_range(self, fieldname: str, start: TermText,
                   end: Optional[TermText]) -> Iterable[bytes]:
        return self._merge_iters([r.term_range(fieldname, start, end)
                                  for r in self.readers])

    def term_info(self, fieldname: str, termbytes: TermText):
        termbytes = self._text_to_bytes(fieldname, termbytes)
        term = fieldname, termbytes

        # Get the term infos for the sub-readers containing the term
        tis = [(r.term_info(fieldname, termbytes), offset)
               for r, offset in list(zip(self.readers, self.doc_offsets))
               if term in r]

        if not tis:
            raise readers.TermNotFound(term)

        return readers.TermInfo.combine(tis)

    def weight(self, fieldname: str, termbytes: TermText) -> float:
        termbytes = self._text_to_bytes(fieldname, termbytes)
        return sum(r.weight(fieldname, termbytes) for r in self.readers)

    def doc_frequency(self, fieldname: str, termbytes) -> int:
        termbytes = self._text_to_bytes(fieldname, termbytes)
        return sum(r.doc_frequency(fieldname, termbytes) for r in self.readers)

    def matcher(self, fieldname: str, termbytes: TermText,
                scorer: weights.Scorer=None,
                include: 'Union[idsets.DocIdSet, Set]'=None,
                exclude: 'Union[idsets.DocIdSet, Set]'=None
                ) -> 'matchers.Matcher':
        from whoosh.matching.wrappers import MultiMatcher

        rs = self.readers
        doc_offsets = self.doc_offsets
        doccount = self.doc_count_all()
        termbytes = self._text_to_bytes(fieldname, termbytes)

        ms = []
        m_offsets = []
        for i, r in enumerate(rs):
            start = doc_offsets[i]
            end = doc_offsets[i + 1] if i < len(rs) - 1 else doccount
            subinclude = subexclude = None
            if include:
                subinclude = idsets.SubSet(include, start, end)
            if exclude:
                subexclude = idsets.SubSet(exclude, start, end)
            try:
                m = r.matcher(fieldname, termbytes, scorer=scorer,
                              include=subinclude, exclude=subexclude)
            except readers.TermNotFound:
                pass
            else:
                ms.append(m)
                m_offsets.append(start)

        if not ms:
            raise readers.TermNotFound(fieldname, termbytes)

        # Even if there's only one matcher, we still wrap it with a MultiMatcher
        # so it adds the correct offset, UNLESS the offset is 0
        if len(ms) == 1 and m_offsets[0] == 0:
            return ms[0]
        else:
            return MultiMatcher(ms, m_offsets, scorer)

    def cursor(self, fieldname: str) -> 'codecs.TermCursor':
        return codecs.MultiCursor([r.cursor(fieldname) for r in self.readers])

    def first_id(self, fieldname, text) -> int:
        for i, r in enumerate(self.readers):
            try:
                docid = r.first_id(fieldname, text)
            except (KeyError, readers.TermNotFound):
                pass
            else:
                if docid is None:
                    raise readers.TermNotFound((fieldname, text))
                else:
                    return self.doc_offsets[i] + docid

        raise readers.TermNotFound((fieldname, text))

    # Deletion methods

    def has_deletions(self) -> bool:
        return any(r.has_deletions() for r in self.readers)

    def is_deleted(self, docnum: int) -> bool:
        segmentnum, segmentdoc = self._segment_and_docnum(docnum)
        return self.readers[segmentnum].is_deleted(segmentdoc)

    def stored_fields(self, docnum: int) -> Dict:
        segmentnum, segmentdoc = self._segment_and_docnum(docnum)
        return self.readers[segmentnum].stored_fields(segmentdoc)

    # Columns

    def has_column(self, fieldname: str) -> bool:
        return any(r.has_column(fieldname) for r in self.readers)

    def column_reader(self, fieldname: str, column: columns.Column=None,
                      reverse: bool=False, translate: bool=True
                      ) -> columns.MultiColumnReader:
        crs = []
        for i, r in enumerate(self.readers):
            cr = r.column_reader(fieldname, column=column, reverse=reverse,
                                 translate=translate)
            crs.append(cr)
        return columns.MultiColumnReader(crs, self.doc_offsets)

    # Per doc methods

    def all_doc_ids(self) -> Iterable[int]:
        for i, reader in enumerate(self.readers):
            reader = self.readers[i]
            docbase = self.doc_offsets[i]
            for docnum in reader.all_doc_ids():
                yield docbase + docnum

    def iter_docs(self) -> Iterable[Tuple[int, Dict]]:
        for i, reader in enumerate(self.readers):
            reader = self.readers[i]
            docbase = self.doc_offsets[i]
            for docnum, stored in reader.iter_docs():
                yield docbase + docnum, stored

    def doc_count_all(self) -> int:
        return sum(dr.doc_count_all() for dr in self.readers)

    def doc_count(self) -> int:
        return sum(dr.doc_count() for dr in self.readers)

    def field_length(self, fieldname: str) -> int:
        return sum(dr.field_length(fieldname) for dr in self.readers)

    def min_field_length(self, fieldname: str) -> int:
        return min(r.min_field_length(fieldname) for r in self.readers)

    def max_field_length(self, fieldname: str) -> int:
        return max(r.max_field_length(fieldname) for r in self.readers)

    def doc_field_length(self, docnum: int, fieldname: str,
                         default: int=0) -> int:
        segmentnum, segmentdoc = self._segment_and_docnum(docnum)
        reader = self.readers[segmentnum]
        return reader.doc_field_length(segmentdoc, fieldname, default=default)

    def has_vector(self, docnum: int, fieldname: str) -> bool:
        segmentnum, segmentdoc = self._segment_and_docnum(docnum)
        return self.readers[segmentnum].has_vector(segmentdoc, fieldname)

    def vector(self, docnum: int, fieldname: str) -> 'postings.VectorReader':
        segmentnum, segmentdoc = self._segment_and_docnum(docnum)
        return self.readers[segmentnum].vector(segmentdoc, fieldname)


# Utility functions

def combine_readers(readers):
    rs = []
    for r in readers:
        if r.is_atomic():
            rs.append(r)
        else:
            rs.extend(r.readers)

    if rs:
        if len(rs) == 1:
            return rs[0]
        else:
            return MultiReader(rs)
    else:
        return EmptyReader()




