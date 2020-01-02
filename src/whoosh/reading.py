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

import typing
from abc import abstractmethod
from bisect import bisect_right
from functools import wraps
from heapq import heapify, heapreplace, heappop
from typing import (cast, Any, Callable, Dict, Iterable, List, Optional,
                    Sequence, Set, Tuple, Union)

import whoosh.scoring
from whoosh import columns, fields, idsets, spelling, storage
from whoosh.codec import codecs
from whoosh.matching import matchers
from whoosh.postings import postings, ptuples
from whoosh.support.levenshtein import distance
from whoosh.util import unclosed

# Typing imports
if typing.TYPE_CHECKING:
    from whoosh import reading


# Typing aliases

TermTuple = Tuple[str, bytes]
TermTupleWithOptReader = 'Iterable[Tuple[TermTuple, Optional[IndexReader]]]'
TermText = Union[str, bytes]


# Decorators and helpers

# Decorator that raises an exception if the reader is closed or the fieldname
# in the first argument doesn't exist
def field_checked(f):
    @wraps(f)
    def check_field_wrapper(self, fieldname, *args, **kwargs):
        if self.closed:
            raise ValueError("Operation on a closed object")
        if fieldname not in self.schema:
            raise TermNotFound("No field %r" % fieldname)
        if not self.schema[fieldname].indexed:
            raise TermNotFound("Field %r is not indexed" % fieldname)
        return f(self, fieldname, *args, **kwargs)
    return check_field_wrapper


def leaf_readers(reader_list: 'Sequence[IndexReader]') -> 'List[IndexReader]':
    rs = []
    for r in reader_list:
        for lr, _ in r.leaf_readers():
            rs.append(lr)
    return rs


# Exceptions

class TermNotFound(Exception):
    pass


class NoVectorError(Exception):
    pass


# Term info object

class TermInfo:
    """
    Represents a set of statistics about a term. This object is returned by
    :meth:`IndexReader.term_info`. These statistics may be useful for
    optimizations and scoring algorithms.
    """

    def __init__(self, weight: float=0, df: int=0, minlength: int=None,
                 maxlength: int=0, maxweight: float=0, minid: int=None,
                 maxid: int=0):
        self._weight = weight
        self._df = df
        self._minlength = minlength
        self._maxlength = maxlength
        self._maxweight = maxweight
        self._minid = minid
        self._maxid = maxid

    @staticmethod
    def combine(tis: 'Sequence[Tuple[TermInfo, int]]') -> 'TermInfo':
        """
        Returns a ``TermInfo`` that aggregates the statistics from mutliple
        ``TermInfo`` instances.

        :param tis: a list of ``(TermInfo, doc_offset)`` tuples.
        """

        if len(tis) == 1:
            ti, offset = tis[0]
            ti._minid += offset
            ti._maxid += offset
            return ti

        # Combine the various statistics
        w = sum(ti.weight() for ti, _ in tis)
        df = sum(ti.doc_frequency() for ti, _ in tis)
        ml = min(ti.min_length() for ti, _ in tis)
        xl = max(ti.max_length() for ti, _ in tis)
        xw = max(ti.max_weight() for ti, _ in tis)

        # For min and max ID, we need to add the doc offsets
        mid = min(ti.min_id() + offset for ti, offset in tis)
        xid = max(ti.max_id() + offset for ti, offset in tis)

        return TermInfo(w, df, ml, xl, xw, mid, xid)

    def _update_minlen(self, length):
        if self._minlength is None:
            self._minlength = length
        else:
            self._minlength = min(self._minlength, length)

    def add_posting(self, docid, weight, length):
        if self._minid is None:
            self._minid = docid
        self._maxid = docid
        self._df += 1
        self._weight += weight
        self._maxlength = max(self._maxlength, length)

    def add_posting_list_stats(self, posts: Sequence[ptuples.PostTuple]):
        # Incorporate the stats from a list of postings into this object.
        # We assume the min doc id of the list > our current max doc id

        self._df += len(posts)

        post_weight = ptuples.post_weight
        weights = [post_weight(p) for p in posts if post_weight(p)]
        post_length = ptuples.post_length
        lengths = [post_length(p) for p in posts if post_length(p)]

        if weights:
            self._weight += sum(weights)
            self._maxweight = max(self._maxweight, max(weights))
        if lengths:
            self._maxlength = max(self._maxlength, max(lengths))
            self._update_minlen(min(lengths))

        if self._minid is None:
            self._minid = posts[0][ptuples.DOCID]
        self._maxid = posts[-1][ptuples.DOCID]

    def add_posting_reader_stats(self, r: postings.DocListReader):
        # Incorporate the stats from the reader into this info object
        # We assume the min docid of the list > our current max docid

        self._weight += r.total_weight()
        self._df += len(r)
        self._maxlength = max(self._maxlength, r.max_length())
        self._maxweight = max(self._maxweight, r.max_weight())
        self._update_minlen(r.min_length())

        if self._minid is None:
            self._minid = r.min_id()
        self._maxid = r.max_id()

    def copy_from(self, terminfo: 'TermInfo',
                  docmap_get: Callable[[int, int], int]=None):
        self._weight = terminfo.weight()
        self._df = terminfo.doc_frequency()
        self._minlength = terminfo.min_length()
        self._maxlength = terminfo.max_length()
        self._maxweight = terminfo.max_weight()

        minid = terminfo.min_id()
        if docmap_get:
            minid = docmap_get(minid, minid)
        maxid = terminfo.max_id()
        if docmap_get:
            maxid = docmap_get(maxid, maxid)
        self._minid = minid
        self._maxid = maxid

    def has_blocks(self) -> bool:
        raise NotImplementedError

    def shift(self, delta):
        self._minid += delta
        self._maxid += delta

    def weight(self) -> float:
        """
        Returns the total frequency of the term across all documents.
        """

        return self._weight

    def doc_frequency(self) -> int:
        """
        Returns the number of documents the term appears in.
        """

        return self._df

    def min_length(self) -> int:
        """
        Returns the length of the shortest field value the term appears
        in.
        """

        return self._minlength

    def max_length(self) -> int:
        """
        Returns the length of the longest field value the term appears
        in.
        """

        return self._maxlength

    def max_weight(self) -> float:
        """
        Returns the number of times the term appears in the document in
        which it appears the most.
        """

        return self._maxweight

    def min_id(self) -> int:
        """
        Returns the lowest document ID this term appears in.
        """

        return self._minid

    def max_id(self) -> int:
        """
        Returns the highest document ID this term appears in.
        """

        return self._maxid


# Base class

class IndexReader:
    """
    Do not instantiate this object directly. Instead use Index.reader().
    """

    def __init__(self, schema: 'fields.Schema'):
        self.schema = schema
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    @abstractmethod
    def __contains__(self, term: TermTuple) -> bool:
        """
        Returns True if the given term tuple (fieldname, text) is
        in this reader.
        """
        raise NotImplementedError

    # def codec(self) -> 'codec.Codec':
    #     """
    #     Returns the :class:`whoosh.codec.base.Codec` object used to read
    #     this reader's segment. If this reader is not atomic
    #     (``reader.is_atomic() == True``), returns None.
    #     """
    #
    #     return None

    def reader(self) -> 'IndexReader':
        """
        Returns self. This is simply for convenience: in certain places that
        can accept a Searcher or Reader, you can call ``.reader()`` on either.
        """

        return self

    def segment(self) -> 'codecs.Segment':
        """
        Returns the :class:`whoosh.index.Segment` object used by this reader.
        If this reader is not atomic (``reader.is_atomic() == True``), returns
        None.
        """

        return None

    def segment_id(self) -> str:
        seg = self.segment()
        if seg:
            return seg.segment_id()

    def is_atomic(self) -> bool:
        return True

    def _text_to_bytes(self, fieldname: str, text: TermText) -> bytes:
        if isinstance(text, bytes):
            return text

        if fieldname not in self.schema:
            raise TermNotFound((fieldname, text))
        return self.schema[fieldname].to_bytes(text)

    def close(self):
        """
        Closes the open files associated with this reader.
        """

        self.closed = True

    def generation(self) -> int:
        """
        Returns the generation of the index being read, or -1 if the backend
        is not versioned.
        """

        return None

    def set_merging_hint(self):
        """
        Signals to the reader that it is being used for merging, so it should
        enable any optimizations to make linear, batch reading faster.
        """

        pass

    @abstractmethod
    def indexed_field_names(self) -> Sequence[str]:
        """
        Returns an iterable of strings representing the names of the fields in
        the reader. This can be different from the fields in the Schema if
        fields have been added or removed to/from the schema, or if you are
        using glob fields.
        """

        raise NotImplementedError

    @abstractmethod
    def field_min_term(self, fieldname: str) -> bytes:
        raise NotImplementedError

    @abstractmethod
    def field_max_term(self, fieldname: str) -> bytes:
        raise NotImplementedError

    @abstractmethod
    def term_info(self, fieldname: str, text: TermText) -> TermInfo:
        """
        Returns a :class:`TermInfo` object allowing access to various
        statistics about the given term.

        :param fieldname: the name of the field containing the term.
        :param text: the term bytestring.
        """

        raise NotImplementedError

    @abstractmethod
    def all_terms(self) -> Iterable[TermTuple]:
        """
        Yields (fieldname, text) tuples for every term in the index.
        """

        raise NotImplementedError

    def all_terms_with_reader(self) -> TermTupleWithOptReader:
        for term in self.all_terms():
            yield term, self

    @unclosed
    def term_range(self, fieldname: str, start: Optional[TermText],
                   end: Optional[TermText]) -> Iterable[bytes]:
        """
        Yields ``termbytes`` for every term in the index starting at ``start``
        and ending before ``end``.

        :param fieldname: the name of the field to start from.
        :param start: the text to start from.
        :param end: the text to end before.
        """

        # The default implementation just scans the whole list of terms
        if start is not None:
            start = self._text_to_bytes(fieldname, start)
        if end is not None:
            end = self._text_to_bytes(fieldname, end)
        for fname, tbytes in self.all_terms():
            if fname < fieldname or tbytes < start:
                continue
            if fname > fieldname or (tbytes is not None and tbytes >= end):
                break
            yield tbytes

    @unclosed
    def expand_prefix(self, fieldname: str,
                      prefix: TermText) -> Iterable[bytes]:
        """
        Yields terms in the given field that start with the given prefix.

        :param fieldname: the name of the field containing the terms.
        :param prefix: yield terms starting with this byte string.
        """

        prefix = self._text_to_bytes(fieldname, prefix)
        for termbytes in self.term_range(fieldname, prefix, None):
            if not termbytes.startswith(prefix):
                return
            yield termbytes

    @unclosed
    def lexicon(self, fieldname: str) -> Iterable[bytes]:
        """
        Yields all bytestrings in the given field.

        :param fieldname: the name of the field to get the terms from.
        """

        # The default implementation is dumb, subclasses should replace it with
        # an implementation-specific optimization
        return self.term_range(fieldname, b'', None)

    @unclosed
    def field_terms(self, fieldname: str) -> Iterable[Any]:
        fieldobj = self.schema[fieldname]
        from_b = fieldobj.from_bytes
        for termbytes in self.lexicon(fieldname):
            yield from_b(termbytes)

    @unclosed
    def __iter__(self) -> Iterable[Tuple[TermTuple, TermInfo]]:
        """
        Yields ``((fieldname, text), terminfo)`` tuples for each term in the
        reader, in lexical order.
        """

        term_info = self.term_info
        for term in self.all_terms():
            yield term, term_info(*term)

    @unclosed
    def iter_field(self, fieldname: str, prefix: TermText=b''
                   ) -> Iterable[Tuple[bytes, TermInfo]]:
        """
        Yields ``(tbytes, terminfo)`` tuples for all terms in the given field.

        :param fieldname: the name of the field.
        :param prefix: the term to start from.
        """

        term_info = self.term_info
        for tbytes in self.expand_prefix(fieldname, prefix):
            yield tbytes, term_info(fieldname, tbytes)

    @abstractmethod
    def has_deletions(self) -> bool:
        """
        Returns True if the underlying index/segment has deleted documents.
        """

        raise NotImplementedError

    def all_doc_ids(self) -> Iterable[int]:
        """
        Returns an iterator of all document IDs in the reader.
        """

        is_deleted = self.is_deleted
        return (docnum for docnum in range(self.doc_count_all())
                if not is_deleted(docnum))

    def iter_docs(self) -> Tuple[int, Dict]:
        """
        Yields a series of ``(docnum, stored_fields_dict)``
        tuples for the undeleted documents in the reader.
        """

        for docnum in self.all_doc_ids():
            yield docnum, self.stored_fields(docnum)

    @abstractmethod
    def is_deleted(self, docnum: int) -> bool:
        """
        Returns True if the given document number is marked deleted.

        :param docnum: the document number to check.
        """

        raise NotImplementedError(self.__class__)

    @abstractmethod
    def stored_fields(self, docnum: int) -> Dict:
        """
        Returns the stored fields for the given document number.

        :param docnum: the number of the document to get the stored fields for.
        """

        raise NotImplementedError(self.__class__)

    @abstractmethod
    def doc_count_all(self) -> int:
        """
        Returns the total number of documents, DELETED OR UNDELETED,
        in this reader.
        """

        raise NotImplementedError(self.__class__)

    @abstractmethod
    def doc_count(self) -> int:
        """
        Returns the total number of UNDELETED documents in this reader.
        """

        raise NotImplementedError(self.__class__)

    @abstractmethod
    def weight(self, fieldname: str, termbytes: TermText) -> float:
        """
        Returns the total number of instances of the given term in the
        collection.

        :param fieldname: the name of the term's field.
        :param termbytes: the term.
        """

        raise NotImplementedError(self.__class__)

    def frequency(self, fieldname: str, termbytes: TermText) -> float:
        # Backwards compatibility
        return self.weight(fieldname, termbytes)

    @abstractmethod
    def doc_frequency(self, fieldname, termbytes: TermText) -> int:
        """
        Returns how many documents the given term appears in.

        :param fieldname: the name of the term's field.
        :param termbytes: the term.
        """

        raise NotImplementedError(self.__class__)

    @abstractmethod
    def field_length(self, fieldname: str) -> int:
        """
        Returns the total number of terms in the given field. This is used
        by some scoring algorithms.

        :param fieldname: the name of the field.
        """

        raise NotImplementedError(self.__class__)

    @abstractmethod
    def min_field_length(self, fieldname: str) -> int:
        """
        Returns the minimum length of the field across all documents. This
        is used by some scoring algorithms.

        :param fieldname: the name of the field.
        """
        raise NotImplementedError(self.__class__)

    @abstractmethod
    def max_field_length(self, fieldname: str) -> int:
        """
        Returns the minimum length of the field across all documents. This
        is used by some scoring algorithms.

        :param fieldname: the name of the field.
        """
        raise NotImplementedError(self.__class__)

    @abstractmethod
    def doc_field_length(self, docnum: int, fieldname: str, default: int=0):
        """
        Returns the number of terms in the given field in the given
        document. This is used by some scoring algorithms.

        :param docnum: the number of the document containing the field.
        :param fieldname: the name of the field to get the length of.
        :param default: number to return if the length of the given field isn't
            available in the index.
        """

        raise NotImplementedError(self.__class__)

    @abstractmethod
    def matcher(self, fieldname: str, termbytes: TermText,
                scorer: whoosh.scoring.Scorer =None,
                include: 'Union[idsets.DocIdSet, Set]'=None,
                exclude: 'Union[idsets.DocIdSet, Set]'=None
                ) -> 'matchers.Matcher':
        """
        Returns a :class:`~whoosh.matching.Matcher` for the postings of the
        given term.

        >>> pr = reader.matcher("content", "render")
        >>> pr.skip_to(10)
        >>> pr.id
        12

        :param fieldname: the field name or field number of the term.
        :param termbytes: the term byte string.
        :param scorer: the scoring object to use for this term.
        :param include: only produce documents whose IDs are in this set.
        :param exclude: don't produce documents whose IDs are in this set.
        """

        raise NotImplementedError(self.__class__.__name__)

    @abstractmethod
    def has_vector(self, docnum: int, fieldname: str) -> bool:
        """
        Returns True if the index contains vector information for the given
        field in the given document.

        :param docnum: the number of the document to check.
        :param fieldname: the name of the field to check.
        """

        raise NotImplementedError(self.__class__.__name__)

    @abstractmethod
    def vector(self, docnum: int, fieldname: str) -> 'postings.VectorReader':
        """
        Returns a :class:`~whoosh.postings.VectorReader` object for the
        given term vector.

        :param docnum: the document number of the document for which you want
            the term vector.
        :param fieldname: the field name or field number of the field for which
            you want the term vector.
        """

        raise NotImplementedError

    @abstractmethod
    def cursor(self, fieldname: str) -> 'codecs.TermCursor':
        raise NotImplementedError(self.__class__.__name__)

    @unclosed
    def corrector(self, fieldname: str) -> 'spelling.ReaderCorrector':
        """
        Returns a :class:`whoosh.spelling.Corrector` object that suggests
        corrections based on the terms in the given field.

        :param fieldname: the name of the field from which to provide
            corrections.
        """

        fieldobj = self.schema[fieldname]
        return spelling.ReaderCorrector(self, fieldname, fieldobj)

    @unclosed
    def terms_within(self, fieldname: str, text: str, maxdist: int,
                     prefix: int=0) -> Iterable[str]:
        """
        Returns a generator of words in the given field within ``maxdist``
        Damerau-Levenshtein edit distance of the given text.

        Important: the terms are returned in **no particular order**. The only
        criterion is that they are within ``maxdist`` edits of ``text``. You
        may want to run this method multiple times with increasing ``maxdist``
        values to ensure you get the closest matches first. You may also have
        additional information (such as term frequency or an acoustic matching
        algorithm) you can use to rank terms with the same edit distance.

        :param fieldname: the name of the field to get terms from.
        :param text: the text of the term.
        :param maxdist: the maximum edit distance.
        :param prefix: require suggestions to share a prefix of this length
            with the given word. This is often justifiable since most
            misspellings do not involve the first letter of the word.
            Using a prefix dramatically decreases the time it takes to generate
            the list of words.
        """

        # The default implementation is dumb. Subclasses should use smarter FSA
        # methods to do this.
        from_bytes = self.schema[fieldname].from_bytes
        for btext in self.expand_prefix(fieldname, text[:prefix]):
            word = from_bytes(btext)
            k = distance(word, text, limit=maxdist)
            if k <= maxdist:
                yield word

    # def most_frequent_terms(self, fieldname, number=5, prefix=''):
    #     """
    #     Returns the top 'number' most frequent terms in the given field as a
    #     list of (frequency, text) tuples.
    #     """
    #
    #     gen = ((terminfo.weight(), text) for text, terminfo
    #            in self.iter_prefix(fieldname, prefix))
    #     return nlargest(number, gen)
    #
    # def most_distinctive_terms(self, fieldname, number=5, prefix=''):
    #     """Returns the top 'number' terms with the highest `tf*idf` scores as
    #     a list of (score, text) tuples.
    #     """
    #
    #     N = float(self.doc_count())
    #     gen = ((terminfo.weight() * log(N / terminfo.doc_frequency()), text)
    #            for text, terminfo in self.iter_prefix(fieldname, prefix))
    #     return nlargest(number, gen)

    def leaf_readers(self) -> 'List[Tuple[IndexReader, int]]':
        """
        Returns a list of (IndexReader, docbase) pairs for the child readers
        of this reader if it is a composite reader. If this is not a composite
        reader, it returns `[(self, 0)]`.
        """

        return [(self, 0)]

    def has_column(self, fieldname: str) -> bool:
        """
        Returns True if the given field has a column in this reader.

        :param fieldname: the name of the field to check.
        """

        return False

    @abstractmethod
    def column_reader(self, fieldname: str, column: columns.Column=None,
                      reverse: bool=False, translate: bool=False
                      ) -> columns.ColumnReader:
        """
        Returns a :class:`~whoosh.columns.ColumnReader` object for the column
        data in the given field.

        :param fieldname: the name of the field for which to get a reader.
        :param column: if passed, use this Column object instead of the one
            associated with the field in the Schema.
        :param reverse: if passed, reverses the order of keys returned by the
            reader's ``sort_key()`` method. If the column type is not
            reversible, this will raise a ``NotImplementedError``.
        :param translate: if True, wrap the reader to call the field's
            ``from_bytes()`` method on the returned values.
        :return: a :class:`whoosh.columns.ColumnReader` object.
        """

        raise NotImplementedError


# Segment-based implementation

class SegmentReader(IndexReader):
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
        self.storage = storage

        # Give the codec a chance to give us a specialized storage object
        # (e.g. for compound segments)
        self.segment_storage = self._codec.segment_storage(storage, segment)
        # Open a read-only session
        self._session = self.segment_storage.open(segment.index_name(),
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
        return "%s(%r, %r)" % (self.__class__.__name__, self.segment_storage,
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
    def field_min_term(self, fieldname: str) -> bytes:
        return self._terms.field_min_term(fieldname)

    @unclosed
    def field_max_term(self, fieldname: str) -> bytes:
        return self._terms.field_max_term(fieldname)

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

    # Delegate doc methods to the per-doc reader

    @unclosed
    def stored_fields(self, docnum) -> Dict:
        assert docnum >= 0
        schema = self.schema
        sfs = self._perdoc.stored_fields(docnum)
        # Double-check with schema to filter out removed fields
        return dict(item for item in sfs.items() if item[0] in schema)

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
    def all_terms(self) -> Iterable[TermTuple]:
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
                  ) -> 'reading.TermInfo':
        termbytes = self._text_to_bytes(fieldname, termbytes)
        try:
            return self._terms.term_info(fieldname, termbytes)
        except KeyError:
            raise TermNotFound("%s:%r" % (fieldname, termbytes))

    @unclosed
    def __iter__(self) -> 'Iterable[Tuple[TermTuple, reading.TermInfo]]':
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
        except TermNotFound:
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
                scorer: whoosh.scoring.Scorer =None,
                include: 'Union[idsets.DocIdSet, Set]'=None,
                exclude: 'Union[idsets.DocIdSet, Set]'=None
                ) -> 'matchers.Matcher':
        termbytes = self._text_to_bytes(fieldname, termbytes)
        format_ = self.schema[fieldname].format
        matcher = self._terms.matcher(fieldname, termbytes, format_, scorer)
        return self._wrap_matcher(matcher, include, exclude)

    def _wrap_matcher(self, matcher: 'matchers.Matcher',
                      include: 'Union[idsets.DocIdSet, Set]',
                      exclude: 'Union[idsets.DocIdSet, Set]',
                      ) -> 'matchers.Matcher':
        from whoosh.matching.wrappers import FilterMatcher

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
            raise TermNotFound("No %r field" % fieldname)
        return self._perdoc.vector(docnum, fieldname)

    @field_checked
    def cursor(self, fieldname) -> 'codecs.TermCursor':
        fieldobj = self.schema[fieldname]
        return self._terms.cursor(fieldname, fieldobj)

    @field_checked
    def terms_within(self, fieldname: str, text: TermText, maxdist: int,
                     prefix: int=0) -> Iterable[str]:
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
        return bool(colobj and self._perdoc.has_column(fieldname))

    @field_checked
    def column_reader(self, fieldname: str, column: columns.Column=None,
                      reverse=False, translate=True) -> columns.ColumnReader:

        try:
            fieldobj = self.schema[fieldname]
        except KeyError:
            raise TermNotFound("No %r field" % fieldname)

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

class EmptyReader(IndexReader):
    def __init__(self, schema=None):
        self.schema = schema or fields.Schema()
        self.closed = False

    def __contains__(self, term: TermTuple) -> bool:
        return False

    def cursor(self, fieldname: str) -> 'codecs.TermCursor':
        return codecs.EmptyCursor()

    def indexed_field_names(self) -> Sequence[str]:
        return ()

    def field_min_term(self, fieldname: str) -> bytes:
        return b''

    def field_max_term(self, fieldname: str) -> bytes:
        return b''

    def all_terms(self) -> Iterable[TermTuple]:
        return iter(())

    def term_info(self, fieldname: str, termbytes: TermText
                  ) -> 'reading.TermInfo':
        raise TermNotFound((fieldname, termbytes))

    def __iter__(self) -> 'Iterable[Tuple[TermTuple, reading.TermInfo]]':
        return iter(())

    def iter_field(self, fieldname: str, prefix: TermText=b''
                   ) -> 'Iterable[Tuple[bytes, reading.TermInfo]]':
        return iter(())

    def iter_prefix(self, fieldname: str, prefix: TermText=b''
                    )-> 'Iterable[Tuple[bytes, reading.TermInfo]]':
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
                scorer: whoosh.scoring.Scorer =None,
                include: 'Union[idsets.DocIdSet, Set]'=None,
                exclude: 'Union[idsets.DocIdSet, Set]'=None
                ) -> 'matchers.Matcher':
        raise TermNotFound("%s:%r" % (fieldname, termbytes))

    def has_vector(self, docnum: int, fieldname: str) -> bool:
        return False

    def vector(self, docnum: int, fieldname: str) -> 'postings.VectorReader':
        raise KeyError("No document number %s" % docnum)


# Multisegment reader class

class MultiReader(IndexReader):
    """
    Do not instantiate this object directly. Instead use Index.reader().
    """

    def __init__(self, readers: 'Sequence[reading.IndexReader]',
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

    def leaf_readers(self) -> 'List[Tuple[reading.IndexReader, int]]':
        return list(zip(self.readers, self.doc_offsets))

    def add_reader(self, reader: 'reading.IndexReader'):
        self.reading.append(reader)
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

    def indexed_field_names(self) -> Sequence[str]:
        names = set()
        for r in self.readers:
            names.update(r.indexed_field_names())
        return sorted(names)

    def field_min_term(self, fieldname: str) -> bytes:
        return min(r.field_min_term(fieldname) for r in self.readers)

    def field_max_term(self, fieldname: str) -> bytes:
        return max(r.field_max_term(fieldname) for r in self.readers)

    def all_terms(self) -> Iterable[TermTuple]:
        return _merge_iters([r.all_terms() for r in self.readers])

    def all_terms_with_reader(self) -> TermTupleWithOptReader:
        return _merge_iters2(list(self.readers))

    def term_range(self, fieldname: str, start: TermText,
                   end: Optional[TermText]) -> Iterable[bytes]:
        return _merge_iters([r.term_range(fieldname, start, end)
                             for r in self.readers])

    def term_info(self, fieldname: str, termbytes: TermText):
        termbytes = self._text_to_bytes(fieldname, termbytes)
        term = fieldname, termbytes

        # Get the term infos for the sub-readers containing the term
        tis = [(r.term_info(fieldname, termbytes), offset)
               for r, offset in list(zip(self.readers, self.doc_offsets))
               if term in r]

        if not tis:
            raise TermNotFound(term)

        return TermInfo.combine(tis)

    def weight(self, fieldname: str, termbytes: TermText) -> float:
        termbytes = self._text_to_bytes(fieldname, termbytes)
        return sum(r.weight(fieldname, termbytes) for r in self.readers)

    def doc_frequency(self, fieldname: str, termbytes) -> int:
        termbytes = self._text_to_bytes(fieldname, termbytes)
        return sum(r.doc_frequency(fieldname, termbytes) for r in self.readers)

    def matcher(self, fieldname: str, termbytes: TermText,
                scorer: whoosh.scoring.Scorer =None,
                include: 'Union[idsets.DocIdSet, Set]'=None,
                exclude: 'Union[idsets.DocIdSet, Set]'=None
                ) -> 'matchers.Matcher':
        from whoosh.matching.wrappers import DocOffsetMatcher, MultiMatcher

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
            except TermNotFound:
                pass
            else:
                ms.append(m)
                m_offsets.append(start)

        if not ms:
            raise TermNotFound(fieldname, termbytes)

        # Even if there's only one matcher, we still wrap it with a MultiMatcher
        # so it adds the correct offset, UNLESS the offset is 0
        if len(ms) == 1 and m_offsets[0] == 0:
            return ms[0]
        elif len(ms) == 1:
            return DocOffsetMatcher(ms[0], m_offsets[0])
        else:
            return MultiMatcher(ms, m_offsets, scorer)

    def cursor(self, fieldname: str) -> 'codecs.TermCursor':
        return codecs.MultiCursor([r.cursor(fieldname) for r in self.readers])

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


def _merge_iters(iterlist: List[Iterable[Any]]) -> Iterable[Any]:
    # Merge-sorts terms coming from a list of term iterators

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

    # Do a streaming heap sort of the terms from the iterators
    heapify(current)
    while active:
        # If only one iterator is active, just yield from it and return
        if active == 1:
            term, itid = current[0]
            it = itermap[itid]
            yield term
            for term in it:
                yield term
            return

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


def _merge_iters2(readers: List[IndexReader]
                  ) -> Iterable[Tuple[TermTuple, Optional[IndexReader]]]:
    # Merge-sorts terms coming from a list of term iterators
    iterlist = [r.all_terms() for r in readers]

    # Create a map so we can look up each iterator by its id() value
    iterlist = []
    itermap = {}
    readermap = {}
    for reader in readers:
        it = reader.all_terms()
        iterlist.append(it)
        itermap[id(it)] = it
        readermap[id(it)] = reader

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

    # Do a streaming heap sort of the terms from the iterators
    heapify(current)
    while active:
        # If only one iterator is active, just yield from it and return
        if active == 1:
            term, itid = current[0]
            it = itermap[itid]
            reader = readermap[itid]
            yield term, reader
            for term in it:
                yield term, reader
            return

        # Peek at the first term in the sorted list
        term = current[0][0]
        reader = readermap[current[0][1]]
        count = 0

        # Re-iterate on all items in the list that have that term
        while active and current[0][0] == term:
            count += 1
            it = itermap[current[0][1]]
            try:
                nextterm = next(it)
                heapreplace(current, (nextterm, id(it)))
            except StopIteration:
                heappop(current)
                active -= 1

        # Yield the term, and the reader if only one reader had the term
        yield term, reader if count == 1 else None
