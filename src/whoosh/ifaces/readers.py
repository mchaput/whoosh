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

from abc import abstractmethod
from typing import (
    Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union,
)

from whoosh import columns, fields, idsets, spelling
from whoosh.ifaces import codecs, matchers, weights
from whoosh.compat import text_type
from whoosh.postings import postings, ptuples
from whoosh.support.levenshtein import distance
from whoosh.util import unclosed


__all__ = ("TermNotFound", "NoVectorError", "TermTuple", "TermText", "TermInfo",
           "IndexReader")


# Exceptions

class TermNotFound(Exception):
    pass


class NoVectorError(Exception):
    pass


# Typing aliases

TermTuple = Tuple[str, bytes]
TermText = Union[text_type, bytes]


# Term Info base class

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


# Reader base class

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
    def term_info(self, fieldname: str, text: TermText) -> TermInfo:
        """
        Returns a :class:`TermInfo` object allowing access to various
        statistics about the given term.

        :param fieldname: the name of the field containing the term.
        :param text: the term bytestring.
        """

        raise NotImplementedError

    @abstractmethod
    def all_terms(self) -> Iterable[Tuple[str, bytes]]:
        """
        Yields (fieldname, text) tuples for every term in the index.
        """

        raise NotImplementedError

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
                scorer: weights.Scorer=None,
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

        raise NotImplementedError(self.__class__)

    @abstractmethod
    def has_vector(self, docnum: int, fieldname: str) -> bool:
        """
        Returns True if the index contains vector information for the given
        field in the given document.

        :param docnum: the number of the document to check.
        :param fieldname: the name of the field to check.
        """

        raise NotImplementedError(self.__class__)

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
        raise NotImplementedError(self.__class__)

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
    def terms_within(self, fieldname: str, text: text_type, maxdist: int,
                     prefix: int=0) -> Iterable[text_type]:
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
        """Returns a list of (IndexReader, docbase) pairs for the child readers
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
