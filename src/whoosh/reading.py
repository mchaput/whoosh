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

import struct
from abc import ABCMeta, abstractmethod
from math import log
from heapq import nlargest

from whoosh.compat import bytes_type
from whoosh.support.levenshtein import distance
from whoosh.system import emptybytes
from whoosh.util import unclosed


# Exceptions

class TermNotFound(Exception):
    pass


class NoStoredFields(Exception):
    pass


class NoTermVector(Exception):
    pass


# Useful functions

def most_frequent_terms(reader, fieldname, number=5, prefix=''):
    """
    Returns the top 'number' most frequent terms in the given field as a
    list of (frequency, text) tuples.
    """

    gen = ((terminfo.weight(), text) for text, terminfo
           in reader.iter_prefix(fieldname, prefix))
    return nlargest(number, gen)


def most_distinctive_terms(reader, fieldname, number=5, prefix=''):
    """
    Returns the top 'number' terms with the highest `tf*idf` scores as
    a list of (score, text) tuples.
    """

    N = float(reader.doc_count())
    gen = ((terminfo.weight() * log(N / terminfo.doc_frequency()), text)
           for text, terminfo in reader.iter_prefix(fieldname, prefix))
    return nlargest(number, gen)


# Term Info base class

class TermInfo(object):
    """
    Represents a set of statistics about a term. This object is returned by
    :meth:`IndexReader.term_info`. These statistics may be useful for
    optimizations and scoring algorithms.
    """

    __slots__ = ("_weight", "_df", "_minlength", "_maxlength", "_maxweight",
                 "_minid", "_maxid")

    def __init__(self, weight=0, df=0, minlength=None,
                 maxlength=0, maxweight=0, minid=None, maxid=0):
        self._weight = weight
        self._df = df
        self._minlength = minlength
        self._maxlength = maxlength
        self._maxweight = maxweight
        self._minid = minid
        self._maxid = maxid

    def update(self, docid, length, weight):
        if self._minid is None:
            self._minid = docid
        self._maxid = docid
        self._weight += weight
        self._df += 1
        self._maxweight = max(self._maxweight, weight)

        if length:
            if self._minlength is None:
                self._minlength = length
            else:
                self._minlength = min(self._minlength, length)
            self._maxlength = max(self._maxlength, length)

    def update_from_list(self, posts):
        if self._minid is None:
            self._minid = posts[0].id
        self._maxid = posts[-1].id
        self._df += len(posts)

        # Use "or 0" because .weight/.length might be None
        weights = [p.weight or 0 for p in posts]
        if any(weights):
            self._weight += sum(weights)
            self._maxweight = max(self._maxweight, max(weights))

        lengths = [p.length for p in posts if p.length]
        if any(lengths):
            minlen = min(lengths)
            if self._minlength is None or minlen < self._minlength:
                self._minlength = minlen
            maxlen = max(lengths)
            if maxlen > self._maxlength:
                self._maxlength = maxlen

    def subtract(self, df, weight):
        self._df -= df
        self._weight -= weight

    def weight(self):
        """
        Returns the total frequency of the term across all documents.
        """

        return self._weight

    def doc_frequency(self):
        """
        Returns the number of documents the term appears in.
        """

        return self._df

    def min_length(self):
        """
        Returns the length of the shortest field value the term appears
        in.
        """

        return self._minlength

    def max_length(self):
        """
        Returns the length of the longest field value the term appears
        in.
        """

        return self._maxlength

    def max_weight(self):
        """
        Returns the number of times the term appears in the document in
        which it appears the most.
        """

        return self._maxweight

    def min_id(self):
        """
        Returns the lowest document ID this term appears in.
        """

        return self._minid

    def max_id(self):
        """
        Returns the highest document ID this term appears in.
        """

        return self._maxid


# Reader base class

class IndexReader(object):
    """
    Do not instantiate this object directly. Instead use Index.reader().
    """

    def __init__(self, codec, schema):
        self.codec = codec
        self.schema = schema
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    @abstractmethod
    def __contains__(self, term):
        """
        Returns True if the given term tuple (fieldname, text) is
        in this reader.
        """

        raise NotImplementedError

    def __iter__(self):
        """
        Yields ((fieldname, text), terminfo) tuples for each term in the
        reader, in lexical order.
        """

        term_info = self.term_info
        for term in self.all_terms():
            yield (term, term_info(*term))

    def codec(self):
        """
        Returns the :class:`whoosh.codec.base.Codec` object used to read
        this reader's segment.
        """

        return self.codec

    @unclosed
    def close(self):
        """
        Closes the open files associated with this reader.
        """

        pass

    @unclosed
    def generation(self):
        """
        Returns the generation of the index being read, or -1 if the backend
        is not versioned.
        """

        return -1

    @abstractmethod
    def indexed_field_names(self):
        """
        Returns an iterable of strings representing the names of the indexed
        fields. This may include additional names not explicitly listed in the
        Schema if you use "glob" fields.
        """

        raise NotImplementedError

    @abstractmethod
    def all_terms(self):
        """
        Yields (fieldname, termbytes) tuples for every term in the index.
        """

        raise NotImplementedError

    @unclosed
    def terms_from(self, fieldname, prefix):
        """
        Yields termbytes for every term in the given field, starting at the
        given prefix.
        """

        # The default implementation just scans the whole list of terms
        for fname, termbytes in self.all_terms():
            if fname < fieldname or termbytes < prefix:
                continue
            if fname > fieldname:
                break
            yield termbytes

    @abstractmethod
    def term_info(self, fieldname, text):
        """
        Returns a :class:`TermInfo` object allowing access to various
        statistics about the given term.
        """

        raise NotImplementedError

    @unclosed
    def expand_prefix(self, fieldname, prefix):
        """
        Yields terms in the given field that start with the given prefix.
        """

        if not isinstance(prefix, bytes_type):
            prefix = self.schema[fieldname].to_bytes(prefix)
        for termbytes in self.terms_from(fieldname, prefix):
            if not termbytes.startswith(prefix):
                return
            yield termbytes

    @unclosed
    def lexicon(self, fieldname):
        """
        Yields all term bytestrings in the given field.
        """

        return self.terms_from(fieldname, emptybytes)

    @unclosed
    def iter_from(self, fieldname, text):
        """
        Yields ((fieldname, text), terminfo) tuples for all terms in the
        reader, starting at the given term.
        """

        term_info = self.term_info
        if not isinstance(text, bytes_type):
            text = self.schema[fieldname].to_bytes(text)
        for term in self.terms_from(fieldname, text):
            yield (term, term_info(*term))

    @unclosed
    def iter_field(self, fieldname, prefix=''):
        """
        Yields (text, terminfo) tuples for all terms in the given field.
        """

        for (fn, text), terminfo in self.iter_from(fieldname, prefix):
            if fn != fieldname:
                return
            yield text, terminfo

    @unclosed
    def iter_prefix(self, fieldname, prefix):
        """
        Yields (text, terminfo) tuples for all terms in the given field with
        a certain prefix.
        """

        for text, terminfo in self.iter_field(fieldname, prefix):
            if not text.startswith(prefix):
                return
            yield (text, terminfo)

    @abstractmethod
    def all_doc_ids(self):
        """
        Returns an iterator of all (undeleted) document IDs in the reader.
        """

        raise NotImplementedError

    @unclosed
    def all_stored_fields(self):
        """
        Yields a series of ``(docid, stored_fields_dict)``
        tuples for the undeleted documents in the reader.
        """

        for docid in self.all_doc_ids():
            yield docid, self.stored_fields(docid)

    @abstractmethod
    def is_deleted(self, docid):
        """
        Returns True if the given document number is marked deleted.
        """

        raise NotImplementedError

    @abstractmethod
    def stored_fields(self, docid):
        """
        Returns the stored fields for the given document number.

        :param numerickeys: use field numbers as the dictionary keys instead of
            field names.
        """

        raise NotImplementedError

    @abstractmethod
    def doc_count(self):
        """
        Returns the total number of UNDELETED documents in this reader.
        """

        raise NotImplementedError

    @abstractmethod
    def doc_id_range(self):
        """
        Returns a tuple of the minimum and maximum document IDs in the index.
        """

        raise NotImplementedError

    def frequency(self, fieldname, text):
        """
        Returns the total number of instances of the given term in the
        collection.
        """

        if not isinstance(text, bytes_type):
            text = self.schema[fieldname].to_bytes(text)
        return self.term_info(fieldname, text).weight()

    def doc_frequency(self, fieldname, text):
        """
        Returns how many documents the given term appears in.
        """

        if not isinstance(text, bytes_type):
            text = self.schema[fieldname].to_bytes(text)
        return self.term_info(fieldname, text).doc_freq()

    @abstractmethod
    def field_length(self, fieldname):
        """
        Returns the total number of terms in the given field. This is used
        by some scoring algorithms.
        """
        raise NotImplementedError

    @abstractmethod
    def min_field_length(self, fieldname):
        """
        Returns the minimum length of the field across all documents. This
        is used by some scoring algorithms.
        """
        raise NotImplementedError

    @abstractmethod
    def max_field_length(self, fieldname):
        """
        Returns the minimum length of the field across all documents. This
        is used by some scoring algorithms.
        """
        raise NotImplementedError

    def unique_id(self, fieldname, text):
        """
        Returns the document ID of the document with ``text`` in the unique
        field ``fieldname``.

        Raises an exception if ``fieldname`` does not name a field with
        ``unique=True``.

        The default implementation simply creates a matcher
        """

        m = self.matcher(fieldname, text)
        if m.is_active():
            return m.id()

    @abstractmethod
    def matcher(self, fieldname, text, scorer=None):
        """
        Returns a :class:`~whoosh.matching.Matcher` for the postings of the
        given term.

        >>> pr = reader.postings("content", "render")
        >>> pr.skip_to(10)
        >>> pr.id
        12

        :param fieldname: the field name or field number of the term.
        :param termbytes: the bytestring of the term.
        :rtype: :class:`whoosh.matching.Matcher`
        """

        raise NotImplementedError

    @unclosed
    def corrector(self, fieldname):
        """
        Returns a :class:`whoosh.spelling.Corrector` object that suggests
        corrections based on the terms in the given field.
        """

        from whoosh.spelling import ReaderCorrector

        return ReaderCorrector(self, fieldname, self.schema[fieldname])

    @unclosed
    def terms_within(self, fieldname, text, maxdist, prefix=0):
        """
        Returns a generator of words in the given field within ``maxdist``
        edit distance of the given text.

        Important: the terms are returned in **no particular order**. The only
        criterion is that they are within ``maxdist`` edits of ``text``. You
        may want to run this method multiple times with increasing ``maxdist``
        values to ensure you get the closest matches first. You may also have
        additional information (such as term frequency or an acoustic matching
        algorithm) you can use to rank terms with the same edit distance.

        :param maxdist: the maximum edit distance.
        :param prefix: require suggestions to share a prefix of this length
            with the given word. This is often justifiable since most
            misspellings do not involve the first letter of the word.
            Using a prefix dramatically decreases the time it takes to generate
            the list of words.
        """

        fieldobj = self.schema[fieldname]
        for btext in self.expand_prefix(fieldname, text[:prefix]):
            word = fieldobj.from_bytes(btext)
            k = distance(word, text, limit=maxdist)
            if k <= maxdist:
                yield word

    def has_column(self, fieldname):
        return False

    @abstractmethod
    def column_reader(self, fieldname, column=None, reverse=False,
                      translate=False):
        """

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

    def term_vector(self, docid, fieldname):
        """
        Returns a :class:`whoosh.formats.BlockReader` object containing the term
        vector for the field in the given document.

        If the codec does not support term vectors, this method will raise
        ``NotImplementedError``.

        :param docid: the ID of the document to get the term vector for.
        :param fieldname: the name of the field to get the terms of.
        :rtype: :class:`whoosh.formats.BlockReader`
        """

        raise NotImplementedError


# DB and Codec based implementation

class DBReader(IndexReader):
    def __init__(self, txn, codec, schema):
        self._txn = txn
        self._codec = codec
        self._info = self._codec.info(self._txn)
        self.schema = schema

        self._docs = self._codec.doc_reader(self._txn)
        self._terms = self._codec.term_reader(self._txn)
        self._colcache = {}
        self.closed = False

    @unclosed
    def __contains__(self, term):
        fieldname, text = term
        if not isinstance(text, bytes_type):
            text = self.schema[fieldname].to_bytes(text)
        return (fieldname, text) in self._terms

    @unclosed
    def __iter__(self):
        return self._terms.items()

    @unclosed
    def close(self):
        self._docs.close()
        self._terms.close()
        self.closed = True

    def is_atomic(self):
        return True

    def codec(self):
        return self._codec

    @unclosed
    def generation(self):
        return self._info.generation

    @unclosed
    def indexed_field_names(self):
        return self._terms.indexed_field_names()

    @unclosed
    def all_terms(self):
        return self._terms.terms()

    @unclosed
    def terms_from(self, fieldname, prefix):
        return self._terms.terms_from(fieldname, prefix)

    @unclosed
    def term_info(self, fieldname, text):
        if not isinstance(text, bytes_type):
            text = self.schema[fieldname].to_bytes(text)
        return self._terms.term_info(fieldname, text)

    # lexicon
    # field_terms

    def iter_from(self, fieldname, text):
        if not isinstance(text, bytes_type):
            text = self.schema[fieldname].to_bytes(text)
        return self._terms.items_from(fieldname, text)

    # iter_field
    # iter_prefix

    @unclosed
    def all_doc_ids(self):
        return self._docs.all_doc_ids()

    @unclosed
    def all_stored_fields(self):
        return self._docs.all_stored_fields()

    @unclosed
    def is_deleted(self, docid):
        return self._docs.is_deleted(docid)

    @unclosed
    def stored_fields(self, docid):
        return self._docs.stored_fields(docid)

    @unclosed
    def doc_count(self):
        return self._docs.doc_count()

    @unclosed
    def doc_id_range(self):
        return self._docs.doc_id_range()

    @unclosed
    def frequency(self, fieldname, text):
        if not isinstance(text, bytes_type):
            text = self.schema[fieldname].to_bytes(text)
        try:
            return self._terms.frequency(fieldname, text)
        except TermNotFound:
            return 0

    @unclosed
    def doc_frequency(self, fieldname, text):
        if not isinstance(text, bytes_type):
            text = self.schema[fieldname].to_bytes(text)
        try:
            return self._terms.doc_frequency(fieldname, text)
        except TermNotFound:
            return 0

    # def doc_field_length(self, docid, fieldname):
    #     return self._docs.doc_field_length(docid, fieldname)

    @unclosed
    def field_length(self, fieldname):
        return self._docs.field_length(fieldname)

    @unclosed
    def min_field_length(self, fieldname):
        return self._docs.min_field_length(fieldname)

    @unclosed
    def max_field_length(self, fieldname):
        return self._docs.max_field_length(fieldname)

    def unique_id(self, fieldname, text):
        fieldobj = self.schema[fieldname]
        if not fieldobj.unique:
            raise Exception("Field %r is not unique" % fieldname)
        if not isinstance(text, bytes_type):
            text = fieldobj.to_bytes(text)
        return self._terms.unique_id(fieldname, fieldobj, text)

    @unclosed
    def matcher(self, fieldname, text, scorer=None):
        from whoosh.matching.wrappers import PredicateMatcher

        if fieldname not in self.schema:
            raise TermNotFound("No  field %r" % fieldname)
        if not isinstance(text, bytes_type):
            text = self.schema[fieldname].to_bytes(text)
        fieldobj = self.schema[fieldname]
        matcher = self._terms.matcher(fieldname, fieldobj, text, scorer=scorer)

        pred = self._docs.is_deleted
        matcher = PredicateMatcher(matcher, pred, exclude=True)
        return matcher

    # corrector
    # terms_within

    # TODO: has_column, column_reader

    def terms_within(self, fieldname, text, maxdist, prefix=0):
        fieldobj = self.schema[fieldname]
        auto = self._codec.automata(self._txn, fieldname, fieldobj)
        return auto.terms_within(text, maxdist, prefix)

    # Column methods

    @unclosed
    def has_column(self, fieldname):
        fieldobj = self.schema[fieldname]
        if fieldobj.column_type:
            if fieldname in self._colcache:
                return self._colcache[fieldname].exists()
            else:
                fieldobj = self.schema[fieldname]
                cr = self._codec.column_reader(self._txn, fieldname, fieldobj)
                return cr.exists()
        return False

    @unclosed
    def column_reader(self, fieldname, column=None, reverse=False,
                      translate=True):
        fieldobj = self.schema[fieldname]
        column = column or fieldobj.column_type
        if not column:
            raise Exception("No column for field %r" % fieldname)

        if fieldname in self._colcache:
            cr = self._colcache[fieldname]
        else:
            cr = self._codec.column_reader(self._txn, fieldname, fieldobj)
            if cr.exists():
                self._colcache[fieldname] = cr
            else:
                from whoosh.codec.codec import EmptyColumnReader
                # The database doesn't have a column for this field, so create
                # a fake column reader that always returns the default value
                default = fieldobj.column_type.default_value()
                cr = EmptyColumnReader(default)

        if translate:
            from whoosh.codec.codec import TranslatingColumnReader
            # Wrap the column in a Translator to give the caller
            # nice values instead of sortable representations
            fcv = fieldobj.from_column_value
            cr = TranslatingColumnReader(cr, fcv)

        return cr

    @unclosed
    def term_vector(self, docid, fieldname):
        fieldobj = self.schema[fieldname]
        return self._docs.term_vector(docid, fieldname, fieldobj)


# Fake IndexReader class for empty indexes

# class EmptyReader(IndexReader):
#     def __init__(self, schema):
#         self.schema = schema
#
#     def __contains__(self, term):
#         return False
#
#     def __iter__(self):
#         return iter([])
#
#     def indexed_field_names(self):
#         return []
#
#     def all_terms(self):
#         return iter([])
#
#     def term_info(self, fieldname, text):
#         raise TermNotFound((fieldname, text))
#
#     def iter_from(self, fieldname, text):
#         return iter([])
#
#     def iter_field(self, fieldname, prefix=''):
#         return iter([])
#
#     def iter_prefix(self, fieldname, prefix=''):
#         return iter([])
#
#     def lexicon(self, fieldname):
#         return iter([])
#
#     def is_deleted(self, docid):
#         return False
#
#     def stored_fields(self, docid):
#         raise KeyError("No document number %s" % docid)
#
#     def all_stored_fields(self):
#         return iter([])
#
#     def doc_count_all(self):
#         return 0
#
#     def doc_count(self):
#         return 0
#
#     def frequency(self, fieldname, text):
#         return 0
#
#     def doc_frequency(self, fieldname, text):
#         return 0
#
#     def field_length(self, fieldname):
#         return 0
#
#     def min_field_length(self, fieldname):
#         return 0
#
#     def max_field_length(self, fieldname):
#         return 0
#
#     def doc_field_length(self, docid, fieldname, default=0):
#         return default
#
#     def postings(self, fieldname, termbytes, scorer=None):
#         raise TermNotFound("%s:%r" % (fieldname, termbytes))
