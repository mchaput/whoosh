# Copyright 2014 Matt Chaput. All rights reserved.
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

from abc import ABCMeta, abstractmethod
from whoosh.automata import lev


class Codec(object):
    __metaclass__ = ABCMeta

    """
    Object that translates high-level commands (such as "add this term to the
    index") into low-level operations on the database interface. This allows
    customizing how the index is written and read.
    """

    @abstractmethod
    def write_info(self, txn, ixinfo):
        """
        Save the given :class:`whoosh.index.IndexInfo` object into the database
        for later retrieval by the ``read_info`` method.

        :param txn: a :class:`whoosh.db.DBWriter` object.
        :param ixinfo: an :class:`whoosh.index.IndexInfo` object.
        """

        raise NotImplementedError

    @abstractmethod
    def info(self, txn):
        """
        Retrieve a saved :class:`whoosh.index.IndexInfo` object from the
        database.

        :param txn: :param txn: a :class:`whoosh.db.DBReader` object.
        """

        raise NotImplementedError

    @abstractmethod
    def doc_writer(self, txn):
        """
        Returns a:class:`DocWriter` object for writing document information
        to the database.

        :param txn: a :class:`whoosh.db.DBWriter` object.
        :rtype: :class:`DocWriter`
        """

        raise NotImplementedError


    @abstractmethod
    def doc_reader(self, txn):
        """
        Returns a :class:`DocReader` object for reading per-document
        information from the database.

        :param txn: a :class:`whoosh.db.DBReader` object.
        :rtype: :class:`DocReader`
        """

        raise NotImplementedError

    @abstractmethod
    def tag_writer(self, txn):
        """
        Returns a :class:`TagWriter` object for writing document tag information
        to the database.

        :param txn: a :class:`whoosh.db.DBWriter` object.
        :rtype: class:`TagWriter`
        """

        raise NotImplementedError

    @abstractmethod
    def tag_reader(self, txn):
        """
        Returns a :class:`TagReader` object for reading document tag
        information from the database.

        :param txn: a :class:`whoosh.db.DBReader` object.
        :rtype: class:`TagReader`
        """

        raise NotImplementedError

    @abstractmethod
    def column_writer(self, txn):
        """
        Returns a :class:`ColumnWriter` object for writing document tag
        information to the database.

        :param txn: a :class:`whoosh.db.DBWriter` object.
        :rtype: class:`ColumnWriter`
        """

        raise NotImplementedError

    @abstractmethod
    def column_reader(self, txn, fieldname, fieldobj):
        """


        :param txn: a :class:`whoosh.db.DBWriter` object.
        :param fieldname: the name of the field to read the column for.
        :param fieldobj: the :class:`whoosh.fields.Field` object corresponding
            to the field in the schema.
        :rtype: class:`ColumnReader`
        """

        raise NotImplementedError

    @abstractmethod
    def term_reader(self, txn):
        """
        Returns a :class:`TermReader` object for reading per-term information
        from the database.

        :param txn: a :class:`whoosh.db.DBReader` object.
        :rtype: :class:`TermReader`
        """

        raise NotImplementedError

    @abstractmethod
    def automata(self, txn, fieldname, fieldobj):
        """
        Returns an object implementing the :class:`Automata` interface for
        running finite automata on the terms in the database.

        :param txn: a :class:`whoosh.db.DBReader` object.
        :param fieldname: the name of the field to read terms from.
        :param fieldobj: the :class:`whoosh.fields.Field` object for the field.
        :rtype: :class:`Automata`
        """

        raise NotImplementedError


class WrappingCodec(Codec):
    """
    Base class for codecs that forward some of their functions to a wrapped
    child object.
    """

    def __init__(self, child):
        """
        :param child: an instance of :class:`Codec`.
        """

        self._child = child

    def doc_writer(self, txn):
        return self._child.doc_writer(txn)

    def doc_reader(self, txn):
        return self._child.doc_reader()

    def tag_writer(self, txn):
        return self._child.tag_writer(txn)

    def tag_reader(self, txn):
        return self._child.tag_reader(txn)

    def column_writer(self, txn):
        return self._child.column_writer(txn)

    def column_reader(self, txn, fieldname, fieldobj):
        return self._child.column_reader(txn, fieldname, fieldobj)

    def term_reader(self, txn):
        return self._child.terms_reader(txn)

    def postings_reader(self, txn, fieldname, bytestring):
        return self._child.postings_reader(txn, fieldname, bytestring)

    def automata(self, txn, fieldname, fieldobj):
        return self._child.automata(txn, fieldname, fieldobj)


class DocWriter(object):
    __metaclass__ = ABCMeta

    """
    Interface for writing per-document information to the database.
    """

    def __init__(self, txn):
        self._txn = txn

    @abstractmethod
    def next_doc_id(self):
        """
        Returns a fresh, unused document ID integer.
        """

        raise NotImplementedError

    @abstractmethod
    def start_doc(self, docid):
        """
        Prepare to write information for the given ID to the database.

        :param docid: an integer representing the document.
        """

        raise NotImplementedError

    @abstractmethod
    def add_field(self, fieldname, fieldobj, value, length, update):
        """
        Set information about the given field for the current document.

        :param fieldname: the name of the field.
        :param fieldobj: the :class:`whoosh.fields.Field` object corresponding
            to the named field.
        :param value: the stored value for the field, or `None`.
        :param length: the length of the field.
        :param update: whether to update the index by deleting old documents
            based on unique fields.
        """

        raise NotImplementedError

    @abstractmethod
    def remove_field_terms(self, fieldname):
        """
        Removes all terms in the given field.

        :param fieldname:
        """

        raise NotImplementedError

    @abstractmethod
    def add_field_postings(self, fieldname, fieldobj, fieldlen, posts):
        """
        Adds an iterator of :class:`whoosh.formats.Posting` objects, as produced
        by the `Format.index()` and `Field.index()` methods, as new postings
        from the given field in the current document.

        :param fieldname: the name of the field to add the postings to.
        :param fieldobj: the :class:`whoosh.fields.Field` object for the field.
        :param fieldlen: the length of the field, if known (otherwise ``None``).
        :param posts: an iterator of :class:`whoosh.formats.Posting` objects.
        """

        raise NotImplementedError

    def store_vector(self, fieldname, fieldobj, posts):
        """
        Stores the postings as a term vector for the current document.

        If the codec does not support term vectors, this method raises
        ``NotImplementedError``.

        :param fieldname: the name of the field the terms are from.
        :param fieldobj: the :class:`whoosh.fields.Field` object for the field.
        :param posts: an iterator of :class:`whoosh.formats.Posting` objects.
        """

        raise NotImplementedError

    def finish_doc(self):
        """
        Finish writing information about the current ID to the database.
        """

        pass

    @abstractmethod
    def add_matcher(self, fieldname, fieldobj, termbytes, matcher,
                    mapping=None):
        """
        Adds the posting information from a matcher to the postings for the
        given term. This need not be called inside ``start_doc()`` or
        ``start_field()``.

        :param fieldname: the name of the term's field.
        :param fieldobj: the field's object from the schema.
        :param termbytes: the bytestring representation of the term.
        :param matcher: the matcher object to get the postings from.
        :param mapping: an optional dictionary mapping old document IDs to new
            document IDs. If you pass this dictionary, the IDs in the matcher
            will be remapped as the postings are added.
        """

        raise NotImplementedError

    @abstractmethod
    def add_tag(self, docid, tagname):
        """
        Set the tag value for the given field in the given document.

        :param docid: the identifier of the document in which to set the tag.
        :param tagname: the tag to add.
        """

        raise NotImplementedError

    @abstractmethod
    def remove_tag(self, docid, tagname):
        """
        Removes the tag value for the given field in the given document.

        :param docid: the identifier of the document in which to set the tag.
        :param tagname: the tag to remove.
        """

        raise NotImplementedError

    @abstractmethod
    def delete(self, docid):
        """
        Mark the given document as deleted.
        """

        raise NotImplementedError

    def optimize(self):
        self._txn.optimize()

    def tidy(self, schema):
        """
        Attempts to remove dead documents/terms/postings from the database.
        """

        pass

    def clean(self, schema):
        """
        Attempts to remove dead documents/terms/postings from the database.
        """

        pass

    def close(self):
        """
        Finish any work and close the writer.
        """

        pass


class DocReader(object):
    __metaclass__ = ABCMeta

    def __init__(self, txn):
        self._txn = txn
        self.closed = False

    @abstractmethod
    def doc_count(self):
        """
        Returns the number of undeleted documents.
        """

        raise NotImplementedError

    @abstractmethod
    def doc_id_range(self):
        """
        Returns a tuple of the minimum and maximum document IDs.
        """

        raise NotImplementedError

    @abstractmethod
    def is_deleted(self, docid):
        """
        Returns True if the given document is marked deleted.
        """

        raise NotImplementedError

    @abstractmethod
    def all_doc_ids(self):
        """
        Returns an iterator of docids of all undeleted documents in the index.
        """

        raise NotImplementedError

    @abstractmethod
    def field_length(self, fieldname):
        """
        Returns the total length of the given field across all documents.
        """

        raise NotImplementedError

    @abstractmethod
    def min_field_length(self, fieldname):
        """
        Returns the shortest length of the given field out of all documents.
        """

        raise NotImplementedError

    @abstractmethod
    def max_field_length(self, fieldname):
        """
        Returns the maximum length of the given field out of all documents.
        """

        raise NotImplementedError

    @abstractmethod
    def stored_fields(self, docid):
        """
        Returns a dict-like object mapping field names to the stored value of
        the field for the given document.

        :param docid: the identifier of the document.
        """

        raise NotImplementedError

    def all_stored_fields(self):
        """
        Returns an iterator of ``(docid, stored_fields_dict)`` pairs for all
        documents in the index.
        """

        for docid in self.all_doc_ids():
            yield docid, self.stored_fields(docid)

    def has_term_vector(self, docid, fieldname, fieldobj):
        """
        Returns True if the given document has a term vector for the given
        field.
        """

        return False

    def term_vector(self, docid, fieldname, fieldobj):
        """
        Returns a reader for the term vector of the given field in the given
        document.

        If the codec does not support term vectors, this method will raise
        ``NotImplementedError``.

        :param docid: the identifier of the document.
        :param fieldname: the name of the field from which to get the terms.
        :param fieldobj: the :class:`whoosh.fields.Field` object for the field.
        :rtype: :class:`whoosh.formats.BlockReader`
        """

        raise NotImplementedError

    def close(self):
        """
        Closes any resources used by this reader.
        """

        self.closed = True


class TermReader(object):
    def __init__(self, txn):
        self._txn = txn
        self.closed = False

    @abstractmethod
    def __contains__(self, term):
        """
        Returns True if the given term exists in the index.
        :param term: A tuple of `(fieldname, bytestring)` representing the term.
        """

        raise NotImplementedError

    @abstractmethod
    def terms(self):
        """
        Returns an iterator of `(fieldname, bytestring)` tuples representing
        the terms in the index.
        """

        raise NotImplementedError

    @abstractmethod
    def terms_from(self, fieldname, prefix):
        """
        Returns an iterator of bytestrings representing the terms in the given
        field, starting at the term equal to or greater than `prefix`.

        :param fieldname: the name of the field to list terms in.
        :param prefix: the term or term prefix to start at.
        """

        raise NotImplementedError

    @abstractmethod
    def items(self):
        """
        Returns an iterator of `((fieldname, bytestring), terminfo)` tuples,
        where the first item is a tuple representing the term, and second item
        is a :class:`whoosh.reading.TermInfo` object containing information
        about the term.
        """

        raise NotImplementedError

    @abstractmethod
    def items_from(self, fieldname, prefix):
        """
        Returns an iterator of `((fieldname, bytestring), terminfo)` tuples,
        where the first item is a tuple representing the term, and second item
        is a :class:`whoosh.reading.TermInfo` object containing information
        about the term. The iterator starts at the term equal to or greater than
        `prefix`.
        """

        raise NotImplementedError

    @abstractmethod
    def term_id_range(self, fieldname, termbytes):
        """
        Returns a tuple of the lowest and highest doc ID containing the given
        term.

        :param fieldname: the name of the field the term is in.
        :param termbytes: the bytestring representing the term.
        """

        raise NotImplementedError

    @abstractmethod
    def term_info(self, fieldname, termbytes):
        """
        Returns a :class:`whoosh.reading.TermInfo` object containing information
        about the given term.

        :param fieldname: the name of the field the term is in.
        :param termbytes: the bytestring representing the term.
        :rtype: :class:`whoosh.reading.TermInfo`
        """

        raise NotImplementedError

    def frequency(self, fieldname, termbytes):
        """
        This is equivalent to `TermReader.term_info().weight()` but may be
        more efficient.
        """

        return self.term_info(fieldname, termbytes).weight()

    def doc_frequency(self, fieldname, termbytes):
        """
        This is equivalent to `TermReader.term_info().doc_frequency()` but may
        be more efficient.
        """

        return self.term_info(fieldname, termbytes).doc_frequency()

    @abstractmethod
    def matcher(self, fieldname, fieldobj, termbytes, scorer=None):
        """
        Returns a :class:`Matcher` for reading the postings of the given term.
        This is equivalent to
        `TermReader.codec().postings_reader(fieldname, bytestring)`.

        :param fieldname: the name of the field the term is in.
        :param fieldobj: the :class:`whoosh.fields.Field` object for the field.
        :param termbytes: the bytestring representing the term.
        :param scorer: a :class:`whoosh.scoring.BaseScorer` object to use
            for scoring documents found by the matcher.
        :rtype: :class:`whoosh.matching.Matcher`
        """

        raise NotImplementedError

    @abstractmethod
    def indexed_field_names(self):
        """
        Returns an iterable containing the names of the fields that have terms
        in the index.
        """

        raise NotImplementedError

    def unique_id(self, fieldname, fieldobj, termbytes):
        # If a subclass tracks unique terms, it can replace this with something
        # more efficient
        m = self.matcher(fieldname, fieldobj, termbytes)
        if m.is_active():
            return m.id()

    def close(self):
        """
        Closes any resources used by this reader.
        """

        self.closed = True


class TagWriter(object):
    def __init__(self, txn):
        self._txn = txn

    def add_tag(self, docid, fieldname, tagname):
        """
        Adds the given tag to the document.

        :param docid: the ID of the document to add the tag to.
        :param fieldname: the name of the tag field.
        :param tagname: the tag string to add to the document.
        """

        raise NotImplementedError

    def remove_tag(self, docid, fieldname, tagname):
        """
        Removes the given tag from the document.

        :param docid: the ID of the document to remove the tag from.
        :param fieldname: the name of the tag field.
        :param tagname: the tag string to add to the document.
        """

        raise NotImplementedError

    def close(self):
        pass


class TagReader(object):
    def __init__(self, txn):
        self._txn = txn

    def has_tag(self, docid, tagname):
        """
        Returns True if the given document has the tag.

        :param docid: the ID of the document.
        :param fieldname: the name of the tag field.
        :param tagname: the tag string to check for.
        :rtype: bool
        """

        raise NotImplementedError

    def ids_for_tag(self, fieldname, tagname):
        """
        Returns an iterator of document IDs that have the given tag string.

        :param fieldname: the name of the tag field.
        :param tagname: the tag string to check for.
        """

        raise NotImplementedError

    def tags_for_id(self, fieldname, docid):
        """
        Returns an iterator of tag strings for the given document

        :param fieldname: the name of the tag field.
        :param docid: the ID of the document.
        """

        raise NotImplementedError

    def all_tags(self, fieldname):
        """
        Returns the list of all tags.

        :param fieldname: the name of the tag field.
        """

        raise NotImplementedError

    def union_tags(self, fieldname, taglist):
        raise NotImplementedError

    def intersect_tags(self, fieldname, taglist):
        raise NotImplementedError

    def close(self):
        pass


class ColumnWriter(object):
    __metaclass__ = ABCMeta

    def __init__(self, txn):
        self._txn = txn

    @abstractmethod
    def add_value(self, fieldname, fieldobj, docid, value):
        raise NotImplementedError

    @abstractmethod
    def remove_value(self, fieldname, fieldobj, docid):
        raise NotImplementedError

    def close(self):
        pass


class ColumnReader(object):
    __metaclass__ = ABCMeta

    def __init__(self, txn, fieldname, fieldobj):
        self._txn = txn
        self._fieldname = fieldname
        self._fieldobj = fieldobj

    @abstractmethod
    def __getitem__(self, docid):
        raise NotImplementedError

    @abstractmethod
    def sort_key(self, docid, reverse=False):
        raise NotImplementedError

    @abstractmethod
    def is_reversible(self):
        raise NotImplementedError

    @abstractmethod
    def exists(self):
        """
        Returns True if column data exists for this field in the database.
        """

        raise NotImplementedError


class Automata(object):
    @staticmethod
    def levenshtein_dfa(uterm, maxdist, prefix=0):
        return lev.levenshtein_automaton(uterm, maxdist, prefix).to_dfa()


# Utility column reader objects

class EmptyColumnReader(ColumnReader):
    """
    Acts like a reader for a column with no stored values. Always returns
    the default.
    """

    def __init__(self, default):
        """
        :param default: the value to return for all requests.
        """

        self._default = default

    def __getitem__(self, docid):
        return self._default

    def exists(self):
        return False

    def sort_key(self, docid, reverse=False):
        return self._default

    def is_reversible(self):
        return True


# class MultiColumnReader(ColumnReader):
#     """Serializes access to multiple column readers, making them appear to be
#     one large column.
#     """
#
#     def __init__(self, readers):
#         """
#         :param readers: a sequence of column reader objects.
#         """
#
#         self._readers = readers
#
#         self._doc_offsets = []
#         self._doccount = 0
#         for r in readers:
#             self._doc_offsets.append(self._doccount)
#             self._doccount += len(r)
#
#     def _document_reader(self, docnum):
#         return max(0, bisect_right(self._doc_offsets, docnum) - 1)
#
#     def _reader_and_docnum(self, docnum):
#         rnum = self._document_reader(docnum)
#         offset = self._doc_offsets[rnum]
#         return rnum, docnum - offset
#
#     def __getitem__(self, docnum):
#         x, y = self._reader_and_docnum(docnum)
#         return self._readers[x][y]
#
#     def __iter__(self):
#         for r in self._readers:
#             for v in r:
#                 yield v


class TranslatingColumnReader(ColumnReader):
    """
    Calls a function to "translate" values from an underlying column reader
    object before returning them.

    ``IndexReader`` objects can wrap a column reader with this object to call
    ``FieldType.from_column_value`` on the stored column value before returning
    it the the user.
    """

    def __init__(self, reader, translate):
        """
        :param reader: the underlying ColumnReader object to get values from.
        :param translate: a function that takes a value from the underlying
            reader and returns a translated value.
        """

        self._reader = reader
        self._translate = translate

    def __getitem__(self, docid):
        return self._translate(self._reader[docid])

    def exists(self):
        return self._reader.exists()

    def raw_column(self):
        """
        Returns the underlying column reader.
        """

        return self._reader

    def sort_key(self, docid, reverse=False):
        return self._reader.sort_key(docid, reverse)

    def is_reversible(self):
        return self._reader.reversible()

