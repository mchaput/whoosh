#===============================================================================
# Copyright 2007 Matt Chaput
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#===============================================================================

"""This module contains classes that allow reading from an index.
"""

from bisect import bisect_right
from heapq import heapify, heapreplace, heappop, nlargest

from whoosh.fields import UnknownFieldError
from whoosh.util import ClosableMixin
from whoosh.postings import MultiPostingReader

# Exceptions

class TermNotFound(Exception):
    pass


# Base class

class IndexReader(ClosableMixin):
    """Do not instantiate this object directly. Instead use Index.reader().
    """

    def __contains__(self, term):
        """Returns True if the given term tuple (fieldid, text) is
        in this reader.
        """
        raise NotImplementedError

    def close(self):
        """Closes the open files associated with this reader.
        """
        raise NotImplementedError

    def has_deletions(self):
        """Returns True if the underlying index/segment has deleted
        documents.
        """
        raise NotImplementedError

    def is_deleted(self, docnum):
        """Returns True if the given document number is marked deleted.
        """
        raise NotImplementedError

    def stored_fields(self, docnum):
        """Returns the stored fields for the given document number.
        """
        raise NotImplementedError

    def all_stored_fields(self):
        """Yields the stored fields for all documents.
        """
        raise NotImplementedError

    def doc_count_all(self):
        """Returns the total number of documents, DELETED OR UNDELETED,
        in this reader.
        """
        raise NotImplementedError

    def doc_count(self):
        """Returns the total number of UNDELETED documents in this reader.
        """
        raise NotImplementedError

    def scorable(self, fieldid):
        """Returns true if the given field stores field lengths.
        """
        return self.schema[fieldid].scorable

    def fieldname_to_num(self, fieldname):
        return self.schema.name_to_number(fieldname)

    def field_length(self, fieldid):
        """Returns the total number of terms in the given field. This is used
        by some scoring algorithms.
        """
        raise NotImplementedError

    def doc_field_length(self, docnum, fieldid):
        """Returns the number of terms in the given field in the given
        document. This is used by some scoring algorithms.
        """
        raise NotImplementedError

    def doc_field_lengths(self, docnum):
        """Returns an array corresponding to the lengths of the scorable fields
        in the given document. It's up to the caller to correlate the positions
        of the numbers in the array with the scorable fields in the schema.
        """
        raise NotImplementedError

    def has_vector(self, docnum, fieldid):
        """Returns True if the given document has a term vector for the given
        field.
        """
        raise NotImplementedError

    def postings(self, fieldid, text, exclude_docs=None):
        """Returns a :class:`~whoosh.postings.PostingReader` for the postings
        of the given term.
        
        >>> pr = searcher.postings("content", "render")
        >>> pr.skip_to(10)
        >>> pr.id
        12
        
        :param fieldid: the field name or field number of the term.
        :param text: the text of the term.
        :exclude_docs: an optional BitVector of documents to exclude from the
            results, or None to not exclude any documents.
        :rtype: :class:`whoosh.postings.PostingReader`
        """

        raise NotImplementedError

    def vector(self, docnum, fieldid):
        """Returns a :class:`~whoosh.postings.PostingReader` object for the
        given term vector.
        
        >>> docnum = searcher.document_number(path=u'/a/b/c')
        >>> v = searcher.vector(docnum, "content")
        >>> v.all_as("frequency")
        [(u"apple", 3), (u"bear", 2), (u"cab", 2)]
        
        :param docnum: the document number of the document for which you want
            the term vector.
        :param fieldid: the field name or field number of the field for which
            you want the term vector.
        :rtype: :class:`whoosh.postings.PostingReader`
        """
        raise NotImplementedError

    def vector_as(self, astype, docnum, fieldid):
        """Returns an iterator of (termtext, value) pairs for the terms in the
        given term vector. This is a convenient shortcut to calling vector()
        and using the PostingReader object when all you want are the terms
        and/or values.
        
        >>> docnum = searcher.document_number(path=u'/a/b/c')
        >>> searcher.vector_as("frequency", docnum, "content")
        [(u"apple", 3), (u"bear", 2), (u"cab", 2)]
        
        :param docnum: the document number of the document for which you want
            the term vector.
        :param fieldid: the field name or field number of the field for which
            you want the term vector.
        :param astype: a string containing the name of the format you want the
            term vector's data in, for example "weights".
        """

        vec = self.vector(docnum, fieldid)
        return vec.all_as(astype)

    def format(self, fieldid):
        """Returns the Format object corresponding to the given field name.
        """
        if fieldid in self.schema:
            return self.schema[fieldid].format
        else:
            raise UnknownFieldError(fieldid)

    def __iter__(self):
        """Yields (fieldnum, text, docfreq, indexfreq) tuples for each term in
        the reader, in lexical order.
        """
        raise NotImplementedError

    def doc_frequency(self, fieldid, text):
        """Returns how many documents the given term appears in.
        """
        raise NotImplementedError

    def frequency(self, fieldid, text):
        """Returns the total number of instances of the given term in the
        collection.
        """
        raise NotImplementedError

    def iter_from(self, fieldnum, text):
        """Yields (field_num, text, doc_freq, index_freq) tuples for all terms
        in the reader, starting at the given term.
        """
        raise NotImplementedError

    def expand_prefix(self, fieldid, prefix):
        """Yields terms in the given field that start with the given prefix.
        """

        fieldid = self.schema.to_number(fieldid)
        for fn, t, _, _ in self.iter_from(fieldid, prefix):
            if fn != fieldid or not t.startswith(prefix):
                return
            yield t

    def all_terms(self):
        """Yields (fieldname, text) tuples for every term in the index.
        """

        num2name = self.schema.number_to_name
        current_fieldnum = None
        current_fieldname = None

        for fn, t, _, _ in self:
            # Only call self.schema.number_to_name when the
            # field number changes.
            if fn != current_fieldnum:
                current_fieldnum = fn
                current_fieldname = num2name(fn)
            yield (current_fieldname, t)

    def iter_field(self, fieldid, prefix=''):
        """Yields (text, doc_freq, index_freq) tuples for all terms in the
        given field.
        """

        fieldid = self.schema.to_number(fieldid)
        for fn, t, docfreq, freq in self.iter_from(fieldid, prefix):
            if fn != fieldid:
                return
            yield t, docfreq, freq

    def iter_prefix(self, fieldid, prefix):
        """Yields (field_num, text, doc_freq, index_freq) tuples for all terms
        in the given field with a certain prefix.
        """

        fieldid = self.schema.to_number(fieldid)
        for fn, t, docfreq, colfreq in self.iter_from(fieldid, prefix):
            if fn != fieldid or not t.startswith(prefix):
                return
            yield (t, docfreq, colfreq)

    def most_frequent_terms(self, fieldid, number=5, prefix=''):
        """Returns the top 'number' most frequent terms in the given field as a
        list of (frequency, text) tuples.
        """

        return nlargest(number, ((tf, token)
                                 for token, _, tf
                                 in self.iter_prefix(fieldid, prefix)))

    def most_distinctive_terms(self, fieldid, number=5, prefix=None):
        """Returns the top 'number' terms with the highest ``tf*idf`` scores as
        a list of (score, text) tuples.
        """

        return nlargest(number, ((tf * (1.0 / df), token)
                                 for token, df, tf
                                 in self.iter_prefix(fieldid, prefix)))

    def lexicon(self, fieldid):
        """Yields all terms in the given field.
        """

        for t, _, _ in self.iter_field(fieldid):
            yield t


# Multisegment reader class

class MultiReader(IndexReader):
    """Do not instantiate this object directly. Instead use Index.reader().
    """

    def __init__(self, readers, doc_offsets, schema):
        self.readers = readers
        self.doc_offsets = doc_offsets
        self.schema = schema
        self._scorable_fields = self.schema.scorable_fields()

        self.is_closed = False

    def __contains__(self, term):
        return any(r.__contains__(term) for r in self.readers)

    def __iter__(self):
        return self._merge_iters([iter(r) for r in self.readers])

    def has_deletions(self):
        return any(r.has_deletions() for r in self.readers)

    def is_deleted(self):
        segmentnum, segmentdoc = self._segment_and_doc
        return self.readers[segmentnum].is_deleted(segmentdoc)

    def stored_fields(self, docnum):
        segmentnum, segmentdoc = self._segment_and_docnum(docnum)
        return self.readers[segmentnum].stored_fields(segmentdoc)

    def all_stored_fields(self):
        for reader in self.readers:
            for result in reader.all_stored_fields():
                yield result

    def close(self):
        for d in self.readers:
            d.close()
        self.is_closed = True

    def doc_count_all(self):
        return sum(dr.doc_count_all() for dr in self.readers)

    def doc_count(self):
        return sum(dr.doc_count() for dr in self.readers)

    def field_length(self, fieldnum):
        return sum(dr.field_length(fieldnum) for dr in self.readers)

    def doc_field_length(self, docnum, fieldid):
        fieldid = self.schema.to_number(fieldid)
        segmentnum, segmentdoc = self._segment_and_docnum(docnum)
        return self.readers[segmentnum].doc_field_length(segmentdoc, fieldid)

    def doc_field_lengths(self, docnum):
        segmentnum, segmentdoc = self._segment_and_docnum(docnum)
        return self.readers[segmentnum].doc_field_lengths(segmentdoc)

    def unique_count(self, docnum):
        segmentnum, segmentdoc = self._segment_and_docnum(docnum)
        return self.readers[segmentnum].unique_count(segmentdoc)

    def _document_segment(self, docnum):
        return max(0, bisect_right(self.doc_offsets, docnum) - 1)

    def _segment_and_docnum(self, docnum):
        segmentnum = self._document_segment(docnum)
        offset = self.doc_offsets[segmentnum]
        return segmentnum, docnum - offset

    def has_vector(self, docnum, fieldid):
        segmentnum, segmentdoc = self._segment_and_docnum(docnum)
        return self.readers[segmentnum].has_vector(segmentdoc, fieldid)

    def postings(self, fieldid, text, exclude_docs=None):
        format = self.schema[fieldid].format
        postreaders = []
        docoffsets = []
        for i, r in enumerate(self.readers):
            if (fieldid, text) in r:
                postreaders.append(r.postings(fieldid, text,
                                              exclude_docs=exclude_docs))
                docoffsets.append(self.doc_offsets[i])
        if not postreaders:
            raise TermNotFound(fieldid, text)
        else:
            return MultiPostingReader(format, postreaders, docoffsets)

    def vector(self, docnum, fieldid):
        segmentnum, segmentdoc = self._segment_and_docnum(docnum)
        return self.readers[segmentnum].vector(segmentdoc, fieldid)

    def vector_as(self, astype, docnum, fieldid):
        segmentnum, segmentdoc = self._segment_and_docnum(docnum)
        return self.readers[segmentnum].vector_as(astype, segmentdoc, fieldid)

    def iter_from(self, fieldnum, text):
        return self._merge_iters([r.iter_from(fieldnum, text)
                                  for r in self.readers])

    def doc_frequency(self, fieldnum, text):
        return sum(r.doc_frequency(fieldnum, text) for r in self.readers)

    def frequency(self, fieldnum, text):
        return sum(r.frequency(fieldnum, text) for r in self.readers)

    def _merge_iters(self, iterlist):
        # Merge-sorts terms coming from a list of
        # term iterators (IndexReader.__iter__() or
        # IndexReader.iter_from()).

        # Fill in the list with the head term from each iterator.
        # infos is a list of [headterm, iterator] lists.

        current = []
        for it in iterlist:
            fnum, text, docfreq, termcount = it.next()
            current.append((fnum, text, docfreq, termcount, it))
        heapify(current)

        # Number of active iterators
        active = len(current)
        while active > 0:
            # Peek at the first term in the sorted list
            fnum, text = current[0][:2]
            docfreq = 0
            termcount = 0

            # Add together all terms matching the first term in the list.
            while current and current[0][0] == fnum and current[0][1] == text:
                docfreq += current[0][2]
                termcount += current[0][3]
                it = current[0][4]
                try:
                    fn, t, df, tc = it.next()
                    heapreplace(current, (fn, t, df, tc, it))
                except StopIteration:
                    heappop(current)
                    active -= 1

            # Yield the term with the summed doc frequency and term count.
            yield (fnum, text, docfreq, termcount)




















