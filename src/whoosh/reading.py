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

"""
This module contains classes that allow reading from an index.
"""

from bisect import bisect_right
from heapq import heapify, heapreplace, heappop, nlargest
from threading import Lock

from whoosh.fields import UnknownFieldError
from whoosh.util import ClosableMixin

# Exceptions

class TermNotFound(Exception):
    pass


# Base classes

class DocReader(ClosableMixin):
    """Do not instantiate this object directly. Instead use Index.doc_reader().
    """

    def __getitem__(self, docnum):
        """Returns the stored fields for the given document number.
        """
        raise NotImplementedError
    
    def __iter__(self):
        """Yields the stored fields for all documents.
        """
        raise NotImplementedError
    
    def close(self):
        """Closes the open files associated with this reader.
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
    
    def field_length(self, fieldid):
        """Returns the total number of terms in the given field. This is used
        by some scoring algorithms.
        """
        raise NotImplementedError
    
    def doc_field_length(self, docnum, fieldid):
        """Returns the number of terms in the given field in the
        given document. This is used by some scoring algorithms.
        """
        raise NotImplementedError
    
    def doc_field_lengths(self, docnum):
        """Returns an array corresponding to the lengths of the
        scorable fields in the given document. It's up to the
        caller to correlate the positions of the numbers in the
        array with the scorable fields in the schema.
        """
        raise NotImplementedError
    
    def vector_format(self, fieldid):
        """Returns the vector format object associated with the given
        field, or None if the field does not store term vectors.
        """
        return self.schema[fieldid].vector
    
    def vector_supports(self, fieldid, astype):
        """Returns True if the vector format for the given field supports
        the given data interpretation.
        
        :param astype: a string containing the name of the format you
            want to check the vector supports, for example "weights".
        """
        format = self.vector_format(fieldid)
        if format is None: return False
        return format.supports(astype)
    
    def vector(self, docnum, fieldid):
        raise NotImplementedError
    
    def vector_as(self, docnum, fieldid, astype):
        """Yields a sequence of interpreted (text, data) tuples
        representing the term vector for the given document and
        field.
        
        :param astype: a string containing the name of the format you
            want the term vector's data in, for example "weights".
        """
        raise NotImplementedError
    

class TermReader(ClosableMixin):
    def __iter__(self):
        """Yields (fieldnum, token, docfreq, indexfreq) tuples for
        each term in the reader, in lexical order.
        """
        raise NotImplementedError
    
    def __contains__(self, term):
        """Returns True if the given term tuple (fieldid, text) is
        in this reader.
        """
        raise NotImplementedError
    
    def close(self):
        """Closes the open files associated with this reader.
        """
        raise NotImplementedError
    
    def format(self, fieldid):
        """Returns the Format object corresponding to the given field name.
        """
        if fieldid in self.schema:
            return self.schema[fieldid].format
        else:
            raise UnknownFieldError(fieldid)
    
    def doc_frequency(self, fieldid, text):
        """Returns the document frequency of the given term (that is,
        how many documents the term appears in).
        """
        raise NotImplementedError
    
    def frequency(self, fieldid, text):
        """Returns the total number of instances of the given term
        in the collection.
        """
        raise NotImplementedError
    
    def doc_count_all(self):
        """Returns the total number of documents, DELETED OR UNDELETED,
        in this reader.
        """
        raise NotImplementedError
    
    def iter_from(self, fieldnum, text):
        """Yields (field_num, text, doc_freq, index_freq) tuples
        for all terms in the reader, starting at the given term.
        """
        raise NotImplementedError
    
    def expand_prefix(self, fieldid, prefix):
        """Yields terms in the given field that start with the given prefix.
        """
        raise NotImplementedError
    
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
    
    def iter_field(self, fieldid, prefix = ''):
        """Yields (text, doc_freq, index_freq) tuples for all terms
        in the given field.
        """
        
        fieldid = self.schema.to_number(fieldid)
        for fn, t, docfreq, freq in self.iter_from(fieldid, prefix):
            if fn != fieldid:
                return
            yield t, docfreq, freq
    
    def iter_prefix(self, fieldid, prefix):
        """Yields (field_num, text, doc_freq, index_freq) tuples
        for all terms in the given field with a certain prefix.
        """
        
        fieldid = self.schema.to_number(fieldid)
        for fn, t, docfreq, colfreq in self.iter_from(fieldid, prefix):
            if fn != fieldid or not t.startswith(prefix):
                return
            yield (t, docfreq, colfreq)
    
    def most_frequent_terms(self, fieldid, number = 5, prefix = None):
        """Yields the top 'number' most frequent terms in the given field as
        a series of (frequency, text) tuples.
        """
        
        if prefix is not None:
            iterator = self.iter_prefix(fieldid, prefix)
        else:
            iterator = self.iter_field(fieldid)
        
        return nlargest(number,
                        ((indexfreq, token)
                         for token, _, indexfreq
                         in iterator))
        
    def lexicon(self, fieldid):
        """Yields all terms in the given field."""
        
        for t, _, _ in self.iter_field(fieldid):
            yield t
    
    def postings(self, fieldid, text):
        raise NotImplementedError
    
    def postings_as(self, fieldid, text, astype, exclude_docs = None):
        """Yields interpreted data for each document containing
        the given term. The current field must have stored positions
        for this to work.
        
        :param astype:
            how to interpret the posting data, for example
            "positions". The field must support the interpretation.
        :param exclude_docs:
            a set of document numbers to ignore. This
            is used by queries to skip documents that have already been
            eliminated from consideration.
        :param boost: a factor by which to multiply each weight.
        """
        raise NotImplementedError
    
    def weights(self, fieldid, text, exclude_docs = None):
        """
        Yields (docnum, term_weight) tuples for each document containing
        the given term. The current field must have stored term weights
        for this to work.
        
        :param exclude_docs:
            a set of document numbers to ignore. This
            is used by queries to skip documents that have already been
            eliminated from consideration.
        """
        
        return self.postings_as(fieldid, text, "weight", exclude_docs = exclude_docs)
    
    def positions(self, fieldid, text, exclude_docs = None):
        """Yields (docnum, [positions]) tuples for each document containing
        the given term. The current field must have stored positions
        for this to work.
        
        :param exclude_docs:
            a set of document numbers to ignore. This
            is used by queries to skip documents that have already been
            eliminated from consideration.
        :param boost: a factor by which to multiply each weight.
        """
        
        return self.postings_as(fieldid, text, "positions", exclude_docs = exclude_docs)


# Multisegment reader classes

class MultiDocReader(DocReader):
    """
    Do not instantiate this object directly. Instead use Index.doc_reader().
    
    Reads document-related information by aggregating the results from
    multiple segments. The main interface is to either iterate on this
    object to yield the document stored fields, or use getitem (e.g. docreader[10])
    to get the stored fields for a specific document number.
    
    Each MultiDocReader represents (number of segments * 2) open files.
    Be sure to close() the reader when you're finished with it.
    """
    
    def __init__(self, doc_readers, doc_offsets, schema):
        self.doc_readers = doc_readers
        self.doc_offsets = doc_offsets
        self.schema = schema
        self._scorable_fields = self.schema.scorable_fields()
        
        self.is_closed = False
        self._sync_lock = Lock()
        
    def __getitem__(self, docnum):
        segmentnum, segmentdoc = self._segment_and_docnum(docnum)
        return self.doc_readers[segmentnum].__getitem__(segmentdoc)
    
    def __iter__(self):
        for reader in self.doc_readers:
            for result in reader:
                yield result
    
    def close(self):
        """Closes the open files associated with this reader.
        """
        
        for d in self.doc_readers:
            d.close()
        self.is_closed = True
    
    def doc_count_all(self):
        return sum(dr.doc_count_all() for dr in self.doc_readers)
    
    def doc_count(self):
        return sum(dr.doc_count() for dr in self.doc_readers)
    
    def field_length(self, fieldnum):
        return sum(dr.field_length(fieldnum) for dr in self.doc_readers)
    
    def doc_field_length(self, docnum, fieldid):
        fieldid = self.schema.to_number(fieldid)
        segmentnum, segmentdoc = self._segment_and_docnum(docnum)
        return self.doc_readers[segmentnum].doc_field_length(segmentdoc, fieldid)
    
    def doc_field_lengths(self, docnum):
        segmentnum, segmentdoc = self._segment_and_docnum(docnum)
        return self.doc_readers[segmentnum].doc_field_lengths(segmentdoc)
    
    def unique_count(self, docnum):
        segmentnum, segmentdoc = self._segment_and_docnum(docnum)
        return self.doc_readers[segmentnum].unique_count(segmentdoc)
    
    def _document_segment(self, docnum):
        return max(0, bisect_right(self.doc_offsets, docnum) - 1)
    
    def _segment_and_docnum(self, docnum):
        segmentnum = self._document_segment(docnum)
        offset = self.doc_offsets[segmentnum]
        return segmentnum, docnum - offset
    
    def vector(self, docnum, fieldid):
        segmentnum, segmentdoc = self._segment_and_docnum(docnum)
        return self.doc_readers[segmentnum].vector(segmentdoc, fieldid)
    
    def _doc_info(self, docnum, key):
        segmentnum, segmentdoc = self._segment_and_docnum(docnum)
        return self.doc_readers[segmentnum]._doc_info(segmentdoc, key)
    

class MultiTermReader(TermReader):
    """Do not instantiate this object directly. Instead use Index.term_reader().
    
    Reads term information by aggregating the results from
    multiple segments.
    
    Each MultiTermReader represents (number of segments * 2) open files.
    Be sure to close() the reader when you're finished with it.
    """
    
    def __init__(self, term_readers, doc_offsets, schema):
        self.term_readers = term_readers
        self.doc_offsets = doc_offsets
        self.schema = schema
        
        self.is_closed = False
        self._sync_lock = Lock()
    
    def __contains__(self, term):
        return any(tr.__contains__(term) for tr in self.term_readers)
    
    def __iter__(self):
        return self._merge_iters([iter(r) for r in self.term_readers])
    
    def iter_from(self, fieldnum, text):
        return self._merge_iters([r.iter_from(fieldnum, text) for r in self.term_readers])
    
    def close(self):
        for tr in self.term_readers:
            tr.close()
        self.is_closed = True
    
    def doc_frequency(self, fieldnum, text):
        if (fieldnum, text) not in self:
            return 0
        
        return sum(r.doc_frequency(fieldnum, text) for r in self.term_readers)
    
    def frequency(self, fieldnum, text):
        if (fieldnum, text) not in self:
            return 0
        
        return sum(r.frequency(fieldnum, text) for r in self.term_readers)
    
    def _merge_iters(self, iterlist):
        # Merge-sorts terms coming from a list of
        # term iterators (TermReader.__iter__() or
        # TermReader.iter_from()).
        
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
            
            # Add together all terms matching the first
            # term in the list.
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
                
            # Yield the term with the summed frequency and
            # term count.
            yield (fnum, text, docfreq, termcount)
    
    def postings(self, fieldnum, text, exclude_docs = None):
        for i, r in enumerate(self.term_readers):
            offset = self.doc_offsets[i]
            if (fieldnum, text) in r:
                for docnum, data in r.postings(fieldnum, text, exclude_docs = exclude_docs):
                    yield (docnum + offset, data)
                    



if __name__ == '__main__':
    pass












    
    
    