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
from threading import Lock, RLock

from whoosh.util import ClosableMixin, protected
from whoosh.fields import FieldConfigurationError, UnknownFieldError

# Exceptions

class TermNotFound(Exception):
    pass

# Reader classes

class DocReader(ClosableMixin):
    """
    Do not instantiate this object directly. Instead use Index.doc_reader().
    
    Reads document-related information from a segment. The main
    interface is to either iterate on this object to yield the document
    stored fields, or use e.g. docreader[10] to get the stored
    fields for a specific document number.
    
    Each DocReader represents two open files. Be sure to close() the
    reader when you're finished with it.
    """
    
    def __init__(self, storage, segment, schema):
        self.storage = storage
        self.segment = segment
        self.schema = schema
        self._scorable_fields = schema.scorable_fields()
        
        self.doclength_table = storage.open_records(segment.doclen_filename)
        self.docs_table = storage.open_table(segment.docs_filename)
        #self.cache = FifoCache()
        
        self.vector_table = None
        self.is_closed = False
        self._sync_lock = Lock()
        
        self._fieldnum_to_pos = dict((fieldnum, i) for i, fieldnum
                                     in enumerate(schema.scorable_fields()))
    
    def _open_vectors(self):
        if not self.vector_table:
            self.vector_table = self.storage.open_table(self.segment.vector_filename)
    
    @protected
    def __getitem__(self, docnum):
        """Returns the stored fields for the given document.
        """
        return self.docs_table.get(docnum)
    
    @protected
    def __iter__(self):
        """Yields the stored fields for all documents.
        """
        
        is_deleted = self.segment.is_deleted
        for docnum in xrange(0, self.segment.max_doc):
            if not is_deleted(docnum):
                yield self.docs_table.get(docnum)
    
    def close(self):
        """Closes the open files associated with this reader.
        """
        
        self.doclength_table.close()
        self.docs_table.close()
        if self.vector_table:
            self.vector_table.close()
        self.is_closed = True
    
    def doc_count_all(self):
        """Returns the total number of documents, DELETED OR UNDELETED,
        in this reader.
        """
        return self.segment.doc_count_all()
    
    def doc_count(self):
        """Returns the total number of UNDELETED documents in this reader.
        """
        return self.segment.doc_count()
    
    def field_length(self, fieldid):
        """Returns the total number of terms in the given field.
        """
        
        fieldid = self.schema.to_number(fieldid)
        return self.segment.field_length(fieldid)
    
    @protected
    def doc_field_length(self, docnum, fieldid):
        """Returns the number of terms in the given field in the
        given document. This is used by some scoring algorithms.
        """
        
        fieldid = self.schema.to_number(fieldid)
        if fieldid not in self._scorable_fields:
            raise FieldConfigurationError("Field %r does not store lengths" % fieldid)
        
        pos = self._fieldnum_to_pos[fieldid]
        return self.doclength_table.get(docnum, pos)
    
    @protected
    def doc_field_lengths(self, docnum):
        """Returns an array corresponding to the lengths of the
        scorable fields in the given document. It's up to the
        caller to correlate the positions of the numbers in the
        array with the scorable fields in the schema.
        """
        
        return self.doclength_table.get_record(docnum)
    
    def vector_format(self, fieldnum):
        """
        Returns the vector format object associated with the given
        field, or None if the field is not vectored.
        """
        return self.schema.field_by_number(fieldnum).vector
    
    def vector_supports(self, fieldnum, name):
        """
        Returns true if the vector format for the given field supports
        the data interpretation.
        """
        format = self.vector_format(fieldnum)
        if format is None: return False
        return format.supports(name)
    
    @protected
    def vector(self, docnum, fieldnum):
        """Yields a sequence of raw (text, data) tuples representing
        the term vector for the given document and field.
        """
        
        self._open_vectors()
        readfn = self.vector_format(fieldnum).read_postvalue
        return self.vector_table.postings((docnum, fieldnum), readfn)
    
    def vector_as(self, docnum, fieldnum, astype):
        """Yields a sequence of interpreted (text, data) tuples
        representing the term vector for the given document and
        field.
        
        This method uses the vector format object's 'data_to_*'
        method to interpret the data. For example, if the vector
        format has a 'data_to_positions()' method, you can use
        vector_as(x, y, "positions") to get a positions vector.
        """
        
        format = self.vector_format(fieldnum)
        
        if format is None:
            raise FieldConfigurationError("Field %r is not vectored" % self.schema.number_to_name(fieldnum))
        elif not format.supports(astype):
            raise FieldConfigurationError("Field %r does not support %r" % (self.schema.number_to_name(fieldnum),
                                                                            astype))
        
        interpreter = format.interpreter(astype)
        for text, data in self.vector(docnum, fieldnum):
            yield (text, interpreter(data))
    

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
    
    def vector(self, docnum):
        segmentnum, segmentdoc = self._segment_and_docnum(docnum)
        return self.doc_readers[segmentnum].vector(segmentdoc)
    
    def _doc_info(self, docnum, key):
        segmentnum, segmentdoc = self._segment_and_docnum(docnum)
        return self.doc_readers[segmentnum]._doc_info(segmentdoc, key)
    

class TermReader(ClosableMixin):
    """
    Do not instantiate this object directly. Instead use Index.term_reader().
    
    Reads term information from a segment.
    
    Each TermReader represents two open files. Remember to close() the reader when
    you're done with it.
    """
    
    def __init__(self, storage, segment, schema):
        """
        :storage: The storage object in which the segment resides.
        :segment: The segment to read from.
        :schema: The index's schema object.
        """
        
        self.segment = segment
        self.schema = schema
        
        self.term_table = storage.open_table(segment.term_filename)
        self.is_closed = False
        self._sync_lock = Lock()
    
    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, self.segment)
    
    @protected
    def __iter__(self):
        """Yields (fieldnum, token, docfreq, indexfreq) tuples for
        each term in the reader, in lexical order.
        """
        
        tt = self.term_table
        for (fn, t), termcount in tt:
            yield (fn, t, tt.posting_count((fn, t)), termcount)
    
    @protected
    def __contains__(self, term):
        """Returns True if the given term tuple (fieldid, text) is
        in this reader.
        """
        return (self.schema.to_number(term[0]), term[1]) in self.term_table
    
    def close(self):
        """Closes the open files associated with this reader.
        """
        self.term_table.close()
        self.is_closed = True
    
    def format(self, fieldname):
        """Returns the Format object corresponding to the given field name.
        """
        if fieldname in self.schema:
            return self.schema.field_by_name(fieldname).format
        else:
            raise UnknownFieldError(fieldname)
    
    @protected
    def _term_info(self, fieldnum, text):
        try:
            return self.term_table.get((fieldnum, text))
        except KeyError:
            raise TermNotFound("%s:%r" % (fieldnum, text))
    
    @protected
    def doc_frequency(self, fieldid, text):
        """Returns the document frequency of the given term (that is,
        how many documents the term appears in).
        """
        
        fieldid = self.schema.to_number(fieldid)
        if (fieldid, text) not in self.term_table:
            return 0
        return self.term_table.posting_count((fieldid, text))
    
    @protected
    def frequency(self, fieldid, text):
        """Returns the total number of instances of the given term
        in the collection.
        """
        
        fieldid = self.schema.to_number(fieldid)
        if (fieldid, text) not in self.term_table:
            return 0
        return self.term_table.get((fieldid, text))
    
    def doc_count_all(self):
        """Returns the total number of documents, DELETED OR UNDELETED,
        in this reader.
        """
        return self.segment.doc_count_all()
    
    @protected
    def iter_from(self, fieldnum, text):
        """Yields (field_num, text, doc_freq, index_freq) tuples
        for all terms in the reader, starting at the given term.
        """
        
        tt = self.term_table
        postingcount = tt.posting_count
        for (fn, t), termcount in tt.iter_from((fieldnum, text)):
            yield (fn, t, postingcount((fn, t)), termcount)
    
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
    
    # Posting retrieval methods
    
    @protected
    def postings(self, fieldnum, text, exclude_docs = None):
        """
        Yields raw (docnum, data) tuples for each document containing
        the current term.
        
        :exclude_docs:
            a set of document numbers to ignore. This
            is used by queries to skip documents that have already been
            eliminated from consideration.
        :boost: a factor by which to multiply each weight.
        """
        
        is_deleted = self.segment.is_deleted
        no_exclude = exclude_docs is None
        
        # The format object is actually responsible for parsing the
        # posting data from disk.
        readfn = self.schema.field_by_number(fieldnum).format.read_postvalue
        
        for docnum, data in self.term_table.postings((fieldnum, text), readfn = readfn):
            if not is_deleted(docnum)\
               and (no_exclude or docnum not in exclude_docs):
                yield docnum, data
    
    def weights(self, fieldnum, text, exclude_docs = None, boost = 1.0):
        """
        Yields (docnum, term_weight) tuples for each document containing
        the given term. The current field must have stored term weights
        for this to work.
        
        :exclude_docs:
            a set of document numbers to ignore. This
            is used by queries to skip documents that have already been
            eliminated from consideration.
        :boost: a factor by which to multiply each weight.
        """
        
        
        is_deleted = self.segment.is_deleted
        no_exclude = exclude_docs is None
        
        # The format object is actually responsible for parsing the
        # posting data from disk.
        readfn = self.schema.field_by_number(fieldnum).format.read_weight
        
        for docnum, weight in self.term_table.postings((fieldnum, text), readfn = readfn):
            if not is_deleted(docnum)\
               and (no_exclude or docnum not in exclude_docs):
                yield docnum, weight * boost
    
    def postings_as(self, fieldnum, text, astype, exclude_docs = None):
        """Yields interpreted data for each document containing
        the given term. The current field must have stored positions
        for this to work.
        
        :astype:
            how to interpret the posting data, for example
            "positions". The field must support the interpretation.
        :exclude_docs:
            a set of document numbers to ignore. This
            is used by queries to skip documents that have already been
            eliminated from consideration.
        :boost: a factor by which to multiply each weight.
        """
        
        format = self.schema.field_by_number(fieldnum).format
        
        if not format.supports(astype):
            raise FieldConfigurationError("Field %r format does not support %r" % (self.schema.name_to_number(fieldnum),
                                                                                   astype))
        
        interp = format.interpreter(astype)
        for docnum, data in self.postings(fieldnum, text, exclude_docs = exclude_docs):
            yield (docnum, interp(data))
    
    def positions(self, fieldnum, text, exclude_docs = None):
        """Yields (docnum, [positions]) tuples for each document containing
        the given term. The current field must have stored positions
        for this to work.
        
        :exclude_docs:
            a set of document numbers to ignore. This
            is used by queries to skip documents that have already been
            eliminated from consideration.
        :boost: a factor by which to multiply each weight.
        """
        
        return self.postings_as(fieldnum, text, "positions", exclude_docs = exclude_docs)


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
        """
        Closes the open files associated with this reader.
        """
        
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
        """Yields raw (docnum, data) tuples for each document containing
        the current term. This is useful if you simply want to know
        which documents contain the current term. Use weights() or
        positions() if you need to term weight or positions in each
        document.
        
        exclude_docs can be a set of document numbers to ignore. This
        is used by queries to skip documents that have already been
        eliminated from consideration.
        """
        
        for i, r in enumerate(self.term_readers):
            offset = self.doc_offsets[i]
            if (fieldnum, text) in r:
                for docnum, data in r.postings(fieldnum, text, exclude_docs = exclude_docs):
                    yield (docnum + offset, data)
                    
    def weights(self, fieldnum, text, exclude_docs = None, boost = 1.0):
        for i, r in enumerate(self.term_readers):
            offset = self.doc_offsets[i]
            if (fieldnum, text) in r:
                for docnum, weight in r.weights(fieldnum, text,
                                                exclude_docs = exclude_docs, boost = boost):
                    yield (docnum + offset, weight)



if __name__ == '__main__':
    pass












    
    
    