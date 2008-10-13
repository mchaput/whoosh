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
from heapq import heapify, heapreplace, heappop

from tables import TableReader, PostingTableReader, RecordReader

# Exceptions

class TermNotFound(Exception):
    pass
class UnknownFieldError(Exception):
    pass

# Utility classes

class EndOfIndex(object):
    # This singleton is intended as a marker that always sorts to the end of
    # a list, hence the implementation of __cmp__.
    
    def __cmp__(self, x):
        if type(x) is type(self):
            return 0
        return 1
    
    def __repr__(self):
        return "EndOfIndex()"

end_of_index = EndOfIndex()

# Reader classes

class DocReader(object):
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
        
        doclength_file = storage.open_file(segment.doclen_filename)
        self.doclength_records = RecordReader(doclength_file, "!ii")
        
        docs_file = storage.open_file(segment.docs_filename)
        self.docs_table = TableReader(docs_file)
        
        self.vector_table = None
        self.is_closed = False
    
    def _open_vectors(self):
        if not self.vector_table:
            vector_file = self.storage.open_file(self.segment.vector_filename)
            self.vector_table = PostingTableReader(vector_file)
    
    def close(self):
        """
        Closes the open files associated with this reader.
        """
        
        self.doclength_records.close()
        self.docs_table.close()
        if self.vector_table:
            self.vector_table.close()
        self.is_closed = True
    
    def _doc_info(self, docnum):
        return self.doclength_records[docnum]
    
    def doc_length(self, docnum):
        """
        Returns the total number of terms in a given document.
        This is used by some scoring algorithms.
        """
        return self._doc_info(docnum)[0]
    
    def unique_count(self, docnum):
        """
        Returns the number of UNIQUE terms in a given document.
        This is used by some scoring algorithms.
        """
        return self._doc_info(docnum)[1]
    
    def _vector(self, fieldnum):
        self._open_vectors()
        if isinstance(fieldnum, basestring):
            fieldnum = self.schema.name_to_number(fieldnum)
        
        field = self.schema.by_number[fieldnum]
        if not field.vector:
            raise KeyError("Field %r has no term vectors" % field)
        
        return field.vector
    
    def _posvector(self, fieldnum):
        v = self._vector(fieldnum)
        if not v.has_positions:
            raise KeyError("Field %r has no position vectors" % fieldnum)
        return v
    
    def _base_vectordata(self, docnum, fieldnum):
        v = self._vector(fieldnum)
        return v.base_data(self.vector_table, docnum, fieldnum)
    
    def vectored_frequencies(self, docnum, fieldnum):
        v = self._vector(fieldnum)
        return v.freqs(self.vector_table, docnum, fieldnum)
    
    def vectored_positions(self, docnum, fieldnum):
        v = self._posvector(fieldnum)
        return v.positions(self.vector_table, docnum, fieldnum)
    
    def vectored_positions_from(self, docnum, fieldnum, startid):
        v = self._posvector(fieldnum)
        return v.positions_from(self.vector_table, docnum, fieldnum, startid)
    
    def __getitem__(self, docnum):
        """
        Returns the stored fields for the given document.
        """
        return self.docs_table.get(docnum)
    
    def __iter__(self):
        """
        Yields the stored fields for all documents.
        """
        
        is_deleted = self.segment.is_deleted
        for docnum in xrange(0, self.segment.max_doc):
            if not is_deleted(docnum):
                yield self.docs_table.get(docnum)


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
    
    def __init__(self, doc_readers, doc_offsets):
        self.doc_readers = doc_readers
        self.doc_offsets = doc_offsets
        self.is_closed = False
    
    def close(self):
        """
        Closes the open files associated with this reader.
        """
        
        for d in self.doc_readers:
            d.close()
        self.is_closed = True
    
    def _document_segment(self, docnum):
        return bisect_right(self.doc_offsets, docnum) - 1
    
    def _segment_and_docnum(self, docnum):
        segmentnum = self._document_segment(docnum)
        offset = self.doc_offsets[segmentnum]
        return segmentnum, docnum - offset
    
    def _doc_info(self, docnum):
        segmentnum, segmentdoc = self._segment_and_docnum(docnum)
        return self.doc_readers[segmentnum]._doc_info(segmentdoc)
    
    def __getitem__(self, docnum):
        segmentnum, segmentdoc = self._segment_and_docnum(docnum)
        return self.doc_readers[segmentnum].__getitem__(segmentdoc)
    
    def __iter__(self):
        for reader in self.doc_readers:
            for result in reader:
                yield result
    

class TermReader(object):
    """
    Do not instantiate this object directly. Instead use Index.term_reader().
    
    Reads term information from a segment.
    
    Each TermReader represents two open files. Remember to close() the reader when
    you're done with it.
    """
    
    def __init__(self, storage, segment, schema):
        """
        storage is the storage object of the index.
        segment is an index.Segment object. schema is an index.Schema object.
        """
        
        self.segment = segment
        self.schema = schema
        
        term_file = storage.open_file(segment.term_filename)
        self.term_table = PostingTableReader(term_file)
        self.is_closed = False
        
    def fieldname_to_num(self, fieldname):
        """
        Returns the field number corresponding to the given field name.
        """
        if fieldname in self.schema.by_name:
            return self.schema.name_to_number(fieldname)
        else:
            raise UnknownFieldError(fieldname)
    
    def field(self, fieldname):
        """
        Returns the field object corresponding to the given field name.
        """
        if fieldname in self.schema.by_name:
            return self.schema.by_name[fieldname]
        else:
            raise UnknownFieldError(fieldname)
    
    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, self.segment)
    
    def close(self):
        """
        Closes the open files associated with this reader.
        """
        self.term_table.close()
        self.is_closed = True
    
    def _term_info(self, fieldnum, text):
        try:
            return self.term_table.get((fieldnum, text))
        except KeyError:
            raise TermNotFound("%s:%r" % (fieldnum, text))
    
    def __iter__(self):
        for (fn, t), (offsets, docfreq, termcount) in self.term_table:
            yield (fn, t, docfreq, termcount)
    
    def iter_from(self, fieldnum, text):
        for (fn, t), (offsets, docfreq, termcount) in self.term_table.iter_from((fieldnum, text)):
            yield (fn, t, docfreq, termcount)
    
    def __contains__(self, term):
        fieldnum = term[0]
        if isinstance(fieldnum, basestring):
            term = (self.schema.name_to_number(fieldnum), term[1])
            
        return term in self.term_table
    
    def doc_frequency(self, fieldnum, text):
        """
        Returns the document frequency of the given term (that is,
        how many documents the term appears in).
        """
        
        if isinstance(fieldnum, basestring):
            fieldnum = self.fieldname_to_num(fieldnum)
            
        if (fieldnum, text) not in self:
            return 0
        
        return self._term_info(fieldnum, text)[1]
    
    def term_count(self, fieldnum, text):
        """
        Returns the total number of instances of the given term
        in the index.
        """
        
        if isinstance(fieldnum, basestring):
            fieldnum = self.fieldname_to_num(fieldnum)
        
        if (fieldnum, text) not in self:
            return 0
        
        return self._term_info(fieldnum, text)[2]
    
    # Posting retrieval methods
    
    def postings(self, fieldnum, text, exclude_docs = None):
        """
        Yields raw (docnum, data) tuples for each document containing
        the current term. This is useful if you simply want to know
        which documents contain the current term. Use weights() or
        positions() if you need to term weight or positions in each
        document.
        
        exclude_docs can be a set of document numbers to ignore. This
        is used by queries to skip documents that have already been
        eliminated from consideration.
        """
        
        is_deleted = self.segment.is_deleted
        
        # The field object is actually responsible for parsing the
        # posting data from disk.
        readfn = self.schema.by_number[fieldnum].read_postvalue
        
        for docnum, data in self.term_table.postings((fieldnum, text), readfn = readfn):
            if not is_deleted(docnum)\
               and (exclude_docs is None or docnum not in exclude_docs):
                yield docnum, data
    
    def weights(self, fieldnum, text, exclude_docs = None, boost = 1.0):
        """
        Yields (docnum, term_weight) tuples for each document containing
        the current term. The current field must have stored term weights
        for this to work.
        
        exclude_docs can be a set of document numbers to ignore. This
        is used by queries to skip documents that have already been
        eliminated from consideration.
        boost is a factor by which to multiply each weight.
        """
        
        field = self.schema.by_number[fieldnum]
        for docnum, data in self.postings(fieldnum, text, exclude_docs = exclude_docs):
            yield (docnum, field.data_to_weight(data) * boost)

    def positions(self, fieldnum, text, exclude_docs = None):
        """
        Yields (docnum, [positions]) tuples for each document containing
        the current term. The current field must have stored positions
        for this to work.
        
        exclude_docs can be a set of document numbers to ignore. This
        is used by queries to skip documents that have already been
        eliminated from consideration.
        """
        
        field = self.schema.by_number[fieldnum]
        for docnum, data in self.postings(fieldnum, text, exclude_docs = exclude_docs):
            yield (docnum, field.data_to_positions(data))

    def position_boosts(self, fieldnum, text, exclude_docs = None):
        """
        Yields (docnum, [(position, boost)]) tuples for each document containing
        the current term. The current field must have stored positions and
        position boosts for this to work.
        
        exclude_docs can be a set of document numbers to ignore. This
        is used by queries to skip documents that have already been
        eliminated from consideration.
        """
        
        field = self.schema.by_number[fieldnum]
        for docnum, data in self.postings(fieldnum, text, exclude_docs = exclude_docs):
            yield (docnum, field.data_to_position_boosts(data))
    
    # Convenience methods
    
    def expand_prefix(self, fieldname, prefix):
        """
        Yields terms in the given field that start with the given prefix.
        """
        
        fieldnum = self.fieldname_to_num(fieldname)
        for fn, t, _, _ in self.iter_from(fieldnum, prefix):
            if fn != fieldnum or not t.startswith(prefix):
                return
            yield t
    
    def all_terms(self):
        """
        Yields (fieldname, text) tuples for every term in the index.
        """
        
        current_fieldnum = None
        current_fieldname = None
        for fn, t, _, _ in self:
            if fn != current_fieldnum:
                current_fieldnum = fn
                current_fieldname = self.schema.number_to_name(fn)
            yield (current_fieldname, t)
    
    def field_words(self, fieldnum):
        """
        Yields all tokens in the given field.
        """
        
        if isinstance(fieldnum, basestring):
            fieldnum = self.schema.name_to_number(fieldnum)
        
        for fn, t, _, _ in self.iter_from(fieldnum, ''):
            if fn != fieldnum:
                return
            yield t
        
    def list_substring(self, fieldname, substring):
        for text in self.field_words(fieldname):
            if text.find(substring) > -1:
                yield text
    

class MultiTermReader(TermReader):
    """
    Do not instantiate this object directly. Instead use Index.term_reader().
    
    Reads term information by aggregating the results from
    multiple segments.
    
    Each MultiTermReader represents (number of segments * 2) open files.
    Be sure to close() the reader when you're finished with it.
    """
    
    def __init__(self, term_readers, doc_offsets):
        self.term_readers = term_readers
        self.schema = term_readers[0].schema
        self.doc_offsets = doc_offsets
        self.is_closed = False
    
    def close(self):
        """
        Closes the open files associated with this reader.
        """
        
        for tr in self.term_readers:
            tr.close()
        self.is_closed = True
    
    def __contains__(self, term):
        return any(tr.__contains__(term) for tr in self.term_readers)
    
    def doc_frequency(self, fieldnum, text):
        if (fieldnum, text) not in self:
            return 0
        
        return sum(r.doc_frequency(fieldnum, text) for r in self.term_readers)
    
    def term_count(self, fieldnum, text):
        if (fieldnum, text) not in self:
            return 0
        
        return sum(r.term_count(fieldnum, text) for r in self.term_readers)
    
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
            while current[0][0] == fnum and current[0][1] == text:
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
            
    def __iter__(self):
        return self._merge_iters([iter(r) for r in self.term_readers])
    
    def iter_from(self, fieldnum, text):
        return self._merge_iters([r.iter_from(fieldnum, text) for r in self.term_readers])
    
    def postings(self, fieldnum, text, exclude_docs = set()):
        """
        Yields raw (docnum, data) tuples for each document containing
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


if __name__ == '__main__':
    import index
    ix = index.open_dir("../kinobenchindex")
    tr = ix.term_reader()
    c = 0
    for x in tr:
        c += 1
    print c












    
    
    