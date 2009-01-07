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

from whoosh import util
from whoosh.fields import FieldConfigurationError

# Exceptions

class TermNotFound(Exception):
    pass
class UnknownFieldError(Exception):
    pass

# Utility classes

#class FifoCache(object):
#    """
#    Simple FIFO cache wrapping a dictionary. Only allows integers as keys.
#    """
#    
#    def __init__(self, size = 20):
#        assert size >= 2
#        self.size = size
#        self.lst = array("i")
#        self.dct = {}
#        
#    def __contains__(self, key):
#        return key in self.dct
#    
#    def __getitem__(self, key):
#        return self.dct[key]
#        
#    def __setitem__(self, key, value):
#        lst = self.lst
#        dct = self.dct
#        
#        if key in self:
#            del self[key]
#
#        lst.append(key)
#        dct[key] = value
#
#        if len(lst) > self.size:
#            del dct[lst.pop(0)]
        
# Reader classes

class DocReader(object, util.ClosableMixin):
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
        
        self.doclength_records = storage.open_table(segment.doclen_filename)
        self.docs_table = storage.open_table(segment.docs_filename)
        #self.cache = FifoCache()
        
        self.vector_table = None
        self.is_closed = False
    
    #def _setup_scorable_indices(self):
    #    # Create a map from field number to its position in the
    #    # list of fields that store length information.
    #    self.scorable_indices = dict((n, i) for i, n
    #                               in enumerate(self.schema.scorable_fields()))
    
    def _open_vectors(self):
        if not self.vector_table:
            self.vector_table = self.storage.open_table(self.segment.vector_filename,
                                                        postings = True)
    
    def close(self):
        """
        Closes the open files associated with this reader.
        """
        
        self.doclength_records.close()
        self.docs_table.close()
        if self.vector_table:
            self.vector_table.close()
        self.is_closed = True
    
    def fieldname_to_num(self, fieldname):
        """
        Returns the field number corresponding to the given field name.
        """
        if fieldname in self.schema:
            return self.schema.name_to_number(fieldname)
        else:
            raise UnknownFieldError(fieldname)
    
    def doc_count_all(self):
        """
        Returns the total number of documents, DELETED OR UNDELETED,
        in this reader.
        """
        return self.segment.doc_count_all()
    
    def doc_count(self):
        """
        Returns the total number of UNDELETED documents in this reader.
        """
        return self.segment.doc_count()
    
    def field_length(self, fieldnum):
        """
        Returns the total number of terms in the given field.
        """
        
        if isinstance(fieldnum, basestring):
            fieldnum = self.fieldname_to_num(fieldnum)
        return self.segment.field_length(fieldnum)
    
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
    
    def vector(self, docnum, fieldnum):
        """
        Returns a sequence of raw (text, data) tuples representing
        the term vector for the given document and field.
        """
        
        self._open_vectors()
        readfn = self.vector_format(fieldnum).read_postvalue
        return self.vector_table.postings((docnum, fieldnum), readfn)
    
    def vector_as(self, docnum, fieldnum, astype):
        """
        Returns a sequence of interpreted (text, data) tuples
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
        
        xform = getattr(format, "data_to_" + astype)
        for text, data in self.vector(docnum, fieldnum):
            yield (text, xform(data))
    
    def _doc_info(self, docnum, key):
        #cache = self.cache
        #if docnum in cache:
        #    return cache[docnum]
        #else:
        #    di = self.doclength_records[docnum]
        #    cache[docnum] = di
        #    return di
        return self.doclength_records[(docnum, key)]
    
    def doc_length(self, docnum):
        """
        Returns the total number of terms in a given document.
        This is used by some scoring algorithms.
        """
        
        #return self._doc_info(docnum)[0]
        return self._doc_info(docnum, -1)
    
    def unique_count(self, docnum):
        """
        Returns the number of UNIQUE terms in a given document.
        This is used by some scoring algorithms.
        """
        #return self._doc_info(docnum)[1]
        return self._doc_info(docnum, -2)
    
    def doc_field_length(self, docnum, fieldnum):
        """
        Returns the number of terms in the given field in the
        given document. This is used by some scoring algorithms.
        """
        
        if isinstance(fieldnum, basestring):
            fieldnum = self.fieldname_to_num(fieldnum)
        
        if fieldnum not in self._scorable_fields:
            raise FieldConfigurationError("Field %r does not store lengths" % fieldnum)
            
        return self._doc_info(docnum, fieldnum)
    
    def __getitem__(self, docnum):
        """
        Returns the stored fields for the given document.
        """
        return self.docs_table[docnum]
    
    def __iter__(self):
        """
        Yields the stored fields for all documents.
        """
        
        is_deleted = self.segment.is_deleted
        for docnum in xrange(0, self.segment.max_doc):
            if not is_deleted(docnum):
                yield self.docs_table[docnum]


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
    
    def __init__(self, storage, segments, schema):
        self.doc_readers = [DocReader(storage, s, schema)
                            for s in segments]
        self.doc_offsets = segments.doc_offsets()
        self.schema = schema
        self._scorable_fields = self.schema.scorable_fields()
        self.is_closed = False
        
    def close(self):
        """
        Closes the open files associated with this reader.
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
    
    def _document_segment(self, docnum):
        return bisect_right(self.doc_offsets, docnum) - 1
    
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
    
    def __getitem__(self, docnum):
        segmentnum, segmentdoc = self._segment_and_docnum(docnum)
        return self.doc_readers[segmentnum].__getitem__(segmentdoc)
    
    def __iter__(self):
        for reader in self.doc_readers:
            for result in reader:
                yield result


class TermReader(object, util.ClosableMixin):
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
        
        self.term_table = storage.open_table(segment.term_filename, postings = True)
        self.is_closed = False
        
    def fieldname_to_num(self, fieldname):
        """
        Returns the field number corresponding to the given field name.
        """
        if fieldname in self.schema:
            return self.schema.name_to_number(fieldname)
        else:
            raise UnknownFieldError(fieldname)
    
    def format(self, fieldname):
        """
        Returns the Format object corresponding to the given field name.
        """
        if fieldname in self.schema:
            return self.schema.field_by_name(fieldname).format
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
            return self.term_table[(fieldnum, text)]
        except KeyError:
            raise TermNotFound("%s:%r" % (fieldnum, text))
    
    def __iter__(self):
        """
        Yields (fieldnum, token, docfreq, indexfreq) tuples for
        each term in the reader, in lexical order.
        """
        
        tt = self.term_table
        for (fn, t), termcount in tt:
            yield (fn, t, tt.posting_count((fn, t)), termcount)
    
    def from_(self, fieldnum, text):
        tt = self.term_table
        postingcount = tt.posting_count
        for (fn, t), termcount in tt.from_((fieldnum, text)):
            yield (fn, t, postingcount((fn, t)), termcount)
    
    def __contains__(self, term):
        fieldnum = term[0]
        if isinstance(fieldnum, basestring):
            term = (self.schema.name_to_number(fieldnum), term[1])
            
        return term in self.term_table
    
    def __enter__(self): pass
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
    
    def doc_frequency(self, fieldnum, text):
        """
        Returns the document frequency of the given term (that is,
        how many documents the term appears in).
        """
        
        if isinstance(fieldnum, basestring):
            fieldnum = self.fieldname_to_num(fieldnum)
            
        if (fieldnum, text) not in self:
            return 0
        
        return self.term_table.posting_count((fieldnum, text))
    
    def term_count(self, fieldnum, text):
        """
        Returns the total number of instances of the given term
        in the corpus.
        """
        
        if isinstance(fieldnum, basestring):
            fieldnum = self.fieldname_to_num(fieldnum)
        
        if (fieldnum, text) not in self:
            return 0
        
        return self.term_table[(fieldnum, text)]
    
    def doc_count_all(self):
        """
        Returns the total number of documents, DELETED OR UNDELETED,
        in this reader.
        """
        return self.segment.doc_count_all()
    
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
        
        # The format object is actually responsible for parsing the
        # posting data from disk.
        readfn = self.schema.field_by_number(fieldnum).format.read_postvalue
        
        for docnum, data in self.term_table.postings((fieldnum, text), readfn = readfn):
            if not is_deleted(docnum)\
               and (exclude_docs is None or docnum not in exclude_docs):
                yield docnum, data
    
    def postings_as(self, fieldnum, text, astype, exclude_docs = None):
        format = self.schema.field_by_number(fieldnum).format
        
        if not format.supports(astype):
            raise FieldConfigurationError("Field %r format does not support %r" % (self.schema.name_to_number(fieldnum),
                                                                                   astype))
        
        xform = getattr(format, "data_to_" + astype)
        for docnum, data in self.postings(fieldnum, text, exclude_docs = exclude_docs):
            yield (docnum, xform(data))
    
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
        
        format = self.schema.field_by_number(fieldnum).format
        xform = format.data_to_weight
        for docnum, data in self.postings(fieldnum, text, exclude_docs = exclude_docs):
            yield (docnum, xform(data) * boost)

    def positions(self, fieldnum, text, exclude_docs = None):
        """
        Yields (docnum, [positions]) tuples for each document containing
        the current term. The current field must have stored positions
        for this to work.
        
        exclude_docs can be a set of document numbers to ignore. This
        is used by queries to skip documents that have already been
        eliminated from consideration.
        """
        
        return self.postings_as(fieldnum, text, "positions", exclude_docs = exclude_docs)

    def expand_prefix(self, fieldname, prefix):
        """
        Yields terms in the given field that start with the given prefix.
        """
        
        fieldnum = self.fieldname_to_num(fieldname)
        for fn, t, _, _ in self.from_(fieldnum, prefix):
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
    
    def iter_field(self, fieldnum):
        if isinstance(fieldnum, basestring):
            fieldnum = self.schema.name_to_number(fieldnum)
        
        for fn, t, docfreq, freq in self.from_(fieldnum, ''):
            if fn != fieldnum:
                return
            yield t, docfreq, freq
    
    def lexicon(self, fieldnum):
        """
        Yields all tokens in the given field.
        """
        
        if isinstance(fieldnum, basestring):
            fieldnum = self.schema.name_to_number(fieldnum)
        
        for t, _, _ in self.iter_field(fieldnum):
            yield t
    
    def most_frequent_terms(self, fieldnum, number = 5):
        return nlargest(number,
                        ((indexfreq, token)
                         for token, _, indexfreq
                         in self.iter_field(fieldnum)))
    
    def list_substring(self, fieldname, substring):
        for text in self.lexicon(fieldname):
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
    
    def __init__(self, storage, segments, schema):
        self.term_readers = [TermReader(storage, s, schema)
                             for s in segments]
        self.doc_offsets = segments.doc_offsets()
        self.schema = schema
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
        # TermReader.from_()).
        
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
            
    def __iter__(self):
        return self._merge_iters([iter(r) for r in self.term_readers])
    
    def from_(self, fieldnum, text):
        return self._merge_iters([r.from_(fieldnum, text) for r in self.term_readers])
    
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












    
    
    