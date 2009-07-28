#===============================================================================
# Copyright 2009 Matt Chaput
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

from threading import Lock

from whoosh.util import protected
from whoosh.fields import FieldConfigurationError, UnknownFieldError
from whoosh.reading import DocReader, TermReader, TermNotFound
from whoosh.filedb.filetables import open_docs_table, open_term_table, open_vector_table


# Reader classes

class FileDocReader(DocReader):
    def __init__(self, storage, segment, schema):
        self.storage = storage
        self.segment = segment
        self.schema = schema
        self._scorable_fields = schema.scorable_fields()
        
        from whoosh.filedb.filewriting import DOCLENGTH_TYPE
        recordformat = "<" + DOCLENGTH_TYPE * len(self._scorable_fields)
        self.doclength_table = storage.open_records(segment.doclen_filename, recordformat)
        self.docs_table = open_docs_table(self.storage, segment, schema)
        #self.cache = FifoCache()
        
        self.vector_table = None
        self.is_closed = False
        self._sync_lock = Lock()
        
        self._fieldnum_to_pos = dict((fieldnum, i) for i, fieldnum
                                     in enumerate(schema.scorable_fields()))
    
    def _open_vectors(self):
        if not self.vector_table:
            self.vector_table = open_vector_table(self.storage, self.segment)
    
    @protected
    def __getitem__(self, docnum):
        return self.docs_table.get(docnum)
    
    @protected
    def __iter__(self):
        is_deleted = self.segment.is_deleted
        for docnum in xrange(0, self.segment.max_doc):
            if not is_deleted(docnum):
                yield self.docs_table.get(docnum)
    
    def close(self):
        self.doclength_table.close()
        self.docs_table.close()
        if self.vector_table:
            self.vector_table.close()
        self.is_closed = True
    
    def doc_count_all(self):
        return self.segment.doc_count_all()
    
    def doc_count(self):
        return self.segment.doc_count()
    
    def field_length(self, fieldid):
        fieldid = self.schema.to_number(fieldid)
        return self.segment.field_length(fieldid)
    
    @protected
    def doc_field_length(self, docnum, fieldid):
        fieldid = self.schema.to_number(fieldid)
        if fieldid not in self._scorable_fields:
            raise FieldConfigurationError("Field %r does not store lengths" % fieldid)
        
        pos = self._fieldnum_to_pos[fieldid]
        return self.doclength_table.get(docnum, pos)
    
    @protected
    def doc_field_lengths(self, docnum):
        return self.doclength_table.get_record(docnum)
    
    @protected
    def vector(self, docnum, fieldid):
        """Yields a sequence of raw (text, data) tuples representing
        the term vector for the given document and field.
        """
        
        self._open_vectors()
        fieldnum = self.schema.to_number(fieldid)
        readfn = self.vector_format(fieldnum).read_postvalue
        return self.vector_table.postings((docnum, fieldnum), readfn)
    
    def vector_as(self, docnum, fieldnum, astype):
        format = self.vector_format(fieldnum)
        
        if format is None:
            raise FieldConfigurationError("Field %r is not vectored" % self.schema.number_to_name(fieldnum))
        elif not format.supports(astype):
            raise FieldConfigurationError("Field %r does not support %r" % (self.schema.number_to_name(fieldnum),
                                                                            astype))
        
        interpreter = format.interpreter(astype)
        for text, data in self.vector(docnum, fieldnum):
            yield (text, interpreter(data))
    

class FileTermReader(TermReader):
    """
    Do not instantiate this object directly. Instead use Index.term_reader().
    
    Reads term information from a segment.
    
    Each TermReader represents two open files. Remember to close() the reader when
    you're done with it.
    """
    
    def __init__(self, storage, segment, schema):
        """
        :param storage: The storage object in which the segment resides.
        :param segment: The segment to read from.
        :param schema: The index's schema object.
        """
        
        self.segment = segment
        self.schema = schema
        
        self.term_table = open_term_table(storage, segment)
        self.is_closed = False
        self._sync_lock = Lock()
    
    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, self.segment)
    
    @protected
    def __iter__(self):
        tt = self.term_table
        for (fn, t), termcount in tt:
            yield (fn, t, tt.posting_count((fn, t)), termcount)
    
    @protected
    def __contains__(self, term):
        return (self.schema.to_number(term[0]), term[1]) in self.term_table
    
    def close(self):
        self.term_table.close()
        self.is_closed = True
    
    def format(self, fieldname):
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
        fieldid = self.schema.to_number(fieldid)
        if (fieldid, text) not in self.term_table:
            return 0
        return self.term_table.posting_count((fieldid, text))
    
    @protected
    def frequency(self, fieldid, text):
        fieldid = self.schema.to_number(fieldid)
        if (fieldid, text) not in self.term_table:
            return 0
        return self.term_table.get((fieldid, text))
    
    def doc_count_all(self):
        return self.segment.doc_count_all()
    
    @protected
    def iter_from(self, fieldnum, text):
        tt = self.term_table
        postingcount = tt.posting_count
        for (fn, t), termcount in tt.items_from((fieldnum, text)):
            yield (fn, t, postingcount((fn, t)), termcount)
    
    @protected
    def lexicon(self, fieldid):
        # The base class has a lexicon() implementation that uses
        # iter_from() and throws away the value, but overriding to
        # use FileTableReader.keys_from() is much, much faster.
        
        tt = self.term_table
        fieldid = self.schema.to_number(fieldid)
        for fn, t in tt.keys_from((fieldid, '')):
            if fn != fieldid:
                return
            yield t
    
    @protected
    def expand_prefix(self, fieldid, prefix):
        # The base class has an expand_prefix() implementation that uses
        # iter_from() and throws away the value, but overriding to
        # use FileTableReader.keys_from() is much, much faster.
        
        tt = self.term_table
        fieldid = self.schema.to_number(fieldid)
        for fn, t in tt.keys_from((fieldid, prefix)):
            if fn != fieldid or not t.startswith(prefix):
                return
            yield t
    
    # Posting retrieval methods
    
    @protected
    def postings(self, fieldnum, text, exclude_docs = None):
        is_deleted = self.segment.is_deleted
        no_exclude = exclude_docs is None
        
        # The format object is actually responsible for parsing the
        # posting data from disk.
        readfn = self.schema.field_by_number(fieldnum).format.read_postvalue
        
        for docnum, data in self.term_table.postings((fieldnum, text), readfn = readfn):
            if not is_deleted(docnum)\
               and (no_exclude or docnum not in exclude_docs):
                yield docnum, data
    
    
















    
    
    