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

from whoosh.filedb.filepostings import FilePostingReader
from whoosh.filedb.filetables import (TermIndexReader, StoredFieldReader,
                                      LengthReader, TermVectorReader)
from whoosh.matching import ExcludeMatcher
from whoosh.reading import IndexReader, TermNotFound
from whoosh.util import protected


# Reader class

class SegmentReader(IndexReader):
    def __init__(self, storage, schema, segment, generation=None):
        self.storage = storage
        self.schema = schema
        self.segment = segment
        self._generation = generation
        
        # Term index
        tf = storage.open_file(segment.termsindex_filename)
        self.termsindex = TermIndexReader(tf)
        
        # Term postings file, vector index, and vector postings: lazy load
        self.postfile = None
        self.vectorindex = None
        self.vpostfile = None
        
        # Stored fields file
        sf = storage.open_file(segment.storedfields_filename, mapped=False)
        self.storedfields = StoredFieldReader(sf)
        
        # Field length file
        self.fieldlengths = None
        if self.schema.has_scorable_fields():
            flf = storage.open_file(segment.fieldlengths_filename)
            self.fieldlengths = LengthReader(flf, segment.doc_count_all())
        
        # Copy methods from underlying segment
        self.has_deletions = segment.has_deletions
        self.is_deleted = segment.is_deleted
        self.doc_count = segment.doc_count
        
        # Postings file
        self.postfile = self.storage.open_file(segment.termposts_filename,
                                               mapped=False)
        
        self.dc = segment.doc_count_all()
        assert self.dc == self.storedfields.length
        
        self.is_closed = False
        self._sync_lock = Lock()

    def _open_vectors(self):
        if self.vectorindex: return
        
        storage, segment = self.storage, self.segment
        
        # Vector index
        vf = storage.open_file(segment.vectorindex_filename)
        self.vectorindex = TermVectorReader(vf)
        
        # Vector postings file
        self.vpostfile = storage.open_file(segment.vectorposts_filename,
                                           mapped=False)
    
    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, self.segment)

    @protected
    def __contains__(self, term):
        return term in self.termsindex

    def generation(self):
        return self._generation

    def close(self):
        self.storedfields.close()
        self.termsindex.close()
        if self.postfile:
            self.postfile.close()
        if self.vectorindex:
            self.vectorindex.close()
        #if self.fieldlengths:
        #    self.fieldlengths.close()
        self.is_closed = True

    def doc_count_all(self):
        return self.dc

    def field(self, fieldname):
        return self.schema[fieldname]

    def scorable(self, fieldname):
        return self.schema[fieldname].scorable
    
    def scorable_names(self):
        return self.schema.scorable_names()
    
    def vector_names(self):
        return self.schema.vector_names()
    
    def format(self, fieldname):
        return self.schema[fieldname].format
    
    def vector_format(self, fieldname):
        return self.schema[fieldname].vector

    @protected
    def stored_fields(self, docnum):
        schema = self.schema
        return dict(item for item
                    in self.storedfields[docnum].iteritems()
                    if item[0] in schema)

    @protected
    def all_stored_fields(self):
        is_deleted = self.segment.is_deleted
        sf = self.stored_fields
        for docnum in xrange(self.segment.doc_count_all()):
            if not is_deleted(docnum):
                yield sf(docnum)

    def field_length(self, fieldname):
        return self.segment.field_length(fieldname)

    @protected
    def doc_field_length(self, docnum, fieldname, default=0):
        if self.fieldlengths is None: return default
        return self.fieldlengths.get(docnum, fieldname, default=default)

    def max_field_length(self, fieldname):
        return self.segment.max_field_length(fieldname)

    @protected
    def has_vector(self, docnum, fieldname):
        self._open_vectors()
        return (docnum, fieldname) in self.vectorindex

    @protected
    def __iter__(self):
        schema = self.schema
        for (fieldname, t), (totalfreq, _, postcount) in self.termsindex:
            if fieldname not in schema:
                continue
            yield (fieldname, t, postcount, totalfreq)

    def _test_field(self, fieldname):
        if fieldname not in self.schema:
            raise TermNotFound("No field %r" % fieldname)
        if self.schema[fieldname].format is None:
            raise TermNotFound("Field %r is not indexed" % fieldname)

    @protected
    def iter_from(self, fieldname, text):
        schema = self.schema
        self._test_field(fieldname)
        for (fn, t), (totalfreq, _, postcount) in self.termsindex.items_from((fieldname, text)):
            if fn not in schema:
                continue
            yield (fn, t, postcount, totalfreq)

    @protected
    def _term_info(self, fieldname, text):
        self._test_field(fieldname)
        try:
            return self.termsindex[(fieldname, text)]
        except KeyError:
            raise TermNotFound("%s:%r" % (fieldname, text))

    def doc_frequency(self, fieldname, text):
        self._test_field(fieldname)
        try:
            return self._term_info(fieldname, text)[2]
        except TermNotFound:
            return 0

    def frequency(self, fieldname, text):
        self._test_field(fieldname)
        try:
            return self._term_info(fieldname, text)[0]
        except TermNotFound:
            return 0

    def lexicon(self, fieldname):
        # The base class has a lexicon() implementation that uses iter_from()
        # and throws away the value, but overriding to use
        # FileTableReader.keys_from() is much, much faster.

        self._test_field(fieldname)
        return self.expand_prefix(fieldname, '')

    @protected
    def expand_prefix(self, fieldname, prefix):
        # The base class has an expand_prefix() implementation that uses
        # iter_from() and throws away the value, but overriding to use
        # FileTableReader.keys_from() is much, much faster.

        self._test_field(fieldname)
        for fn, t in self.termsindex.keys_from((fieldname, prefix)):
            if fn != fieldname or not t.startswith(prefix):
                return
            yield t

    def postings(self, fieldname, text, exclude_docs=frozenset(), scorer=None):
        self._test_field(fieldname)
        format = self.format(fieldname)
        try:
            offset = self.termsindex[(fieldname, text)][1]
        except KeyError:
            raise TermNotFound("%s:%r" % (fieldname, text))

        if self.segment.deleted and exclude_docs:
            exclude_docs = self.segment.deleted | exclude_docs
        elif self.segment.deleted:
            exclude_docs = self.segment.deleted

        postreader = FilePostingReader(self.postfile, offset, format,
                                       scorer=scorer, fieldname=fieldname,
                                       text=text)
        if exclude_docs:
            postreader = ExcludeMatcher(postreader, exclude_docs)
            
        return postreader
    
    def vector(self, docnum, fieldname):
        if fieldname not in self.schema:
            raise TermNotFound("No  field %r" % fieldname)
        vformat = self.vector_format(fieldname)
        if not vformat:
            raise Exception("No vectors are stored for field %r" % fieldname)
        
        self._open_vectors()
        offset = self.vectorindex.get((docnum, fieldname))
        if offset is None:
            raise Exception("No vector found"
                            " for document %s field %r" % (docnum, fieldname))
        
        return FilePostingReader(self.vpostfile, offset, vformat, stringids=True)

        















