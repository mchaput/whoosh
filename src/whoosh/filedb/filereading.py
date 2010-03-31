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
from marshal import loads

from whoosh.fields import FieldConfigurationError
from whoosh.filedb.filepostings import FilePostingReader
from whoosh.filedb.filetables import (FileTableReader, FileListReader,
                                      StructHashReader, LengthReader)
from whoosh.filedb import misc
from whoosh.matching import ExcludeMatcher
from whoosh.reading import IndexReader, TermNotFound
from whoosh.util import protected


# Reader class

class SegmentReader(IndexReader):
    def __init__(self, storage, segment):
        self.storage = storage
        self.segment = segment
        self.schema = segment.schema
        
        self.storedfieldnums = self.schema.stored_field_nums()
        self.storedfieldnames = self.schema.stored_field_names()

        # Term index
        tf = storage.open_file(segment.termsindex_filename)
        self.termsindex = FileTableReader(tf,
                                          keycoder=misc.encode_termkey,
                                          keydecoder=misc.decode_termkey,
                                          valuedecoder=misc.decode_terminfo)
        
        # Term postings file, vector index, and vector postings: lazy load
        self.postfile = None
        self.vectorindex = None
        self.vpostfile = None
        
        # Stored fields file
        sf = storage.open_file(segment.storedfields_filename, mapped=False)
        self.storedfields = FileListReader(sf, valuedecoder=loads)
        
        # Field length file
        self.fieldlengths = None
        scorables = schema.scorable_fields()
        if scorables:
            flf = storage.open_file(segment.fieldlengths_filename)
            self.fieldlengths = LengthReader.load(flf, segment.doc_count_all(),
                                                  scorables)
        
        # Copy methods from underlying segment
        self.has_deletions = segment.has_deletions
        self.is_deleted = segment.is_deleted
        self.doc_count = segment.doc_count
        
        self.dc = segment.doc_count_all()
        self.is_closed = False
        self._sync_lock = Lock()

    def _open_vectors(self):
        if self.vectorindex: return
        
        storage, segment = self.storage, self.segment
        
        # Vector index
        vf = storage.open_file(segment.vectorindex_filename)
        self.vectorindex = StructHashReader(vf, "!IH", "!I")
        
        # Vector postings file
        self.vpostfile = storage.open_file(segment.vectorposts_filename,
                                           mapped=False)
    
    def _open_postfile(self):
        if self.postfile: return
        self.postfile = self.storage.open_file(self.segment.termposts_filename,
                                               mapped=False)
    
    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, self.segment)

    @protected
    def __contains__(self, term):
        return (self.schema.to_number(term[0]), term[1]) in self.termsindex

    def close(self):
        self.storedfields.close()
        self.termsindex.close()
        if self.postfile:
            self.postfile.close()
        if self.vectorindex:
            self.vectorindex.close()
        if self.fieldlengths:
            self.fieldlengths.close()
        self.is_closed = True

    def doc_count_all(self):
        return self.dc

    @protected
    def stored_fields(self, docnum, numerickeys=False):
        if numerickeys:
            keys = self.storedfieldnums
        else:
            keys = self.storedfieldnames
        
        return dict(zip(keys, self.storedfields[docnum]))

    @protected
    def all_stored_fields(self, numerickeys=False):
        is_deleted = self.segment.is_deleted
        sf = self.stored_fields
        for docnum in xrange(self.segment.doc_count_all()):
            if not is_deleted(docnum):
                yield sf(docnum, numerickeys=numerickeys)

    def field_length(self, fieldnum):
        return self.segment.field_length(fieldnum)

    @protected
    def doc_field_length(self, docnum, fieldnum, default=0):
        if self.fieldlengths is None: return default
        return self.fieldlengths.get(docnum, fieldnum, default=default)

    def max_field_length(self, fieldnum):
        return self.segment.max_field_length(fieldnum)

    @protected
    def has_vector(self, docnum, fieldnum):
        self._open_vectors()
        return (docnum, fieldnum) in self.vectorindex

    @protected
    def __iter__(self):
        for (fn, t), (totalfreq, _, postcount) in self.termsindex:
            yield (fn, t, postcount, totalfreq)

    @protected
    def iter_from(self, fieldnum, text):
        tt = self.termsindex
        for (fn, t), (totalfreq, _, postcount) in tt.items_from((fieldnum, text)):
            yield (fn, t, postcount, totalfreq)

    @protected
    def _term_info(self, fieldnum, text):
        try:
            return self.termsindex[(fieldnum, text)]
        except KeyError:
            raise TermNotFound("%s:%r" % (fieldnum, text))

    def doc_frequency(self, fieldid, text):
        try:
            fieldnum = self.schema.to_number(fieldid)
            return self._term_info(fieldnum, text)[2]
        except TermNotFound:
            return 0

    def frequency(self, fieldid, text):
        try:
            fieldnum = self.schema.to_number(fieldid)
            return self._term_info(fieldnum, text)[0]
        except TermNotFound:
            return 0

    @protected
    def lexicon(self, fieldid):
        # The base class has a lexicon() implementation that uses iter_from()
        # and throws away the value, but overriding to use
        # FileTableReader.keys_from() is much, much faster.

        tt = self.termsindex
        fieldid = self.schema.to_number(fieldid)
        for fn, t in tt.keys_from((fieldid, '')):
            if fn != fieldid:
                return
            yield t

    @protected
    def expand_prefix(self, fieldid, prefix):
        # The base class has an expand_prefix() implementation that uses
        # iter_from() and throws away the value, but overriding to use
        # FileTableReader.keys_from() is much, much faster.

        tt = self.termsindex
        fieldid = self.schema.to_number(fieldid)
        for fn, t in tt.keys_from((fieldid, prefix)):
            if fn != fieldid or not t.startswith(prefix):
                return
            yield t

    def postings(self, fieldid, text, exclude_docs=frozenset(), scorefns=None):
        schema = self.schema
        fieldnum = schema.to_number(fieldid)
        format = schema[fieldnum].format

        try:
            offset = self.termsindex[(fieldnum, text)][1]
        except KeyError:
            raise TermNotFound("%s:%r" % (fieldid, text))

        if self.segment.deleted and exclude_docs:
            exclude_docs = self.segment.deleted | exclude_docs
        elif self.segment.deleted:
            exclude_docs = self.segment.deleted

        self._open_postfile()
        postreader = FilePostingReader(self.postfile, offset, format,
                                       scorefns=scorefns)
        if exclude_docs:
            postreader = ExcludeMatcher(postreader, exclude_docs)
        return postreader
    
    def vector(self, docnum, fieldid):
        schema = self.schema
        fieldnum = schema.to_number(fieldid)
        vformat = schema[fieldnum].vector
        if not vformat:
            raise Exception("No vectors are stored for field %r" % fieldid)
        
        self._open_vectors()
        offset = self.vectorindex.get((docnum, fieldnum))
        if offset is None:
            raise Exception("No vector found for document %s field %r" % (docnum, fieldid))
        
        return FilePostingReader(self.vpostfile, offset, vformat,
                                 stringids=True)

















