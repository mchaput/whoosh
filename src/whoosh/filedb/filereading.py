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
from whoosh.filedb.filetables import (FileTableReader, StoredFieldReader,
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
        self.schema = self.segment.schema
        
        # Field names to code numbers
        self._name_to_num = self.segment.fieldmap
        # Code numbers to field names
        self._num_to_name = dict((num, name) for name, num
                                 in self.segment.fieldmap.iteritems())
        
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
        self.storedfields = StoredFieldReader(sf, self.schema.stored_field_names())
        
        # Field length file
        self.fieldlengths = None
        scorables = self.schema.scorable_field_names()
        if scorables:
            flf = storage.open_file(segment.fieldlengths_filename)
            self.fieldlengths = LengthReader(flf, segment.doc_count_all())
        
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
        # Change the first item from a field name to a number using the
        # segment's field map
        term = (self._name_to_num[term[0]], term[1])
        return term in self.termsindex

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

    def field(self, fieldid):
        return self.schema[fieldid]

    def scorable(self, fieldid):
        return self.schema[fieldid].scorable
    
    def scorable_field_names(self):
        return self.schema.scorable_field_names()
    
    def format(self, fieldid):
        return self.schema[fieldid].format
    
    def vector_format(self, fieldid):
        return self.schema[fieldid].vector

    @protected
    def stored_fields(self, docnum):
        return self.storedfields[docnum]

    @protected
    def all_stored_fields(self):
        is_deleted = self.segment.is_deleted
        sf = self.stored_fields
        for docnum in xrange(self.segment.doc_count_all()):
            if not is_deleted(docnum):
                yield sf(docnum)

    def field_length(self, fieldid):
        return self.segment.field_length(fieldid)

    @protected
    def doc_field_length(self, docnum, fieldid, default=0):
        if self.fieldlengths is None: return default
        return self.fieldlengths.get(docnum, fieldid, default=default)

    def max_field_length(self, fieldid):
        return self.segment.max_field_length(fieldid)

    @protected
    def has_vector(self, docnum, fieldid):
        self._open_vectors()
        fnum = self._name_to_num[fieldid]
        return (docnum, fnum) in self.vectorindex

    @protected
    def __iter__(self):
        current_fnum = None
        fieldname = None
        for (fnum, t), (totalfreq, _, postcount) in self.termsindex:
            if fnum != current_fnum:
                fieldname = self._num_to_name[fnum]
                current_fnum = fnum
            yield (fieldname, t, postcount, totalfreq)

    @protected
    def iter_from(self, fieldid, text):
        current_fnum = None
        fieldname = None
        fieldnum = self._name_to_num[fieldid]
        for (fnum, t), (totalfreq, _, postcount) in self.termsindex.items_from((fieldnum, text)):
            if fnum != current_fnum:
                fieldname = self._num_to_name[fnum]
                current_fnum = fnum
            yield (fieldname, t, postcount, totalfreq)

    @protected
    def _term_info(self, fieldid, text):
        try:
            fnum = self._name_to_num[fieldid]
            return self.termsindex[(fnum, text)]
        except KeyError:
            raise TermNotFound("%s:%r" % (fieldid, text))

    def doc_frequency(self, fieldid, text):
        try:
            return self._term_info(fieldid, text)[2]
        except TermNotFound:
            return 0

    def frequency(self, fieldid, text):
        try:
            return self._term_info(fieldid, text)[0]
        except TermNotFound:
            return 0

    def lexicon(self, fieldid):
        # The base class has a lexicon() implementation that uses iter_from()
        # and throws away the value, but overriding to use
        # FileTableReader.keys_from() is much, much faster.

        return self.expand_prefix(fieldid, '')

    @protected
    def expand_prefix(self, fieldid, prefix):
        # The base class has an expand_prefix() implementation that uses
        # iter_from() and throws away the value, but overriding to use
        # FileTableReader.keys_from() is much, much faster.

        fnum = self._name_to_num[fieldid]
        for fn, t in self.termsindex.keys_from((fnum, prefix)):
            if fn != fnum or not t.startswith(prefix):
                return
            yield t

    def postings(self, fieldid, text, exclude_docs=frozenset(), scorefns=None):
        format = self.format(fieldid)

        try:
            fnum = self._name_to_num[fieldid]
            offset = self.termsindex[(fnum, text)][1]
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
        vformat = self.vector_format(fieldid)
        if not vformat:
            raise Exception("No vectors are stored for field %r" % fieldid)
        
        self._open_vectors()
        fnum = self._name_to_num[fieldid]
        offset = self.vectorindex.get((docnum, fnum))
        if offset is None:
            raise Exception("No vector found"
                            " for document %s field %r" % (docnum, fieldid))
        
        return FilePostingReader(self.vpostfile, offset, vformat, stringids=True)

















