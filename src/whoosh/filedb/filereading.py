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

from whoosh.fields import FieldConfigurationError
from whoosh.filedb.filepostings import FilePostingReader
from whoosh.filedb.filetables import (FileTableReader, FileRecordReader,
                                      FileListReader, encode_termkey,
                                      decode_termkey, encode_vectorkey,
                                      decode_vectorkey, decode_terminfo,
                                      depickle, unpackint)
from whoosh.postings import Exclude
from whoosh.reading import IndexReader, TermNotFound
from whoosh.util import protected


# Convenience functions

def open_terms(storage, segment):
    termfile = storage.open_file(segment.term_filename)
    return FileTableReader(termfile,
                           keycoder=encode_termkey,
                           keydecoder=decode_termkey,
                           valuedecoder=decode_terminfo)

def open_doclengths(storage, segment, fieldcount):
    from whoosh.filedb.filewriting import DOCLENGTH_TYPE
    rformat = "!" + DOCLENGTH_TYPE * fieldcount
    recordfile = storage.open_file(segment.doclen_filename)
    return FileRecordReader(recordfile, rformat)

def open_storedfields(storage, segment, storedfieldnames):
    def dictifier(value):
        value = depickle(value)
        return dict(zip(storedfieldnames, value))
    listfile = storage.open_file(segment.docs_filename, mapped=False)
    return FileListReader(listfile, segment.doc_count_all(),
                          valuedecoder=dictifier)

def open_vectors(storage, segment):
    vectorfile = storage.open_file(segment.vector_filename)
    return FileTableReader(vectorfile, keycoder=encode_vectorkey,
                            keydecoder=decode_vectorkey,
                            valuedecoder=unpackint)


# Reader class

class SegmentReader(IndexReader):
    def __init__(self, storage, segment, schema):
        self.storage = storage
        self.segment = segment
        self.schema = schema

        self._scorable_fields = schema.scorable_fields()
        self._fieldnum_to_scorable_pos = dict((fnum, i) for i, fnum
                                              in enumerate(self._scorable_fields))

        self.termtable = open_terms(storage, segment)
        self.postfile = None
        self.docstable = open_storedfields(storage, segment,
                                           schema.stored_field_names())
        self.doclengths = None
        if self._scorable_fields:
            self.doclengths = open_doclengths(storage, segment,
                                              len(self._scorable_fields))

        self.has_deletions = segment.has_deletions
        self.is_deleted = segment.is_deleted
        self.doc_count = segment.doc_count
        self.doc_count_all = segment.doc_count_all

        self.vectortable = None
        self.is_closed = False
        self._sync_lock = Lock()

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, self.segment)

    @protected
    def __contains__(self, term):
        return (self.schema.to_number(term[0]), term[1]) in self.termtable

    def close(self):
        self.docstable.close()
        self.termtable.close()
        if self.postfile:
            self.postfile.close()
        if self.vectortable:
            self.vectortable.close()
        if self.doclengths:
            self.doclengths.close()
        self.is_closed = True

    def _open_vectors(self):
        if not self.vectortable:
            storage, segment = self.storage, self.segment
            self.vectortable = open_vectors(storage, segment)
            self.vpostfile = storage.open_file(segment.vectorposts_filename,
                                               mapped=False)

    def vector(self, docnum, fieldid):
        self._open_vectors()
        schema = self.schema
        fieldnum = schema.to_number(fieldid)
        vformat = schema[fieldnum].vector

        offset = self.vectortable[(docnum, fieldnum)]
        return FilePostingReader(self.vpostfile, offset, vformat,
                                 stringids=True)

    @protected
    def stored_fields(self, docnum):
        return self.docstable[docnum]

    @protected
    def all_stored_fields(self):
        is_deleted = self.segment.is_deleted
        for docnum in xrange(0, self.segment.doc_count_all()):
            if not is_deleted(docnum):
                yield self.docstable[docnum]

    def field_length(self, fieldid):
        fieldid = self.schema.to_number(fieldid)
        return self.segment.field_length(fieldid)

    @protected
    def doc_field_length(self, docnum, fieldid):
        fieldid = self.schema.to_number(fieldid)
        if fieldid not in self._scorable_fields:
            raise FieldConfigurationError("Field %r does not store lengths" % fieldid)

        pos = self._fieldnum_to_scorable_pos[fieldid]
        return self.doclengths.at(docnum, pos)

    @protected
    def doc_field_lengths(self, docnum):
        if not self.doclengths:
            return []
        return self.doclengths.record(docnum)

    @protected
    def has_vector(self, docnum, fieldnum):
        self._open_vectors()
        return (docnum, fieldnum) in self.vectortable

    @protected
    def __iter__(self):
        for (fn, t), (totalfreq, _, postcount) in self.termtable:
            yield (fn, t, postcount, totalfreq)

    @protected
    def iter_from(self, fieldnum, text):
        tt = self.termtable
        for (fn, t), (totalfreq, _, postcount) in tt.items_from((fieldnum, text)):
            yield (fn, t, postcount, totalfreq)

    @protected
    def _term_info(self, fieldnum, text):
        try:
            return self.termtable[(fieldnum, text)]
        except KeyError:
            raise TermNotFound("%s:%r" % (fieldnum, text))

    def doc_frequency(self, fieldid, text):
        try:
            fieldid = self.schema.to_number(fieldid)
            return self._term_info(fieldid, text)[2]
        except TermNotFound:
            return 0

    def frequency(self, fieldid, text):
        try:
            fieldid = self.schema.to_number(fieldid)
            return self._term_info(fieldid, text)[0]
        except TermNotFound:
            return 0

    @protected
    def lexicon(self, fieldid):
        # The base class has a lexicon() implementation that uses iter_from()
        # and throws away the value, but overriding to use
        # FileTableReader.keys_from() is much, much faster.

        tt = self.termtable
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

        tt = self.termtable
        fieldid = self.schema.to_number(fieldid)
        for fn, t in tt.keys_from((fieldid, prefix)):
            if fn != fieldid or not t.startswith(prefix):
                return
            yield t

    def postings(self, fieldid, text, exclude_docs=frozenset()):
        schema = self.schema
        fieldnum = schema.to_number(fieldid)
        format = schema[fieldnum].format

        try:
            totalfreq, offset, postcount = self.termtable[(fieldnum, text)] #@UnusedVariable
        except KeyError:
            raise TermNotFound("%s:%r" % (fieldid, text))

        if self.segment.deleted and exclude_docs:
            exclude_docs = self.segment.deleted | exclude_docs
        elif self.segment.deleted:
            exclude_docs = self.segment.deleted

        if not self.postfile:
            self.postfile = self.storage.open_file(self.segment.posts_filename,
                                                   mapped=False)
        postreader = FilePostingReader(self.postfile, offset, format)
        if exclude_docs:
            postreader = Exclude(postreader, exclude_docs)
        return postreader

















