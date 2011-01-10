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

from bisect import bisect_left
from heapq import nlargest
from threading import Lock

from whoosh.filedb.fieldcache import CacheSet
from whoosh.filedb.filepostings import FilePostingReader
from whoosh.filedb.filetables import (TermIndexReader, StoredFieldReader,
                                      LengthReader, TermVectorReader)
from whoosh.matching import FilterMatcher, ListMatcher
from whoosh.reading import IndexReader, TermNotFound
from whoosh.util import protected


# Reader class

class SegmentReader(IndexReader):
    def __init__(self, storage, schema, segment):
        self.storage = storage
        self.schema = schema
        self.segment = segment
        
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
        
        self.caches = CacheSet(self.storage, self.segment.name)
        
        self.is_closed = False
        self._sync_lock = Lock()

    def generation(self):
        return self.segment.generation

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
        
        # If a field cache happens to already be loaded for this field, use it
        # instead of loading the field values from disk
        if self.fieldcache_loaded(fieldname):
            fieldcache = self.fieldcache(fieldname)
            return fieldcache.texts
        
        return self.expand_prefix(fieldname, '')

    @protected
    def expand_prefix(self, fieldname, prefix):
        # The base class has an expand_prefix() implementation that uses
        # iter_from() and throws away the value, but overriding to use
        # FileTableReader.keys_from() is much, much faster.

        self._test_field(fieldname)

        if self.fieldcache_loaded(fieldname):
            texts = self.fieldcache(fieldname).texts
            i = bisect_left(texts, prefix)
            while i < len(texts) and texts[i].startswith(prefix):
                yield texts[i]
                i += 1
        else:
            for fn, t in self.termsindex.keys_from((fieldname, prefix)):
                if fn != fieldname or not t.startswith(prefix):
                    break
                yield t

    def first_ids(self, fieldname):
        self._test_field(fieldname)
        format = self.schema[fieldname].format
        
        for (fn, t), (totalfreq, offset, postcount) in self.termsindex.items_from((fieldname, '')):
            if fn != fieldname:
                break
            
            if isinstance(offset, (int, long)):
                postreader = FilePostingReader(self.postfile, offset, format)
                id = postreader.id()
            else:
                id = offset[0][0]
            
            yield (t, id)

    def first_id(self, fieldname, text):
        self._test_field(fieldname)
        format = self.schema[fieldname].format
        
        offset = self.termsindex[(fieldname, text)][1]
        if isinstance(offset, (int, long)):
            postreader = FilePostingReader(self.postfile, offset, format)
            return postreader.id()
        else:
            return offset[0][0]

    def postings(self, fieldname, text, scorer=None):
        self._test_field(fieldname)
        format = self.schema[fieldname].format
        try:
            offset = self.termsindex[(fieldname, text)][1]
        except KeyError:
            raise TermNotFound("%s:%r" % (fieldname, text))

        if isinstance(offset, (int, long)):
            postreader = FilePostingReader(self.postfile, offset, format,
                                           scorer=scorer, fieldname=fieldname,
                                           text=text)
        else:
            docids, weights, values, maxwol, minlength = offset
            postreader = ListMatcher(docids, weights, values, format, scorer,
                                     maxwol=maxwol, minlength=minlength)
        
        deleted = self.segment.deleted
        if deleted:
            postreader = FilterMatcher(postreader, deleted, exclude=True)
            
        return postreader
    
    def vector(self, docnum, fieldname):
        if fieldname not in self.schema:
            raise TermNotFound("No  field %r" % fieldname)
        vformat = self.schema[fieldname].vector
        if not vformat:
            raise Exception("No vectors are stored for field %r" % fieldname)
        
        self._open_vectors()
        offset = self.vectorindex.get((docnum, fieldname))
        if offset is None:
            raise Exception("No vector found"
                            " for document %s field %r" % (docnum, fieldname))
        
        return FilePostingReader(self.vpostfile, offset, vformat, stringids=True)

    def fieldcache(self, fieldname, save=True):
        return self.caches.get_cache(self, fieldname, save=save)
    
    def fieldcache_available(self, fieldname):
        return self.caches.is_cached(fieldname)
    
    def fieldcache_loaded(self, fieldname):
        return self.caches.is_loaded(fieldname)

    def fieldcache_create(self, fieldname, save=True, name=None, default=u''):
        self.caches.create_cache(self, fieldname, save=save, name=name,
                                 default=default)
    
    def sort_docs_by(self, fieldname, docnums, reverse=False):
        if isinstance(fieldname, (tuple, list)):
            # The "fieldname" is actually a sequence of field names to sort by
            fcs = [self.fieldcache(fn) for fn in fieldname]
            keyfn = lambda docnum: tuple(fc.order[docnum] for fc in fcs)
        else:
            fieldcache = self.fieldcache(fieldname)
            keyfn = fieldcache.order.__getitem__
        
        return sorted(docnums, key=keyfn, reverse=reverse)
    
    def key_sort_docs_by(self, fieldname, docnums, limit, reverse=False, offset=0):
        
        # We have to invert the keys to use nlargest (which is more efficient
        # than nsmallest), so if reverse is True we use key_for and if reverse
        # is False we use reverse_key_for
        
        if isinstance(fieldname, (tuple, list)):
            # The "fieldname" is actually a sequence of field names to sort by
            fcs = [self.fieldcache(fn) for fn in fieldname]
            
            if reverse:
                keyfn = lambda docnum: tuple(fc.key_for(docnum) for fc in fcs)
            else:
                keyfn = lambda docnum: tuple(fc.reverse_key_for(docnum)
                                             for fc in fcs)
        else:
            fc = self.fieldcache(fieldname)
            if reverse:
                keyfn = fc.key_for
            else:
                keyfn = fc.reverse_key_for
        
        if limit is None:
            return sorted([(keyfn(docnum), docnum + offset)
                           for docnum in docnums])
        else:
            return nlargest(limit, ((keyfn(docnum), docnum + offset)
                                    for docnum in docnums))
        
#    def group_docs_by(self, fieldname, docnums, counts=False):
#        fieldcache = self.caches.get_cache(self, fieldname)
#        return fieldcache.groups(docnums, counts=counts)
#    
#    def group_scored_docs_by(self, fieldname, scores_and_docnums, limit=None):
#        fieldcache = self.caches.get_cache(self, fieldname)
#        return fieldcache.groups(scores_and_docnums, limit=limit)
#    
#    def collapse_docs_by(self, fieldname, scores_and_docnums):
#        fieldcache = self.caches.get_cache(self, fieldname)
#        return fieldcache.collapse(scores_and_docnums)









