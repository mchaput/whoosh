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
from heapq import nlargest, nsmallest
from threading import Lock

from whoosh.filedb.fieldcache import FieldCache
from whoosh.filedb.filepostings import FilePostingReader
from whoosh.filedb.filestore import ReadOnlyError
from whoosh.filedb.filetables import (TermIndexReader, StoredFieldReader,
                                      LengthReader, TermVectorReader)
from whoosh.matching import FilterMatcher, ListMatcher
from whoosh.reading import IndexReader, TermNotFound
from whoosh.util import protected

SAVE_BY_DEFAULT = False

# Reader class

class SegmentReader(IndexReader):
    GZIP_CACHES = False
    
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
        
        self.caches = {}
        
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
            return iter(fieldcache.texts)
        
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
            raise Exception("No vector found for document"
                            " %s field %r" % (docnum, fieldname))
        
        return FilePostingReader(self.vpostfile, offset, vformat, stringids=True)

    # Field cache methods

    def supports_caches(self):
        return True

    def _fieldcache_filename(self, fieldname):
        return "%s.%s.fc" % (self.segment.name, fieldname)

    def _put_fieldcache(self, name, fieldcache):
        self.caches[name] = fieldcache

    def _load_fieldcache(self, fieldname):
        storage = self.storage
        filename = self._fieldcache_filename(fieldname)
        gzipped = False
        
        # It's possible to load GZip'd caches but for it's MUCH slower,
        # especially for large caches
        gzname = filename + ".gz"
        if storage.file_exists(gzname) and not storage.file_exists(filename):
            filename = gzname
            gzipped = True
        
        f = storage.open_file(filename, mapped=False, gzip=gzipped)
        cache = FieldCache.from_file(f)
        f.close()
        return cache

    def _cachefile_exists(self, fieldname):
        storage = self.storage
        filename = self._fieldcache_filename(fieldname)
        gzname = filename + ".gz"
        return storage.file_exists(filename) or storage.file_exists(gzname)

    def _save_fieldcache(self, name, cache):
        filename = self._fieldcache_filename(name)
        if self.GZIP_CACHES:
            filename += ".gz"
            
        f = self.storage.create_file(filename, gzip=self.GZIP_CACHES)
        cache.to_file(f)
        f.close()

    def _create_fieldcache(self, fieldname, save=SAVE_BY_DEFAULT, name=None,
                           default=u''):
        if name in self.schema:
            raise Exception("Custom name %r is the name of a field")
        savename = name if name else fieldname
        
        if self.fieldcache_available(savename):
            # Don't recreate the cache if it already exists
            return None
        
        cache = FieldCache.from_field(self, fieldname, default=default)
        if save and not self.storage.readonly:
            self._save_fieldcache(savename, cache)
        return cache

    def define_facets(self, name, qs, save=SAVE_BY_DEFAULT):
        if name in self.schema:
            raise Exception("Can't define facets using the name of a field (%r)" % name)
        
        if self.fieldcache_available(name):
            # Don't recreate the cache if it already exists
            return
        
        cache = FieldCache.from_lists(qs, self.doc_count_all())
        if save and not self.storage.readonly:
            self._save_fieldcache(name, cache)
        self._put_fieldcache(name, cache)

    def fieldcache(self, fieldname, save=SAVE_BY_DEFAULT):
        """Returns a :class:`whoosh.filedb.fieldcache.FieldCache` object for
        the given field.
        
        :param fieldname: the name of the field to get a cache for.
        :param save: if True (the default), the cache is saved to disk if it
            doesn't already exist.
        """
        
        if fieldname in self.caches:
            return self.caches[fieldname]
        elif self._cachefile_exists(fieldname):
            fc = self._load_fieldcache(fieldname)
        else:
            fc = self._create_fieldcache(fieldname, save=SAVE_BY_DEFAULT)
        self._put_fieldcache(fieldname, fc)
        return fc
    
    def fieldcache_available(self, fieldname):
        """Returns True if a field cache exists for the given field (either in
        memory already or on disk).
        """
        
        return fieldname in self.caches or self._cachefile_exists(fieldname)
    
    def fieldcache_loaded(self, fieldname):
        """Returns True if a field cache for the given field is in memory.
        """
        
        return fieldname in self.caches

    def unload_fieldcache(self, name):
        try:
            del self.caches[name]
        except:
            pass
        
    def delete_fieldcache(self, name):
        self.unload_fieldcache(name)
        filename = self._fieldcache_filename(name)
        if self.storage.file_exists(filename):
            try:
                self.storage.delete_file(filename)
            except:
                pass

    # Sorting and faceting methods
    
    def key_fn(self, fieldname):
        if isinstance(fieldname, (tuple, list)):
            # The "fieldname" is actually a sequence of field names to sort by
            fcs = [self.fieldcache(fn) for fn in fieldname]
            keyfn = lambda docnum: tuple(fc.key_for(docnum) for fc in fcs)
        else:
            fc = self.fieldcache(fieldname)
            keyfn = fc.key_for
            
        return keyfn
    
    def sort_docs_by(self, fieldname, docnums, reverse=False):
        keyfn = self.key_fn(fieldname)
        return sorted(docnums, key=keyfn, reverse=reverse)
    
    def key_docs_by(self, fieldname, docnums, limit, reverse=False, offset=0):
        keyfn = self.key_fn(fieldname)
        
        if limit is None:
            # Don't bother sorting, the caller will do that
            return [(keyfn(docnum), docnum + offset) for docnum in docnums]
        else:
            # A non-reversed sort (the usual case) is inefficient because we
            # have to use nsmallest, but I can't think of a cleverer thing to
            # do right now. I thought I had an idea, but I was wrong.
            op = nlargest if reverse else nsmallest
            
            return op(limit, ((keyfn(docnum), docnum + offset)
                              for docnum in docnums))

            
        

#    def collapse_docs_by(self, fieldname, scores_and_docnums):
#        fieldcache = self.caches.get_cache(self, fieldname)
#        return fieldcache.collapse(scores_and_docnums)









