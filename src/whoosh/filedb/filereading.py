# Copyright 2009 Matt Chaput. All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#    1. Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#
#    2. Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY MATT CHAPUT ``AS IS'' AND ANY EXPRESS OR
# IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
# EVENT SHALL MATT CHAPUT OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
# OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
# LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
# NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE,
# EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
# The views and conclusions contained in the software and documentation are
# those of the authors and should not be interpreted as representing official
# policies, either expressed or implied, of Matt Chaput.

from bisect import bisect_left
from heapq import nlargest, nsmallest
from threading import Lock

from whoosh.compat import iteritems, string_type, integer_types, xrange
from whoosh.filedb.fieldcache import FieldCache, DefaultFieldCachingPolicy
from whoosh.filedb.filepostings import FilePostingReader
from whoosh.filedb.filetables import (TermIndexReader, StoredFieldReader,
                                      LengthReader, TermVectorReader)
from whoosh.matching import FilterMatcher, ListMatcher
from whoosh.reading import IndexReader, TermNotFound
from whoosh.support.dawg import DiskNode


SAVE_BY_DEFAULT = True


# Reader class

class SegmentReader(IndexReader):
    GZIP_CACHES = False

    def __init__(self, storage, schema, segment):
        self.storage = storage
        self.schema = schema
        self.segment = segment

        if hasattr(self.segment, "uuid"):
            self.uuid_string = str(self.segment.uuid)
        else:
            import uuid
            self.uuid_string = str(uuid.uuid4())

        # Term index
        tf = storage.open_file(segment.termsindex_filename)
        self.termsindex = TermIndexReader(tf)

        # Term vector index, and vector postings: lazy load
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

        # Copy info from underlying segment
        self._has_deletions = segment.has_deletions()
        self._doc_count = segment.doc_count()

        # Postings file
        self.postfile = self.storage.open_file(segment.termposts_filename,
                                               mapped=False)

        # Dawg file
        self.dawg = None
        if any(field.spelling for field in self.schema):
            fname = segment.dawg_filename
            if self.storage.file_exists(fname):
                dawgfile = self.storage.open_file(fname, mapped=False)
                self.dawg = DiskNode.load(dawgfile, expand=False)

        self.dc = segment.doc_count_all()
        assert self.dc == self.storedfields.length

        self.set_caching_policy()

        self.is_closed = False
        self._sync_lock = Lock()

    def has_deletions(self):
        return self._has_deletions

    def doc_count(self):
        return self._doc_count

    def is_deleted(self, docnum):
        return self.segment.is_deleted(docnum)

    def generation(self):
        return self.segment.generation

    def _open_vectors(self):
        if self.vectorindex:
            return

        storage, segment = self.storage, self.segment

        # Vector index
        vf = storage.open_file(segment.vectorindex_filename)
        self.vectorindex = TermVectorReader(vf)

        # Vector postings file
        self.vpostfile = storage.open_file(segment.vectorposts_filename,
                                           mapped=False)

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, self.segment)

    def __contains__(self, term):
        return term in self.termsindex

    def close(self):
        self.storedfields.close()
        self.termsindex.close()
        if self.postfile:
            self.postfile.close()
        if self.vectorindex:
            self.vectorindex.close()
        if self.vpostfile:
            self.vpostfile.close()
        #if self.fieldlengths:
        #    self.fieldlengths.close()
        self.caching_policy = None
        self.is_closed = True

    def doc_count_all(self):
        return self.dc

    def stored_fields(self, docnum):
        assert docnum >= 0
        schema = self.schema
        return dict(item for item
                    in iteritems(self.storedfields[docnum])
                    if item[0] in schema)

    def all_stored_fields(self):
        is_deleted = self.segment.is_deleted
        sf = self.stored_fields
        for docnum in xrange(self.segment.doc_count_all()):
            if not is_deleted(docnum):
                yield sf(docnum)

    def field_length(self, fieldname):
        return self.segment.field_length(fieldname)

    def min_field_length(self, fieldname):
        return self.segment.min_field_length(fieldname)

    def max_field_length(self, fieldname):
        return self.segment.max_field_length(fieldname)

    def doc_field_length(self, docnum, fieldname, default=0):
        if self.fieldlengths is None:
            return default
        return self.fieldlengths.get(docnum, fieldname, default=default)

    def has_vector(self, docnum, fieldname):
        if self.schema[fieldname].vector:
            self._open_vectors()
            return (docnum, fieldname) in self.vectorindex
        else:
            return False

    def _test_field(self, fieldname):
        if fieldname not in self.schema:
            raise TermNotFound("No field %r" % fieldname)
        if self.schema[fieldname].format is None:
            raise TermNotFound("Field %r is not indexed" % fieldname)

    def all_terms(self):
        schema = self.schema
        return ((fieldname, text) for fieldname, text
                in self.termsindex.keys()
                if fieldname in schema)

    def terms_from(self, fieldname, prefix):
        self._test_field(fieldname)
        schema = self.schema
        return ((fname, text) for fname, text
                in self.termsindex.keys_from((fieldname, prefix))
                if fname in schema)

    def term_info(self, fieldname, text):
        self._test_field(fieldname)
        try:
            return self.termsindex[fieldname, text]
        except KeyError:
            raise TermNotFound("%s:%r" % (fieldname, text))

    def _texts_in_fieldcache(self, fieldname, prefix=''):
        # The first value in a fieldcache is the default
        texts = self.fieldcache(fieldname).texts[1:]
        if prefix:
            i = bisect_left(texts, prefix)
            while i < len(texts) and texts[i].startswith(prefix):
                yield texts[i]
                i += 1
        else:
            for text in texts:
                yield text

    def expand_prefix(self, fieldname, prefix):
        self._test_field(fieldname)
        # If a fieldcache for the field is already loaded, we already have the
        # values for the field in memory, so just yield them from there
        if self.fieldcache_loaded(fieldname):
            return self._texts_in_fieldcache(fieldname, prefix)
        else:
            return IndexReader.expand_prefix(self, fieldname, prefix)

    def lexicon(self, fieldname):
        self._test_field(fieldname)
        # If a fieldcache for the field is already loaded, we already have the
        # values for the field in memory, so just yield them from there
        if self.fieldcache_loaded(fieldname):
            return self._texts_in_fieldcache(fieldname)
        else:
            return IndexReader.lexicon(self, fieldname)

    def __iter__(self):
        schema = self.schema
        return ((term, terminfo) for term, terminfo
                in self.termsindex.items()
                if term[0] in schema)

    def iter_from(self, fieldname, text):
        schema = self.schema
        self._test_field(fieldname)
        for term, terminfo in self.termsindex.items_from((fieldname, text)):
            if term[0] not in schema:
                continue
            yield (term, terminfo)

    def frequency(self, fieldname, text):
        self._test_field(fieldname)
        try:
            return self.termsindex.frequency((fieldname, text))
        except KeyError:
            return 0

    def doc_frequency(self, fieldname, text):
        self._test_field(fieldname)
        try:
            return self.termsindex.doc_frequency((fieldname, text))
        except KeyError:
            return 0

    def postings(self, fieldname, text, scorer=None):
        try:
            terminfo = self.termsindex[fieldname, text]
        except KeyError:
            raise TermNotFound("%s:%r" % (fieldname, text))

        format = self.schema[fieldname].format
        postings = terminfo.postings
        if isinstance(postings, integer_types):
            postreader = FilePostingReader(self.postfile, postings, format,
                                           scorer=scorer,
                                           term=(fieldname, text))
        else:
            docids, weights, values = postings
            postreader = ListMatcher(docids, weights, values, format,
                                     scorer=scorer, term=(fieldname, text),
                                     terminfo=terminfo)

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
        try:
            offset = self.vectorindex.get((docnum, fieldname))
        except KeyError:
            raise KeyError("No vector found for document "
                           "%s field %r" % (docnum, fieldname))

        return FilePostingReader(self.vpostfile, offset, vformat,
                                 stringids=True)

    # DAWG methods

    def has_word_graph(self, fieldname):
        if fieldname not in self.schema:
            return False
        if not self.schema[fieldname].spelling:
            return False
        if self.dawg:
            return fieldname in self.dawg
        return False

    def word_graph(self, fieldname):
        if not self.has_word_graph(fieldname):
            raise Exception("No word graph for field %r" % fieldname)
        return self.dawg.edge(fieldname)

    # Field cache methods

    def supports_caches(self):
        return True

    def set_caching_policy(self, cp=None, save=True, storage=None):
        """This method lets you control the caching policy of the reader. You
        can either pass a :class:`whoosh.filedb.fieldcache.FieldCachingPolicy`
        as the first argument, *or* use the `save` and `storage` keywords to
        alter the default caching policy::
        
            # Use a custom field caching policy object
            reader.set_caching_policy(MyPolicy())
            
            # Use the default caching policy but turn off saving caches to disk
            reader.set_caching_policy(save=False)
            
            # Use the default caching policy but save caches to a custom
            # storage
            from whoosh.filedb.filestore import FileStorage
            mystorage = FileStorage("path/to/cachedir")
            reader.set_caching_policy(storage=mystorage)
        
        :param cp: a :class:`whoosh.filedb.fieldcache.FieldCachingPolicy`
            object. If this argument is not given, the default caching policy
            is used.
        :param save: save field caches to disk for re-use. If a caching policy
            object is specified using `cp`, this argument is ignored.
        :param storage: a custom :class:`whoosh.store.Storage` object to use
            for saving field caches. If a caching policy object is specified
            using `cp` or `save` is `False`, this argument is ignored. 
        """

        if not cp:
            if save and storage is None:
                storage = self.storage
            elif not save:
                storage = None
            cp = DefaultFieldCachingPolicy(self.segment.name, storage=storage)

        if type(cp) is type:
            cp = cp()

        self.caching_policy = cp

    def _fieldkey(self, fieldname):
        return "%s/%s" % (self.uuid_string, fieldname)

    def fieldcache(self, fieldname, save=SAVE_BY_DEFAULT):
        """Returns a :class:`whoosh.filedb.fieldcache.FieldCache` object for
        the given field.
        
        :param fieldname: the name of the field to get a cache for.
        :param save: if True (the default), the cache is saved to disk if it
            doesn't already exist.
        """

        key = self._fieldkey(fieldname)
        fc = self.caching_policy.get(key)
        if not fc:
            fc = FieldCache.from_field(self, fieldname)
            self.caching_policy.put(key, fc, save=save)
        return fc

    def fieldcache_available(self, fieldname):
        """Returns True if a field cache exists for the given field (either in
        memory already or on disk).
        """

        return self._fieldkey(fieldname) in self.caching_policy

    def fieldcache_loaded(self, fieldname):
        """Returns True if a field cache for the given field is in memory.
        """

        return self.caching_policy.is_loaded(self._fieldkey(fieldname))

    def unload_fieldcache(self, name):
        self.caching_policy.delete(self._fieldkey(name))
