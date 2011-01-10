#===============================================================================
# Copyright 2011 Matt Chaput
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

import operator
from array import array
from collections import defaultdict
from heapq import nsmallest, nlargest, heappush, heapreplace
from struct import Struct

from whoosh.support.times import long_to_datetime
from whoosh.system import _INT_SIZE, unpack_int, unpack_float, unpack_long
from whoosh.util import utf8encode


GZIP_CACHES = False


class CacheSet(object):
    """Manages a set of FieldCache objects.
    """
    
    def __init__(self, storage, basename):
        self.storage = storage
        self.basename = basename
        self.caches = {}
    
    def _put(self, key, obj):
        self.caches[key] = obj
    
    def fieldcache_filename(self, fieldname):
        return "%s.%s.fc" % (self.basename, fieldname)
    
    def cache_file_exists(self, fieldname):
        storage = self.storage
        filename = self.fieldcache_filename(fieldname)
        gzname = filename + ".gz"
        return storage.file_exists(filename) or storage.file_exists(gzname)
    
    def create_cache(self, reader, fieldname, save=True, name=None,
                     default=None):
        savename = name if name else fieldname
        if name in reader.schema:
            raise Exception("Custom name %r is the name of a field")
        
        cache = FieldCache.from_reader(reader, fieldname, default=default)
        
        if save:
            filename = self.fieldcache_filename(savename)
            if GZIP_CACHES:
                filename += ".gz"
            
            f = self.storage.create_file(filename, gzip=GZIP_CACHES)
            cache.to_file(f)
            f.close()
        
        return cache
    
    def load_cache(self, fieldname):
        storage = self.storage
        filename = self.fieldcache_filename(fieldname)
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
    
    def get_cache(self, reader, fieldname, save=True):
        if fieldname in self.caches:
            return self.caches[fieldname]
        elif self.cache_file_exists(fieldname):
            fc = self.load_cache(fieldname)
        else:
            fc = self.create_cache(reader, fieldname, save=save)
        self._put(fieldname, fc)
        return fc
    
    def is_cached(self, fieldname):
        return fieldname in self.caches or self.cache_file_exists(fieldname)
    
    def is_loaded(self, fieldname):
        return fieldname in self.caches


pack_int_le = Struct("<i").pack
def unipickle(u):
    # Returns the unicode string as a pickle protocol 2 operator
    return "X%s%s" % (pack_int_le(len(u)), utf8encode(u)[0])


class FieldCache(object):
    """Keeps a list of the sorted text values of a field and an array of ints
    where each place in the array corresponds to a document, and the value
    at a place in the array is a pointer to a text in the list of texts.
    
    This structure allows fast sorting and grouping of documents by associating
    each document with a value through the array.
    """
    
    code = "I"
    hastexts = True
    default = u""
    
    def __init__(self, order=None, texts=None, default=None):
        """
        :param order: an array of ints.
        :param texts: a list of text values.
        :param default: the value to use for documents without the field.
        """
        
        if default is None:
            default = self.default
        
        self.order = order or array(self.code)
        self.texts = texts or [default]
        self.maxord = len(self.texts) - 1
    
    def __eq__(self, other):
        return (other and self.__class__ is other.__class__
                and self.order == other.order
                and self.texts == other.texts)
    
    @classmethod
    def from_reader(cls, ixreader, fieldname, default=None):
        """Creates an in-memory field cache from a reader.
        
        >>> r = ix.reader()
        >>> fc = FieldCache.from_reader(r, "chapter")
        
        :param ixreader: a :class:`whoosh.reading.IndexReader` object.
        :param fieldname: the name of the field to cache.
        :param key: a key function to use to order the values of the field,
            as in the built-in sort functions, or None to use the lexical
            ordering.
        :param default: the value to use for documents without the field.
        """
        
        if default is None:
            default = cls.default
        
        order = array(cls.code, [0] * ixreader.doc_count_all())
        field = ixreader.schema[fieldname]
        texts = list(field.sortable_values(ixreader, fieldname))
        for i, text in enumerate(texts):
            ps = ixreader.postings(fieldname, text)
            for id in ps.all_ids():
                order[id] = i + 1
        return cls(order, [default] + texts)
    
    @classmethod
    def from_file(cls, dbfile):
        """Loads an in-memory field cache from a saved file created with
        :meth:`FieldCache.to_file`.
        
        >>> fc = FieldCache.from_file(f)
        """
        
        # Read the number of documents
        doc_count = dbfile.read_uint()
        
        if cls.hastexts:
            # Seek past the number of texts
            dbfile.seek(_INT_SIZE, 1)
            # Read the texts
            texts = dbfile.read_pickle()
        
        # Read the order array
        code = dbfile.read(1)
        order = dbfile.read_array(code, doc_count)
        
        return cls(order, texts)
    
    def to_file(self, dbfile):
        """Saves an in-memory field cache to a file.
        
        >>> fc = FieldCache.from_reader(r, "tag")
        >>> fc.to_file(f)
        """
        
        dbfile.write_uint(len(self.order)) # Number of documents
        
        if self.hastexts:
            write = dbfile.write
            dbfile.write_uint(len(self.texts)) # Number of texts
            write("(") # Pickle mark
            for text in self.texts:
                write(unipickle(text))
            write("l.")
        
            code = "I"
            # Compact the order array if possible
            if len(self.texts) < 255:
                code = "B"
            elif len(self.texts) < 65535:
                code = "H"
            
            if code != "I":
                self.order = array(code, self.order)
        
        # Write the order array
        dbfile.write(code)
        dbfile.write_array(self.order)
        dbfile.flush()
    
    def key_for(self, docnum):
        """Returns the key corresponding to a document number.
        """
        
        return self.texts[self.order[docnum]]
    
    def reverse_key_for(self, docnum):
        return self.texts[self.maxord - self.order[docnum]]
    
    def keys(self):
        """Returns a list of all key values in the cache.
        """
        
        return self.texts
    
    def ords(self):
        """Yields a series of (docnum, order) pairs.
        """
        
        return enumerate(self.order)
    
    def groups(self, docnums, counts=False):
        """Returns a dictionary mapping key values to document numbers. If
        ``counts_only`` is True, the returned dictionary maps key values to the
        number of documents in that 
        """
        
        defaulttype = int if counts else list
        groups = defaultdict(defaulttype)
        key_for = self.key_for
        
        for docnum in docnums:
            key = key_for(docnum)
            if counts:
                groups[key] += 1
            else:
                groups[key].append(docnum)
        
        return groups
    
    def scored_groups(self, scores_and_docnums, limit=None):
        """Takes a sequence of (score, docnum) pairs and returns a dictionary
        mapping key values to sorted lists of (score, docnum) pairs.
        
        If you specify the ``limit`` keyword, the sorted lists will contain
        only the ``limit`` highest-scoring items.
        """
        
        groups = defaultdict(list)
        key_for = self.key_for
        
        for score, docnum in scores_and_docnums:
            key = key_for(docnum)
            ritem = (0-score, docnum)
            ls = groups[key]
            if limit:
                if len(ls) < limit:
                    heappush(ls, ritem)
                elif ritem[0] > ls[0][0]:
                    heapreplace(ls, ritem)
            else:
                ls.append(ritem)
        
        for v in groups.values():
            v.sort()
        
        return groups
    
    def collapse(self, scores_and_docnums):
        """Takes a sequence of (score, docnum) pairs and returns a list of
        docnums. If any docnums in the original list had the same key value,
        all but the highest scoring duplicates are removed from the result
        list.
        """
        
        maxes = {}
        key_for = self.key_for
        
        for score, docnum in scores_and_docnums:
            key = key_for(docnum)
            if score > maxes[key][1]:
                maxes[key] = (docnum, score)
        
        return sorted(maxes.keys())


class NumericFieldCache(FieldCache):
    hastexts = False
    default = 0
    
    def key_for(self, docnum):
        return self.order[docnum]
    
    def reverse_key_for(self, docnum):
        return 0 - self.order[docnum]
    
    def keys(self):
        return sorted(set(self.order))
    
class IntFieldCache(NumericFieldCache):
    code = "i"
    unpack = unpack_int
class FloatFieldCache(NumericFieldCache):
    code = "f"
    unpack = unpack_float
class LongFieldCache(NumericFieldCache):
    code = "q"
    unpack = unpack_long
class DateTimeFieldCache(LongFieldCache):
    unpack = long_to_datetime

class FieldCacheWriter(object):
    code = "I"
    default = u""
    
    def __init__(self, dbfile, size=0, hastexts=True, default=None):
        if default is None:
            default = self.default
        
        self.dbfile = dbfile
        self.order = array(self.code, [0] * size)
        self.hastexts = hastexts
        
        self.key = 0
        self.keycount = 1
        
        self.start = dbfile.tell()
        dbfile.write_uint(0) # Number of docs
        
        if self.hastexts:
            dbfile.write_uint(0) # Number of texts
            # Start the pickled list of texts
            dbfile.write("(" + unipickle(default))
    
    def add_key(self, value):
        if self.hastexts:
            self.key += 1
            self.dbfile.write(unipickle(value))
        else:
            self.key = value
        self.keycount += 1
    
    def add_doc(self, docnum):
        order = self.order
        if len(order) < docnum + 1:
            order.extend([0] * (docnum + 1 - len(order)))
        order[docnum] = self.key
    
    def close(self):
        dbfile = self.dbfile
        order = self.order
        keycount = self.keycount
        
        # Finish the pickled list of texts
        dbfile.write("l.")
        
        # Compact the order array if possible
        if self.hastexts:
            if keycount < 255:
                code = "B"
                order = array(code, order)
            elif keycount < 65535:
                code = "H"
                order = array(code, order)
        
        # Write the order array
        dbfile.write(code)
        dbfile.write_array(self.order)
        
        # Seek back to the start and write numbers of docs
        dbfile.flush()
        dbfile.seek(self.start)
        dbfile.write_uint(len(order))
        if self.hastexts:
            dbfile.write_uint(keycount)
        
        dbfile.close()
    


if __name__ == "__main__":
    import random
    
    from whoosh import index
    from whoosh.filedb.structfile import StructFile
    from whoosh.util import now
    
    
    ix = index.open_dir("e:/workspace/whoosh/benchmark/dictionary_index_whoosh")
    s = ix.searcher()
    r = s.reader()
    
    print r
    for sr in r.readers:
        print sr.segment
    
#    t = now()
#    fc = FieldCache.from_reader(r, "head")
#    print "make field cache", now() - t
    
#    t = now()
#    f = StructFile(open("e:/workspace/whoosh/bmark/combined.fc", "wb"))
#    fc.to_file(f)
#    print "tofile", now() - t
    
#    t = now()
#    for sr in r.readers:
#        f = StructFile(open("e:/workspace/whoosh/bmark/perseg_%s.fc" % (id(sr)), "wb"))
#        fc = FieldCache.from_reader(sr, "body")
#        fc.to_file(f)
#    print now() - t
    
#    t = now()
#    f = StructFile(open("e:/workspace/whoosh/bmark/combined_w.fc", "wb"))
#    fcw = FieldCacheWriter(f)
#    for w in ix.schema["head"].sortable_values(r, "head"):
#        fcw.add_key(w)
#        p = r.postings("head", w)
#        for docnum in p.all_ids():
#            fcw.add_doc(docnum)
#    fcw.close()
#    print "writer", now() - t
    
#    t = now()
#    f = StructFile(open("e:/workspace/whoosh/bmark/combined.fc", "rb"))
#    rfc = FieldCache.from_file(f)
#    print now() - t
#    
#    f = StructFile(open("e:/workspace/whoosh/bmark/combined_w.fc", "rb"))
#    rfc2 = FieldCache.from_file(f)
#    
#    print fc == rfc, fc == rfc2
#    
#    t = now()
#    print rfc.key_sort(xrange(1000), 10)
#    print now() - t
    
    




