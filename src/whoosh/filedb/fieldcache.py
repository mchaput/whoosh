# Copyright 2011 Matt Chaput. All rights reserved.
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

from __future__ import with_statement
import threading
import weakref
from array import array
from collections import defaultdict
from heapq import heappush, heapreplace
from struct import Struct

from whoosh.compat import u, b, xrange
from whoosh.system import _INT_SIZE, _FLOAT_SIZE, _LONG_SIZE
from whoosh.util import utf8encode


pack_int_le = Struct("<i").pack


def pickled_unicode(u):
    # Returns the unicode string as a pickle protocol 2 operator
    return "X%s%s" % (pack_int_le(len(u)), utf8encode(u)[0])


class BadFieldCache(Exception):
    pass


# Python does not support arrays of long long see Issue 1172711
# These functions help write/read a simulated an array of q/Q using lists

def write_qsafe_array(typecode, arry, dbfile):
    if typecode == "q":
        for num in arry:
            dbfile.write_long(num)
    elif typecode == "Q":
        for num in arry:
            dbfile.write_ulong(num)
    else:
        dbfile.write_array(arry)


def read_qsafe_array(typecode, size, dbfile):
    if typecode == "q":
        arry = [dbfile.read_long() for _ in xrange(size)]
    elif typecode == "Q":
        arry = [dbfile.read_ulong() for _ in xrange(size)]
    else:
        arry = dbfile.read_array(typecode, size)

    return arry


def make_array(typecode, size=0, default=None):
    if typecode.lower() == "q":
        # Python does not support arrays of long long see Issue 1172711
        if default is not None and size:
            arry = [default] * size
        else:
            arry = []
    else:
        if default is not None and size:
            arry = array(typecode, (default for _ in xrange(size)))
        else:
            arry = array(typecode)
    return arry


class FieldCache(object):
    """Keeps a list of the sorted text values of a field and an array of ints
    where each place in the array corresponds to a document, and the value
    at a place in the array is a pointer to a text in the list of texts.
    
    This structure allows fast sorting and grouping of documents by associating
    each document with a value through the array.
    """

    def __init__(self, order=None, texts=None, hastexts=True,
                 default=u('\uFFFF'), typecode="I"):
        """
        :param order: an array of ints.
        :param texts: a list of text values.
        :param default: the value to use for documents without the field.
        """

        self.order = order or make_array(typecode)
        self.typecode = typecode

        self.hastexts = hastexts
        self.texts = None
        if hastexts:
            self.texts = texts or [default]

    def __eq__(self, other):
        return (other and self.__class__ is other.__class__
                and self.hastexts == other.hastexts
                and self.order == other.order
                and self.texts == other.texts)

    def __ne__(self, other):
        return not self.__eq__(other)

    def size(self):
        """Returns the size in bytes (or as accurate an estimate as is
        practical, anyway) of this cache.
        """

        orderlen = len(self.order)
        if self.typecode == "B":
            total = orderlen
        elif self.typecode in "Ii":
            total = orderlen * _INT_SIZE
        elif self.typecode == "f":
            total = orderlen * _FLOAT_SIZE
        elif self.typecode in "Qq":
            total = orderlen * _LONG_SIZE

        if self.hastexts:
            total += sum(len(t) for t in self.texts)

        return total

    # Class constructor for building a field cache from a reader

    @classmethod
    def from_field(cls, ixreader, fieldname):
        """Creates an in-memory field cache from a reader.
        
        >>> r = ix.reader()
        >>> fc = FieldCache.from_field(r, "chapter")
        
        :param ixreader: a :class:`whoosh.reading.IndexReader` object.
        :param fieldname: the name of the field to cache.
        :param key: a key function to use to order the values of the field,
            as in the built-in sort functions, or None to use the lexical
            ordering.
        :param default: the value to use for documents without the field.
        """

        field = ixreader.schema[fieldname]
        hastexts = field.sortable_typecode in (None, "unicode")

        texts = None
        if hastexts:
            typecode = "I"
            texts = [field.sortable_default()]
            defaultnum = 0
        else:
            typecode = field.sortable_typecode
            defaultnum = field.sortable_default()

        doccount = ixreader.doc_count_all()
        order = make_array(typecode, doccount, defaultnum)

        enum = enumerate(field.sortable_values(ixreader, fieldname))
        for i, (text, sortable) in enum:
            if hastexts:
                texts.append(sortable)

            ps = ixreader.postings(fieldname, text)
            for id in ps.all_ids():
                if hastexts:
                    order[id] = i + 1
                else:
                    order[id] = sortable

        # Compact the order array if possible
        if hastexts:
            newcode = order.typecode
            if len(texts) < 255:
                newcode = "B"
            elif len(texts) < 65535:
                newcode = "H"

            if newcode != order.typecode:
                # Can't use an array as the source for another array
                order = array(newcode, iter(order))
                typecode = newcode

        return cls(order, texts, hastexts=hastexts, typecode=typecode)

    # Class constructor for defining a field cache using arbitrary queries

    @classmethod
    def from_lists(cls, doclists, doccount, default=u("")):
        texts = sorted(doclists.keys())
        order = array("I", [0] * doccount)

        # Run the queries to populate the order array
        for i, text in enumerate(texts):
            doclist = doclists[text]
            for id in doclist:
                order[id] = i + 1

        texts.insert(0, default)
        return cls(order, texts)

    # Class constructor for loading a field cache from a file

    @classmethod
    def from_file(cls, dbfile):
        """Loads an in-memory field cache from a saved file created with
        :meth:`FieldCache.to_file`.
        
        >>> fc = FieldCache.from_file(f)
        """

        # Read the finished tag
        tag = dbfile.read(1)
        if tag != "+":
            raise BadFieldCache

        # Read the number of documents
        doccount = dbfile.read_uint()
        textcount = dbfile.read_uint()

        texts = None
        if textcount:
            # Read the texts
            texts = dbfile.read_pickle()

        typecode = dbfile.read(1)
        order = read_qsafe_array(typecode, doccount, dbfile)
        return cls(order, texts, typecode=typecode, hastexts=bool(texts))

    def to_file(self, dbfile):
        """Saves an in-memory field cache to a file.
        
        >>> fc = FieldCache.from_field(r, "tag")
        >>> fc.to_file(f)
        """

        # Write a tag at the start of the file indicating the file write is in
        # progress, to warn other processes that might open the file. We'll
        # seek back and change this when the file is done.
        dbfile.write(b("-"))

        dbfile.write_uint(len(self.order))  # Number of documents

        if self.hastexts:
            dbfile.write_uint(len(self.texts))  # Number of texts
            dbfile.write_pickle(self.texts)

            # Compact the order array if possible
            if len(self.texts) < 255:
                newcode = "B"
            elif len(self.texts) < 65535:
                newcode = "H"
            if newcode != self.order.typecode:
                self.order = array(newcode, iter(self.order))
                self.typecode = newcode
        else:
            dbfile.write_uint(0)  # No texts

        dbfile.write(b(self.typecode))
        write_qsafe_array(self.typecode, self.order, dbfile)
        dbfile.flush()

        # Seek back and change the tag byte at the start of the file
        dbfile.seek(0)
        dbfile.write(b("+"))

    # Field cache operations

    def key_for(self, docnum):
        """Returns the key corresponding to a document number.
        """

        o = self.order[docnum]
        if self.hastexts:
            return self.texts[o]
        else:
            return o

    def keys(self):
        """Returns a list of all key values in the cache.
        """

        if self.hastexts:
            return self.texts
        else:
            return sorted(set(self.order))

    def ords(self):
        """Yields a series of (docnum, order) pairs.
        """

        return enumerate(self.order)

    def groups(self, docnums, counts=False):
        """Returns a dictionary mapping key values to document numbers. If
        ``counts`` is True, the returned dictionary maps key values to the
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
            ritem = (0 - score, docnum)
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


# Streaming cache file writer

class FieldCacheWriter(object):
    def __init__(self, dbfile, size=0, hastexts=True, code="I",
                 default=u('\uFFFF')):
        self.dbfile = dbfile
        self.hastexts = hastexts
        self.code = code
        self.order = make_array(code, size, 0)

        self.key = 0
        self.keycount = 1

        self.tagpos = dbfile.tell()
        dbfile.write(b("-"))
        self.start = dbfile.tell()
        dbfile.write_uint(0)  # Number of docs
        dbfile.write_uint(0)  # Number of texts

        if self.hastexts:
            # Start the pickled list of texts
            dbfile.write(b("(") + pickled_unicode(default))

    def add_key(self, value):
        if self.hastexts:
            self.key += 1
            self.dbfile.write(pickled_unicode(value))
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
        dbfile.write(b("l."))

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
        dbfile.flush()

        # Seek back and write the finished file tag
        dbfile.seek(self.tagpos)
        dbfile.write(b("+"))

        dbfile.close()


# Caching policies

class FieldCachingPolicy(object):
    """Base class for field caching policies.
    """

    def put(self, key, obj, save=True):
        """Adds the given object to the cache under the given key.
        """

        raise NotImplementedError

    def __contains__(self, key):
        """Returns True if an object exists in the cache (either in memory
        or on disk) under the given key.
        """

        raise NotImplementedError

    def is_loaded(self, key):
        """Returns True if an object exists in memory for the given key. This
        might be useful for scenarios where code can use a field cache if it's
        already loaded, but is not important enough to load it for its own
        sake.
        """

        raise NotImplementedError

    def get(self, key):
        """Returns the object for the given key, or ``None`` if the key does
        not exist in the cache.
        """

        raise NotImplementedError

    def delete(self, key):
        """Removes the object for the given key from the cache.
        """

        pass

    def get_class(self):
        """Returns the class to use when creating field caches. This class
        should implement the same protocol as FieldCache.
        """

        return FieldCache


class NoCaching(FieldCachingPolicy):
    """A field caching policy that does not save field caches at all.
    """

    def put(self, key, obj, save=True):
        pass

    def __contains__(self, key):
        return False

    def is_loaded(self, key):
        return False

    def get(self, key):
        return None


class DefaultFieldCachingPolicy(FieldCachingPolicy):
    """A field caching policy that saves generated caches in memory and also
    writes them to disk by default.
    """

    shared_cache = weakref.WeakValueDictionary()
    sharedlock = threading.Lock()

    def __init__(self, basename, storage=None, gzip_caches=False,
                 fcclass=FieldCache):
        """
        :param basename: a prefix for filenames. This is usually the name of
            the reader's segment.
        :param storage: a custom :class:`whoosh.store.Storage` object to use
            for saving field caches. If this is ``None``, this object will not
            save caches to disk.
        :param gzip_caches: if True, field caches saved to disk by this object
            will be compressed. Loading compressed caches is very slow, so you
            should not turn this option on.
        :param fcclass: 
        """

        self.basename = basename
        self.storage = storage
        self.caches = {}
        self.gzip_caches = gzip_caches
        self.fcclass = fcclass

    def __contains__(self, key):
        return self.is_loaded(key) or self._file_exists(key)

    def _filename(self, key):
        if "/" in key:
            savename = key[key.rfind("/") + 1:]
        else:
            savename = key
        return "%s.%s.fc" % (self.basename, savename)

    def _file_exists(self, key):
        if not self.storage:
            return False

        filename = self._filename(key)
        gzfilename = filename + ".gz"
        return (self.storage.file_exists(filename)
                or self.storage.file_exists(gzfilename))

    def _save(self, key, cache):
        filename = self._filename(key)
        if self.gzip_caches:
            filename += ".gz"

        try:
            f = self.storage.create_file(filename, gzip=self.gzip_caches,
                                         excl=True)
        except OSError:
            pass
        else:
            cache.to_file(f)
            f.close()

    def _load(self, key):
        storage = self.storage
        filename = self._filename(key)
        gzfilename = filename + ".gz"
        gzipped = False
        if (storage.file_exists(gzfilename)
            and not storage.file_exists(filename)):
            filename = gzfilename
            gzipped = True

        f = storage.open_file(filename, gzip=gzipped)
        try:
            cache = self.fcclass.from_file(f)
        finally:
            f.close()
        return cache

    def is_loaded(self, key):
        return key in self.caches or key in self.shared_cache

    def put(self, key, cache, save=True):
        self.caches[key] = cache
        if save:
            if self.storage:
                self._save(key, cache)
            with self.sharedlock:
                if key not in self.shared_cache:
                    self.shared_cache[key] = cache

    def get(self, key):
        if key in self.caches:
            return self.caches.get(key)

        with self.sharedlock:
            if key in self.shared_cache:
                return self.shared_cache[key]

        if self._file_exists(key):
            try:
                fc = self._load(key)
                self.put(key, fc)
                return fc
            except (OSError, BadFieldCache):
                return None

    def delete(self, key):
        try:
            del self.caches[key]
        except KeyError:
            pass

    def get_class(self):
        return self.fcclass
