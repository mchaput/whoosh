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

from array import array
from collections import defaultdict
from heapq import heappush, heapreplace
from struct import Struct

from whoosh.system import _INT_SIZE
from whoosh.util import utf8encode


pack_int_le = Struct("<i").pack
def pickled_unicode(u):
    # Returns the unicode string as a pickle protocol 2 operator
    return "X%s%s" % (pack_int_le(len(u)), utf8encode(u)[0])

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
        

class FieldCache(object):
    """Keeps a list of the sorted text values of a field and an array of ints
    where each place in the array corresponds to a document, and the value
    at a place in the array is a pointer to a text in the list of texts.
    
    This structure allows fast sorting and grouping of documents by associating
    each document with a value through the array.
    """
    
    def __init__(self, order=None, texts=None, hastexts=True, default=u"",
                 typecode="I"):
        """
        :param order: an array of ints.
        :param texts: a list of text values.
        :param default: the value to use for documents without the field.
        """
        
        self.order = order or array(self.code)
        self.hastexts = hastexts
        self.texts = None
        if hastexts:
            self.texts = texts or [default]
        self.typecode = typecode
    
    def __eq__(self, other):
        return (other and self.__class__ is other.__class__
                and self.hastexts == other.hastexts
                and self.order == other.order
                and self.texts == other.texts)
    
    # Class constructor for building a field cache from a reader
    
    @classmethod
    def from_reader(cls, ixreader, fieldname, default=u""):
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
        
        field = ixreader.schema[fieldname]
        hastexts = field.sortable_typecode in (None, "unicode")
        
        texts = None
        if hastexts:
            typecode = "I"
            texts = [default]
        else:
            typecode = field.sortable_typecode
        
        doccount = ixreader.doc_count_all()
        # Python does not support arrays of long long see Issue 1172711
        if typecode.lower() == "q":
            order = [0] * doccount
        else:
            order = array(typecode, [0] * doccount)
        
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
        
        return cls(order, texts, hastexts=hastexts, typecode=typecode)
    
    # Class constructor for loading a field cache from a file
    
    @classmethod
    def from_file(cls, dbfile):
        """Loads an in-memory field cache from a saved file created with
        :meth:`FieldCache.to_file`.
        
        >>> fc = FieldCache.from_file(f)
        """
        
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
        
        >>> fc = FieldCache.from_reader(r, "tag")
        >>> fc.to_file(f)
        """
        
        dbfile.write_uint(len(self.order)) # Number of documents
        
        if self.hastexts:
            dbfile.write_uint(len(self.texts)) # Number of texts
            dbfile.write_pickle(self.texts)
        
            # Compact the order array if possible
            if len(self.texts) < 255:
                newcode = "B"
            elif len(self.texts) < 65535:
                newcode = "H"
            
            if newcode != self.order.typecode:
                self.order = array(newcode, self.order)
                self.typecode = newcode
        else:
            dbfile.write_uint(0) # No texts
        
        dbfile.write(self.typecode)
        write_qsafe_array(self.typecode, self.order, dbfile)
        dbfile.flush()
    
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


# Streaming cache file writer

class FieldCacheWriter(object):
    def __init__(self, dbfile, size=0, hastexts=True, code="I", default=u""):
        self.dbfile = dbfile
        self.order = array(self.code, [0] * size)
        self.hastexts = hastexts
        self.code = code
        
        self.key = 0
        self.keycount = 1
        
        self.start = dbfile.tell()
        dbfile.write_uint(0) # Number of docs
        dbfile.write_uint(0) # Number of texts
        
        if self.hastexts:
            # Start the pickled list of texts
            dbfile.write("(" + pickled_unicode(default))
    
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
    


