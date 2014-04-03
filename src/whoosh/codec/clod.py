# Copyright 2014 Matt Chaput. All rights reserved.
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

import struct
import zlib
from array import array
from bisect import bisect_left, bisect_right
from collections import defaultdict, deque
from hashlib import md5

from whoosh import reading
from whoosh.compat import pickle
from whoosh.compat import array_frombytes, array_tobytes
from whoosh.compat import b, byte, iteritems, unichr, xrange
from whoosh.compat import bytes_type
from whoosh.codec import codec
from whoosh.idsets import BitSet
from whoosh.matching import LeafMatcher, ReadTooFar, NullMatcher
from whoosh.system import emptybytes, pack_long, unpack_long, IS_LITTLE
from whoosh.system import pack_long_le, unpack_long_le
from whoosh.util import now
from whoosh.util.numeric import length_to_byte, byte_to_length
from whoosh.util.numlists import min_array_code


# Keys of a certain type are prefixed with a couple of identifier bytes
COLUMN_PREFIX = b"c0"
DOC_PREFIX = b"d0"
DOCMAP_PREFIX = b"b0"
FIELD_PREFIX = b"f0"
LENGTH_PREFIX = b"l0"
POSTING_PREFIX = b"p0"
STORED_PREFIX = b"s0"
TAG_PREFIX = b"g0"
TERM_PREFIX = b"t0"
UNIQUE_PREFIX = b"u0"
VECTOR_PREFIX = b"v0"

# "Metadata" keys start with "m"
INDEXINFO_PREFIX = b"mi0"
DOCCOUNT_PREFIX = b"mc0"
MAXDOC_PREFIX = b"mx0"
FIELDNAMES_PREFIX = b"mf0"
TAGNAMES_PREFIX = b"mg0"

blocksize = 256


def offsets(x, size):
    offset = x // size * size
    return offset, x - offset


# Structures

doc_keystruct = struct.Struct(">2sI")
pack_dockey = doc_keystruct.pack
unpack_dockey = doc_keystruct.unpack

docid_struct = struct.Struct(">I")
docid_size = docid_struct.size
pack_docid = docid_struct.pack
unpack_docid = docid_struct.unpack

docids_struct = struct.Struct(">II")
docids_size = docids_struct.size
pack_docids = docids_struct.pack
unpack_docids = docids_struct.unpack

# Min length, max length, total length
fielddata_struct = struct.Struct("<iiI")

# Min length, max length, min weight, max weight
termdata_struct = struct.Struct("<BBff")

null = b"\x00"


def encode_fieldname(fieldname):
    return fieldname.encode("utf8")


def decode_fieldname(fieldbytes):
    return fieldbytes.decode("utf8")


def pack_fielddockey(prefix, fieldbytes, docid):
    return prefix + fieldbytes + pack_docid(docid)


def unpack_fielddockey(key):
    return key[:2], key[2:-docid_size], unpack_docid(key[-docid_size:])[0]


def pack_docfieldkey(prefix, docid, fieldbytes):
    return prefix + pack_docid(docid) + fieldbytes


def unpack_docfieldkey(key):
    return key[:2], unpack_docid(key[2:10]), key[10:]


def pack_termkey(prefix, fieldbytes, termbytes):
    return prefix + fieldbytes + null + termbytes


def unpack_termkey(key, end=None):
    ni = key.find(null)
    end = end or len(key)
    return key[:2], key[2:ni], key[ni + 1:end]


def unpack_key_field(key):
    return key[2:key.find(null)]


def pack_postingkey(fieldbytes, termbytes, startid, endid):
    return (
        POSTING_PREFIX +
        fieldbytes + null + termbytes +
        pack_docids(startid, endid)
    )


def unpack_postingkey(key):
    prefix, fieldbytes, termbytes = unpack_termkey(key, -docids_size)
    startid, endid = unpack_docids(key[-docids_size:])
    return prefix, fieldbytes, termbytes, startid, endid


def unpack_key_ids(key):
    return unpack_docids(key[-docids_size:])


def posting_keys(cur, fieldbytes, termbytes):
    prefix = pack_termkey(POSTING_PREFIX, fieldbytes, termbytes)
    size = len(prefix) + docids_size
    for key in cur.expand_prefix(prefix):
        if len(key) == size:
            yield key


def prefixed_items(txn, prefix):
    cur = txn.cursor()
    for key in cur.expand_prefix(prefix):
        yield key, cur.value()


# TermInfo subclass

class ClodTermInfo(reading.TermInfo):
    # B   | Flags
    # f   | Total weight
    # I   | Total doc freq
    # B   | Min length (encoded as byte)
    # B   | Max length (encoded as byte)
    # f   | Max weight
    # q   | Minimum (first) ID
    # q   | Maximum (last) ID
    _struct = struct.Struct("<BfIBBfqq")

    __slots__ = ("_weight", "_df", "_minlength", "_maxlength", "_maxweight",
                 "_minid", "_maxid", "_single")

    def __init__(self, *args, **kwargs):
        reading.TermInfo.__init__(self, *args, **kwargs)
        self._single = None

    def to_bytes(self):
        minlen = length_to_byte(self._minlength)
        maxlen = length_to_byte(self._maxlength)
        return self._struct.pack(0, self._weight, self._df, minlen, maxlen,
                                 self._maxweight, self._minid, self._maxid)

    @classmethod
    def from_bytes(cls, bs):
        size = cls._struct.size
        hasextra = len(bs) > size
        hbytes = bs[:size] if hasextra else bs
        (
            flags, w, df, minlen, maxlen, maxw, minid, maxid
        ) = cls._struct.unpack(hbytes)
        minlen = byte_to_length(minlen)
        maxlen = byte_to_length(maxlen)
        return cls(w, df, minlen, maxlen, maxw, minid, maxid)


# Cache objects

class SlotCache(object):
    slotsize = 256

    def __init__(self, txn, readonly=False, cachesize=16):
        self._txn = txn
        self._map = {}
        self._queue = deque()
        self._queuesize = cachesize
        self._readonly = readonly

    def _get_slot(self, slotkey, create=True):
        # Gets a cached bitmap corresponding to the given document's area,
        # or creates it and returns it

        # If we already have that slot cached, return it
        try:
            return self._map[slotkey]
        except KeyError:
            pass

        # Try to read the slot from the database
        try:
            bs = self._txn[slotkey]
        except KeyError:
            bs = None

        if bs is not None or create:
            # If the cache is full, flush the oldest item out
            _queue = self._queue
            if len(_queue) >= self._queuesize:
                self._flush_slot(_queue.popleft())

            # Create or decode the object
            if bs is None:
                obj = self._create_obj(slotkey)
            else:
                obj = self._from_bytes(bs)

            # Add the object to the cache and return it
            self._queue.append(slotkey)
            self._map[slotkey] = obj
            return obj
        else:
            return None

    def _flush_slot(self, slotkey):
        txn = self._txn
        obj = self._map[slotkey]
        if self._obj_is_dirty(slotkey, obj):
            if not self._obj_is_empty(obj):
                txn[slotkey] = self._to_bytes(obj)
            else:
                try:
                    del txn[slotkey]
                except KeyError:
                    pass
            self._was_flushed(slotkey)

    def _obj_is_dirty(self, slotkey, obj):
        return True

    def _was_flushed(self, slotkey):
        pass

    def _obj_is_empty(self, obj):
        raise NotImplementedError

    def _to_bytes(self, obj):
        raise NotImplementedError

    def _from_bytes(self, bs):
        raise NotImplementedError

    def _create_obj(self, slotkey):
        raise NotImplementedError


class DocmapCache(object):
    slotsize = 2048

    def __init__(self, txn, readonly=False):
        self._txn = txn
        self._first = None
        self._bitset = BitSet()
        self._readonly = readonly

        try:
            self._maxdoc = unpack_long_le(txn[MAXDOC_PREFIX])[0]
        except KeyError:
            self._maxdoc = 0

        self._existing = []
        cur = txn.cursor()
        for key in cur.expand_prefix(DOCMAP_PREFIX):
            self._existing.append(key)
            _, offset = unpack_dockey(key)
            if self._first is None:
                self._first = offset
            bs = BitSet.from_bytes(zlib.decompress(cur.value()))
            for n in bs:
                self._bitset.add(n + offset - self._first)
        cur.close()

    def __contains__(self, docid):
        return docid - self._first in self._bitset

    def next_doc_id(self):
        maxdoc = self._maxdoc
        self._maxdoc += 1
        return maxdoc

    def is_deleted(self, docid):
        first = self._first
        if docid < first:
            return True
        return docid - first not in self._bitset

    def doc_count(self):
        return len(self._bitset)

    def doc_id_range(self):
        bs = self._bitset
        offset = self._first
        return bs.first() + offset, bs.last() + offset

    def add(self, docid):
        if self._first is None:
            self._first = docid
        self._bitset.add(docid - self._first)

    def discard(self, docid):
        self._bitset.discard(docid - self._first)

    def all_ids(self):
        first = self._first
        for n in self._bitset:
            yield n + first

    def last_id(self):
        return self._bitset.last()

    def flush(self):
        slotsize = self.slotsize
        txn = self._txn
        first = self._first

        txn[MAXDOC_PREFIX] = pack_long_le(self._maxdoc)

        # Track what docmap keys exists and what ones we write so we can delete
        # out-of-date keys at the end
        written = []
        block = BitSet()
        lastoffset = key = None
        for n in self._bitset:
            n += first
            offset, delta = offsets(n, slotsize)
            if offset != lastoffset:
                if key:
                    written.append(key)
                    txn[key] = zlib.compress(block.to_bytes(), 3)
                    block.clear()
                key = pack_dockey(DOCMAP_PREFIX, offset)
                lastoffset = offset
            block.add(delta)
        if key and block:
            written.append(key)
            txn[key] = zlib.compress(block.to_bytes(), 3)

        for obsoletekey in set(self._existing).difference(written):
            del txn[obsoletekey]


class LengthsCache(SlotCache):
    def __init__(self, txn):
        SlotCache.__init__(self, txn)
        self._totals = defaultdict(int)
        self._dirty = False
        self._tozero = defaultdict(set)
        self._changed = set()

    def add_length(self, fieldbytes, docid, length):
        offset, delta = offsets(docid, self.slotsize)
        slotkey = pack_fielddockey(FIELD_PREFIX, fieldbytes, offset)
        arry = self._get_slot(slotkey)
        arry[delta] = length_to_byte(length)
        self._changed.add(slotkey)
        self._dirty = True

    def get_length(self, fieldbytes, docid, default=0):
        offset, delta = offsets(docid, self.slotsize)
        slotkey = pack_fielddockey(FIELD_PREFIX, fieldbytes, offset)
        arry = self._get_slot(slotkey, create=False)
        if arry is None:
            return default
        else:
            return byte_to_length(arry[docid - offset]) or default

    def zero_out(self, docid):
        offset, delta = offsets(docid, self.slotsize)
        self._tozero[offset].add(delta)

    def _all_values(self, fieldbytes):
        cur = self._txn.cursor()
        prefix = FIELD_PREFIX + fieldbytes + null
        for _ in cur.expand_prefix(prefix):
            arry = self._from_bytes(cur.value())
            for v in arry:
                if v:
                    yield v

    def total_length(self, fieldbytes):
        try:
            return sum(self._all_values(fieldbytes))
        except ValueError:
            return 0

    def min_length(self, fieldbytes):
        try:
            minbyte = min(self._all_values(fieldbytes))
        except ValueError:
            return 0
        else:
            return byte_to_length(minbyte)

    def max_length(self, fieldbytes):
        try:
            maxbyte = max(self._all_values(fieldbytes))
        except ValueError:
            return 0
        else:
            return byte_to_length(maxbyte)

    def flush(self):
        for slotkey in list(self._changed):
            self._flush_slot(slotkey)

        txn = self._txn
        changed = self._changed
        tozero = self._tozero
        if tozero:
            cur = txn.cursor()
            for key in cur.expand_prefix(FIELD_PREFIX):
                _, fieldname, offset = unpack_fielddockey(key)
                if offset in tozero:
                    arry = self._from_bytes(cur.value())
                    if arry is not None:
                        for delta in tozero[offset]:
                            arry[delta] = 0
                        changed.add(key)
                    txn[key] = self._to_bytes(arry)

    def _obj_is_empty(self, arry):
        return not any(arry)

    def _create_obj(self, slotkey):
        return array("B", (0 for _ in xrange(self.slotsize)))

    def _to_bytes(self, arry):
        return array_tobytes(arry)

    def _from_bytes(self, bs):
        arry = array("B")
        array_frombytes(arry, bs)
        return arry

    def _obj_is_dirty(self, slotkey, obj):
        return slotkey in self._changed

    def _was_flushed(self, slotkey):
        self._changed.discard(slotkey)


# class TagmapCache(DocmapCache):
#     def __contains__(self, item):
#         tagbytes, docid = item
#         offset, delta = offsets(docid, blocksize)
#         slotkey = pack_tag_key(TAG_PREFIX, tagbytes, offset)
#         return delta in self._get_slot(slotkey)
#
#     def add(self, docid, tagbytes):
#         offset, delta = offsets(docid, blocksize)
#         slotkey = pack_tag_key(TAG_PREFIX, tagbytes, offset)
#         self._add(slotkey, delta)
#
#     def remove(self, docid, tagbytes):
#         offset, delta = offsets(docid, blocksize)
#         slotkey = pack_tag_key(TAG_PREFIX, tagbytes, offset)
#         self._remove(slotkey, delta)
#
#     def all_tagbytes(self):
#         last = None
#         # maxid = b"\xff\xff\xff\xff\xff\xff\xff\xff"
#         for key in self._txn.expand_prefix(TAG_PREFIX):
#             _, tagbytes, offset = unpack_tag_key(key)
#             if tagbytes != last:
#                 yield tagbytes
#                 last = tagbytes
#
#     def ids_for_tag(self, tagbytes):
#         cur = self._txn.cursor()
#         prefix = pack_tag_key(TAG_PREFIX, tagbytes, 0)
#         for key in cur.expand_prefix(prefix):
#             _, _, offset = unpack_doc_key(key)
#             docmap = self._from_bytes(cur.value())
#             for n in docmap:
#                 yield offset + n
#
#     def tags_for_id(self, docid, taglist):
#         offset, delta = offsets(docid, blocksize)
#         for tagbytes in taglist:
#             slotkey = pack_tag_key(TAG_PREFIX, tagbytes, offset)
#             try:
#                 tagmap = self._get_slot(slotkey, create=False)
#             except KeyError:
#                 pass
#             else:
#                 if delta in tagmap:
#                     yield tagbytes
#
#     def flush(self):
#         for slotkey in self._changed:
#             self._flush_slot(slotkey, discard=False)


class ColumnCache(SlotCache):
    def __init__(self, txn, fieldbytes, columnobj, readonly=False,
                 cachesize=32):
        SlotCache.__init__(self, txn, cachesize=cachesize)
        self._fieldbytes = fieldbytes
        self._columnobj = columnobj
        self._readonly = readonly

    def _obj_is_empty(self, colwriter):
        return colwriter.is_empty()

    def _obj_is_dirty(self, slotkey, obj):
        return obj.is_dirty()

    def _to_bytes(self, colwriter):
        return colwriter.to_bytes()

    def _from_bytes(self, bs):
        block = self._columnobj.reader(blocksize, bs)
        if not self._readonly:
            # Convert the reader into a writer
            block = self._columnobj.writer(blocksize, list(block.values()))
        return block

    def _create_obj(self, slotkey):
        return self._columnobj.writer(blocksize)

    def _key(self, docid):
        offset, delta = offsets(docid, blocksize)
        slotkey = pack_fielddockey(COLUMN_PREFIX, self._fieldbytes, offset)
        return slotkey, offset, delta

    def all_items(self):
        fieldbytes = self._fieldbytes
        cur = self._txn.cursor()
        cur.find(pack_fielddockey(COLUMN_PREFIX, fieldbytes, 0))
        while cur.is_active():
            slotkey = cur.key()
            prefix, fb, startid = unpack_fielddockey(slotkey)
            if prefix != COLUMN_PREFIX or fb != fieldbytes:
                break
            block = self._get_slot(slotkey, create=False)
            if block:
                for i, value in enumerate(block.values()):
                    yield startid + i, value
            cur.next()
        cur.close()

    def add(self, docid, value):
        slotkey, offset, delta = self._key(docid)
        writer = self._get_slot(slotkey)
        writer[delta] = value

    def get(self, docid):
        slotkey, offset, delta = self._key(docid)
        reader = self._get_slot(slotkey, create=not self._readonly)
        if reader is None:
            return self._columnobj.default_value()
        else:
            return reader[delta]

    def sort_key(self, docid, reverse=False):
        slotkey, offset, delta = self._key(docid)
        reader = self._get_slot(slotkey, create=not self._readonly)
        if reader is None:
            return self._columnobj.default_value()
        else:
            return reader.sort_key(delta, reverse)

    def remove(self, docid):
        slotkey, offset, delta = self._key(docid)
        writer = self._get_slot(slotkey, create=False)
        if writer is not None:
            del writer[delta]

    def flush(self):
        for slotkey in self._map:
            self._flush_slot(slotkey)


# Codec

class ClodCodec(codec.Codec):
    def write_info(self, txn, ixinfo):
        txn[INDEXINFO_PREFIX] = pickle.dumps(ixinfo, 2)

    def info(self, txn):
        return pickle.loads(txn[INDEXINFO_PREFIX])

    def doc_writer(self, txn):
        return ClodDocWriter(txn)

    def doc_reader(self, txn):
        return ClodDocReader(txn)

    # def tag_writer(self, txn):
    #     return ClodTagWriter(txn)
    #
    # def tag_reader(self, txn):
    #     return ClodTagReader(txn)

    def column_writer(self, txn):
        return ClodColumnWriter(txn)

    def column_reader(self, txn, fieldname, fieldobj):
        return ClodColumnReader(txn, fieldname, fieldobj)

    def term_reader(self, txn):
        return ClodTermReader(txn)

    def automata(self, txn, fieldname, fieldobj):
        return ClodAutomata(txn, fieldname, fieldobj)


# Readers and writers

class ClodDocWriter(codec.DocWriter):
    def __init__(self, txn):
        self._txn = txn
        self._postinglimit = 100000

        self._docid = -1
        self._lastid = -1
        self._indoc = False
        self._stored = None

        # Keeps track of existing document IDs
        self._docmaps = DocmapCache(self._txn)
        # Maps terms to buffered posting lists
        self._postings = defaultdict(list)
        # Caches doc field length statistics
        self._lengths = LengthsCache(self._txn)
        # Caches fieldname -> fieldbytes encodings
        self._fieldcache = {}
        # Track the IDs of deleted documents to remove them from posting lists
        self._deleted = set()

        # self._tagmaps = TagmapCache(self._txn)

        self._postingcount = 0

    def clear(self):
        assert not self._indoc
        self._txn.clear()
        self._reset()

    def _reset(self):
        self._docmaps = DocmapCache(self._txn)
        self._postings = defaultdict(list)
        self._lengths = LengthsCache(self._txn)
        self._fieldcache = {}
        self._deleted = set()
        self._postingcount = 0

    def next_doc_id(self):
        return self._docmaps.next_doc_id()

    def start_doc(self, docid):
        assert not self._indoc
        assert docid >= 0
        if docid <= self._lastid:
            raise Exception("Doc ID is out of order (%r..%r)"
                            % (self._lastid, docid))

        self._docid = docid
        self._stored = {}
        self._indoc = True

    def _cache_field(self, fieldname, fieldobj):
        if fieldname in self._fieldcache:
            fieldbytes = self._fieldcache[fieldname][0]
        else:
            fieldbytes = encode_fieldname(fieldname)
            self._fieldcache[fieldname] = fieldbytes, fieldobj
        return fieldbytes

    def add_field(self, fieldname, fieldobj, storedval, fieldlen, update=False):
        fieldbytes = self._cache_field(fieldname, fieldobj)

        if fieldobj.stored and storedval is not None:
            self._stored[fieldname] = storedval

        if fieldobj.unique:
            if storedval is None:
                raise Exception("No value provided for unique field %r"
                                % fieldname)
            uniquebytes = fieldobj.to_bytes(storedval)
            key = pack_termkey(UNIQUE_PREFIX, fieldbytes, uniquebytes)

            if update:
                try:
                    oldidbytes = self._txn[key]
                except KeyError:
                    pass
                else:
                    oldid = unpack_long_le(oldidbytes)[0]
                    self.delete(oldid)

            self._txn[key] = pack_long_le(self._docid)

        if fieldlen:
            self._lengths.add_length(fieldbytes, self._docid, fieldlen)

    def add_field_postings(self, fieldname, fieldobj, fieldlen, posts):
        docid = self._docid
        postinglimit = self._postinglimit
        postings = self._postings
        pc = self._postingcount

        for post in posts:
            # Get the posting buffer list for this term. post.id contains the
            # term bytestring
            postlist = postings[fieldname, post.id]

            # Change the post's ID to the current document ID
            post.id = docid
            # Add the posting to the posting buffer
            postlist.append(post)

            # If the total number of postings is too large, flush them
            pc += 1
            if self._postingcount > postinglimit:
                self._flush_postings()
                pc = self._postingcount

        self._postingcount = pc

    def store_vector(self, fieldname, fieldobj, posts):
        form = fieldobj.vector
        buff = form.buffer(vector=True).from_list(posts)

        fieldbytes = self._cache_field(fieldname, fieldobj)
        key = pack_docfieldkey(VECTOR_PREFIX, self._docid, fieldbytes)
        self._txn[key] = zlib.compress(buff.to_bytes())

    def finish_doc(self):
        assert self._indoc
        txn = self._txn
        docid = self._docid
        stored = self._stored

        # Add the doc ID to the document bitmap
        self._docmaps.add(docid)

        # Write the stored fields
        if self._stored:
            txn[pack_dockey(STORED_PREFIX, docid)] = pickle.dumps(stored, -1)

        self._docid = None
        self._stored = None
        self._indoc = False
        self._lastid = docid

    def add_matcher(self, fieldname, fieldobj, termbytes, m, mapping=None):
        postinglimit = self._postinglimit
        pc = self._postingcount
        # Get the posting buffer list for this term
        postlist = self._postings[fieldname, termbytes]
        self._cache_field(fieldname, fieldobj)

        # Matcher.all_items() returns an iterator of a Posting object
        for post in m.all_values():
            if mapping:
                post.docid = mapping[post.docid]

            # Add the posting to the posting buffer list
            postlist.append(post)

            # If the total number of postings is too large, flush them
            pc += 1
            if self._postingcount > postinglimit:
                self._flush_postings()
                pc = self._postingcount
        self._postingcount = pc

    def delete(self, docid):
        txn = self._txn

        # Delete the document metadata
        # try:
        #     del txn[pack_doc_key(DOC_PREFIX, docid)]
        # except KeyError:
        #     pass

        # Delete the stored fields
        del txn[pack_dockey(STORED_PREFIX, docid)]
        # Remove any term vectors
        self._txn.delete_by_prefix(pack_dockey(VECTOR_PREFIX, docid))

        # Remove the doc ID from the document bitmap
        existed = docid in self._docmaps
        self._docmaps.discard(docid)

        if existed:
            # Remove the doc from length maps
            self._lengths.zero_out(docid)

            # Remember this ID was deleted for tidying
            self._deleted.add(docid)

    def remove_field_terms(self, fieldname):
        fieldbytes = encode_fieldname(fieldname)

        # Delete term entries
        prefix = pack_termkey(TERM_PREFIX, fieldbytes, emptybytes)
        self._txn.delete_by_prefix(prefix)

        # Delete posting entries
        prefix = pack_termkey(POSTING_PREFIX, fieldbytes, emptybytes)
        keys = list(self._txn.expand_prefix(prefix))
        for key in keys:
            del self._txn[key]

    def _flush_postings(self):
        cur = self._txn.cursor()
        postings = self._postings
        for (fieldname, termbytes), postlist in iteritems(postings):
            self._flush_term(fieldname, termbytes, postlist, cur)
        cur.close()
        self._postings = defaultdict(list)
        self._postingcount = 0

    def _flush_term(self, fieldname, termbytes, postlist, cur=None):
        txn = self._txn
        docmaps = self._docmaps
        deleted = self._deleted
        cur = cur or txn.cursor()

        fieldbytes, fieldobj = self._fieldcache[fieldname]
        form = fieldobj.format
        assert form
        if any((post.id in deleted) for post in postlist):
            postlist = [post for post in postlist if post.docid not in deleted]
        if not postlist:
            return

        # Update term stats
        tikey = pack_termkey(TERM_PREFIX, fieldbytes, termbytes)
        try:
            # Try loading the stored TermInfo from the database
            tibytes = self._txn[tikey]
        except KeyError:
            # It's not in the database, must be a new term, create a new TI
            terminfo = ClodTermInfo()
        else:
            # We got the bytes from the database, decode them into a TI
            terminfo = ClodTermInfo.from_bytes(tibytes)
        # Update the TermInfo object
        terminfo.update_from_list(postlist)
        # Save TermInfo back to the database
        txn[tikey] = terminfo.to_bytes()

        # Check for a sparse previous block and merge it if it exists
        prevkey = None

        for prevkey in posting_keys(cur, fieldbytes, termbytes):
            pass

        if prevkey is not None:
            pbytes = txn[prevkey]
            blockreader = form.reader().from_bytes(pbytes)
            if len(blockreader) < blocksize // 2:
                # Eliminate deleted documents from the old block
                existing = [post for post in blockreader.all_values()
                            if post.id in docmaps]
                if existing:
                    # Prepend the old postings to the new ones
                    assert existing[-1].id < postlist[0].id
                    postlist = existing + postlist

                # If the first document of the old block was deleted,
                # the start ID will be different, so remove the old key
                pstart, pend = unpack_key_ids(prevkey)
                if postlist[0].id != pstart or postlist[-1].id != pend:
                    del txn[prevkey]

        # Write out new blocks
        buff = form.buffer()
        for i in xrange(0, len(postlist), blocksize):
            buff.from_list(postlist[i:i + blocksize])
            minid = buff.min_id()
            maxid = buff.max_id()
            key = pack_postingkey(fieldbytes, termbytes, minid, maxid)
            txn[key] = buff.to_bytes()

    def _walk_terms(self, schema):
        txn = self._txn
        lastfieldbytes = None
        fieldname = None
        fieldobj = None
        postreader = None

        for key in txn.expand_prefix(TERM_PREFIX):
            _, fieldbytes, termbytes = unpack_termkey(key)
            if fieldbytes != lastfieldbytes:
                fieldname = decode_fieldname(fieldbytes)
                fieldobj = schema[fieldname]
                postreader = fieldobj.format.reader()
                lastfieldbytes = fieldbytes
            yield fieldname, fieldbytes, fieldobj, termbytes, postreader

    def tidy(self, schema):
        if not self._deleted:
            return

        txn = self._txn
        cur = txn.cursor()
        delset = self._deleted
        dellist = sorted(delset)

        deletekeys = set()
        for x in self._walk_terms(schema):
            fname, fbytes, fobj, termbytes, postreader = x
            postkeys = list(posting_keys(cur, fbytes, termbytes))
            ranges = [unpack_key_ids(pk) for pk in postkeys]
            count = len(postkeys)
            assert count
            tikey = pack_termkey(TERM_PREFIX, fbytes, termbytes)
            terminfo = None
            for i, (startid, endid) in enumerate(ranges):
                left = bisect_left(dellist, startid)
                right = bisect_left(dellist, endid + 1)
                if right > left:
                    postkey = postkeys[i]
                    postreader.from_bytes(txn[postkey])
                    gone = delset.intersection(postreader.all_ids())
                    if gone:
                        if terminfo is None:
                            terminfo = ClodTermInfo.from_bytes(txn[tikey])

                        gone_w = sum(postreader.weight(i) or 0
                                     for i in xrange(len(postreader))
                                     if postreader.id(i) in gone)
                        terminfo.subtract(len(gone), gone_w)

                        if len(gone) == len(postreader):
                            deletekeys.add(postkey)
                            count -= 1
                        else:
                            live = [p for p in postreader.all_values()
                                    if p.id not in delset]
                            buff = fobj.format.buffer().from_list(live)
                            newkey = pack_postingkey(fbytes, termbytes,
                                                     buff.min_id(),
                                                     buff.max_id())
                            if newkey != postkey:
                                deletekeys.add(postkey)
                            txn[newkey] = buff.to_bytes()
            if count <= 0:
                deletekeys.add(pack_termkey(TERM_PREFIX, fbytes, termbytes))
            elif terminfo:
                txn[tikey] = terminfo.to_bytes()

        cur.close()
        for key in deletekeys:
            del txn[key]
        self._deleted = set()

    def close(self):
        if self._postingcount:
            self._flush_postings()
        self._docmaps.flush()
        self._lengths.flush()


class ClodDocReader(codec.DocReader):
    def __init__(self, txn):
        self._txn = txn
        self._docmaps = DocmapCache(self._txn)
        self._doccount = self._docmaps.doc_count()
        self._lengths = LengthsCache(self._txn)
        self._fieldcache = {}
        self._length_totals = {}
        self._length_mins = {}
        self._length_maxes = {}

        self.all_doc_ids = self._docmaps.all_ids
        self.is_deleted = self._docmaps.is_deleted

    def _field_bytes(self, fieldname):
        try:
            fieldbytes = self._fieldcache[fieldname]
        except KeyError:
            fieldbytes = encode_fieldname(fieldname)
            self._fieldcache[fieldname] = fieldbytes
        return fieldbytes

    def doc_count(self):
        return self._doccount

    def doc_id_range(self):
        return self._docmaps.doc_id_range()

    def is_deleted(self, docid):
        return self._docmaps.is_deleted(docid)
        # return pack_doc_key(DOC_PREFIX, docid) in self._txn

    # def doc_field_length(self, docid, fieldname, default=0):
    #     fieldbytes = self._field_bytes(fieldname)
    #     return self._lengths.get_length(fieldbytes, docid, default)

    def all_doc_ids(self):
        return self._docmaps.all_ids()

    def field_length(self, fieldname):
        try:
            return self._length_totals[fieldname]
        except KeyError:
            pass
        length = self._lengths.total_length(self._field_bytes(fieldname))
        self._length_totals[fieldname] = length
        return length

    def min_field_length(self, fieldname):
        try:
            return self._length_mins[fieldname]
        except KeyError:
            pass
        length = self._lengths.min_length(self._field_bytes(fieldname))
        self._length_mins[fieldname] = length
        return length

    def max_field_length(self, fieldname):
        try:
            return self._length_maxes[fieldname]
        except KeyError:
            pass
        length = self._lengths.max_length(self._field_bytes(fieldname))
        self._length_maxes[fieldname] = length
        return length

    def stored_fields(self, docid):
        key = pack_dockey(STORED_PREFIX, docid)
        try:
            storedbytes = self._txn[key]
        except KeyError:
            raise reading.NoStoredFields(docid)
        else:
            return pickle.loads(storedbytes)

    def all_stored_fields(self):
        cur = self._txn.cursor()
        for key in cur.expand_prefix(STORED_PREFIX):
            _, docid = unpack_dockey(key)
            yield docid, pickle.loads(cur.value())

    def has_term_vector(self, docid, fieldname, fieldobj):
        fieldbytes = self._field_bytes(fieldname)
        key = pack_docfieldkey(VECTOR_PREFIX, docid, fieldbytes)
        return key in self._txn

    def term_vector(self, docid, fieldname, fieldobj):
        form = fieldobj.format
        fieldbytes = self._field_bytes(fieldname)
        key = pack_docfieldkey(VECTOR_PREFIX, docid, fieldbytes)
        try:
            vbytes = self._txn[key]
        except KeyError:
            raise reading.NoTermVector((docid, fieldname))
        else:
            vbytes = zlib.decompress(vbytes)
            return form.reader(vector=True).from_bytes(vbytes)


# class ClodTagWriter(codec.TagWriter):
#     def __init__(self, txn):
#         self._txn = txn
#         self._fields = FieldCache(self._txn, TAGNAMES_PREFIX)
#         self._tagmaps = TagmapCache(self._txn)
#
#     def add_tag(self, docid, tagname):
#         tagbytes = self._fields.add(tagname)
#         self._tagmaps.add(docid, tagbytes)
#
#     def remove_tag(self, docid, tagname):
#         tagbytes = self._fields.get_hash(tagname)
#         self._tagmaps.remove(docid, tagbytes)
#
#     def close(self):
#         pass
#         # self._fields.flush(self._tagmaps.all_tagbytes())
#
#
# class ClodTagReader(codec.TagReader):
#     def __init__(self, txn):
#         self._txn = txn
#         self._tagmaps = TagmapCache(self._txn)
#
#     def has_tag(self, docid, tagname):
#         tagbytes = self._tags.get_hash(tagname)
#         return (tagbytes, docid) in self._tagmaps
#
#     def ids_for_tag(self, tagname):
#         tagbytes = self._tags.get_hash(tagname)
#         return self._tagmaps.ids_for_tag(tagbytes)
#
#     def tags_for_id(self, docid):
#         taglist = self._tagmaps.all_tagbytes()
#         return self._tagmaps.tags_for_id(docid, taglist)
#
#     def all_tags(self):
#         tags = self._tags
#         for tagbytes in self._tagmaps.all_tagbytes():
#             yield tags.get_name(tagbytes)


class ClodColumnWriter(codec.ColumnWriter):
    def __init__(self, txn):
        self._txn = txn
        self._caches = {}

    def _cache_for_fieldname(self, fieldname, fieldobj):
        if fieldname in self._caches:
            cache = self._caches[fieldname]
        else:
            fieldbytes = encode_fieldname(fieldname)
            columnobj = fieldobj.column_type
            cache = ColumnCache(self._txn, fieldbytes, columnobj)
            self._caches[fieldname] = cache
        return cache

    def add_value(self, fieldname, fieldobj, docid, value):
        colcache = self._cache_for_fieldname(fieldname, fieldobj)
        colcache.add(docid, value)

    def remove_value(self, fieldname, fieldobj, docid):
        colcache = self._cache_for_fieldname(fieldname, fieldobj)
        colcache.remove(docid)

    def close(self):
        for cache in self._caches.values():
            cache.flush()


class ClodColumnReader(codec.ColumnReader):
    def __init__(self, txn, fieldname, fieldobj):
        self._txn = txn
        self._fieldname = fieldname
        self._fieldbytes = encode_fieldname(fieldname)
        self._columnobj = fieldobj.column_type
        self._cache = ColumnCache(self._txn, self._fieldbytes, self._columnobj,
                                  readonly=True)

    def __getitem__(self, docid):
        return self._cache.get(docid)

    def all_items(self):
        return self._cache.all_items()

    def sort_key(self, docid, reverse=False):
        return self._cache.sort_key(docid, reverse=reverse)

    def is_reversible(self):
        return self._columnobj.is_reversible()

    def exists(self):
        target = pack_fielddockey(COLUMN_PREFIX, self._fieldbytes, 0)
        key = self._txn.find(target)
        if key.startswith(COLUMN_PREFIX):
            _, fb, _ = unpack_fielddockey(key)
            return self._fieldbytes == fb
        return False


class ClodTermReader(codec.TermReader):
    def __init__(self, txn):
        self._txn = txn
        self._fieldcache = {}

    def __contains__(self, term):
        fieldname, termbytes = term
        assert isinstance(termbytes, bytes_type)
        fieldbytes = self._get_fieldbytes(fieldname)
        key = pack_termkey(TERM_PREFIX, fieldbytes, termbytes)
        return key in self._txn

    def _get_fieldbytes(self, fieldname):
        fc = self._fieldcache
        if fieldname in fc:
            fb = fc[fieldname]
        else:
            fb = fc[fieldname] = encode_fieldname(fieldname)
        return fb

    @staticmethod
    def _decode_terms(keys):
        fieldname = None
        last_fieldbytes = None
        for termkey in keys:
            _, fieldbytes, termbytes = unpack_termkey(termkey)
            if fieldname is None or fieldbytes != last_fieldbytes:
                fieldname = decode_fieldname(fieldbytes)
            yield fieldname, termbytes

    def terms(self):
        return self._decode_terms(self._txn.expand_prefix(TERM_PREFIX))

    def terms_from(self, fieldname, termbytes):
        fieldbytes = self._get_fieldbytes(fieldname)
        assert isinstance(termbytes, bytes_type)
        cur = self._txn.cursor()
        cur.find(pack_termkey(TERM_PREFIX, fieldbytes, termbytes))
        while cur.is_active():
            key = cur.key()
            if not key.startswith(TERM_PREFIX):
                return
            _, fb, tb = unpack_termkey(key)
            if fb != fieldbytes:
                return
            yield tb
            cur.next()

    def _decode_items(self, items):
        fieldname = None
        last_fieldbytes = None
        for termkey, value in items:
            _, fieldbytes, termbytes = unpack_termkey(termkey)
            if fieldname is None or fieldbytes != last_fieldbytes:
                fieldname = decode_fieldname(fieldbytes)
            ti = ClodTermInfo.from_bytes(value)
            yield (fieldname, termbytes), ti

    def _items_from(self, prefix):
        cur = self._txn.cursor()
        v = cur.value
        for key in cur.expand_prefix(prefix):
            yield key, v()

    def items(self):
        return self._decode_items(self._items_from(TERM_PREFIX))

    def items_from(self, fieldname, prefix):
        fieldbytes = self._get_fieldbytes(fieldname)
        keyprefix = pack_termkey(TERM_PREFIX, fieldbytes, prefix)
        return self._decode_items(self._items_from(keyprefix))

    def term_id_range(self, fieldname, termbytes):
        fieldbytes = self._get_fieldbytes(fieldname)
        cur = self._txn.cursor()
        minid = maxid = None
        for key in posting_keys(cur, fieldbytes, termbytes):
            startid, endid = unpack_docids(key)
            if minid is None:
                minid = startid
            maxid = endid
        return minid, maxid

    def term_info(self, fieldname, termbytes):
        fieldbytes = self._get_fieldbytes(fieldname)
        assert isinstance(termbytes, bytes_type)
        key = pack_termkey(TERM_PREFIX, fieldbytes, termbytes)
        try:
            tibytes = self._txn[key]
        except KeyError:
            raise reading.TermNotFound((fieldname, termbytes))
        return ClodTermInfo.from_bytes(tibytes)

    def indexed_field_names(self):
        cur = self._txn.cursor()
        cur.find(TERM_PREFIX)
        while cur.is_active():
            key = cur.key()
            if key.startswith(TERM_PREFIX):
                fieldbytes = unpack_key_field(key)
                yield decode_fieldname(fieldbytes)
                cur.find(TERM_PREFIX + fieldbytes + b"\x01", fromfirst=False)
            else:
                break

        # last = None
        # for termkey in self._txn.expand_prefix(TERM_PREFIX):
        #     if last and termkey.startswith(last):
        #         continue
        #     fieldbytes = unpack_key_field(termkey)
        #     yield decode_fieldname(fieldbytes)
        #     last = TERM_PREFIX + fieldbytes + null

    def matcher(self, fieldname, fieldobj, termbytes, scorer=None):
        terminfo = self.term_info(fieldname, termbytes)
        form = fieldobj.format
        return ClodMatcher(self._txn, fieldname, termbytes, terminfo, form,
                           scorer=scorer)

    def unique_id(self, fieldname, fieldobj, termbytes):
        fieldbytes = self._get_fieldbytes(fieldname)
        key = pack_termkey(UNIQUE_PREFIX, fieldbytes, termbytes)
        try:
            uniquebytes = self._txn[key]
        except KeyError:
            return None
        else:
            return unpack_long_le(uniquebytes)[0]


class ClodMatcher(LeafMatcher):
    def __init__(self, txn, fieldname, termbytes, terminfo, form, scorer=None):
        self._txn = txn
        self._fieldname = fieldname
        self._fieldbytes = encode_fieldname(fieldname)
        self._termbytes = termbytes
        self._term = self._fieldname, self._termbytes
        self._prefix = pack_termkey(POSTING_PREFIX, self._fieldbytes,
                                    self._termbytes)
        self._terminfo = terminfo
        self._format = form
        self._scorer = scorer

        self._cursor = self._txn.cursor()
        self._active = False
        self._block = self._format.reader()
        self._i = 0
        self._blocklen = 0
        self._blockq = 0
        self.reset()

    def is_active(self):
        return self._active

    def reset(self):
        self._cursor.find(self._prefix)
        self._read_block()

    def replace(self, minquality=0):
        if minquality > self.max_quality():
            return NullMatcher
        else:
            return self

    def supports_block_quality(self):
        return self._scorer and self._scorer.supports_block_quality()

    def max_quality(self):
        return self._scorer.max_quality()

    def block_quality(self):
        return self._scorer.block_quality(self)

    def id(self):
        return self._block.id(self._i)

    def all_ids(self):
        for block in self._all_blocks():
            for docid in block.all_ids():
                yield docid

    def all_values(self):
        for block in self._all_blocks():
            for post in block.all_values():
                yield post

    def value(self):
        return self._block.value(self._i)

    def supports(self, name):
        return self._format.supports(name)

    def skip_to(self, docid):
        if not self._active:
            raise ReadTooFar

        skipped = 0
        while self._active and docid > self._block.max_id():
            self._next_block()
            skipped += 1
        if self._active:
            self._i = self._block.find(docid)
        return skipped

    def skip_to_quality(self, minquality):
        bqfn = self.block_quality
        next_block = self._next_block
        count = 0
        while self._active and bqfn() <= minquality:
            count += 1
            next_block()
        return count

    def next(self):
        if not self._active:
            raise ReadTooFar
        self._i += 1
        if self._i == self._blocklen:
            self._next_block()
            return True
        else:
            return False

    def length(self):
        length = self._block.length(self._i)
        return length if length is not None else 0

    def weight(self):
        w = self._block.weight(self._i)
        return w if w is not None else 1.0

    def positions(self):
        return self._block.positions(self._i)

    def chars(self):
        return self._block.chars(self._i)

    def payloads(self):
        return self._block.payloads(self._i)

    def score(self):
        return self._scorer.score(self)

    # Block info methods

    def block_min_id(self):
        return self._block.min_id()

    def block_max_id(self):
        return self._block.max_id()

    def block_min_length(self):
        return self._block.min_length()

    def block_max_length(self):
        return self._block.max_length()

    def block_max_weight(self):
        return self._block.max_weight()

    # Support methods

    def _all_blocks(self):
        block = self._block
        while self._active:
            yield block
            self._next_block()

    def _next_block(self):
        t = now()
        cur = self._cursor
        cur.next()
        self._read_block()

    def _read_block(self):
        cur = self._cursor
        block = self._block
        self._i = 0
        if cur.is_active():
            key = cur.key()
            prefix = self._prefix
            if (
                key.startswith(self._prefix)
                and len(key) == len(prefix) + docids_size
            ):
                self._active = True
                block.from_bytes(cur.value())
                self._blocklen = len(block)
                return

        self._active = False


class FieldCursor(object):
    def __init__(self, fieldbytes, fieldobj, cur):
        self._fieldbytes = fieldbytes
        self._fieldobj = fieldobj
        self._tobytes = fieldobj.to_bytes
        self._frombytes = fieldobj.from_bytes
        self._prefix = pack_termkey(TERM_PREFIX, self._fieldbytes, emptybytes)
        self._cur = cur
        self._term = None
        self.first()

    def _check(self):
        prefix = self._prefix
        if self._cur.is_active():
            key = self._cur.key()
            if key.startswith(prefix):
                self._term = self._frombytes(key[len(prefix):])
                return
        self._term = None

    def first(self):
        self._cur.find(self._prefix)
        self._check()

    def find(self, string):
        self._cur.find(self._prefix + self._tobytes(string), fromfirst=False)
        self._check()

    def next(self):
        self._cur.next()
        self._check()

    def term(self):
        return self._term


class ClodAutomata(codec.Automata):
    def __init__(self, txn, fieldname, fieldobj):
        self._txn = txn
        self._fieldname = fieldname
        self._fieldbytes = encode_fieldname(fieldname)
        self._fieldobj = fieldobj

    def _key_to_term(self, key):
        if key:
            prefix, fieldbytes, termbytes = unpack_termkey(key)
            if fieldbytes == self._fieldbytes:
                return self._fieldobj.from_bytes(termbytes)
        return None

    def _term_to_key(self, term):
        return pack_termkey(TERM_PREFIX, self._fieldbytes,
                            self._fieldobj.to_bytes(term))

    def find_matches(self, dfa):
        cur = self._txn.cursor()
        fieldcur = FieldCursor(self._fieldbytes, self._fieldobj, cur)
        unull = unichr(0)

        term = fieldcur.term()
        if term is None:
            return

        match = dfa.next_valid_string(term)
        while match:
            fieldcur.find(match)
            term = fieldcur.term()
            if term is None:
                return
            if match == term:
                yield match
                term += unull
            match = dfa.next_valid_string(term)

    def terms_within(self, uterm, maxdist, prefix=0):
        dfa = self.levenshtein_dfa(uterm, maxdist, prefix)
        return self.find_matches(dfa)
