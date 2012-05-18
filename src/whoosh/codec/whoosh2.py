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


from array import array
from collections import defaultdict
from struct import Struct

from whoosh.compat import (loads, dumps, xrange, iteritems, itervalues, b,
                           bytes_type, string_type, integer_types)
from whoosh.codec import base
from whoosh.codec.base import (minimize_ids, deminimize_ids, minimize_weights,
                               deminimize_weights, minimize_values,
                               deminimize_values)
from whoosh.filedb.fileindex import TOC, clean_files
from whoosh.filedb.filetables import CodedOrderedWriter, CodedOrderedReader
from whoosh.matching import ListMatcher
from whoosh.reading import TermNotFound
from whoosh.store import Storage
from whoosh.support.dawg import GraphWriter, GraphReader
from whoosh.system import (pack_ushort, pack_long, unpack_ushort, unpack_long,
                           _INT_SIZE, _LONG_SIZE)
from whoosh.util import byte_to_length, length_to_byte, utf8encode, utf8decode


# Standard codec top-level object

class W2Codec(base.Codec):
    TERMS_EXT = ".trm"  # Term index
    POSTS_EXT = ".pst"  # Term postings
    DAWG_EXT = ".dag"  # Spelling graph file
    LENGTHS_EXT = ".fln"  # Field lengths file
    VECTOR_EXT = ".vec"  # Vector index
    VPOSTS_EXT = ".vps"  # Vector postings
    STORED_EXT = ".sto"  # Stored fields file

    def __init__(self, blocklimit=128, compression=3, loadlengths=False,
                 inlinelimit=1):
        self.blocklimit = blocklimit
        self.compression = compression
        self.loadlengths = loadlengths
        self.inlinelimit = inlinelimit

    # Per-document value writer
    def per_document_writer(self, storage, segment):
        return W2PerDocWriter(storage, segment, blocklimit=self.blocklimit,
                              compression=self.compression)

    # Inverted index writer
    def field_writer(self, storage, segment):
        return W2FieldWriter(storage, segment, blocklimit=self.blocklimit,
                             compression=self.compression,
                             inlinelimit=self.inlinelimit)

    # Readers

    def terms_reader(self, storage, segment):
        tifile = segment.open_file(storage, self.TERMS_EXT)
        postfile = segment.open_file(storage, self.POSTS_EXT)
        return W2TermsReader(tifile, postfile)

    def lengths_reader(self, storage, segment):
        flfile = segment.open_file(storage, self.LENGTHS_EXT)
        doccount = segment.doc_count_all()

        # Check the first byte of the file to see if it's an old format
        firstbyte = flfile.read(1)
        flfile.seek(0)
        if firstbyte != b("~"):
            from whoosh.codec.legacy import load_old_lengths
            lengths = load_old_lengths(InMemoryLengths(), flfile, doccount)
        elif self.loadlengths:
            lengths = InMemoryLengths.from_file(flfile, doccount)
        else:
            lengths = OnDiskLengths(flfile, doccount)
        return lengths

    def vector_reader(self, storage, segment):
        vifile = segment.open_file(storage, self.VECTOR_EXT)
        postfile = segment.open_file(storage, self.VPOSTS_EXT)
        return W2VectorReader(vifile, postfile)

    def stored_fields_reader(self, storage, segment):
        sffile = segment.open_file(storage, self.STORED_EXT)
        return StoredFieldReader(sffile)

    def graph_reader(self, storage, segment):
        dawgfile = segment.open_file(storage, self.DAWG_EXT)
        return GraphReader(dawgfile)

    # Segments and generations

    def new_segment(self, storage, indexname):
        return W2Segment(indexname)

    def commit_toc(self, storage, indexname, schema, segments, generation,
                   clean=True):
        toc = TOC(schema, segments, generation)
        toc.write(storage, indexname)
        # Delete leftover files
        if clean:
            clean_files(storage, indexname, generation, segments)


# Per-document value writer

class W2PerDocWriter(base.PerDocumentWriter):
    def __init__(self, storage, segment, blocklimit=128, compression=3):
        if not isinstance(blocklimit, int):
            raise ValueError
        self.storage = storage
        self.segment = segment
        self.blocklimit = blocklimit
        self.compression = compression
        self.doccount = 0

        sffile = segment.create_file(storage, W2Codec.STORED_EXT)
        self.stored = StoredFieldWriter(sffile)
        self.storedfields = None

        self.lengths = InMemoryLengths()

        # We'll wait to create the vector files until someone actually tries
        # to add a vector
        self.vindex = self.vpostfile = None

    def _make_vector_files(self):
        vifile = self.segment.create_file(self.storage, W2Codec.VECTOR_EXT)
        self.vindex = VectorWriter(vifile)
        self.vpostfile = self.segment.create_file(self.storage,
                                                  W2Codec.VPOSTS_EXT)

    def start_doc(self, docnum):
        self.docnum = docnum
        self.storedfields = {}
        self.doccount = max(self.doccount, docnum + 1)

    def add_field(self, fieldname, fieldobj, value, length):
        if length:
            self.lengths.add(self.docnum, fieldname, length)
        if value is not None:
            self.storedfields[fieldname] = value

    def _new_block(self, vformat):
        postingsize = vformat.posting_size
        return W2Block(postingsize, stringids=True)

    def add_vector_items(self, fieldname, fieldobj, items):
        if self.vindex is None:
            self._make_vector_files()

        # items = (text, freq, weight, valuestring) ...
        postfile = self.vpostfile
        blocklimit = self.blocklimit
        block = self._new_block(fieldobj.vector)

        startoffset = postfile.tell()
        postfile.write(block.magic)  # Magic number
        blockcount = 0
        postfile.write_uint(0)  # Placeholder for block count

        countdown = blocklimit
        for text, _, weight, valuestring in items:
            block.add(text, weight, valuestring)
            countdown -= 1
            if countdown == 0:
                block.to_file(postfile, compression=self.compression)
                block = self._new_block(fieldobj.vector)
                blockcount += 1
                countdown = blocklimit
        # If there are leftover items in the current block, write them out
        if block:
            block.to_file(postfile, compression=self.compression)
            blockcount += 1

        # Seek back to the start of this list of posting blocks and write the
        # number of blocks
        postfile.flush()
        here = postfile.tell()
        postfile.seek(startoffset + 4)
        postfile.write_uint(blockcount)
        postfile.seek(here)

        # Add to the index
        self.vindex.add((self.docnum, fieldname), startoffset)

    def add_vector_matcher(self, fieldname, fieldobj, vmatcher):
        def readitems():
            while vmatcher.is_active():
                text = vmatcher.id()
                weight = vmatcher.weight()
                valuestring = vmatcher.value()
                yield (text, None, weight, valuestring)
                vmatcher.next()
        self.add_vector_items(fieldname, fieldobj, readitems())

    def finish_doc(self):
        self.stored.add(self.storedfields)
        self.storedfields = None

    def lengths_reader(self):
        return self.lengths

    def close(self):
        if self.storedfields is not None:
            self.stored.add(self.storedfields)
        self.stored.close()
        flfile = self.segment.create_file(self.storage, W2Codec.LENGTHS_EXT)
        self.lengths.to_file(flfile, self.doccount)
        if self.vindex:
            self.vindex.close()
            self.vpostfile.close()


# Inverted index writer

class W2FieldWriter(base.FieldWriter):
    def __init__(self, storage, segment, blocklimit=128, compression=3,
                 inlinelimit=1):
        assert isinstance(storage, Storage)
        assert isinstance(segment, base.Segment)
        assert isinstance(blocklimit, int)
        assert isinstance(compression, int)
        assert isinstance(inlinelimit, int)

        self.storage = storage
        self.segment = segment
        self.fieldname = None
        self.text = None
        self.field = None
        self.format = None
        self.spelling = False

        tifile = segment.create_file(storage, W2Codec.TERMS_EXT)
        self.termsindex = TermIndexWriter(tifile)
        self.postfile = segment.create_file(storage, W2Codec.POSTS_EXT)

        # We'll wait to create the DAWG builder until someone actually adds
        # a spelled field
        self.dawg = None

        self.blocklimit = blocklimit
        self.compression = compression
        self.inlinelimit = inlinelimit
        self.block = None
        self.terminfo = None
        self._infield = False

    def _make_dawg_files(self):
        dawgfile = self.segment.create_file(self.storage, W2Codec.DAWG_EXT)
        self.dawg = GraphWriter(dawgfile)

    def _new_block(self):
        return W2Block(self.format.posting_size)

    def _reset_block(self):
        self.block = self._new_block()

    def _write_block(self):
        self.terminfo.add_block(self.block)
        self.block.to_file(self.postfile, compression=self.compression)
        self._reset_block()
        self.blockcount += 1

    def _start_blocklist(self):
        postfile = self.postfile
        self._reset_block()

        # Magic number
        self.startoffset = postfile.tell()
        postfile.write(W2Block.magic)
        # Placeholder for block count
        self.blockcount = 0
        postfile.write_uint(0)

    def start_field(self, fieldname, fieldobj):
        self.fieldname = fieldname
        self.field = fieldobj
        self.format = fieldobj.format
        self.spelling = fieldobj.spelling and not fieldobj.separate_spelling()
        self._dawgfield = False
        if self.spelling or fieldobj.separate_spelling():
            if self.dawg is None:
                self._make_dawg_files()
            self.dawg.start_field(fieldname)
            self._dawgfield = True
        self._infield = True

    def start_term(self, text):
        if self.block is not None:
            raise Exception("Called start_term in a block")
        self.text = text
        self.terminfo = base.FileTermInfo()
        if self.spelling:
            self.dawg.insert(text)
        self._start_blocklist()

    def add(self, docnum, weight, valuestring, length):
        self.block.add(docnum, weight, valuestring, length)
        if len(self.block) > self.blocklimit:
            self._write_block()

    def add_spell_word(self, fieldname, text):
        if self.dawg is None:
            self._make_dawg_files()
        self.dawg.insert(text)

    def finish_term(self):
        block = self.block
        if block is None:
            raise Exception("Called finish_term when not in a block")

        terminfo = self.terminfo
        if self.blockcount < 1 and block and len(block) < self.inlinelimit:
            # Inline the single block
            terminfo.add_block(block)
            vals = None if not block.values else tuple(block.values)
            postings = (tuple(block.ids), tuple(block.weights), vals)
        else:
            if block:
                # Write the current unfinished block to disk
                self._write_block()

            # Seek back to the start of this list of posting blocks and write
            # the number of blocks
            postfile = self.postfile
            postfile.flush()
            here = postfile.tell()
            postfile.seek(self.startoffset + 4)
            postfile.write_uint(self.blockcount)
            postfile.seek(here)

            self.block = None
            postings = self.startoffset

        self.block = None
        terminfo.postings = postings
        self.termsindex.add((self.fieldname, self.text), terminfo)

    def finish_field(self):
        if not self._infield:
            raise Exception("Called finish_field before start_field")
        self._infield = False

        if self._dawgfield:
            self.dawg.finish_field()
            self._dawgfield = False

    def close(self):
        self.termsindex.close()
        self.postfile.close()
        if self.dawg is not None:
            self.dawg.close()


# Matcher

class PostingMatcher(base.BlockPostingMatcher):
    def __init__(self, postfile, startoffset, fmt, scorer=None, term=None,
                 stringids=False):
        self.postfile = postfile
        self.startoffset = startoffset
        self.format = fmt
        self.scorer = scorer
        self._term = term
        self.stringids = stringids

        postfile.seek(startoffset)
        magic = postfile.read(4)
        if magic != W2Block.magic:
            from whoosh.codec.legacy import old_block_type
            self.blockclass = old_block_type(magic)
        else:
            self.blockclass = W2Block

        self.blockcount = postfile.read_uint()
        self.baseoffset = postfile.tell()

        self._active = True
        self.currentblock = -1
        self._next_block()

    def is_active(self):
        return self._active

    def _read_block(self, offset):
        pf = self.postfile
        pf.seek(offset)
        return self.blockclass.from_file(pf, self.format.posting_size,
                                         stringids=self.stringids)

    def _consume_block(self):
        self.block.read_ids()
        self.block.read_weights()
        self.i = 0

    def _next_block(self, consume=True):
        if not (self.currentblock < self.blockcount):
            raise Exception("No next block")

        self.currentblock += 1
        if self.currentblock == self.blockcount:
            self._active = False
            return

        if self.currentblock == 0:
            pos = self.baseoffset
        else:
            pos = self.block.nextoffset

        self.block = self._read_block(pos)
        if consume:
            self._consume_block()

    def _skip_to_block(self, targetfn):
        skipped = 0
        while self._active and targetfn():
            self._next_block(consume=False)
            skipped += 1

        if self._active:
            self._consume_block()

        return skipped

    def score(self):
        return self.scorer.score(self)


# Tables

# Term index

class TermIndexWriter(CodedOrderedWriter):
    def __init__(self, dbfile):
        super(TermIndexWriter, self).__init__(dbfile)
        self.fieldcounter = 0
        self.fieldmap = {}

    def keycoder(self, key):
        # Encode term
        fieldmap = self.fieldmap
        fieldname, text = key

        if fieldname in fieldmap:
            fieldnum = fieldmap[fieldname]
        else:
            fieldnum = self.fieldcounter
            fieldmap[fieldname] = fieldnum
            self.fieldcounter += 1

        key = pack_ushort(fieldnum) + utf8encode(text)[0]
        return key

    def valuecoder(self, terminfo):
        return terminfo.to_string()

    def close(self):
        self._write_hashes()
        dbfile = self.dbfile

        dbfile.write_uint(len(self.index))
        for n in self.index:
            dbfile.write_long(n)
        dbfile.write_pickle(self.fieldmap)

        self._write_directory()
        self.dbfile.close()


class PostingIndexBase(CodedOrderedReader):
    # Shared base class for terms index and vector index readers
    def __init__(self, dbfile, postfile):
        CodedOrderedReader.__init__(self, dbfile)
        self.postfile = postfile

        dbfile.seek(self.indexbase + self.length * _LONG_SIZE)
        self.fieldmap = dbfile.read_pickle()
        self.names = [None] * len(self.fieldmap)
        for name, num in iteritems(self.fieldmap):
            self.names[num] = name

    def close(self):
        CodedOrderedReader.close(self)
        self.postfile.close()


class W2TermsReader(PostingIndexBase):
    # Implements whoosh.codec.base.TermsReader

    def terminfo(self, fieldname, text):
        return self[fieldname, text]

    def matcher(self, fieldname, text, format_, scorer=None):
        # Note this does not filter out deleted documents; a higher level is
        # expected to wrap this matcher to eliminate deleted docs
        pf = self.postfile
        term = (fieldname, text)
        try:
            terminfo = self[term]
        except KeyError:
            raise TermNotFound("No term %s:%r" % (fieldname, text))

        p = terminfo.postings
        if isinstance(p, integer_types):
            # terminfo.postings is an offset into the posting file
            pr = PostingMatcher(pf, p, format_, scorer=scorer, term=term)
        else:
            # terminfo.postings is an inlined tuple of (ids, weights, values)
            docids, weights, values = p
            pr = ListMatcher(docids, weights, values, format_, scorer=scorer,
                             term=term, terminfo=terminfo)
        return pr

    def keycoder(self, key):
        fieldname, text = key
        fnum = self.fieldmap.get(fieldname, 65535)
        return pack_ushort(fnum) + utf8encode(text)[0]

    def keydecoder(self, v):
        assert isinstance(v, bytes_type)
        return (self.names[unpack_ushort(v[:2])[0]], utf8decode(v[2:])[0])

    def valuedecoder(self, v):
        assert isinstance(v, bytes_type)
        return base.FileTermInfo.from_string(v)

    def frequency(self, key):
        datapos = self.range_for_key(key)[0]
        return base.FileTermInfo.read_weight(self.dbfile, datapos)

    def doc_frequency(self, key):
        datapos = self.range_for_key(key)[0]
        return base.FileTermInfo.read_doc_freq(self.dbfile, datapos)


# Vectors

# docnum, fieldnum
_vectorkey_struct = Struct("!IH")


class VectorWriter(TermIndexWriter):
    def keycoder(self, key):
        fieldmap = self.fieldmap
        docnum, fieldname = key

        if fieldname in fieldmap:
            fieldnum = fieldmap[fieldname]
        else:
            fieldnum = self.fieldcounter
            fieldmap[fieldname] = fieldnum
            self.fieldcounter += 1

        return _vectorkey_struct.pack(docnum, fieldnum)

    def valuecoder(self, offset):
        return pack_long(offset)


class W2VectorReader(PostingIndexBase):
    # Implements whoosh.codec.base.VectorReader

    def matcher(self, docnum, fieldname, format_):
        pf = self.postfile
        offset = self[(docnum, fieldname)]
        pr = PostingMatcher(pf, offset, format_, stringids=True)
        return pr

    def keycoder(self, key):
        return _vectorkey_struct.pack(key[0], self.fieldmap[key[1]])

    def keydecoder(self, v):
        docnum, fieldnum = _vectorkey_struct.unpack(v)
        return (docnum, self.names[fieldnum])

    def valuedecoder(self, v):
        return unpack_long(v)[0]


# Field lengths

class LengthsBase(base.LengthsReader):
    magic = b("~LN1")

    def __init__(self):
        self.starts = {}
        self.totals = {}
        self.minlens = {}
        self.maxlens = {}

    def _read_header(self, dbfile, doccount):
        first = dbfile.read(4)  # Magic
        assert first == self.magic
        version = dbfile.read_int()  # Version number
        assert version == 1

        dc = dbfile.read_uint()  # Number of documents saved
        if doccount is None:
            doccount = dc
        assert dc == doccount, "read=%s argument=%s" % (dc, doccount)
        self._count = doccount

        fieldcount = dbfile.read_ushort()  # Number of fields
        # Read per-field info
        for i in xrange(fieldcount):
            fieldname = dbfile.read_string().decode('utf-8')
            self.totals[fieldname] = dbfile.read_long()
            self.minlens[fieldname] = byte_to_length(dbfile.read_byte())
            self.maxlens[fieldname] = byte_to_length(dbfile.read_byte())
            self.starts[fieldname] = i * doccount

        # Add header length to per-field offsets
        eoh = dbfile.tell()  # End of header
        for fieldname in self.starts:
            self.starts[fieldname] += eoh

    def doc_count_all(self):
        return self._count

    def field_length(self, fieldname):
        return self.totals.get(fieldname, 0)

    def min_field_length(self, fieldname):
        return self.minlens.get(fieldname, 0)

    def max_field_length(self, fieldname):
        return self.maxlens.get(fieldname, 0)


class InMemoryLengths(LengthsBase):
    def __init__(self):
        LengthsBase.__init__(self)
        self.totals = defaultdict(int)
        self.lengths = {}
        self._count = 0

    # IO

    def to_file(self, dbfile, doccount):
        self._pad_arrays(doccount)
        fieldnames = list(self.lengths.keys())

        dbfile.write(self.magic)
        dbfile.write_int(1)  # Format version number
        dbfile.write_uint(doccount)  # Number of documents
        dbfile.write_ushort(len(self.lengths))  # Number of fields

        # Write per-field info
        for fieldname in fieldnames:
            dbfile.write_string(fieldname.encode('utf-8'))  # Fieldname
            dbfile.write_long(self.field_length(fieldname))
            dbfile.write_byte(length_to_byte(self.min_field_length(fieldname)))
            dbfile.write_byte(length_to_byte(self.max_field_length(fieldname)))

        # Write byte arrays
        for fieldname in fieldnames:
            dbfile.write_array(self.lengths[fieldname])
        dbfile.close()

    @classmethod
    def from_file(cls, dbfile, doccount=None):
        obj = cls()
        obj._read_header(dbfile, doccount)
        for fieldname, start in iteritems(obj.starts):
            obj.lengths[fieldname] = dbfile.get_array(start, "B", obj._count)
        dbfile.close()
        return obj

    # Get

    def doc_field_length(self, docnum, fieldname, default=0):
        try:
            arry = self.lengths[fieldname]
        except KeyError:
            return default
        if docnum >= len(arry):
            return default
        return byte_to_length(arry[docnum])

    # Min/max cache setup -- not meant to be called while adding

    def _minmax(self, fieldname, op, cache):
        if fieldname in cache:
            return cache[fieldname]
        else:
            ls = self.lengths[fieldname]
            if ls:
                result = byte_to_length(op(ls))
            else:
                result = 0
            cache[fieldname] = result
            return result

    def min_field_length(self, fieldname):
        return self._minmax(fieldname, min, self.minlens)

    def max_field_length(self, fieldname):
        return self._minmax(fieldname, max, self.maxlens)

    # Add

    def _create_field(self, fieldname, docnum):
        dc = max(self._count, docnum + 1)
        self.lengths[fieldname] = array("B", (0 for _ in xrange(dc)))
        self._count = dc

    def _pad_arrays(self, doccount):
        # Pad out arrays to full length
        for fieldname in self.lengths.keys():
            arry = self.lengths[fieldname]
            if len(arry) < doccount:
                for _ in xrange(doccount - len(arry)):
                    arry.append(0)
        self._count = doccount

    def add(self, docnum, fieldname, length):
        lengths = self.lengths
        if length:
            if fieldname not in lengths:
                self._create_field(fieldname, docnum)

            arry = self.lengths[fieldname]
            count = docnum + 1
            if len(arry) < count:
                for _ in xrange(count - len(arry)):
                    arry.append(0)
            if count > self._count:
                self._count = count
            byte = length_to_byte(length)
            arry[docnum] = byte
            self.totals[fieldname] += length

    def add_other(self, other):
        lengths = self.lengths
        totals = self.totals
        doccount = self._count
        for fname in other.lengths:
            if fname not in lengths:
                lengths[fname] = array("B")
        self._pad_arrays(doccount)

        for fname in other.lengths:
            lengths[fname].extend(other.lengths[fname])
        self._count = doccount + other._count
        self._pad_arrays(self._count)

        for fname in other.totals:
            totals[fname] += other.totals[fname]


class OnDiskLengths(LengthsBase):
    def __init__(self, dbfile, doccount=None):
        LengthsBase.__init__(self)
        self.dbfile = dbfile
        self._read_header(dbfile, doccount)

    def doc_field_length(self, docnum, fieldname, default=0):
        try:
            start = self.starts[fieldname]
        except KeyError:
            return default
        return byte_to_length(self.dbfile.get_byte(start + docnum))

    def close(self):
        self.dbfile.close()


# Stored fields

_stored_pointer_struct = Struct("!qI")  # offset, length
stored_pointer_size = _stored_pointer_struct.size
pack_stored_pointer = _stored_pointer_struct.pack
unpack_stored_pointer = _stored_pointer_struct.unpack


class StoredFieldWriter(object):
    def __init__(self, dbfile):
        self.dbfile = dbfile
        self.length = 0
        self.directory = []

        self.dbfile.write_long(0)
        self.dbfile.write_uint(0)

        self.names = []
        self.name_map = {}

    def add(self, vdict):
        f = self.dbfile
        names = self.names
        name_map = self.name_map

        vlist = [None] * len(names)
        for k, v in iteritems(vdict):
            if k in name_map:
                vlist[name_map[k]] = v
            else:
                name_map[k] = len(names)
                names.append(k)
                vlist.append(v)

        vstring = dumps(tuple(vlist), -1)[2:-1]
        self.length += 1
        self.directory.append(pack_stored_pointer(f.tell(), len(vstring)))
        f.write(vstring)

    def add_reader(self, sfreader):
        add = self.add
        for vdict in sfreader:
            add(vdict)

    def close(self):
        f = self.dbfile
        dirpos = f.tell()
        f.write_pickle(self.names)
        for pair in self.directory:
            f.write(pair)
        f.flush()
        f.seek(0)
        f.write_long(dirpos)
        f.write_uint(self.length)
        f.close()


class StoredFieldReader(object):
    def __init__(self, dbfile):
        self.dbfile = dbfile

        dbfile.seek(0)
        dirpos = dbfile.read_long()
        self.length = dbfile.read_uint()
        self.basepos = dbfile.tell()

        dbfile.seek(dirpos)

        nameobj = dbfile.read_pickle()
        if isinstance(nameobj, dict):
            # Previous versions stored the list of names as a map of names to
            # positions... it seemed to make sense at the time...
            self.names = [None] * len(nameobj)
            for name, pos in iteritems(nameobj):
                self.names[pos] = name
        else:
            self.names = nameobj
        self.directory_offset = dbfile.tell()

    def close(self):
        self.dbfile.close()

    def __iter__(self):
        dbfile = self.dbfile
        names = self.names
        lengths = array("I")

        dbfile.seek(self.directory_offset)
        for i in xrange(self.length):
            dbfile.seek(_LONG_SIZE, 1)
            lengths.append(dbfile.read_uint())

        dbfile.seek(self.basepos)
        for length in lengths:
            vlist = loads(dbfile.read(length) + b("."))
            vdict = dict((names[i], vlist[i]) for i in xrange(len(vlist))
                     if vlist[i] is not None)
            yield vdict

    def __getitem__(self, num):
        if num > self.length - 1:
            raise IndexError("Tried to get document %s, file has %s"
                             % (num, self.length))

        dbfile = self.dbfile
        start = self.directory_offset + num * stored_pointer_size
        dbfile.seek(start)
        ptr = dbfile.read(stored_pointer_size)
        if len(ptr) != stored_pointer_size:
            raise Exception("Error reading %r @%s %s < %s"
                            % (dbfile, start, len(ptr), stored_pointer_size))
        position, length = unpack_stored_pointer(ptr)
        dbfile.seek(position)
        vlist = loads(dbfile.read(length) + b("."))

        names = self.names
        # Recreate a dictionary by putting the field names and values back
        # together by position. We can't just use dict(zip(...)) because we
        # want to filter out the None values.
        vdict = dict((names[i], vlist[i]) for i in xrange(len(vlist))
                     if vlist[i] is not None)
        return vdict


# Segment object

class W2Segment(base.Segment):
    def __init__(self, indexname, doccount=0, segid=None, deleted=None):
        """
        :param name: The name of the segment (the Index object computes this
            from its name and the generation).
        :param doccount: The maximum document number in the segment.
        :param term_count: Total count of all terms in all documents.
        :param deleted: A set of deleted document numbers, or None if no
            deleted documents exist in this segment.
        """

        assert isinstance(indexname, string_type)
        self.indexname = indexname
        assert isinstance(doccount, integer_types)
        self.doccount = doccount
        self.segid = self._random_id() if segid is None else segid
        self.deleted = deleted
        self.compound = False

    def codec(self, **kwargs):
        return W2Codec(**kwargs)

    def doc_count_all(self):
        return self.doccount

    def doc_count(self):
        return self.doccount - self.deleted_count()

    def has_deletions(self):
        return self.deleted_count() > 0

    def deleted_count(self):
        if self.deleted is None:
            return 0
        return len(self.deleted)

    def delete_document(self, docnum, delete=True):
        if delete:
            if self.deleted is None:
                self.deleted = set()
            self.deleted.add(docnum)
        elif self.deleted is not None and docnum in self.deleted:
            self.deleted.clear(docnum)

    def is_deleted(self, docnum):
        if self.deleted is None:
            return False
        return docnum in self.deleted


# Posting blocks

class W2Block(base.BlockBase):
    magic = b("Blk3")

    infokeys = ("count", "maxid", "maxweight", "minlength", "maxlength",
                "idcode", "compression", "idslen", "weightslen")

    def to_file(self, postfile, compression=3):
        ids = self.ids
        idcode, idstring = minimize_ids(ids, self.stringids, compression)
        wtstring = minimize_weights(self.weights, compression)
        vstring = minimize_values(self.postingsize, self.values, compression)

        info = (len(ids), ids[-1], self.maxweight,
                length_to_byte(self.minlength), length_to_byte(self.maxlength),
                idcode, compression, len(idstring), len(wtstring))
        infostring = dumps(info, -1)

        # Offset to next block
        postfile.write_uint(len(infostring) + len(idstring) + len(wtstring)
                            + len(vstring))
        # Block contents
        postfile.write(infostring)
        postfile.write(idstring)
        postfile.write(wtstring)
        postfile.write(vstring)

    @classmethod
    def from_file(cls, postfile, postingsize, stringids=False):
        block = cls(postingsize, stringids=stringids)
        block.postfile = postfile

        delta = postfile.read_uint()
        block.nextoffset = postfile.tell() + delta
        info = postfile.read_pickle()
        block.dataoffset = postfile.tell()

        for key, value in zip(cls.infokeys, info):
            if key in ("minlength", "maxlength"):
                value = byte_to_length(value)
            setattr(block, key, value)

        return block

    def read_ids(self):
        offset = self.dataoffset
        self.postfile.seek(offset)
        idstring = self.postfile.read(self.idslen)
        ids = deminimize_ids(self.idcode, self.count, idstring,
                             self.compression)
        self.ids = ids
        return ids

    def read_weights(self):
        if self.weightslen == 0:
            weights = [1.0] * self.count
        else:
            offset = self.dataoffset + self.idslen
            self.postfile.seek(offset)
            wtstring = self.postfile.read(self.weightslen)
            weights = deminimize_weights(self.count, wtstring,
                                         self.compression)
        self.weights = weights
        return weights

    def read_values(self):
        postingsize = self.postingsize
        if postingsize == 0:
            values = [None] * self.count
        else:
            offset = self.dataoffset + self.idslen + self.weightslen
            self.postfile.seek(offset)
            vstring = self.postfile.read(self.nextoffset - offset)
            values = deminimize_values(postingsize, self.count, vstring,
                                       self.compression)
        self.values = values
        return values


