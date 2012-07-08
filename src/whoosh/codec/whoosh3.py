# Copyright 2012 Matt Chaput. All rights reserved.
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
from array import array
from collections import defaultdict

from whoosh import columns
from whoosh.compat import b, bytes_type, string_type, integer_types
from whoosh.compat import dumps, loads, iteritems
from whoosh.codec import base
from whoosh.filedb import compound, filetables
from whoosh.fst import GraphWriter, GraphReader
from whoosh.matching import ListMatcher, ReadTooFar
from whoosh.reading import TermInfo, TermNotFound
from whoosh.system import emptybytes
from whoosh.system import _SHORT_SIZE, _INT_SIZE, _LONG_SIZE, _FLOAT_SIZE
from whoosh.system import pack_ushort, unpack_ushort
from whoosh.system import pack_long, unpack_long
from whoosh.util.numlists import delta_encode, delta_decode
from whoosh.util.numeric import length_to_byte, byte_to_length

try:
    import zlib
except ImportError:
    zlib = None


WHOOSH3_HEADER_MAGIC = b("W3Bl")

LENGTHS_COLUMN = columns.NumericColumn("B", default=0)
VECTOR_COLUMN = columns.NumericColumn("I", default=0)
STORED_COLUMN = columns.CompressedBytesColumn()


class W3Codec(base.Codec):
    TERMS_EXT = ".trm"  # Term index
    POSTS_EXT = ".pst"  # Term postings
    DAWG_EXT = ".dag"  # Spelling graph file
    VPOSTS_EXT = ".vps"  # Vector postings
    COLUMN_EXT = ".col"  # Per-document value columns

    def __init__(self, blocklimit=128, compression=3, inlinelimit=1):
        self._blocklimit = blocklimit
        self._compression = compression
        self._inlinelimit = inlinelimit

        self.stored_column = STORED_COLUMN

    # Per-document value writer
    def per_document_writer(self, storage, segment):
        return W3PerDocWriter(storage, segment, blocklimit=self._blocklimit,
                              compression=self._compression)

    # Inverted index writer
    def field_writer(self, storage, segment):
        return W3FieldWriter(storage, segment, blocklimit=self._blocklimit,
                             compression=self._compression,
                             inlinelimit=self._inlinelimit)

    # Readers

    def per_document_reader(self, storage, segment):
        return W3PerDocReader(storage, segment)

    def terms_reader(self, storage, segment):
        tifile = segment.open_file(storage, self.TERMS_EXT)
        postfile = segment.open_file(storage, self.POSTS_EXT)
        return W3TermsReader(tifile, postfile)

    def graph_reader(self, storage, segment):
        dawgfile = segment.open_file(storage, self.DAWG_EXT)
        return GraphReader(dawgfile)

    # Columns

    def supports_columns(self):
        return True

    # Segments and generations

    def new_segment(self, storage, indexname):
        return W3Segment(indexname)

    def commit_toc(self, storage, indexname, schema, segments, generation,
                   clean=True):
        from whoosh.index import TOC, clean_files

        toc = TOC(schema, segments, generation)
        toc.write(storage, indexname)
        # Delete leftover files
        if clean:
            clean_files(storage, indexname, generation, segments)


# Common functions

def _vecfield(fieldname):
    return "_%s_vec" % fieldname

def _lenfield(fieldname):
    return "_%s_len" % fieldname

# Per-doc information writer

class W3PerDocWriter(base.PerDocumentWriter):
    def __init__(self, storage, segment, blocklimit=128, compression=3):
        self._storage = storage
        self._segment = segment
        self._blocklimit = blocklimit
        self._compression = compression

        colfile = self._create_file(W3Codec.COLUMN_EXT)
        self._cols = compound.CompoundWriter(colfile)
        self._colwriters = {}
        self._add_column("_stored", STORED_COLUMN)

        self._fieldlengths = defaultdict(int)
        self._doccount = 0
        self._docnum = None
        self._storedfields = None
        self._indoc = False
        self.is_closed = False

        # We'll wait to create the vector file until someone actually tries
        # to add a vector
        self._vpostfile = None

    def _create_file(self, ext):
        return self._segment.create_file(self._storage, ext)

    def _has_column(self, fieldname):
        return fieldname in self._colwriters

    def _add_column(self, fieldname, column):
        writers = self._colwriters
        if fieldname in writers:
            raise Exception("Already added column %r" % fieldname)

        f = self._cols.create_file(fieldname)
        writers[fieldname] = column.writer(f)

    def _prep_vectors(self):
        self._vpostfile = self._create_file(W3Codec.VPOSTS_EXT)
        # We'll use offset==0 as a marker for "no vectors", so we can't start
        # postings at position 0, so just write a few header bytes :)
        self._vpostfile.write(b("VPST"))

    def start_doc(self, docnum):
        if self._indoc:
            raise Exception("Called start_doc when already in a doc")
        if docnum != self._doccount:
            raise Exception("Called start_doc(%r) was expecting %r"
                            % (docnum, self._doccount))

        self._docnum = docnum
        self._doccount += 1
        self._storedfields = {}
        self._indoc = True

    def _add_length(self, fieldname, length):
        lenfield = _lenfield(fieldname)
        if not self._has_column(lenfield):
            self._add_column(lenfield, LENGTHS_COLUMN)
        self._colwriters[lenfield].add(self._docnum, length_to_byte(length))

    def add_field(self, fieldname, fieldobj, value, length):
        if length:
            self._add_length(fieldname, length)
            self._fieldlengths[fieldname] += length

        if value is not None:
            if fieldobj.column_type:
                if not self._has_field(fieldname):
                    self._add_field(fieldname, fieldobj.column_type)
                self._colwriters[fieldname].add(self._docnum, value)

            self._storedfields[fieldname] = value

    def add_vector_items(self, fieldname, fieldobj, items):
        if self._vpostfile is None:
            self._prep_vectors()

        # Write vector postings
        bwriter = BlockWriter(self._vpostfile, fieldobj.vector,
                              self._blocklimit, byteids=True,
                              compression=self._compression)
        bwriter.start(W3TermInfo())
        for text, weight, vbytes in items:
            bwriter.add(text, weight, vbytes)
        startoffset = bwriter.finish()

        # Add row to vector column
        vecfield = _vecfield(fieldname)
        if not self._has_column(vecfield):
            self._add_column(vecfield, VECTOR_COLUMN)
        self._colwriters[vecfield].add(self._docnum, startoffset)

    def finish_doc(self):
        sf = self._storedfields
        if sf:
            self._colwriters["_stored"].add(self._docnum, dumps(sf, -1))
            sf.clear()
        self._indoc = False

    def close(self):
        if self._indoc is not None:
            # Called close without calling finish_doc
            self.finish_doc()

        self._segment._fieldlengths = self._fieldlengths

        # Finish open columns and close the columns writer
        for writer in self._colwriters.values():
            writer.finish(self._doccount)
        self._cols.close()

        # If vectors were written, close the vector writers
        if self._vpostfile:
            self._vpostfile.close()

        self.is_closed = True


class W3FieldWriter(base.FieldWriter):
    def __init__(self, storage, segment, blocklimit=128, compression=3,
                 inlinelimit=1):
        self._storage = storage
        self._segment = segment
        self._blocklimit = blocklimit
        self._compression = compression
        self._inlinelimit = inlinelimit

        self._fieldname = None
        self._fieldid = None
        self._token = None
        self._fieldobj = None
        self._format = None
        self._spelling = None

        self._fieldmap = {}
        _tifile = self._create_file(W3Codec.TERMS_EXT)
        self._tindex = filetables.OrderedHashWriter(_tifile)
        self._tindex.extras["fieldmap"] = self._fieldmap

        self._postfile = self._create_file(W3Codec.POSTS_EXT)

        # We'll wait to create the DAWG builder until someone actually adds
        # a spelled field
        self._gwriter = None

        self._blockwriter = None
        self._terminfo = None
        self._infield = False

    def _create_file(self, ext):
        return self._segment.create_file(self._storage, ext)

    def _prep_dawg(self):
        self._gwriter = GraphWriter(self._create_file(W3Codec.DAWG_EXT))

    def start_field(self, fieldname, fieldobj):
        fmap = self._fieldmap
        if fieldname in fmap:
            self._fieldid = fmap[fieldname]
        else:
            self._fieldid = len(fmap)
            fmap[fieldname] = self._fieldid

        self._fieldname = fieldname
        self._fieldobj = fieldobj
        self._format = fieldobj.format
        self._infield = True

        # True if this field is added to graph in any way
        self._grfield = fieldobj.spelling or fieldobj.separate_spelling()
        # True if the tokens should be added to the graph directly
        self._grtokens = fieldobj.spelling and not fieldobj.separate_spelling()

        # If we're adding this field to the graph, tell the graph writer to
        # start a new field
        if self._grfield:
            if self._gwriter is None:
                self._prep_dawg()
            self._gwriter.start_field(fieldname)

        # Start a new blockwriter for this field
        self._blockwriter = BlockWriter(self._postfile, self._format,
                                        self._blocklimit,
                                        compression=self._compression)

    def start_term(self, token):
        if self._blockwriter is None:
            raise Exception("Called start_term before start_field")
        self._token = token
        if self._grtokens:
            self._gwriter.insert(token)

        self._terminfo = W3TermInfo()
        self._blockwriter.start(self._terminfo)

    def add(self, docnum, weight, vbytes, length):
        self._blockwriter.add(docnum, weight, vbytes, length)

    def add_spell_word(self, fieldname, token):
        if self._gwriter is None:
            self._prep_dawg()
        self._gwriter.insert(token)

    def finish_term(self):
        blockwriter = self._blockwriter
        blockcount = blockwriter.blockcount
        terminfo = self._terminfo

        if blockcount < 1 and len(blockwriter) < self._inlinelimit:
            # Inline the single block
            postings = blockwriter.finish_inline()
        else:
            postings = blockwriter.finish()

        keybytes = pack_ushort(self._fieldid) + self._token
        valbytes = terminfo.to_bytes(postings)
        self._tindex.add(keybytes, valbytes)

    def finish_field(self):
        if not self._infield:
            raise Exception("Called finish_field before start_field")
        self._infield = False

        if self._grfield:
            self._gwriter.finish_field()

        self._blockwriter = None

    def close(self):
        self._tindex.close()
        self._postfile.close()
        if self._gwriter is not None:
            self._gwriter.close()


# Reader objects

class W3PerDocReader(base.PerDocReader):
    def __init__(self, storage, segment):
        self._storage = storage
        self._segment = segment
        self._doccount = segment.doc_count_all()

        self._colfile = segment.open_file(storage, W3Codec.COLUMN_EXT)
        self._cols = compound.CompoundStorage(self._colfile, use_mmap=False)
        self._vpostfile = None

        self._readers = {}
        self._minlengths = {}
        self._maxlengths = {}

    def close(self):
        self._cols.close()

    def doc_count(self):
        return self._doccount - self._segment.deleted_count()

    def doc_count_all(self):
        return self._doccount

    # Deletions

    def has_deletions(self):
        return self._segment.has_deletions()

    def is_deleted(self, docnum):
        return self._segment.is_deleted(docnum)

    def deleted_docs(self):
        return self._segment.deleted_docs()

    # Columns

    def has_column(self, fieldname):
        return self._cols.file_exists(fieldname)

    def column_reader(self, fieldname, column):
        offset, length = self._cols.range(fieldname)
        return column.reader(self._colfile, offset, length, self._doccount)

    # Lengths

    def _cached_reader(self, fieldname, column):
        if fieldname in self._readers:
            return self._readers[fieldname]
        else:
            reader = self.column_reader(fieldname, column)
            self._readers[fieldname] = reader
            return reader

    def doc_field_length(self, docnum, fieldname, default=0):
        if docnum > self._doccount:
            raise IndexError("Asked for docnum %r of %d"
                             % (docnum, self._doccount))
        lenfield = _lenfield(fieldname)
        if self.has_column(lenfield):
            reader = self._cached_reader(lenfield, LENGTHS_COLUMN)
            lbyte = reader[docnum]
            if lbyte:
                return byte_to_length(lbyte)
        return default

    def field_length(self, fieldname):
        return self._segment._fieldlengths.get(fieldname, 0)

    def _minmax_length(self, fieldname, op, cache):
        if fieldname in cache:
            return cache[fieldname]

        lenfield = _lenfield(fieldname)
        reader = self._cached_reader(lenfield, LENGTHS_COLUMN)
        length = byte_to_length(op(reader))
        cache[fieldname] = length
        return length

    def min_field_length(self, fieldname):
        return self._minmax_length(fieldname, min, self._minlengths)

    def max_field_length(self, fieldname):
        return self._minmax_length(fieldname, max, self._maxlengths)

    # Vectors

    def _prep_vectors(self):
        f = self._segment.open_file(self._storage, W3Codec.VPOSTS_EXT)
        self._vpostfile = f

    def _vector_offset(self, docnum, fieldname):
        if docnum > self._doccount:
            raise IndexError("Asked for document %r of %d"
                             % (docnum, self._doccount))
        reader = self._cached_reader(_vecfield(fieldname), VECTOR_COLUMN)
        return reader[docnum]

    def has_vector(self, docnum, fieldname):
        return (self.has_column(_vecfield(fieldname))
                 and self._vector_offset(docnum, fieldname) != 0)

    def vector(self, docnum, fieldname, format_):
        if self._vpostfile is None:
            self._prep_vectors()
        offset = self._vector_offset(docnum, fieldname)
        m = W3PostingMatcher(self._vpostfile, offset, format_, byteids=True)
        return m

    # Stored fields

    def stored_fields(self, docnum):
        reader = self._cached_reader("_stored", STORED_COLUMN)
        pck = reader[docnum]
        if pck:
            return loads(pck)
        else:
            return {}


class W3TermsReader(base.TermsReader):
    def __init__(self, dbfile, postfile):
        self._dbfile = dbfile
        self._tindex = filetables.OrderedHashReader(dbfile)
        self._fieldmap = self._tindex.extras["fieldmap"]
        self._postfile = postfile

        self._fieldunmap = [None] * len(self._fieldmap)
        for fieldname, num in iteritems(self._fieldmap):
            self._fieldunmap[num] = fieldname

    def _keycoder(self, fieldname, tbytes):
        fnum = self._fieldmap.get(fieldname, 65535)
        return pack_ushort(fnum) + tbytes

    def _keydecoder(self, keybytes):
        fieldid = unpack_ushort(keybytes[:_SHORT_SIZE])[0]
        return self._fieldunmap[fieldid], keybytes[_SHORT_SIZE:]

    def _range_for_key(self, fieldname, tbytes):
        return self._tindex.range_for_key(self._keycoder(fieldname, tbytes))

    def __contains__(self, term):
        return self._keycoder(*term) in self._tindex

    def terms(self):
        keydecoder = self._keydecoder
        return (keydecoder(keybytes) for keybytes in self._tindex.keys())

    def terms_from(self, fieldname, prefix):
        prefixbytes = self._keycoder(fieldname, prefix)
        keydecoder = self._keydecoder
        return (keydecoder(keybytes) for keybytes
                in self._tindex.keys_from(prefixbytes))

    def items(self):
        tidecoder = W3TermInfo.from_bytes
        keydecoder = self._keydecoder
        return ((keydecoder(keybytes), tidecoder(valbytes))
                for keybytes, valbytes in self._tindex.items())

    def items_from(self, fieldname, prefix):
        prefixbytes = self._keycoder(fieldname, prefix)
        tidecoder = W3TermInfo.from_bytes
        keydecoder = self._keydecoder
        return ((keydecoder(keybytes), tidecoder(valbytes))
                for keybytes, valbytes in self._tindex.items_from(prefixbytes))

    def term_info(self, fieldname, tbytes):
        key = self._keycoder(fieldname, tbytes)
        try:
            return W3TermInfo.from_bytes(self._tindex[key])
        except KeyError:
            raise TermNotFound("No term %s:%r" % (fieldname, tbytes))

    def frequency(self, fieldname, tbytes):
        datapos = self._range_for_key(fieldname, tbytes)[0]
        return W3TermInfo.read_weight(self._dbfile, datapos)

    def doc_frequency(self, fieldname, tbytes):
        datapos = self._range_for_key(fieldname, tbytes)[0]
        return W3TermInfo.read_doc_freq(self._dbfile, datapos)

    def matcher(self, fieldname, tbytes, format_, scorer=None):
        terminfo = self.term_info(fieldname, tbytes)
        p = terminfo.postings
        term = (fieldname, tbytes)
        if isinstance(p, integer_types):
            # p is an offset into the posting file
            pr = W3PostingMatcher(self._postfile, p, format_, scorer=scorer,
                                  term=term)
        else:
            # p is an inlined tuple of (ids, weights, values)
            docids, weights, values = p
            pr = ListMatcher(docids, weights, values, format_, scorer=scorer,
                             term=term, terminfo=terminfo)
        return pr

    def close(self):
        self._tindex.close()
        self._postfile.close()


# Posting matcher

class W3PostingMatcher(base.FilePostingMatcher):
    def __init__(self, postfile, startoffset, format_, scorer=None, term=None,
                 byteids=False):
        self._postfile = postfile
        self._startoffset = startoffset
        self.format = format_
        self.scorer = scorer
        self._term = term
        self._byteids = byteids

        postfile.seek(startoffset)
        magic = postfile.read(4)
        if magic != WHOOSH3_HEADER_MAGIC:
            raise Exception("Can't read a block with signature %r" % magic)

        self._blockcount = postfile.read_uint()
        self._baseoffset = postfile.tell()
        self._currentblock = 0
        self._i = 0

        self._blockreader = BlockReader(postfile, self._baseoffset, format_,
                                        byteids)

    def is_active(self):
        return (self._currentblock < self._blockcount
                and self._i < len(self._blockreader))

    def id(self):
        return self._blockreader.id(self._i)

    def weight(self):
        return self._blockreader.weight(self._i)

    def value(self):
        return self._blockreader.value(self._i)

    def next(self):
        if self._i == len(self._blockreader) - 1:
            self._next_block()
            return True
        else:
            self._i += 1
            return False

    def _next_block(self):
        if self._currentblock >= self._blockcount:
            raise Exception("No next block")

        self._currentblock += 1
        if self._currentblock == self._blockcount:
            # Reached the end of the postings
            return

        self._blockreader.next_block()
        self._i = 0

    def skip_to(self, targetid):
        if not self.is_active():
            raise ReadTooFar
        i = self._i
        br = self._blockreader

        # If we're already at or past target ID, do nothing
        if targetid <= br.id(i):
            return

        # Skip to the block that would contain the target ID
        if targetid > br.max_id():
            self._skip_to_block(lambda: targetid > br.max_id())
        if not self.is_active():
            return

        # Iterate through the IDs in the block until we find or pass the
        # target
        ids = br.all_ids()
        i = self._i
        while ids[i] < targetid:
            i += 1
            if i == len(ids):
                # This should never happen
                self._next_block()
                return
        self._i = i

    def skip_to_quality(self, minquality):
        bq = self.block_quality
        if bq() > minquality:
            return 0
        return self._skip_to_block(lambda: bq() <= minquality)

    def _skip_to_block(self, targetfn):
        skipped = 0
        while self.is_active() and targetfn():
            self._next_block()
            skipped += 1
        return skipped

    def block_min_length(self):
        return self._blockreader.min_length()

    def block_max_length(self):
        return self._blockreader.max_length()

    def block_max_weight(self):
        return self._blockreader.max_weight()


# Support objects

# Block writer/reader

class BlockWriter(object):
    def __init__(self, postfile, format_, blocklimit, byteids=False,
                 compression=3):
        self._postfile = postfile
        self._format = format_
        self._blocklimit = blocklimit
        self._byteids = byteids
        self._compression = compression
        self._terminfo = None

    def __len__(self):
        return len(self._ids)

    def min_id(self):
        return self._ids[0]

    def max_id(self):
        return self._ids[-1]

    def min_length(self):
        return self._minlength

    def max_length(self):
        return self._maxlength

    def max_weight(self):
        return self._maxweight

    def start(self, terminfo):
        if self._terminfo:
            raise Exception("Called start in a term")
        self.blockcount = 0
        self.new_block()
        self._terminfo = terminfo

    def new_block(self):
        self._ids = [] if self._byteids else array("I")
        self._weights = array("f")
        self._values = []
        self._minlength = None
        self._maxlength = 0
        self._maxweight = 0

    def add(self, id_, weight, vbytes, length=None):
        if self._byteids:
            assert isinstance(id_, string_type), "id_=%r" % id_
        else:
            assert isinstance(id_, integer_types), "id_=%r" % id_
        assert isinstance(weight, (int, float)), "weight=%r" % weight
        assert isinstance(vbytes, bytes_type), "vbytes=%r" % vbytes
        assert length is None or isinstance(length, integer_types)

        values = self._values
        minlength = self._minlength

        self._ids.append(id_)
        self._weights.append(weight)

        if weight > self._maxweight:
            self._maxweight = weight
        if vbytes:
            values.append(vbytes)
        if length:
            if minlength is None or length < minlength:
                self._minlength = length
            if length > self._maxlength:
                self._maxlength = length

        if len(self._ids) > self._blocklimit:
            self._write_block()

    def finish_inline(self):
        self._terminfo.add_block(self)
        self._terminfo = None
        return (tuple(self._ids), tuple(self._weights), tuple(self._values))

    def finish(self):
        postfile = self._postfile

        if self._ids:
            # If there are leftover items in the current block, write them out
            self._write_block()

        if self.blockcount:
            # Seek back to the start of this list of posting blocks and write
            # the number of blocks
            postfile.flush()
            here = postfile.tell()
            postfile.seek(self._startoffset + 4)
            postfile.write_uint(self.blockcount)
            postfile.seek(here)

        self._terminfo = None
        return self._startoffset

    def _mini_ids(self):
        ids = self._ids
        if not self._byteids:
            ids = delta_encode(ids)
        return tuple(ids)

    def _mini_weights(self):
        weights = self._weights

        if all(w == 1.0 for w in weights):
            return None
        elif all(w == weights[0] for w in weights):
            return weights[0]
        else:
            return tuple(weights)

    def _mini_values(self):
        fixedsize = self._format.fixed_value_size()
        values = self._values

        if fixedsize is None or fixedsize < 0:
            vs = tuple(values)
        elif fixedsize == 0:
            vs = None
        else:
            vs = emptybytes.join(values)
        return vs

    def _write_header(self):
        postfile = self._postfile

        self._startoffset = postfile.tell()
        postfile.write(WHOOSH3_HEADER_MAGIC)  # Posting list header
        postfile.write_uint(0)  # Block count

    def _write_block(self):
        postfile = self._postfile
        terminfo = self._terminfo
        ids = self._ids
        comp = self._compression

        if not self.blockcount:
            self._write_header()

        terminfo.add_block(self)

        data = (self._mini_ids(), self._mini_weights(), self._mini_values())
        databytes = dumps(data)
        if len(databytes) < 20:
            comp = 0
        if comp:
            databytes = zlib.compress(databytes, comp)

        infobytes = dumps((len(ids), ids[-1], self._maxweight, comp,
                           length_to_byte(self._minlength),
                           length_to_byte(self._maxlength),
                           ))

        # Write block length
        postfile.write_int(len(infobytes) + len(databytes))
        # Write block contents
        postfile.write(infobytes)
        postfile.write(databytes)

        self.blockcount += 1
        self.new_block()


class BlockReader(object):
    def __init__(self, postfile, baseoffset, format_, byteids=False):
        self._postfile = postfile
        self._fixedsize = format_.fixed_value_size()
        self._byteids = byteids

        self._postcount = None
        self._maxid = None
        self._maxweight = None
        self._compression = None
        self._minlength = None
        self._maxlength = None

        self.go_to(baseoffset)

    def go_to(self, position):
        postfile = self._postfile

        self._data = None
        self._ids = None
        self._weights = None
        self._values = None

        postfile.seek(position)
        length = postfile.read_int()
        self._nextoffset = position + _INT_SIZE + length
        info = postfile.read_pickle()
        self._dataoffset = postfile.tell()

        (self._postcount, self._maxid, self._maxweight, self._compression,
         mnlen, mxlen) = info
        self._minlength = byte_to_length(mnlen)
        self._maxlength = byte_to_length(mxlen)

    def next_block(self):
        self.go_to(self._nextoffset)

    def __len__(self):
        return self._postcount

    def id(self, n):
        if self._ids is None:
            self._read_ids()
        return self._ids[n]

    def all_ids(self):
        if self._ids is None:
            self._read_ids()
        return self._ids

    def weight(self, n):
        if self._weights is None:
            self._read_weights()
        return self._weights[n]

    def value(self, n):
        if self._values is None:
            self._read_values()
        return self._values[n]

    def min_id(self):
        return self.id(0)

    def max_id(self):
        return self._maxid

    def min_length(self):
        return self._minlength

    def max_length(self):
        return self._maxlength

    def max_weight(self):
        return self._maxweight

    def _read_data(self):
        postfile = self._postfile
        postfile.seek(self._dataoffset)
        b = postfile.read(self._nextoffset - self._dataoffset)
        if self._compression:
            b = zlib.decompress(b)
        self._data = loads(b)

    def _read_ids(self):
        if self._data is None:
            self._read_data()
        ids = self._data[0]

        if not self._byteids:
            ids = tuple(delta_decode(ids))

        self._ids = ids

    def _read_weights(self):
        if self._data is None:
            self._read_data()
        postcount = self._postcount
        wts = self._data[1]

        if wts is None:
            self._weights = array("f", (1.0 for _ in xrange(postcount)))
        elif isinstance(wts, float):
            self._weights = array("f", (wts for _ in xrange(postcount)))
        else:
            self._weights = wts

    def _read_values(self):
        if self._data is None:
            self._read_data()

        fixedsize = self._fixedsize
        vs = self._data[2]
        if fixedsize is None or fixedsize < 0:
            self._values = vs
        elif fixedsize is 0:
            self._values = (None,) * self._postcount
        else:
            assert isinstance(vs, bytes_type)
            self._values = tuple(vs[i:i + fixedsize]
                                 for i in xrange(0, len(vs), fixedsize))


# Term info implementation

class W3TermInfo(TermInfo):
    # B   | Flags
    # f   | Total weight
    # I   | Total doc freq
    # B   | Min length (encoded as byte)
    # B   | Max length (encoded as byte)
    # f   | Max weight
    # I   | Minimum (first) ID
    # I   | Maximum (last) ID
    _struct = struct.Struct("!BfIBBfII")

    def add_block(self, block):
        self._weight += sum(block._weights)
        self._df += len(block)

        ml = block.min_length()
        if self._minlength is None:
            self._minlength = ml
        else:
            self._minlength = min(self._minlength, ml)

        self._maxlength = max(self._maxlength, block.max_length())
        self._maxweight = max(self._maxweight, block.max_weight())
        if self._minid is None:
            self._minid = block.min_id()
        self._maxid = block.max_id()

    def to_bytes(self, postings):
        isinlined = int(isinstance(postings, tuple))

        # Encode the lengths as 0-255 values
        minlength = (0 if self._minlength is None
                     else length_to_byte(self._minlength))
        maxlength = length_to_byte(self._maxlength)
        # Convert None values to the out-of-band NO_ID constant so they can be
        # stored as unsigned ints
        minid = 0xffffffff if self._minid is None else self._minid
        maxid = 0xffffffff if self._maxid is None else self._maxid

        # Pack the term info into bytes
        st = self._struct.pack(isinlined, self._weight, self._df,
                               minlength, maxlength, self._maxweight,
                               minid, maxid)

        if isinlined:
            # Postings are inlined - dump them using the pickle protocol
            postbytes = dumps(postings, -1)
        else:
            postbytes = pack_long(postings)
        st += postbytes
        return st

    @classmethod
    def from_bytes(cls, s):
        st = cls._struct
        vals = st.unpack(s[:st.size])
        terminfo = cls()

        flags = vals[0]
        terminfo._weight = vals[1]
        terminfo._df = vals[2]
        terminfo._minlength = byte_to_length(vals[3])
        terminfo._maxlength = byte_to_length(vals[4])
        terminfo._maxweight = vals[5]
        terminfo._minid = None if vals[6] == 0xffffffff else vals[6]
        terminfo._maxid = None if vals[7] == 0xffffffff else vals[7]

        if flags:
            # Postings are stored inline
            terminfo.postings = loads(s[st.size:])
        else:
            # Last bytes are pointer into posting file
            terminfo.postings = unpack_long(s[st.size:st.size + _LONG_SIZE])[0]

        return terminfo

    @classmethod
    def read_weight(cls, dbfile, datapos):
        return dbfile.get_float(datapos + 1)

    @classmethod
    def read_doc_freq(cls, dbfile, datapos):
        return dbfile.get_uint(datapos + 1 + _FLOAT_SIZE)

    @classmethod
    def read_min_and_max_length(cls, dbfile, datapos):
        lenpos = datapos + 1 + _FLOAT_SIZE + _INT_SIZE
        ml = byte_to_length(dbfile.get_byte(lenpos))
        xl = byte_to_length(dbfile.get_byte(lenpos + 1))
        return ml, xl

    @classmethod
    def read_max_weight(cls, dbfile, datapos):
        weightspos = datapos + 1 + _FLOAT_SIZE + _INT_SIZE + 2
        return dbfile.get_float(weightspos)


# Segment implementation

class W3Segment(base.Segment):
    def __init__(self, indexname, doccount=0, segid=None, deleted=None):
        self._indexname = indexname
        self._doccount = doccount
        self._segid = self._random_id() if segid is None else segid
        self._deleted = deleted
        self.compound = False

    def codec(self, **kwargs):
        return W3Codec(**kwargs)

    def segment_id(self):
        return self._segid

    def set_doc_count(self, dc):
        self._doccount = dc

    def doc_count_all(self):
        return self._doccount

    def deleted_count(self):
        if self._deleted is None:
            return 0
        return len(self._deleted)

    def deleted_docs(self):
        if self._deleted is None:
            return ()
        else:
            return iter(self._deleted)

    def delete_document(self, docnum, delete=True):
        if delete:
            if self._deleted is None:
                self._deleted = set()
            self._deleted.add(docnum)
        elif self._deleted is not None and docnum in self.deleted:
            self._deleted.clear(docnum)

    def is_deleted(self, docnum):
        if self._deleted is None:
            return False
        return docnum in self._deleted


## Object that layers a growing update file over static column reader
#
#class StackedColumnReader(columns.ColumnReader):
#    def __init__(self, storage, stackname, reader):
#        self._storage = storage
#        self._stackname = stackname
#        self._reader = reader
#        self._values = {}
#
#        self._bookmark = 0
#        self.update()
#
#    def update(self):
#        reader = self._reader
#
#        try:
#            stackfile = self._storage.open_filename(self._stackname)
#        except IOError:
#            return
#
#        stackfile.seek(self._bookmark)
#        updates = stackfile.read()
#        self._bookmark = stackfile.tell()
#        stackfile.close()
#
#        if updates:
#            buf = StructFile(BytesIO(updates))
#            while True:
#                fieldid = buf.read(2)
#                if not len(fieldid):
#                    break
#                fieldid = unpack_ushort(fieldid)[0]
#                docnum = buf.read_uint()
#                self._values[docnum] = reader.destack(buf)
#
#    def __len__(self):
#        return len(self._reader)
#
#    def __getitem__(self, docnum):
#        try:
#            return self._values[docnum]
#        except KeyError:
#            return self._reader[docnum]
#
#    def __iter__(self):
#        for docnum in xrange(len(self._reader)):
#            yield self[docnum]



