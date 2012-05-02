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

"""
This module contains base classes/interfaces for "codec" objects.
"""

import random
from array import array
from struct import Struct, pack
from bisect import bisect_right

from whoosh.compat import loads, dumps, b, bytes_type, string_type, xrange
from whoosh.filedb.compound import CompoundStorage
from whoosh.matching import Matcher, ReadTooFar
from whoosh.reading import TermInfo
from whoosh.spans import Span
from whoosh.system import (_INT_SIZE, _FLOAT_SIZE, pack_long, unpack_long,
                           IS_LITTLE)
from whoosh.util import byte_to_length, length_to_byte


try:
    from zlib import compress, decompress
    can_compress = True
except ImportError:
    can_compress = False


# Base classes

class Codec(object):
    # Per document value writer
    def per_document_writer(self, storage, segment):
        raise NotImplementedError

    # Inverted index writer
    def field_writer(self, storage, segment):
        raise NotImplementedError

    # Readers

    def terms_reader(self, storage, segment):
        raise NotImplementedError

    def lengths_reader(self, storage, segment):
        raise NotImplementedError

    def vector_reader(self, storage, segment):
        raise NotImplementedError

    def stored_fields_reader(self, storage, segment):
        raise NotImplementedError

    def graph_reader(self, storage, segment):
        raise NotImplementedError

    # Segments and generations

    def new_segment(self, storage, indexname):
        raise NotImplementedError

    def commit_toc(self, storage, indexname, schema, segments, generation):
        raise NotImplementedError


# Writer classes

class PerDocumentWriter(object):
    def start_doc(self, docnum):
        raise NotImplementedError

    def add_field(self, fieldname, fieldobj, value, length):
        raise NotImplementedError

    def add_vector_items(self, fieldname, fieldobj, items):
        raise NotImplementedError

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
        pass

    def lengths_reader(self):
        raise NotImplementedError


class FieldWriter(object):
    def add_postings(self, schema, lengths, items):
        start_field = self.start_field
        start_term = self.start_term
        add = self.add
        finish_term = self.finish_term
        finish_field = self.finish_field

        # items = (fieldname, text, docnum, weight, valuestring) ...
        lastfn = None
        lasttext = None
        dfl = lengths.doc_field_length
        for fieldname, text, docnum, weight, valuestring in items:
            # Items where docnum is None indicate words that should be added
            # to the spelling graph
            if docnum is None and (fieldname != lastfn or text != lasttext):
                self.add_spell_word(fieldname, text)
                lastfn = fieldname
                lasttext = text
                continue

            # This comparison is so convoluted because Python 3 removed the
            # ability to compare a string to None
            if ((lastfn is not None and fieldname < lastfn)
                or (fieldname == lastfn and lasttext is not None
                    and text < lasttext)):
                raise Exception("Postings are out of order: %r:%s .. %r:%s" %
                                (lastfn, lasttext, fieldname, text))
            if fieldname != lastfn or text != lasttext:
                if lasttext is not None:
                    finish_term()
                if fieldname != lastfn:
                    if lastfn is not None:
                        finish_field()
                    start_field(fieldname, schema[fieldname])
                    lastfn = fieldname
                start_term(text)
                lasttext = text
            length = dfl(docnum, fieldname)
            add(docnum, weight, valuestring, length)
        if lasttext is not None:
            finish_term()
            finish_field()

    def start_field(self, fieldname, fieldobj):
        raise NotImplementedError

    def start_term(self, text):
        raise NotImplementedError

    def add(self, docnum, weight, valuestring, length):
        raise NotImplementedError

    def add_spell_word(self, fieldname, text):
        raise NotImplementedError

    def finish_term(self):
        raise NotImplementedError

    def finish_field(self):
        pass

    def close(self):
        pass


# Reader classes

class TermsReader(object):
    def __contains__(self, term):
        raise NotImplementedError

    def terms(self):
        raise NotImplementedError

    def terms_from(self, fieldname, prefix):
        raise NotImplementedError

    def items(self):
        raise NotImplementedError

    def items_from(self, fieldname, prefix):
        raise NotImplementedError

    def terminfo(self, fieldname, text):
        raise NotImplementedError

    def frequency(self, fieldname, text):
        return self.terminfo(fieldname, text).weight()

    def doc_frequency(self, fieldname, text):
        return self.terminfo(fieldname, text).doc_frequency()

    def graph_reader(self, fieldname, text):
        raise NotImplementedError

    def matcher(self, fieldname, text, format_, scorer=None):
        raise NotImplementedError

    def close(self):
        pass


class VectorReader(object):
    def __contains__(self, key):
        raise NotImplementedError

    def matcher(self, docnum, fieldname, format_):
        raise NotImplementedError


class LengthsReader(object):
    def doc_count_all(self):
        raise NotImplementedError

    def doc_field_length(self, docnum, fieldname, default=0):
        raise NotImplementedError

    def field_length(self, fieldname):
        raise NotImplementedError

    def min_field_length(self, fieldname):
        raise NotImplementedError

    def max_field_length(self, fieldname):
        raise NotImplementedError

    def close(self):
        pass


class MultiLengths(LengthsReader):
    def __init__(self, lengths, offset=0):
        self.lengths = []
        self.doc_offsets = []
        self._count = 0
        for lr in lengths:
            if lr.doc_count_all():
                self.lengths.append(lr)
                self.doc_offsets.append(self._count)
                self._count += lr.doc_count_all()
        self.is_closed = False

    def _document_reader(self, docnum):
        return max(0, bisect_right(self.doc_offsets, docnum) - 1)

    def _reader_and_docnum(self, docnum):
        lnum = self._document_reader(docnum)
        offset = self.doc_offsets[lnum]
        return lnum, docnum - offset

    def doc_count_all(self):
        return self._count

    def doc_field_length(self, docnum, fieldname, default=0):
        x, y = self._reader_and_docnum(docnum)
        return self.lengths[x].doc_field_length(y, fieldname, default=default)

    def min_field_length(self):
        return min(lr.min_field_length() for lr in self.lengths)

    def max_field_length(self):
        return max(lr.max_field_length() for lr in self.lengths)

    def close(self):
        for lr in self.lengths:
            lr.close()
        self.is_closed = True


class StoredFieldsReader(object):
    def __iter__(self):
        raise NotImplementedError

    def __getitem__(self, docnum):
        raise NotImplementedError

    def cell(self, docnum, fieldname):
        fielddict = self.get(docnum)
        return fielddict.get(fieldname)

    def column(self, fieldname):
        for fielddict in self:
            yield fielddict.get(fieldname)

    def close(self):
        pass


# File posting matcher middleware

class FilePostingMatcher(Matcher):
    # Subclasses need to set
    #   self._term -- (fieldname, text) or None
    #   self.scorer -- a Scorer object or None
    #   self.format -- Format object for the posting values

    def __repr__(self):
        return "%s(%r, %r, %s)" % (self.__class__.__name__, str(self.postfile),
                                   self.term(), self.is_active())

    def term(self):
        return self._term

    def items_as(self, astype):
        decoder = self.format.decoder(astype)
        for id, value in self.all_items():
            yield (id, decoder(value))

    def supports(self, astype):
        return self.format.supports(astype)

    def value_as(self, astype):
        decoder = self.format.decoder(astype)
        return decoder(self.value())

    def spans(self):
        if self.supports("characters"):
            return [Span(pos, startchar=startchar, endchar=endchar)
                    for pos, startchar, endchar in self.value_as("characters")]
        elif self.supports("positions"):
            return [Span(pos) for pos in self.value_as("positions")]
        else:
            raise Exception("Field does not support positions (%r)"
                            % self._term)

    def supports_block_quality(self):
        return self.scorer and self.scorer.supports_block_quality()

    def max_quality(self):
        return self.scorer.max_quality

    def block_quality(self):
        return self.scorer.block_quality(self)


class BlockPostingMatcher(FilePostingMatcher):
    # Subclasses need to set
    #   self.block -- BlockBase object for the current block
    #   self.i -- Numerical index to the current place in the block
    # And implement
    #   _read_block()
    #   _next_block()
    #   _skip_to_block()

    def id(self):
        return self.block.ids[self.i]

    def weight(self):
        weights = self.block.weights
        if not weights:
            weights = self.block.read_weights()
        return weights[self.i]

    def value(self):
        values = self.block.values
        if values is None:
            values = self.block.read_values()
        return values[self.i]

    def all_ids(self):
        nextoffset = self.baseoffset
        for _ in xrange(self.blockcount):
            block = self._read_block(nextoffset)
            nextoffset = block.nextoffset
            ids = block.read_ids()
            for id in ids:
                yield id

    def next(self):
        if self.i == self.block.count - 1:
            self._next_block()
            return True
        else:
            self.i += 1
            return False

    def skip_to(self, id):
        if not self.is_active():
            raise ReadTooFar

        i = self.i
        # If we're already in the block with the target ID, do nothing
        if id <= self.block.ids[i]:
            return

        # Skip to the block that would contain the target ID
        if id > self.block.maxid:
            self._skip_to_block(lambda: id > self.block.maxid)
        if not self.is_active():
            return

        # Iterate through the IDs in the block until we find or pass the
        # target
        ids = self.block.ids
        i = self.i
        while ids[i] < id:
            i += 1
            if i == len(ids):
                self._active = False
                return
        self.i = i

    def skip_to_quality(self, minquality):
        bq = self.block_quality
        if bq() > minquality:
            return 0
        return self._skip_to_block(lambda: bq() <= minquality)

    def block_min_length(self):
        return self.block.min_length()

    def block_max_length(self):
        return self.block.max_length()

    def block_max_weight(self):
        return self.block.max_weight()

    def block_max_wol(self):
        return self.block.max_wol()


# File TermInfo

NO_ID = 0xffffffff


class FileTermInfo(TermInfo):
    # Freq, Doc freq, min len, max length, max weight, unused, min ID, max ID
    struct = Struct("!fIBBffII")

    def __init__(self, *args, **kwargs):
        self.postings = None
        if "postings" in kwargs:
            self.postings = kwargs["postings"]
            del kwargs["postings"]
        TermInfo.__init__(self, *args, **kwargs)

    # filedb specific methods

    def add_block(self, block):
        self._weight += sum(block.weights)
        self._df += len(block)

        ml = block.min_length()
        if self._minlength is None:
            self._minlength = ml
        else:
            self._minlength = min(self._minlength, ml)

        self._maxlength = max(self._maxlength, block.max_length())
        self._maxweight = max(self._maxweight, block.max_weight())
        if self._minid is None:
            self._minid = block.ids[0]
        self._maxid = block.ids[-1]

    def to_string(self):
        # Encode the lengths as 0-255 values
        ml = 0 if self._minlength is None else length_to_byte(self._minlength)
        xl = length_to_byte(self._maxlength)
        # Convert None values to the out-of-band NO_ID constant so they can be
        # stored as unsigned ints
        mid = NO_ID if self._minid is None else self._minid
        xid = NO_ID if self._maxid is None else self._maxid

        # Pack the term info into bytes
        st = self.struct.pack(self._weight, self._df, ml, xl, self._maxweight,
                              0, mid, xid)

        if isinstance(self.postings, tuple):
            # Postings are inlined - dump them using the pickle protocol
            isinlined = 1
            st += dumps(self.postings, -1)[2:-1]
        else:
            # Append postings pointer as long to end of term info bytes
            isinlined = 0
            # It's possible for a term info to not have a pointer to postings
            # on disk, in which case postings will be None. Convert a None
            # value to -1 so it can be stored as a long.
            p = -1 if self.postings is None else self.postings
            st += pack_long(p)

        # Prepend byte indicating whether the postings are inlined to the term
        # info bytes
        return pack("B", isinlined) + st

    @classmethod
    def from_string(cls, s):
        assert isinstance(s, bytes_type)

        if isinstance(s, string_type):
            hbyte = ord(s[0])  # Python 2.x - str
        else:
            hbyte = s[0]  # Python 3 - bytes

        if hbyte < 2:
            st = cls.struct
            # Weight, Doc freq, min len, max len, max w, unused, min ID, max ID
            w, df, ml, xl, xw, _, mid, xid = st.unpack(s[1:st.size + 1])
            mid = None if mid == NO_ID else mid
            xid = None if xid == NO_ID else xid
            # Postings
            pstr = s[st.size + 1:]
            if hbyte == 0:
                p = unpack_long(pstr)[0]
            else:
                p = loads(pstr + b("."))
        else:
            # Old format was encoded as a variable length pickled tuple
            v = loads(s + b("."))
            if len(v) == 1:
                w = df = 1
                p = v[0]
            elif len(v) == 2:
                w = df = v[1]
                p = v[0]
            else:
                w, p, df = v
            # Fake values for stats which weren't stored before
            ml = 1
            xl = 255
            xw = 999999999
            mid = -1
            xid = -1

        ml = byte_to_length(ml)
        xl = byte_to_length(xl)
        obj = cls(w, df, ml, xl, xw, mid, xid)
        obj.postings = p
        return obj

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


# Segment base class

class Segment(object):
    """Do not instantiate this object directly. It is used by the Index object
    to hold information about a segment. A list of objects of this class are
    pickled as part of the TOC file.
    
    The TOC file stores a minimal amount of information -- mostly a list of
    Segment objects. Segments are the real reverse indexes. Having multiple
    segments allows quick incremental indexing: just create a new segment for
    the new documents, and have the index overlay the new segment over previous
    ones for purposes of reading/search. "Optimizing" the index combines the
    contents of existing segments into one (removing any deleted documents
    along the way).
    """

    # These must be valid separate characters in CASE-INSENSTIVE filenames
    IDCHARS = "0123456789abcdefghijklmnopqrstuvwxyz"
    # Extension for compound segment files
    COMPOUND_EXT = ".seg"

    # self.indexname
    # self.segid

    @classmethod
    def _random_id(cls, size=12):
        return "".join(random.choice(cls.IDCHARS) for _ in xrange(size))

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__, getattr(self, "segid", ""))

    def codec(self):
        raise NotImplementedError

    def segment_id(self):
        if hasattr(self, "name"):
            # Old segment class
            return self.name
        else:
            return "%s_%s" % (self.indexname, self.segid)

    def is_compound(self):
        if not hasattr(self, "compound"):
            return False
        return self.compound

    # File convenience methods

    def make_filename(self, ext):
        return "%s%s" % (self.segment_id(), ext)

    def list_files(self, storage):
        prefix = "%s." % self.segment_id()
        return [name for name in storage.list() if name.startswith(prefix)]

    def create_file(self, storage, ext, **kwargs):
        """Convenience method to create a new file in the given storage named
        with this segment's ID and the given extension. Any keyword arguments
        are passed to the storage's create_file method.
        """

        fname = self.make_filename(ext)
        return storage.create_file(fname, **kwargs)

    def open_file(self, storage, ext, **kwargs):
        """Convenience method to open a file in the given storage named with
        this segment's ID and the given extension. Any keyword arguments are
        passed to the storage's open_file method.
        """

        fname = self.make_filename(ext)
        return storage.open_file(fname, **kwargs)

    def create_compound_file(self, storage):
        segfiles = self.list_files(storage)
        assert not any(name.endswith(self.COMPOUND_EXT) for name in segfiles)
        cfile = self.create_file(storage, self.COMPOUND_EXT)
        CompoundStorage.assemble(cfile, storage, segfiles)
        for name in segfiles:
            storage.delete_file(name)

    def open_compound_file(self, storage):
        name = self.make_filename(self.COMPOUND_EXT)
        return CompoundStorage(storage, name)

    # Abstract methods dealing with document counts and deletions

    def doc_count_all(self):
        """
        :returns: the total number of documents, DELETED OR UNDELETED, in this
            segment.
        """

        raise NotImplementedError

    def doc_count(self):
        """
        :returns: the number of (undeleted) documents in this segment.
        """

        raise NotImplementedError

    def has_deletions(self):
        """
        :returns: True if any documents in this segment are deleted.
        """

        raise NotImplementedError

    def deleted_count(self):
        """
        :returns: the total number of deleted documents in this segment.
        """

        raise NotImplementedError

    def delete_document(self, docnum, delete=True):
        """Deletes the given document number. The document is not actually
        removed from the index until it is optimized.

        :param docnum: The document number to delete.
        :param delete: If False, this undeletes a deleted document.
        """

        raise NotImplementedError

    def is_deleted(self, docnum):
        """:returns: True if the given document number is deleted."""

        raise NotImplementedError


# Posting block format

class BlockBase(object):
    def __init__(self, postingsize, stringids=False):
        self.postingsize = postingsize
        self.stringids = stringids
        self.ids = [] if stringids else array("I")
        self.weights = array("f")
        self.values = None

        self.minlength = None
        self.maxlength = 0
        self.maxweight = 0

    def __len__(self):
        return len(self.ids)

    def __nonzero__(self):
        return bool(self.ids)

    def min_id(self):
        if self.ids:
            return self.ids[0]
        else:
            raise IndexError

    def max_id(self):
        if self.ids:
            return self.ids[-1]
        else:
            raise IndexError

    def min_length(self):
        return self.minlength

    def max_length(self):
        return self.maxlength

    def max_weight(self):
        return self.maxweight

    def add(self, id_, weight, valuestring, length=None):
        self.ids.append(id_)
        self.weights.append(weight)
        if weight > self.maxweight:
            self.maxweight = weight
        if valuestring:
            if self.values is None:
                self.values = []
            self.values.append(valuestring)
        if length:
            if self.minlength is None or length < self.minlength:
                self.minlength = length
            if length > self.maxlength:
                self.maxlength = length

    def to_file(self, postfile):
        raise NotImplementedError


# Utility functions

def minimize_ids(arry, stringids, compression=0):
    amax = arry[-1]

    if stringids:
        typecode = ''
        string = dumps(arry)
    else:
        typecode = arry.typecode
        if amax <= 255:
            typecode = "B"
        elif amax <= 65535:
            typecode = "H"

        if typecode != arry.typecode:
            arry = array(typecode, iter(arry))
        if not IS_LITTLE:
            arry.byteswap()
        string = arry.tostring()
    if compression:
        string = compress(string, compression)
    return (typecode, string)


def deminimize_ids(typecode, count, string, compression=0):
    if compression:
        string = decompress(string)
    if typecode == '':
        return loads(string)
    else:
        arry = array(typecode)
        arry.fromstring(string)
        if not IS_LITTLE:
            arry.byteswap()
        return arry


def minimize_weights(weights, compression=0):
    if all(w == 1.0 for w in weights):
        string = b("")
    else:
        if not IS_LITTLE:
            weights.byteswap()
        string = weights.tostring()
    if string and compression:
        string = compress(string, compression)
    return string


def deminimize_weights(count, string, compression=0):
    if not string:
        return array("f", (1.0 for _ in xrange(count)))
    if compression:
        string = decompress(string)
    arry = array("f")
    arry.fromstring(string)
    if not IS_LITTLE:
        arry.byteswap()
    return arry


def minimize_values(postingsize, values, compression=0):
    if postingsize < 0:
        string = dumps(values, -1)[2:]
    elif postingsize == 0:
        string = b('')
    else:
        string = b('').join(values)
    if string and compression:
        string = compress(string, compression)
    return string


def deminimize_values(postingsize, count, string, compression=0):
    if compression:
        string = decompress(string)

    if postingsize < 0:
        return loads(string)
    elif postingsize == 0:
        return [None] * count
    else:
        return [string[i:i + postingsize] for i
                in xrange(0, len(string), postingsize)]







