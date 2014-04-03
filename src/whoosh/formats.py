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

"""
"Format" objects specify how a block of postings are encoded into a byte string
for storage in the database. This module contains base classes and a basic
implementation that tries for speed over small size. The basic implementation
can store lengths, weights, lists of positions, lists of character ranges, and
arbitrary payloads (all are optional).

Formats may support a selection of "features" (pieces of information associated
with each posting). Lengths (the length of the term's field in the document),
weights (the weight of the term in the document), positions (the positions at
which the term occurs in the document), characters (the character ranges at
which the term occurs in the document), and "payloads" (arbitrary per-posting
data). The specification of explicit feature types is meant to theoretically
allow a format to be smart about storing the different feature types, with the
"payload" allowing customization without necessarily having to design a whole
new Format.

The ``Format`` object is the main object representing a storage format. A Format
object implements an ``analyze()`` method that integrates the results of an
analyzer into a series of postings. This method is on the Format object so that
a custom Formats could massage or augment the information from the analyzer.

The ``Format.analyze`` method returns the length of the content and a series of
``Posting`` objects.

The higher-level indexing machinery (a ``DocWriter``) collates this to map terms
to posting lists. When it is ready to save postings to the database, it calls
``Format.buffer()`` to get a ``BlockBuffer`` object. This object accepts
postings using its ``append()`` or ``from_list()`` methods, and then encodes
them into a byte string using the ``to_bytes()`` method.

Finally, to read the postings a leaf matcher will use ``Format.reader()`` to get
a ``BlockReader`` object. This object decodes the byte strings created by
``BlockBuffer.to_bytes()`` using the ``BlockReader.from_bytes()`` method. The
matcher can then use the ``BlockReader`` API to access the information in the
block.

For efficiency, the ``BlockReader`` API is designed so the block reader can be
instantiated once and re-used to read multiple blocks. You can call
``BlockReader.from_bytes()`` multiple times, with each call overwriting the
previous information.
"""

import struct
from abc import ABCMeta, abstractmethod
from array import array
from bisect import bisect_left
from collections import defaultdict

from whoosh.compat import pickle
from whoosh.compat import BytesIO
from whoosh.compat import bytes_type, integer_types
from whoosh.compat import array_frombytes, array_tobytes, iteritems, xrange
from whoosh.analysis import unstopped, entoken
from whoosh.system import pack_ushort_le, unpack_ushort_le
from whoosh.system import emptybytes, IS_LITTLE
from whoosh.util.numeric import length_to_byte, byte_to_length
from whoosh.util.numlists import min_array_code, delta_encode, delta_decode


def delta_array(nums):
    return min_array(list(delta_encode(nums)))


def min_array(nums):
    code = min_array_code(max(nums))
    return array(code, nums)


def tokens(value, analyzer, kwargs):
    if isinstance(value, (tuple, list)):
        gen = entoken(value, **kwargs)
    else:
        gen = analyzer(value, **kwargs)
    return unstopped(gen)


class Posting(object):
    __slots__ = ("id", "length", "weight", "positions", "chars", "payloads")

    def __init__(self, id=None, length=None, weight=None, positions=None,
                 chars=None, payloads=None):
        self.id = id
        self.length = length
        self.weight = weight
        self.positions = positions
        self.chars = chars
        self.payloads = payloads

    def __repr__(self):
        string = "<id=%s" % self.id
        if self.length:
            string += " len=%r" % self.length
        if self.weight:
            string += " w=%r" % self.weight
        if self.positions:
            string += " p=%r" % self.positions
        if self.chars:
            string += " c=%r" % self.chars
        if self.payloads:
            string += " pay=%r" % self.payloads
        return string + ">"

    def __eq__(self, other):
        return (
            type(self) is type(other) and
            self.id == other.id and
            self.length == other.length and
            self.weight == other.weight and
            self.positions == other.positions and
            self.chars == other.chars and
            self.payloads == other.payloads
        )

    def to_spans(self):
        from whoosh.query.spans import Span

        poses = self.positions
        chars = self.chars
        payloads = self.payloads

        if poses:
            length = len(poses)
        elif chars:
            length = len(chars)
        elif payloads:
            length = len(payloads)
        else:
            raise Exception("No positional information")

        spans = []
        for i in xrange(length):
            pos = poses[i] if poses else None
            startchar, endchar = chars[i] if chars else (None, None)
            payload = payloads[i] if payloads else None
            spans.append(Span(pos, startchar=startchar, endchar=endchar,
                              payload=payload))
        return spans


# Base classes

class Format(object):
    """
    Base class of objects representing a format for storing postings in the
    database.
    """

    __metadata__ = ABCMeta

    @abstractmethod
    def supports(self, name):
        """
        Returns True if this format object supports the named information type:
        "length", "weight", "positions", "chars", or "payload".

        :param name: a string naming a posting value type.
        :rtype: bool
        """

        raise NotImplementedError

    @abstractmethod
    def buffer(self, vector=False):
        """
        Returns an empty, writable instance of :class:`BlockBuffer`, suitable
        for buffering postings.

        :param vector: if True, the buffer is intended to store a term vector
            (so it needs to store a term bytestring instead of a document ID for
            each posting).
        :rtype: :class:`BlockBuffer`
        """

        raise NotImplementedError

    @abstractmethod
    def reader(self, vector=True):
        """
        Returns an instance of :class:`BlockReader`.

        :param vector: if True, the buffer is intended to read a term vector
            (so it needs to expect a term bytestring instead of a document ID
            for each posting).
        :rtype: :class:`BlockReader`
        """

        raise NotImplementedError

    @abstractmethod
    def index(self, analyzer, to_bytes, value, boost=1.0, **kwargs):
        """
        Calls the given analyzer on the field value (passing through any keyword
        arguments to the analyzer) and groups the resulting tokens. Returns a
        tuple of (field_length, iterator), where ``field_length`` is the total
        number of terms in the value, and ``iterator`` is an iterator of
        :class:`Posting` objects for each **unique** term in the value.

        :param analyzer: the analyzer to use to find terms in the value string.
        :param to_bytes: a function to call to convert unicode terms into
            bytestrings.
        :param value: the value (such as a unicode string) to analyze.
        :param kwargs: keyword arguments to pass to the analyzer.
        """

        raise NotImplementedError

    def chunks(self, postiter, blocksize):
        ls = []
        for post in postiter:
            ls.append(post)
            if len(ls) >= blocksize:
                yield self.buffer().from_list(ls)
                ls = []
        if ls:
            yield self.buffer().from_list(ls)

        # for i in xrange(0, len(postlist), blocksize):
        #     yield self.buffer().from_list(postlist[i:i + blocksize])


class BlockReader(object):
    """
    Interface for decoding posting lists from byte strings, and accessing the
    posting information.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def __len__(self):
        """
        Returns the number of postings in this block.
        """

        raise NotImplementedError

    def __bool__(self):
        return not len(self)

    def __nonzero__(self):
        return self.__bool__()

    @abstractmethod
    def from_bytes(self, bs):
        """
        Loads the posting information encoded in the given byte string.
        This should return ``self`` so you can do
        ``format.reader().from_bytes(bs)``.

        :param bs: a byte string encoded by the corresponding ``BlockBuffer``
            object from the same ``Format`` as this reader.
        """

        raise NotImplementedError

    # Posting Access

    @abstractmethod
    def find(self, docid):
        """
        Returns the index of the document with ID equal to or greater than
        the given ID.
        """

        raise NotImplementedError

    def value(self, i):
        """
        Returns a :class:`Posting` object representing the document at the nth
        position.
        """

        return Posting(id=self.id(i),
                       length=self.length(i),
                       weight=self.weight(i),
                       positions=self.positions(i),
                       chars=self.chars(i),
                       payloads=self.payloads(i))

    def is_vector(self):
        """
        Returns True if this block represents a term vector.
        """

        return False

    @abstractmethod
    def id(self, n):
        """
        Returns the document ID of the posting at the nth position. For term
        vectors, this returns the term bytestring instead.
        """

        raise NotImplementedError

    @abstractmethod
    def length(self, n):
        """
        Returns the field length for the posting at the nth position.
        If the format is not configured to store lengths, returns ``1``.
        """

        raise NotImplementedError

    @abstractmethod
    def weight(self, n):
        """
        Returns the weight of the term in the posting at the nth position.
        If the format is not configured to store lengths, returns ``1.0``.
        """

        raise NotImplementedError

    @abstractmethod
    def positions(self, n):
        """
        Returns a list of positions at which the term occured in the posting at
        the nth position. If the format is not configured to store positions,
        returns ``None``.
        """

        raise NotImplementedError

    @abstractmethod
    def chars(self, n):
        """
        Returns a list of ``(startchar, endchar)`` pairs at which the term
        occured in the posting at the nth position. If the format is not'
        configured to store character ranges, returns ``None``.
        """

        raise NotImplementedError

    @abstractmethod
    def payloads(self, n):
        """
        Returns the list of arbitary payloads for the posting at the nth
        position. If the format is not configured to store payloads, returns
        ``None``.
        """

        raise NotImplementedError

    # Bulk access

    def all_ids(self):
        """
        Returns an iterator of all document IDs in this block.
        """

        doc_id = self.id
        for i in xrange(len(self)):
            yield doc_id(i)

    def all_values(self):
        """
        Returns an iterator of ``Posting`` objects.
        """

        value = self.value
        for i in xrange(len(self)):
            yield value(i)

    # Metadata

    @abstractmethod
    def min_length(self):
        """
        Returns the shortest field length of the documents in this block.
        """

        raise NotImplementedError

    @abstractmethod
    def max_length(self):
        """
        Returns the longest field length of the documents in this block.
        """

        raise NotImplementedError

    @abstractmethod
    def max_weight(self):
        """
        Returns the highest term weight of the documents in this block.
        """

        raise NotImplementedError

    @abstractmethod
    def min_id(self):
        """
        Returns the ID of the first document in this block.
        """

        raise NotImplementedError

    @abstractmethod
    def max_id(self):
        """
        Returns the ID of the last document in this block.
        """

        raise NotImplementedError


class BlockBuffer(BlockReader):
    """
    Organizes per-term posting information into blocks and encodes the blocks
    as byte strings using the ``to_bytes()`` method.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def clear(self):
        """
        Empties the contents of the buffer.
        """

        raise NotImplementedError

    @abstractmethod
    def append(self, post):
        """
        Adds a single :class:`Posting` object to the buffer.
        """

        raise NotImplementedError

    def from_list(self, postlist, clear=True):
        """
        Extends or replaces the current contents of the buffer using a list of
        ``Posting`` objects.
        This method should return ``self`` so you can do
        ``format.buffer().from_list(postings)``
        """

        if clear:
            self.clear()
        for item in postlist:
            self.append(*item)
        return self

    @abstractmethod
    def to_bytes(self):
        """
        Encodes the buffered block of postings into a byte string.
        You should not call this multiple times on the same contents, since the
        encoding method is allowed to change the contents.

        :rtype: bytes
        """

        raise NotImplementedError


# Basic format implementation

class BasicFormat(Format):
    # This is a fast but ugly implementation of a Format. It's not the least
    # bit elegant, but it has support for all the optional features

    def __init__(self, lengths=False, weights=False, positions=False,
                 characters=False, payloads=False, boost=1.0):
        self._haslengths = lengths
        self._hasweights = weights
        self._haspositions = positions
        self._hascharacters = characters
        self._haspayloads = payloads
        self._boost = boost

    def __repr__(self):
        return (
            "<%s lengths=%s weights=%s positions=%s characters=%s payloads=%s"
            " boost=%s>" % (
                self.__class__.__name__, self._haslengths, self._hasweights,
                self._haspositions, self._hascharacters, self._haspayloads,
                self._boost
            )
        )

    def supports(self, name):
        if name == "length":
            return self._haslengths
        elif name == "weight":
            return self._hasweights
        elif name == "positions":
            return self._haspositions
        elif name == "chars" or name == "characters":
            return self._hascharacters
        elif name == "payloads":
            return self._haspayloads
        else:
            return False

    def buffer(self, vector=False):
        return BasicBuffer(self._haslengths, self._hasweights,
                           self._haspositions, self._hascharacters,
                           self._haspayloads, vector=vector)

    def reader(self, vector=False):
        return BasicReader(self._haslengths, self._hasweights,
                           self._haspositions, self._hascharacters,
                           self._haspayloads, vector=vector)

    def index(self, analyzer, to_bytes, value, boost=1.0, **kwargs):
        boost *= self._boost
        haslens = self._haslengths
        hasweights = self._hasweights
        haspositions = self._haspositions
        hascharacters = self._hascharacters
        haspayloads = self._haspayloads
        weights = poses = chars = payloads = None

        # Turn on analyzer features based and set up buffers on what information
        # this format is configured to store
        if hasweights:
            kwargs["boosts"] = True
            weights = defaultdict(float)
        if haspositions:
            kwargs["positions"] = True
            poses = defaultdict(list)
        if hascharacters:
            kwargs["chars"] = True
            chars = defaultdict(list)
        if haspayloads:
            kwargs["payloads"] = True
            payloads = defaultdict(list)

        # Read tokens from the analyzer
        fieldlen = 0
        termset = set()
        kwargs["boost"] = boost
        for token in tokens(value, analyzer, kwargs):
            fieldlen += 1
            text = token.text
            termset.add(text)

            # Buffer information from the token based on which features are
            # enabled in this format
            if hasweights:
                weights[text] += token.boost
            if haspositions:
                poses[text].append(token.pos)
            if hascharacters:
                chars[text].append((token.startchar, token.endchar))
            if haspayloads:
                payloads[text].append(token.payload)

        # Create a generator of Postings, with individual components set
        # to None if the feature is not enabled
        gen = (Posting(id=to_bytes(text),
                       length=fieldlen if haslens else None,
                       weight=weights[text] if hasweights else None,
                       positions=poses[text] if haspositions else None,
                       chars=chars[text] if hascharacters else None,
                       payloads=payloads[text] if haspayloads else None)
               for text in sorted(termset))
        return fieldlen, gen


class BasicReader(BlockReader):
    header_struct = struct.Struct("<hBHciii")
    magic = -100

    def __init__(self, haslengths=True, hasweights=True, haspositions=False,
                 haschars=False, haspayloads=False, vector=False):
        self._haslengths = haslengths and not vector
        self._hasweights = hasweights
        self._haspositions = haspositions
        self._haschars = haschars
        self._haspayloads = haspayloads
        self._vector = vector

        self._bytes = None
        self._ids = []
        self._lens = None
        self._weights = None
        self._poses = None
        self._chars = None
        self._payloads = None

        self._maxweight = None
        self._minlength = None
        self._maxlength = None

        self._positions_offset = None
        self._chars_offset = None
        self._payloads_offset = None

    def from_bytes(self, bs):
        hstruct = self.header_struct
        self._bytes = bs

        # Pull information out of the header
        (
            magic,  # Magic number identifying the format
            flags,  # Information about the contents of the block
            count,  # Number of postings in the block
            idcode,  # Type code of the ID array ('s' if term vector)
            poslen,  # Length of the positions pickle
            charlen,  # Length of the characters pickle
            paylen  # Length of the payloads pickle
        ) = hstruct.unpack(bs[:hstruct.size])
        idcode = idcode.decode("ascii")
        idbase = hstruct.size

        # Double-check the information in the header
        assert magic == self.magic
        assert bool(flags & (1 << 0)) == self._haslengths
        assert bool(flags & (1 << 1)) == self._hasweights
        assert bool(flags & (1 << 2)) == self._haspositions
        assert bool(flags & (1 << 3)) == self._haschars
        assert bool(flags & (1 << 4)) == self._haspayloads

        if idcode == "s":
            # This is a term vector. After the header is a series of
            # [ushort length][bytes term] bytes.
            ids = []
            end = idbase
            for _ in xrange(count):
                length = unpack_ushort_le(bs[end:end + 2])[0]
                ids.append(bs[end + 2:end + 2 + length])
                end += 2 + length
            self._ids = ids
        else:
            # Unpack the Doc IDs array from the front of the byte string.
            # This is complicated because (a) we might need to byteswap,
            # and (b) until around Python 3.3 there was a bug where you
            # couldn't have array("q"), so longs must be treated separately
            # using struct :/ It's no use trying to be clever and (e.g.) use
            # delta-encoded varints because it'll be too slow -- just store
            # the actual doc IDs and use array.frombytes() to load it.
            if idcode == "q":
                end = idbase + struct.calcsize("q")
                docids = struct.unpack("<%dq" % count, bs[idbase:end])
            else:
                docids = array(idcode)
                end = idbase + count * docids.itemsize
                array_frombytes(docids, bs[idbase:end])
                if not IS_LITTLE:
                    docids.byteswap()
            self._ids = docids

        # If this format has lengths, read them from after the Doc IDs
        if self._haslengths:
            self._lens = lens = array("B")
            array_frombytes(lens, bs[end:end + count])
            end += count
        else:
            self._lens = None

        # If this format has weights, read them from after the lengths. Once
        # again we may need to byteswap
        if self._hasweights:
            self._weights = weights = array("f")
            wlen = count * weights.itemsize
            array_frombytes(weights, bs[end:end + wlen])
            if not IS_LITTLE:
                weights.byteswap()
            end += wlen
        else:
            self._weights = None

        # It turns out that trying to store positions, character ranges, and
        # payloads compactly/efficiently is just too slow, so if they're present
        # we just pickle them (ugh). The reader doesn't unpickle them
        # immediately -- we remember the ranges and load them lazily
        self._poses = self._chars = self._payloads = None
        if self._haspositions:
            self._positions_offset = (end, poslen)
            end += poslen
        if self._haschars:
            self._chars_offset = (end, charlen)
            end += charlen
        if self._haspayloads:
            self._payloads_offset = (end, paylen)
            end += paylen

        # Recalculate min_length, max_length, max_weight based on new contents
        self._recalc()
        return self

    def __len__(self):
        return len(self._ids)

    def __bool__(self):
        return bool(self._ids)

    def _recalc(self):
        # Computes and stashes min_length, max_length, max_weight based on
        # current contents

        if self._hasweights:
            self._maxweight = max(self._weights)
        else:
            self._maxweight = 1.0

        if self._haslengths:
            self._minlength = min(self._lens)
            self._maxlength = max(self._lens)
        else:
            self._minlength = self._maxlength = 1

    def _load_all(self):
        docids = self._ids
        lens = self._lens
        weights = self._weights
        poses = self._get_poses() if self._haspositions else None
        chars = self._get_chars() if self._haschars else None
        payloads = self._get_payloads() if self._haspayloads else None
        return docids, lens, weights, poses, chars, payloads

    def find(self, docid):
        return bisect_left(self._ids, docid)

    def term_vector(self):
        return self._vector

    def id(self, n):
        return self._ids[n]

    def length(self, n):
        if self._haslengths:
            return byte_to_length(self._lens[n])

    def weight(self, n):
        if self._hasweights:
            return self._weights[n]

    def _get_poses(self):
        # If positions haven't been loaded, read them from the stored bytes
        poses = self._poses
        if poses is None:
            off, ln = self._positions_offset
            poses = self._poses = pickle.loads(self._bytes[off:off + ln])
        return poses

    def _get_chars(self):
        # If chars haven't been loaded, read them from the stored bytes
        chars = self._chars
        if chars is None:
            off, ln = self._chars_offset
            chars = self._chars = pickle.loads(self._bytes[off:off + ln])
        return chars

    def _get_payloads(self):
        # If payloads haven't been loaded, read them from the stored bytes
        payloads = self._payloads
        if payloads is None:
            off, ln = self._payloads_offset
            payloads = self._payloads = pickle.loads(self._bytes[off:off + ln])
        return payloads

    def positions(self, n):
        if self._haspositions:
            return self._get_poses()[n]
        else:
            return None

    def chars(self, n):
        if self._haschars:
            return self._get_chars()[n]
        else:
            return None

    def payloads(self, n):
        if self._haspayloads:
            return self._get_payloads()[n]
        else:
            return None

    def all_ids(self):
        return iter(self._ids)

    def all_values(self):
        ids, lens, weights, poses, chars, payloads = self._load_all()
        for i, id_ in enumerate(ids):
            yield Posting(id=id_,
                          length=lens[i] if lens else None,
                          weight=weights[i] if weights else None,
                          positions=poses[i] if poses else None,
                          chars=chars[i] if chars else None,
                          payloads=payloads[i] if payloads else None)

    def min_length(self):
        return self._minlength

    def max_length(self):
        return self._maxlength

    def max_weight(self):
        return self._maxweight

    def min_id(self):
        return self._ids[0]

    def max_id(self):
        return self._ids[-1]


class BasicBuffer(BasicReader, BlockBuffer):
    def __init__(self, *args, **kwargs):
        BasicReader.__init__(self, *args, **kwargs)

        # Set up buffers
        self._ids = []
        self._lens = array("B") if self._haslengths else None
        self._weights = array("f") if self._hasweights else None
        self._poses = [] if self._haspositions else None
        self._chars = [] if self._haschars else None
        self._payloads = [] if self._haspayloads else None

    def __repr__(self):
        return "<%s %r>" % (self.__class__.__name__, self._ids)

    def clear(self):
        self._ids = []
        if self._haslengths:
            self._lens = array("B")
        if self._hasweights:
            self._weights = array("f")
        if self._haspositions:
            self._poses = []
        if self._haschars:
            self._chars = []
        if self._haspayloads:
            self._payloads = []

    def from_list(self, postings, clear=True):
        if clear:
            self.clear()

        ids = self._ids
        lens = self._lens
        ws = self._weights
        ps = self._poses
        cs = self._chars
        ys = self._payloads

        hasl = self._haslengths
        hasw = self._hasweights
        hasp = self._haspositions
        hasc = self._haschars
        hasy = self._haspayloads

        for post in postings:
            ids.append(post.id)
            if hasl:
                lens.append(length_to_byte(post.length))
            if hasw:
                ws.append(post.weight)
            if hasp:
                ps.append(post.positions)
            if hasc:
                cs.append(post.chars)
            if hasy:
                ys.append(post.payloads)

        self._recalc()
        return self

    def append(self, post):
        self._ids.append(post.id)
        if self._haslengths:
            length = post.length
            self._lens.append(length_to_byte(length))
            if self._minlength is None or length < self._minlength:
                self._minlength = length
            if self._maxlength is None or length > self._maxlength:
                self._maxlength = length

        if self._hasweights:
            weight = post.weight
            self._weights.append(weight)
            if self._maxweight is None or weight > self._maxweight:
                self._maxweight = weight

        if self._haspositions:
            self._poses.append(post.positions)
        if self._haschars:
            self._chars.append(post.chars)
        if self._haspayloads:
            self._payloads.append(post.payloads)

    def _load_all(self):
        return (self._ids, self._lens, self._weights, self._poses,
                self._chars, self._payloads)

    def to_bytes(self):
        ids = self._ids
        count = len(ids)

        flags = (
            self._haslengths << 0 |
            self._hasweights << 1 |
            self._haspositions << 2 |
            self._haschars << 3 |
            self._haspayloads << 4
        )
        bio = BytesIO()
        if self._vector:
            assert all(isinstance(t, bytes_type) for t in ids)
            # This is a term vector and the "ID"s are term bytestrings
            idcode = "s"
            for termbytes in ids:
                bio.write(pack_ushort_le(len(termbytes)) + termbytes)
        else:
            assert all(isinstance(docid, integer_types) for docid in ids)
            if max(ids) > 2**32-1:
                # Because of a bug in Python < 3.3, we can't store 64-bit ints
                # in an array, so we have to special case them as a big struct
                idcode = "q"
                bio.write(struct.pack("<%dq" % count, ids))
            else:
                docids = min_array(ids)
                idcode = docids.typecode
                bio.write(array_tobytes(docids))

        if self._haslengths:
            bio.write(array_tobytes(self._lens))

        if self._hasweights:
            weights = self._weights
            if not IS_LITTLE:
                weights.byteswap()
            bio.write(array_tobytes(weights))

        poslen = charlen = paylen = 0
        if self._haspositions:
            pospickle = pickle.dumps(self._poses, -1)
            poslen = len(pospickle)
            bio.write(pospickle)
        if self._haschars:
            charpickle = pickle.dumps(self._chars, -1)
            charlen = len(charpickle)
            bio.write(charpickle)
        if self._haspayloads:
            paypickle = pickle.dumps(self._payloads, -1)
            paylen = len(paypickle)
            bio.write(paypickle)

        hbytes = self.header_struct.pack(self.magic, flags, count,
                                         idcode.encode("ascii"),
                                         poslen, charlen, paylen)
        return hbytes + bio.getvalue()


