# Copyright 2017 Matt Chaput. All rights reserved.
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
from typing import Iterable, Sequence, Tuple

from whoosh.postings import postform, postings, ptuples
from whoosh.postings.postio import BasicIO


# Enum specifying how weights are stored in a posting block
NO_WEIGHTS = 0  # the block has no weights
ALL_ONES = 1  # all the weights were 1.0, so we stored this flag instead of them
ALL_INTS = 2  # all the weights were whole, so they're stored as ints not floats
FLOAT_WEIGHTS = 4  # weights are stored as floats


class BasicPostingReader(postings.PostingReader):
    # Common superclass for Doclist and Vector readers
    def __init__(self, fmt: postform.Format, source: bytes, offset: int):
        self._format = fmt
        self._src = source
        self._offset = offset

        # Dummy slots so the IDE won't complain about methods on this class
        # accessing them
        self._count = None  # type: int
        self._end_offset = None  # type: int

        self._lens_offset = None  # type: int
        self._weights_tc = None  # type: str
        self._weights_offset = None  # type: int
        self._weights_size = None  # type: int

        self._poses_offset = None  # type: int
        self._poses_size = None  # type: int
        self._chars_offset = None  # type: int
        self._chars_size = None  # type: int
        self._pays_offset = None  # type: int
        self._pays_size = None  # type: int

        # Slots for demand-loaded data
        self._weights_type = NO_WEIGHTS
        self._weights = None
        self._chunk_indexes = [None, None, None]

    def _setup_offsets(self, offset: int):
        wtc = self._weights_tc
        if wtc == "0":
            self._weights_type = NO_WEIGHTS
        elif wtc == "1":
            self._weights_type = ALL_ONES
        elif wtc == "f":
            self._weights_type = FLOAT_WEIGHTS
        else:
            self._weights_type = ALL_INTS

        # Set up the weights offsets
        self._weights_offset = offset
        wts_itemsize = BasicIO.compute_weights_size(wtc)

        self._weights_size = wts_itemsize * self._count

        # Compute the offset of feature sections based on their sizes
        self._poses_offset = offset + self._weights_size
        self._chars_offset = self._poses_offset + self._poses_size
        self._pays_offset = self._chars_offset + self._chars_size
        self._end_offset = self._pays_offset + self._pays_size

    def raw_bytes(self) -> bytes:
        return self._src[self._offset: self._end_offset]

    def can_copy_raw_to(self, fmt: postform.Format) -> bool:
        return self._format.can_copy_raw_to(fmt)

    def end_offset(self) -> int:
        return self._end_offset

    def _get_weights(self) -> Sequence[float]:
        if self._weights is None:
            self._weights = BasicIO.decode_weights(
                self._src, self._weights_offset, self._weights_tc, self._count
            )
        return self._weights

    def weight(self, n: int) -> float:
        if n < 0 or n >= self._count:
            raise IndexError

        wt = self._weights_type
        if wt == NO_WEIGHTS or wt == ALL_ONES:
            return 1.0
        else:
            return self._get_weights()[n]

    def total_weight(self) -> float:
        wt = self._weights_type
        if wt == NO_WEIGHTS or wt == ALL_ONES:
            return self._count
        else:
            return sum(self._get_weights())

    def max_weight(self):
        wt = self._weights_type
        if wt == NO_WEIGHTS or wt == ALL_ONES:
            return 1.0
        else:
            return max(self._get_weights())

    def _chunk_offsets(self, n: int, offset: int, size: int,
                       ix_pos: int) -> Tuple[int, int]:
        if n < 0 or n >= self._count:
            raise IndexError
        if not size:
            raise postings.UnsupportedFeature

        ix = self._chunk_indexes[ix_pos]
        if ix is None:
            ix = BasicIO.decode_chunk_index(self._src, offset)
            self._chunk_indexes[ix_pos] = ix

        return ix[n]

    def positions(self, n: int) -> Sequence[int]:
        if not self._poses_size:
            return ()

        offset, length = self._chunk_offsets(n, self._poses_offset,
                                             self._poses_size, 0)
        return BasicIO.decode_positions(self._src, offset, length)

    def raw_positions(self, n: int) -> bytes:
        offset, length = self._chunk_offsets(n, self._poses_offset,
                                             self._poses_size, 0)
        return self._src[offset: offset + length]

    def chars(self, n: int) -> Sequence[Tuple[int, int]]:
        if not self._chars_size:
            return ()

        offset, length = self._chunk_offsets(n, self._chars_offset,
                                             self._chars_size, 1)
        return BasicIO.decode_chars(self._src, offset, length)

    def raw_chars(self, n: int) -> bytes:
        offset, length = self._chunk_offsets(n, self._chars_offset,
                                             self._chars_size, 1)
        return self._src[offset: offset + length]

    def payloads(self, n: int) -> Sequence[bytes]:
        if not self._pays_size:
            return ()

        offset, length = self._chunk_offsets(n, self._pays_offset,
                                             self._pays_size, 2)
        return BasicIO.decode_payloads(self._src, offset, length)

    def raw_payloads(self, n: int) -> Sequence[bytes]:
        offset, length = self._chunk_offsets(n, self._pays_offset,
                                             self._pays_size, 2)
        return self._src[offset: offset + length]


class BasicDocListReader(BasicPostingReader, postings.DocListReader):
    def __init__(self, fmt: postform.Format, src: bytes, offset: int=0):
        super(BasicDocListReader, self).__init__(fmt, src, offset)
        self._lens = None

        # Copy feature flags from format
        self.has_lengths = fmt.has_lengths
        self.has_weights = fmt.has_weights
        self.has_positions = fmt.has_positions
        self.has_chars = fmt.has_chars
        self.has_payloads = fmt.has_payloads

        # Unpack the header
        (self._count, ids_tc, self._weights_tc, self._min_len,
         self._max_len, self._poses_size, self._chars_size, self._pays_size,
         h_end) = BasicIO.unpack_doc_header(src, offset)

        # Read the IDs
        offset, self._ids = BasicIO.decode_docids(src, h_end, ids_tc,
                                                  self._count)

        # Set up lengths if the format stores them
        if fmt.has_lengths:
            self._lens_offset = offset
            offset += self._count

        # Set up offsets/sizes for other features (also self._end_offset)
        self._setup_offsets(offset)

    def __repr__(self):
        return "<%s %d>" % (type(self).__name__, self._count)

    def id(self, n: int) -> int:
        if n < 0 or n >= self._count:
            raise IndexError("%r/%s" % (n, self._count))

        return self._ids[n]

    def id_slice(self, start: int, end: int) -> Sequence[int]:
        return self._ids[start:end]

    def all_ids(self):
        return self._ids

    def _get_lens(self) -> Sequence[int]:
        if self._lens is None:
            if self._lens_offset is None:
                raise postings.UnsupportedFeature
            self._lens = BasicIO.decode_lengths(self._src, self._lens_offset,
                                                self._count)
        return self._lens

    def length(self, n: int):
        if n < 0 or n >= self._count:
            raise IndexError
        if not self._count:
            raise postings.UnsupportedFeature

        return self._get_lens()[n]

    def min_length(self):
        return self._min_len

    def max_length(self):
        return self._max_len

    def raw_posting_at(self, n: int) -> ptuples.RawPost:
        docid = self.id(n)
        length = self.length(n) if self.has_lengths else None
        weight = self.weight(n) if self.has_weights else None

        posbytes = charbytes = paybytes = None
        if self.has_positions:
            posbytes = self.raw_positions(n)
        if self.has_chars:
            charbytes = self.raw_chars(n)
        if self.has_payloads:
            paybytes = self.raw_payloads(n)

        return docid, None, length, weight, posbytes, charbytes, paybytes


class BasicVectorReader(BasicPostingReader, postings.VectorReader):
    def __init__(self, fmt: postform.Format, src: bytes, offset: int=0):
        super(BasicVectorReader, self).__init__(fmt, src, offset)

        # Unpack the header
        (self._count, t_typecode, self._weights_tc,
         self._poses_size, self._chars_size, self._pays_size,
         h_end) = BasicIO.unpack_vector_header(src, offset)

        # Read the terms
        offset, self._terms = BasicIO.decode_terms(src, h_end, t_typecode,
                                                   self._count)

        # Set up offsets/sizes for other features (also self._end_offset)
        self._setup_offsets(offset)

    def all_terms(self) -> Iterable[bytes]:
        for tbytes in self._terms:
            yield tbytes

    def termbytes(self, n: int) -> bytes:
        if n < 0 or n >= self._count:
            raise IndexError

        return self._terms[n]

    def seek(self, termbytes: bytes) -> int:
        return bisect_left(self._terms, termbytes)

    def term_index(self, termbytes: bytes) -> int:
        i = self.seek(termbytes)
        if i < len(self) and self._terms[i] == termbytes:
            return i
        else:
            raise KeyError(termbytes)
