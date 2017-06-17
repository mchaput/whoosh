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

import struct
from array import array
from typing import List, Sequence, Tuple

from whoosh.compat import array_tobytes, array_frombytes
from whoosh.postings import postform, postings, ptuples
from whoosh.postings.postings import RawPost, PostTuple
from whoosh.postings.ptuples import (DOCID, TERMBYTES, LENGTH, WEIGHT, POSITIONS,
                                     CHARS, PAYLOADS)
from whoosh.system import IS_LITTLE
from whoosh.util.numlists import delta_encode, delta_decode, min_array_code

if False:
    from whoosh.postings import basic


# Struct for encoding the length typecode and count of a list of byte chunks
tcodes_and_len = struct.Struct("<ccI")


# Helper functions

def min_array(nums: Sequence[int]) -> array:
    code = min_array_code(max(nums))
    return array(code, nums)


# Basic implementations of on-disk posting format

class BasicIO(postings.PostingsIO):
    # H   - Number of postings in block
    # 2c  - IDs and weights typecodes
    # ii  - Min/max length
    # iii - positions, characters, payloads data lengths
    doc_header = struct.Struct("<H2ciiiii")

    # H   - Number of terms in vector
    # 2c  - IDs and weights typecodes
    # iii - positions, characters, payloads data lengths
    vector_header = struct.Struct("<i2ciii")

    @classmethod
    def pack_doc_header(cls, count: int, minlen: int, maxlen: int,
                        ids_typecode: str, weights_typecode: str,
                        poslen: int, charlen: int, paylen: int
                        ) -> bytes:
        return cls.doc_header.pack(
            count,
            ids_typecode.encode("ascii"), weights_typecode.encode("ascii"),
            minlen, maxlen,
            poslen, charlen, paylen
        )

    @classmethod
    def unpack_doc_header(cls, src: bytes, offset: int) -> Tuple:
        h = cls.doc_header
        count, idc, wc, minlen, maxlen, poslen, charlen, paylen = \
            h.unpack(src[offset:offset + h.size])

        ids_typecode = str(idc.decode("ascii"))
        weights_typecode = str(wc.decode("ascii"))

        return (count, ids_typecode, weights_typecode, minlen, maxlen,
                poslen, charlen, paylen, offset + h.size)

    @classmethod
    def pack_vector_header(cls, count: int,
                           terms_typecode: str, weights_typecode: str,
                           poslen: int, charlen: int, paylen: int
                           ) -> bytes:
        return cls.vector_header.pack(
            count,
            terms_typecode.encode("ascii"), weights_typecode.encode("ascii"),
            poslen, charlen, paylen
        )

    @classmethod
    def unpack_vector_header(cls, src: bytes, offset: int) -> Tuple:
        h = cls.vector_header
        count, idc, wc, poslen, charlen, paylen = \
            h.unpack(src[offset:offset + h.size])

        ids_typecode = str(idc.decode("ascii"))
        weights_typecode = str(wc.decode("ascii"))

        return (count, ids_typecode, weights_typecode, poslen, charlen, paylen,
                offset + h.size)

    def can_copy_raw_to(self, io: postings.PostingsIO) -> bool:
        return type(io) is type(self)

    def doclist_reader(self, fmt: postform.Format, src: bytes,
                       offset: int=0) -> 'basic.BasicDocListReader':
        from whoosh.postings.basic import BasicDocListReader
        return BasicDocListReader(fmt, src, offset)

    def vector_reader(self, fmt: postform.Format, src: bytes,
                      offset: int=0) -> 'basic.BasicVectorReader':
        from whoosh.postings.basic import BasicVectorReader
        return BasicVectorReader(fmt, src, offset)

    def doclist_to_bytes(self, fmt: postform.Format,
                         posts: Sequence[RawPost]) -> bytes:
        if not posts:
            raise ValueError("Empty document postings list")

        ids_code, ids_bytes = self.encode_docids([p[DOCID] for p in posts])
        minlen, maxlen, len_bytes = self.extract_lengths(fmt, posts)
        weights_code, weight_bytes = self.extract_weights(fmt, posts)
        pos_bytes, char_bytes, pay_bytes = self.extract_features(fmt, posts)
        header = self.pack_doc_header(
            len(posts), minlen, maxlen, ids_code, weights_code,
            len(pos_bytes), len(char_bytes), len(pay_bytes)
        )
        return b''.join((header, ids_bytes, len_bytes, weight_bytes,
                         pos_bytes, char_bytes, pay_bytes))

    def vector_to_bytes(self, fmt: postform.Format,
                        posts: List[ptuples.PostTuple]) -> bytes:
        if not posts:
            raise ValueError("Empty vector postings list")

        posts = [self.condition_post(p) for p in posts]
        t_code, t_bytes = self.encode_terms(self._extract(posts, TERMBYTES))
        weights_code, weight_bytes = self.extract_weights(fmt, posts)
        pos_bytes, char_bytes, pay_bytes = self.extract_features(fmt, posts)
        header = self.pack_vector_header(
            len(posts), t_code, weights_code,
            len(pos_bytes), len(char_bytes), len(pay_bytes)
        )
        return b''.join((header, t_bytes, weight_bytes,
                         pos_bytes, char_bytes, pay_bytes))

    def extract_lengths(self, fmt: postform.Format,
                        posts: Sequence[RawPost]
                        ) -> Tuple[int, int, bytes]:
        len_bytes = b''
        minlen = maxlen = 1
        if fmt.has_lengths or fmt.has_weights:
            # Even if the format doesn't store lengths, we still need to compute
            # the maximum and minimum lengths for scoring
            lengths = self._extract(posts, LENGTH)
            minlen = min(lengths)
            maxlen = max(lengths)

            if fmt.has_lengths:
                len_bytes = self.encode_lengths(lengths)

        return minlen, maxlen, len_bytes

    def extract_weights(self, fmt: postform.Format, posts: Sequence[RawPost]
                        ) -> Tuple[str, bytes]:
        if fmt.has_weights:
            weights = self._extract(posts, WEIGHT)
            return self.encode_weights(weights)
        return "0", b''

    def extract_features(self, fmt: postform.Format, posts: Sequence[RawPost]):
        pos_bytes = b''
        if fmt.has_positions:
            poslists = self._extract(posts, POSITIONS)  # type: List[bytes]
            pos_bytes = self.encode_chunk_list(poslists)

        char_bytes = b''
        if fmt.has_chars:
            charlists = self._extract(posts, CHARS)  # type: List[bytes]
            char_bytes = self.encode_chunk_list(charlists)

        pay_bytes = b''
        if fmt.has_payloads:
            paylists = self._extract(posts, PAYLOADS)  # type: List[bytes]
            pay_bytes = self.encode_chunk_list(paylists)

        return pos_bytes, char_bytes, pay_bytes

    # Encoding methods

    def condition_post(self, post: PostTuple) -> RawPost:
        poses = post[POSITIONS]
        enc_poses = self.encode_positions(poses) if poses else None
        chars = post[CHARS]
        enc_chars = self.encode_chars(chars) if chars else None
        pays = post[PAYLOADS]
        enc_pays = self.encode_payloads(pays) if pays else None

        return (
            post[DOCID],
            post[TERMBYTES],
            post[LENGTH],
            post[WEIGHT],
            enc_poses,
            enc_chars,
            enc_pays,
        )

    @staticmethod
    def encode_docids(docids: Sequence[int]) -> Tuple[str, bytes]:
        if not docids:
            raise ValueError
        if any(n < 0 for n in docids):
            raise ValueError("Negative docid in %s" % docids)

        deltas = min_array(list(delta_encode(docids)))
        if not IS_LITTLE:
            deltas.byteswap()
        return deltas.typecode, array_tobytes(deltas)

    @staticmethod
    def decode_docids(src: bytes, offset: int, typecode: str,
                      count: int) -> Tuple[int, Sequence[int]]:
        deltas = array(typecode)
        end = offset + deltas.itemsize * count
        array_frombytes(deltas, src[offset: end])
        if not IS_LITTLE:
            deltas.byteswap()
        return end, tuple(delta_decode(deltas))

    @staticmethod
    def encode_terms(terms: Sequence[bytes]) -> Tuple[str, bytes]:
        lens = min_array([len(t) for t in terms])
        if not IS_LITTLE:
            lens.byteswap()
        return lens.typecode, array_tobytes(lens) + b''.join(terms)

    @staticmethod
    def decode_terms(src: bytes, offset: int, typecode: str, count: int
                     ) -> Tuple[int, Sequence[bytes]]:
        lens = array(typecode)
        lens_size = lens.itemsize * count
        array_frombytes(lens, src[offset: offset + lens_size])
        offset += lens_size

        terms = []
        for length in lens:
            terms.append(src[offset:offset + length])
            offset += length
        return offset, terms

    @staticmethod
    def encode_lengths(lengths: Sequence[int]) -> bytes:
        if any(not isinstance(n, int) or n < 0 or n > 255 for n in lengths):
            raise ValueError("Bad byte in %r" % lengths)
        arry = array("B", lengths)
        return array_tobytes(arry)

    @staticmethod
    def decode_lengths(src: bytes, offset: int, count: int) -> Sequence[int]:
        end = offset + count
        len_array = array("B")
        array_frombytes(len_array, src[offset:end])
        return len_array

    @staticmethod
    def encode_weights(weights: Sequence[float]) -> Tuple[str, bytes]:
        if not weights or any(not isinstance(w, (int, float)) for w in weights):
            raise ValueError("Bad weight in %r" % weights)

        if all(w == 1 for w in weights):
            return "1", b""

        intweights = [int(w) for w in weights]
        if all(w == wi for w, wi in zip(weights, intweights)):
            arr = min_array(intweights)
        else:
            arr = array("f", weights)
        if not IS_LITTLE:
            arr.byteswap()

        return arr.typecode, array_tobytes(arr)

    @staticmethod
    def decode_weights(src: bytes, offset: int, typecode: str, count: int
                       ) -> Sequence[float]:
        if typecode == "0":
            raise Exception("Weights were not encoded")
        elif typecode == "1":
            return array("f", (1.0 for _ in range(count)))

        weights = array(typecode)
        array_frombytes(weights, src[offset: offset + weights.itemsize * count])
        if not IS_LITTLE:
            weights.byteswap()
        return weights

    @staticmethod
    def compute_weights_size(typecode: str) -> int:
        if typecode == "0":
            return 0
        if typecode == "1":
            return 0
        else:
            return struct.calcsize(typecode)

    @staticmethod
    def encode_positions(poses: Sequence[int]) -> bytes:
        deltas = min_array(list(delta_encode(poses)))
        if not IS_LITTLE:
            deltas.byteswap()
        return deltas.typecode.encode("ascii") + array_tobytes(deltas)

    @staticmethod
    def decode_positions(src: bytes, offset: int, size: int) -> Sequence[int]:
        typecode = str(bytes(src[offset:offset + 1]).decode("ascii"))
        deltas = array(typecode)
        array_frombytes(deltas, src[offset + 1:offset + size])
        if not IS_LITTLE:
            deltas.byteswap()
        return tuple(delta_decode(deltas))

    @staticmethod
    def encode_chars(chars: Sequence[Tuple[int, int]]) -> bytes:
        base = 0
        deltas = []
        for startchar, endchar in chars:
            if startchar < base:
                raise ValueError("Chars out of order: %s %s"
                                 % (base, startchar))
            if endchar < startchar:
                raise ValueError("Negative char range: %s %s"
                                 % (startchar, endchar))

            deltas.append(startchar - base)
            deltas.append(endchar - startchar)
            base = endchar
        deltas = min_array(deltas)
        return deltas.typecode.encode("ascii") + array_tobytes(deltas)

    @staticmethod
    def decode_chars(src: bytes, offset: int, size: int
                     ) -> Sequence[Tuple[int, int]]:
        typecode = str(bytes(src[offset:offset + 1]).decode("ascii"))
        indices = array(typecode)
        array_frombytes(indices, src[offset + 1:offset + size])
        if IS_LITTLE:
            indices.byteswap()

        if len(indices) % 2:
            raise Exception("Odd number of char indices: %r" % indices)

        # Zip up the linear list into pairs, and at the same time delta-decode
        # the numbers
        base = 0
        cs = []
        for i in range(0, len(indices), 2):
            start = base + indices[i]
            end = start + indices[i + 1]
            cs.append((start, end))
            base = end
        return cs

    @staticmethod
    def encode_payloads(payloads: Sequence[bytes]) -> bytes:
        return BasicIO.encode_chunk_list(payloads)

    @staticmethod
    def decode_payloads(src: bytes, offset: int, size: int) -> Sequence[bytes]:
        return BasicIO.decode_chunk_list(src, offset, size)

    @staticmethod
    def encode_chunk_list(chunks: Sequence[bytes]) -> bytes:
        # Encode the lengths of the chunks
        lens = [len(chunk) for chunk in chunks]
        len_array = min_array(lens)
        if not IS_LITTLE:
            len_array.byteswap()

        # Encode the offsets from the lengths (unfortunately rebuilding this
        # information from the lengths is SLOW, so we have to encode it)
        base = 0
        offsets = []
        for length in len_array:
            offsets.append(base)
            base += length
        offsets_array = min_array(offsets)

        # Encode the header
        header = tcodes_and_len.pack(offsets_array.typecode.encode("ascii"),
                                     len_array.typecode.encode("ascii"),
                                     len(chunks))
        index = [header, array_tobytes(offsets_array), array_tobytes(len_array)]
        return b"".join(index + chunks)

    @staticmethod
    def decode_chunk_index(src: bytes, offset: int
                           ) -> Sequence[Tuple[int, int]]:
        # Decode the header
        h_end = offset + tcodes_and_len.size
        off_code, lens_code, count = tcodes_and_len.unpack(src[offset:h_end])
        off_code = str(off_code.decode("ascii"))
        lens_code = str(lens_code.decode("ascii"))

        # Load the offsets array
        off_array = array(off_code)
        off_end = h_end + off_array.itemsize * count
        array_frombytes(off_array, src[h_end: off_end])
        if not IS_LITTLE:
            off_array.byteswap()

        # Load the lengths array
        len_array = array(lens_code)
        lens_end = off_end + len_array.itemsize * count
        array_frombytes(len_array, src[off_end: lens_end])
        if not IS_LITTLE:
            len_array.byteswap()

        # Translate the local offsets to global offsets
        offsets = [lens_end + off for off in off_array]
        return list(zip(offsets, len_array))

    @staticmethod
    def decode_chunk_list(src: bytes, offset: int, size: int
                          ) -> Sequence[bytes]:
        ix = BasicIO.decode_chunk_index(src, offset)
        return tuple(bytes(src[chunk_off:chunk_off + length])
                     for chunk_off, length in ix)



