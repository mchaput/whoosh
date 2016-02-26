import logging
import struct
from abc import abstractmethod
from bisect import bisect_left
from collections import defaultdict
from operator import itemgetter
from typing import (Any, Callable, Iterable, List, Optional, Sequence, Tuple,
                    Union, cast)

from array import array

from whoosh.ifaces import analysis
from whoosh.compat import array_tobytes, array_frombytes, text_type
from whoosh.system import IS_LITTLE
from whoosh.util.numlists import min_array_code, delta_encode, delta_decode
from whoosh.util.varints import varint, decode_varint


logger = logging.getLogger(__name__)


# Typing aliases

# docid, termbytes, length, weight, positions, chars, payloads
PosList = Sequence[int]
CharList = Sequence[Tuple[int, int]]
PayList = Sequence[bytes]
PostTuple = Tuple[
    Optional[int],  # docid
    Optional[bytes],  # termbytes
    Optional[int],  # encoded length
    Optional[float],  # weight
    Optional[PosList],  # positions
    Optional[CharList],  # chars
    Optional[PayList],  # payloads
]
RawPost = Tuple[
    Optional[int],  # docid
    Optional[bytes],  # termbytes
    Optional[int],  # length
    Optional[float],  # weight
    Optional[bytes],  # positions
    Optional[bytes],  # chars
    Optional[bytes],  # payloads
]


# Exceptions

class UnsupportedFeature(Exception):
    pass


# Functions for working with posting tuples

# We should use an object instead of a tuple, but it's just too damn slow.
# Indexing generates very large numbers of postings, and an object is many times
# slower to instantiate than a tuple.

def posting(docid: int=None, termbytes: bytes=None, length: int=None,
            weight: float=None, positions: PosList=None, chars: CharList=None,
            payloads: PayList=None) -> PostTuple:
    """
    Returns a standardized tuple representing a posting.

    :param docid: the ID of the document this posting is from.
    :param termbytes: the bytes of the term this posting is from.
    :param length: the length of the document field.
    :param weight: the term weight.
    :param positions: a list of positions in the document.
    :param chars: a list of (startchar, endchar) tuples.
    :param payloads: a list of payloads for each position.
    """

    return docid, termbytes, length, weight, positions, chars, payloads


# Assign names to the members of the posting tuple to make them easier to get
DOCID = 0
TERMBYTES = 1
LENGTH = 2
WEIGHT = 3
POSITIONS = 4
CHARS = 5
PAYLOADS = 6
postfield_name = (
    "docid termbytes length weight positions chars payloads"
).split()

post_docid = itemgetter(DOCID)
post_weight = itemgetter(WEIGHT)
post_length = itemgetter(LENGTH)


def update_post(post, docid: int=None, termbytes: bytes=None, length: int=None,
                weight: float=None, positions: PosList=None,
                chars: CharList=None, payloads: PayList=None) -> PostTuple:
    """
    Returns a new tuple with the given keywords replaced.
    """

    return (
        docid if docid is not None else post[DOCID],
        termbytes if termbytes is not None else post[TERMBYTES],
        length if length is not None else post[LENGTH],
        weight if weight is not None else post[WEIGHT],
        positions if positions is not None else post[POSITIONS],
        chars if chars is not None else post[CHARS],
        payloads if payloads is not None else post[PAYLOADS]
    )


# Helper functions

def tokens(value: Union[Sequence, text_type], analyzer: 'analysis.Analyzer',
           kwargs: dict) -> 'Iterable[analysis.Token]':
    if isinstance(value, (tuple, list)):
        gen = analysis.entoken(value, **kwargs)
    else:
        gen = analyzer(value, **kwargs)
    return analysis.unstopped(gen)


def min_array(nums: Sequence[int]) -> array:
    code = min_array_code(max(nums))
    return array(code, nums)


# Struct for encoding the length typecode and count of a list of byte chunks
tcodes_and_len = struct.Struct("<ccI")


# Interfaces

# Classes

class PostingReader(object):
    def __init__(self, fmt: 'Format', src: bytes, offset: int=0):
        self._format = fmt
        self._src = src
        self._offset = offset
        self._count = 0

        self.has_lengths = fmt.has_lengths
        self.has_weights = fmt.has_weights
        self.has_positions = fmt.has_positions
        self.has_chars = fmt.has_chars
        self.has_payloads = fmt.has_payloads

    def __len__(self) -> int:
        return self._count

    def can_copy_raw_to(self, fmt: 'Format') -> bool:
        return self._format.can_copy_raw_to(fmt)

    def supports(self, feature: str) -> bool:
        return getattr(self, "has_%s" % feature, False)

    def total_weight(self) -> float:
        # Sublclasses should replace this with something more efficient
        return sum(self.weight(i) for i in range(len(self)))

    @abstractmethod
    def raw_bytes(self) -> bytes:
        raise NotImplementedError

    @abstractmethod
    def end_offset(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def length(self, n: int) -> int:
        raise NotImplementedError

    @abstractmethod
    def weight(self, n: int) -> float:
        raise NotImplementedError

    @abstractmethod
    def positions(self, n: int) -> List[int]:
        raise NotImplementedError

    @abstractmethod
    def chars(self, n: int) -> List[Tuple[int, int]]:
        raise NotImplementedError

    @abstractmethod
    def payloads(self, n: int) -> List[bytes]:
        raise NotImplementedError

    @abstractmethod
    def min_length(self):
        raise NotImplementedError

    @abstractmethod
    def max_length(self):
        raise NotImplementedError

    @abstractmethod
    def max_weight(self):
        raise NotImplementedError


class DocListReader(PostingReader):
    @abstractmethod
    def id(self, n: int) -> int:
        raise NotImplementedError

    def id_slice(self, start: int, end: int) -> Sequence[int]:
        return [self.id(i) for i in range(start, end)]

    def min_id(self):
        return self.id(0)

    def max_id(self):
        return self.id(len(self) - 1)

    def all_ids(self) -> Iterable[int]:
        for i in range(len(self)):
            yield self.id(i)

    def posting_at(self, i, termbytes: bytes=None) -> PostTuple:
        """
        Generates a posting tuple corresponding to the data at the given index.

        :param i: the position in the reader.
        :param termbytes: set this as the termbytes for the postings.
        """

        weight = self.weight(i) if self.has_weights else None
        length = self.length(i) if self.has_lengths else None
        poses = self.positions(i) if self.has_positions else None
        chars = self.chars(i) if self.has_chars else None
        pays = self.payloads(i) if self.has_payloads else None

        return posting(self.id(i), termbytes=termbytes, length=length,
                       weight=weight, positions=poses, chars=chars,
                       payloads=pays)

    @abstractmethod
    def raw_posting_at(self, i) -> RawPost:
        raise NotImplementedError

    def postings(self, termbytes: bytes=None) -> Iterable[PostTuple]:
        """
        Generates a series posting tuples corresponding to the data in the
        reader.

        :param termbytes: set this as the termbytes for the postings.
        :return:
        """

        for i in range(len(self)):
            yield self.posting_at(i, termbytes)

    def raw_postings(self) -> Iterable[RawPost]:
        for i in range(len(self)):
            yield self.raw_posting_at(i)


class VectorReader(PostingReader):
    @abstractmethod
    def termbytes(self, n: int) -> bytes:
        raise NotImplementedError

    def min_term(self):
        return self.termbytes(0)

    def max_term(self):
        return self.termbytes(len(self) - 1)

    def seek(self, tbytes: bytes) -> int:
        termbytes = self.termbytes
        for i in range(len(self)):
            this = termbytes(i)
            if this >= tbytes:
                return i
        return len(self)

    def all_terms(self) -> Iterable[bytes]:
        for i in range(len(self)):
            yield self.termbytes(i)

    def term_index(self, tbytes: bytes) -> int:
        termbytes = self.termbytes
        for i in range(len(self)):
            this = termbytes(i)
            if this == tbytes:
                return i
            elif this > tbytes:
                raise KeyError(tbytes)

    def posting_for(self, tbytes: bytes) -> PostTuple:
        return self.posting_at(self.term_index(tbytes))

    def weight_for(self, tbytes: bytes) -> float:
        return self.weight(self.term_index(tbytes))

    def positions_for(self, tbytes: bytes) -> Sequence[int]:
        return self.positions(self.term_index(tbytes))

    def items_as(self, feature: str) -> Iterable[Tuple[bytes, Any]]:
        termbytes = self.termbytes
        if feature not in ("weight", "lengths", "positions", "chars",
                           "payloads"):
            raise ValueError("Unknown feature %r" % feature)
        if not self.supports(feature):
            raise ValueError("Vector does not support %r" % feature)

        feature_method = getattr(self, feature)
        for i in range(len(self)):
            yield termbytes(i), feature_method(i)

    def terms_and_weights(self) -> Iterable[Tuple[bytes, float]]:
        termbytes = self.termbytes
        weight = self.weight
        for i in range(len(self)):
            yield termbytes(i), weight(i)

    def posting_at(self, i, docid: int=None) -> PostTuple:
        """
        Generates a posting tuple corresponding to the data at the given index.

        :param i: the position in the reader.
        :param docid: set this as the document ID for the postings.
        """

        weight = self.weight(i) if self.has_weights else None
        length = self.length(i) if self.has_lengths else None
        poses = self.positions(i) if self.has_positions else None
        chars = self.chars(i) if self.has_chars else None
        pays = self.payloads(i) if self.has_payloads else None

        return posting(docid, termbytes=self.termbytes(i), length=length,
                       weight=weight, positions=poses, chars=chars,
                       payloads=pays)

    def postings(self, docid: int=None) -> Iterable[PostTuple]:
        has_lengths = self.has_lengths
        has_weights = self.has_weights
        has_poses = self.has_positions
        has_chars = self.has_chars
        has_payloads = self.has_payloads

        for i in range(len(self)):
            yield posting(
                docid, termbytes=self.termbytes(i),
                length=self.length(i) if has_lengths else None,
                weight=self.weight(i) if has_weights else None,
                positions=self.positions(i) if has_poses else None,
                chars=self.chars(i) if has_chars else None,
                payloads=self.payloads(i) if has_payloads else None,
            )


class PostingsIO(object):
    def __eq__(self, other):
        return type(self) is type(other) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not self == other

    @staticmethod
    def _extract(posts: Sequence[RawPost], member: int) -> Sequence:
        # Return a list of all the values of a certain member of a list of
        # posting tuples

        vals = [p[member] for p in posts]

        # Check for missing values
        if any(v is None for v in vals):
            n = postfield_name[member]
            for post in posts:
                if post[member] is None:
                    raise ValueError("Post %r is missing %s" % (post, n))

        return vals

    def can_copy_raw_to(self, io: 'PostingsIO') -> bool:
        return False

    @abstractmethod
    def condition_post(self, post: PostTuple) -> RawPost:
        raise NotImplementedError

    @abstractmethod
    def doclist_to_bytes(self, fmt: 'Format', posts: Sequence[PostTuple]
                         ) -> bytes:
        raise NotImplementedError

    @abstractmethod
    def vector_to_bytes(self, fmt: 'Format', posts: Sequence[PostTuple]
                        ) -> bytes:
        raise NotImplementedError

    @abstractmethod
    def doclist_reader(self, fmt: 'Format', bs: bytes,
                       offset: int=0) -> DocListReader:
        raise NotImplementedError

    @abstractmethod
    def vector_reader(self, fmt: 'Format', bs: bytes) -> VectorReader:
        raise NotImplementedError


# Classes

class Format(object):
    """
    Base class of objects representing a format for storing postings in the
    database.
    """

    def __init__(self, has_lengths: bool=False, has_weights: bool=False,
                 has_positions: bool=False, has_chars: bool=False,
                 has_payloads: bool=False, io: PostingsIO=None,
                 boost=1.0):
        self._io = io or BasicIO()
        self.has_lengths = has_lengths
        self.has_weights = has_weights
        self.has_positions = has_positions
        self.has_chars = has_chars
        self.has_payloads = has_payloads
        self.boost = boost

    def __repr__(self):
        r = "<%s" % type(self).__name__
        for feature in "lengths weights positions chars payloads".split():
            if getattr(self, "has_" + feature):
                r += " " + feature
        if self.boost != 1.0:
            r += " boost=%s" % self.boost
        r += ">"
        return r

    def __eq__(self, other):
        return type(self) is type(other) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not self == other

    def can_copy_raw_to(self, fmt: 'Format') -> bool:
        return (
            fmt.has_lengths == self.has_lengths and
            fmt.has_weights == self.has_weights and
            fmt.has_positions == self.has_positions and
            fmt.has_chars == self.has_chars and
            fmt.has_payloads == self.has_payloads and
            fmt.io().can_copy_raw_to(self.io())
        )

    def supports(self, feature: str) -> bool:
        """
        Returns True if this format object supports the named information type:
        "lengths", "weights", "positions", "chars", or "payloads".

        :param feature: a string naming a posting feature to check.
        :rtype: bool
        """

        return getattr(self, "has_%s" % feature, False)

    def io(self) -> PostingsIO:
        return self._io

    def condition_post(self, post: PostTuple) -> RawPost:
        return self.io().condition_post(post)

    def doclist_to_bytes(self, posts: Sequence[RawPost]) -> bytes:
        return self.io().doclist_to_bytes(self, posts)

    def doclist_reader(self, bs: bytes, offset: int=0) -> DocListReader:
        return self.io().doclist_reader(self, bs, offset)

    def vector_to_bytes(self, posts: Sequence[PostTuple]) -> bytes:
        return self.io().vector_to_bytes(self, posts)

    def vector_reader(self, bs: bytes, offset: int=0) -> VectorReader:
        return self.io().vector_reader(self, bs, offset)

    def index(self, analyzer: 'analysis.Analyzer',
              to_bytes: Callable[[text_type], bytes],
              value: Any, docid: int=None, boost: float=1.0,
              **kwargs) -> Tuple[int, Sequence[PostTuple]]:
        """
        Calls the given analyzer on the field value (passing through any keyword
        arguments to the analyzer) and groups the resulting tokens. Returns a
        tuple of (field_length, iterator), where ``field_length`` is the total
        number of terms in the value, and ``iterator`` is an iterator of
        :class:`Posting` objects for each **unique** term in the value.

        :param analyzer: the analyzer to use to find terms in the value string.
        :param to_bytes: a function to call to convert unicode terms into
            bytes.
        :param value: the value (such as a unicode string) to analyze.
        :param docid: the ID for the document being indexed.
        :param boost: the weight to use for each occurrence.
        :param kwargs: keyword arguments to pass to the analyzer.
        """

        boost *= self.boost

        hasweights = self.has_weights
        hasposes = self.has_positions
        haschars = self.has_chars
        haspayloads = self.has_payloads

        weights = poses = chars = payloads = None

        # Turn on analyzer features based and set up buffers on what information
        # this format is configured to store
        if hasweights:
            kwargs["field_boost"] = boost
            weights = defaultdict(float)
        if hasposes:
            kwargs["positions"] = True
            poses = defaultdict(list)
        if haschars:
            kwargs["chars"] = True
            chars = defaultdict(list)
        if haspayloads:
            kwargs["payloads"] = True
            payloads = defaultdict(list)

        # Let the analyzer know we're indexing this content
        kwargs["mode"] = "index"

        fieldlen = 0
        termset = set()

        # Read tokens from the analyzer
        for token in tokens(value, analyzer, kwargs):
            fieldlen += 1
            text = token.text
            termset.add(text)

            # Buffer information from the token based on which features are
            # enabled in this format
            if hasweights:
                weights[text] += token.boost
            if hasposes:
                poses[text].append(token.pos)
            if haschars:
                chars[text].append((token.startchar, token.endchar))
            if haspayloads:
                payloads[text].append(token.payload)

        # Sort the terms in the document
        sterms = sorted(termset)
        # Create a list of Postings, with individual components set
        # to None if the feature is not enabled. Note that we always include
        # the length, so that the consumer can calculate the minlength and
        # maxlength, even if the format doesn't store per-document lengths.
        posts = [posting(docid=docid, termbytes=to_bytes(text),
                         length=fieldlen,
                         weight=weights[text] if hasweights else None,
                         positions=poses[text] if hasposes else None,
                         chars=chars[text] if haschars else None,
                         payloads=payloads[text] if haspayloads else None)
                 for text in sterms]
        return fieldlen, posts


    # @classmethod
    # def unpack(cls, source: bytes, offset: int) -> 'BlockHeader':
    #     v_f = cls.v_f
    #
    #     version, flags = v_f.unpack(source[offset:offset + v_f.size])
    #     offset += v_f.size
    #
    #     # Decode flags
    #     has_lengths = flags & (1 << 0)
    #     has_weights = flags & (1 << 1)
    #     has_positions = flags & (1 << 2)
    #     has_chars = flags & (1 << 3)
    #     has_payloads = flags & (1 << 4)
    #     is_vector = flags & (1 << 7)
    #
    #     # Decode count and min/max lengths
    #     count, offset = decode_varint(source, offset)
    #     min_length, offset = decode_varint(source, offset)
    #     max_length, offset = decode_varint(source, offset)
    #
    #     # Decode section sizes
    #     ids_size, offset = decode_varint(source, offset)
    #     lens_size, offset = decode_varint(source, offset)
    #     weights_size, offset = decode_varint(source, offset)
    #     poses_size, offset = decode_varint(source, offset)
    #     chars_size, offset = decode_varint(source, offset)
    #     payloads_size, offset = decode_varint(source, offset)
    #
    #     return cls(is_vector, has_lengths, has_weights, has_positions,
    #                has_chars, has_payloads, count, min_length, max_length,
    #                ids_size, lens_size, weights_size, poses_size, chars_size,
    #                payloads_size, offset)


class BasicIO(PostingsIO):
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

    def can_copy_raw_to(self, io: PostingsIO) -> bool:
        return type(io) is type(self)

    def doclist_reader(self, fmt: Format, src: bytes,
                       offset: int=0) -> 'BasicDocListReader':
        return BasicDocListReader(fmt, src, offset)

    def vector_reader(self, fmt: Format, src: bytes,
                      offset: int=0) -> 'BasicVectorReader':
        return BasicVectorReader(fmt, src, offset)

    def doclist_to_bytes(self, fmt: Format,
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

    def vector_to_bytes(self, fmt: Format, posts: List[PostTuple]) -> bytes:
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

    def extract_lengths(self, fmt: Format, posts: Sequence[RawPost]
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

    def extract_weights(self, fmt: Format, posts: Sequence[RawPost]
                        ) -> Tuple[str, bytes]:
        if fmt.has_weights:
            weights = self._extract(posts, WEIGHT)
            return self.encode_weights(weights)
        return "0", b''

    def extract_features(self, fmt: Format, posts: Sequence[RawPost]):
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


NO_WEIGHTS = 0
ALL_ONES = 1
ALL_INTS = 2
FLOAT_WEIGHTS = 4


class BasicPostingReader(PostingReader):
    # Common superclass for Doclist and Vector readers
    def __init__(self, fmt: Format, source: bytes, offset: int):
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

    def can_copy_raw_to(self, fmt: Format) -> bool:
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
            raise UnsupportedFeature

        ix = self._chunk_indexes[ix_pos]
        if ix is None:
            ix = BasicIO.decode_chunk_index(self._src, offset)
            self._chunk_indexes[ix_pos] = ix

        return ix[n]

    def positions(self, n: int) -> Sequence[int]:
        offset, length = self._chunk_offsets(n, self._poses_offset,
                                             self._poses_size, 0)
        return BasicIO.decode_positions(self._src, offset, length)

    def raw_positions(self, n: int) -> bytes:
        offset, length = self._chunk_offsets(n, self._poses_offset,
                                             self._poses_size, 0)
        return self._src[offset: offset + length]

    def chars(self, n: int) -> Sequence[Tuple[int, int]]:
        offset, length = self._chunk_offsets(n, self._chars_offset,
                                             self._chars_size, 1)
        return BasicIO.decode_chars(self._src, offset, length)

    def raw_chars(self, n: int) -> bytes:
        offset, length = self._chunk_offsets(n, self._chars_offset,
                                             self._chars_size, 1)
        return self._src[offset: offset + length]

    def payloads(self, n: int) -> Sequence[bytes]:
        offset, length = self._chunk_offsets(n, self._pays_offset,
                                             self._pays_size, 2)
        return BasicIO.decode_payloads(self._src, offset, length)

    def raw_payloads(self, n: int) -> Sequence[bytes]:
        offset, length = self._chunk_offsets(n, self._pays_offset,
                                             self._pays_size, 2)
        return self._src[offset: offset + length]


class BasicDocListReader(BasicPostingReader, DocListReader):
    def __init__(self, fmt: Format, src: bytes, offset: int=0):
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
                raise UnsupportedFeature
            self._lens = BasicIO.decode_lengths(self._src, self._lens_offset,
                                                self._count)
        return self._lens

    def length(self, n: int):
        if n < 0 or n >= self._count:
            raise IndexError
        if not self._count:
            raise UnsupportedFeature

        return self._get_lens()[n]

    def min_length(self):
        return self._min_len

    def max_length(self):
        return self._max_len

    def raw_posting_at(self, n: int) -> RawPost:
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


class BasicVectorReader(BasicPostingReader, VectorReader):
    def __init__(self, fmt: Format, src: bytes, offset: int=0):
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

    def all_terms(self):
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


