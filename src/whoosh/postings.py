import logging
from abc import abstractmethod
from bisect import bisect_left
from collections import defaultdict, namedtuple
from operator import itemgetter
from struct import Struct, calcsize, pack
from typing import (Any, Callable, Iterable, List, Optional,
                    Sequence, Tuple, Union, cast)

from array import array

from whoosh.ifaces import analysis
from whoosh.compat import array_tobytes, array_frombytes, izip
from whoosh.compat import text_type, xrange, zip_
from whoosh.system import IS_LITTLE
from whoosh.util.numlists import (min_array_code, delta_encode, delta_decode,
                                  delta_decode_inplace)


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


def change_docid(post, newdoc):
    return posting(docid=newdoc, termbytes=post[TERMBYTES],
                   length=post[LENGTH], weight=post[WEIGHT],
                   positions=post[POSITIONS], chars=post[CHARS],
                   payloads=post[PAYLOADS])


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
tcodes_and_len = Struct("<ccI")


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

    def supports(self, feature: str) -> bool:
        return getattr(self, "has_%s" % feature, False)

    def total_weight(self) -> float:
        # Sublclasses should replace this with something more efficient
        return sum(self.weight(i) for i in xrange(len(self)))

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
        return [self.id(i) for i in xrange(start, end)]

    def min_id(self):
        return self.id(0)

    def max_id(self):
        return self.id(len(self) - 1)

    def all_ids(self) -> Iterable[int]:
        for i in xrange(len(self)):
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

        for i in xrange(len(self)):
            yield self.posting_at(i, termbytes)

    def raw_postings(self) -> Iterable[RawPost]:
        for i in xrange(len(self)):
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
        for i in xrange(len(self)):
            this = termbytes(i)
            if this >= tbytes:
                return i
        return len(self)

    def all_terms(self) -> Iterable[bytes]:
        for i in xrange(len(self)):
            yield self.termbytes(i)

    def term_index(self, tbytes: bytes) -> int:
        termbytes = self.termbytes
        for i in xrange(len(self)):
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
        for i in xrange(len(self)):
            yield termbytes(i), feature_method(i)

    def terms_and_weights(self) -> Iterable[Tuple[bytes, float]]:
        termbytes = self.termbytes
        weight = self.weight
        for i in xrange(len(self)):
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

        for i in xrange(len(self)):
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
    def _extract(posts: Sequence[RawPost], member: int):
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

        haslens = self.has_lengths
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


class BasicIO(PostingsIO):
    MAGIC_NUMBER = 51966

    # Encoded post list header
    # H - magic number
    # H - number of items in the block
    # H - flags
    # xx - unused
    # i - minimum field length of docs in the block, or 0xffffffff
    # i - maximum field length of docs in the block, or 0
    # i - length of IDs section
    # i - length of lengths section
    # i - length of weights section
    # i - length of poses section
    # i - length of chars section
    # i - length of payloads section
    header = Struct("<HHHxxiiiiiiii")

    def can_copy_raw_to(self, io: PostingsIO) -> bool:
        return type(io) is type(self)

    def doclist_reader(self, fmt: Format, src: bytes,
                       offset: int=0) -> 'BasicDocListReader':
        return BasicDocListReader(fmt, src, offset)

    def vector_reader(self, fmt: Format, src: bytes,
                      offset: int=0) -> 'BasicVectorReader':
        return BasicVectorReader(fmt, src, offset)

    def doclist_to_bytes(self, fmt: Format,
                         posts: Sequence[PostTuple]) -> bytes:
        if not posts:
            raise ValueError("Empty document postings list")

        flags = 0
        id_bytes = self.encode_docids([p[DOCID] for p in posts])
        return self._posts_to_bytes(fmt, posts, flags, id_bytes)

    def vector_to_bytes(self, fmt: Format, posts: List[PostTuple]) -> bytes:
        if not posts:
            raise ValueError("Empty vector postings list")

        flags = 1
        posts = [self.condition_post(p) for p in posts]
        id_bytes = self.encode_terms([p[TERMBYTES] for p in posts])
        return self._posts_to_bytes(fmt, posts, flags, id_bytes)

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

    def _posts_to_bytes(self, fmt: Format, posts: Sequence[RawPost],
                        flags: int, id_bytes: bytes) -> bytes:
        len_bytes = b''
        if fmt.has_lengths:
            lengths = self._extract(posts, LENGTH)  # type: List[int]
            len_bytes = self.encode_lengths(lengths)
            min_len = min(lengths)
            max_len = max(lengths)
        else:
            # Even if the format doesn't STORE lengths, if the postings have
            # lengths (which they should if they come from the index() method),
            # use them to calculte the min and max length
            min_len = 0xffffffff
            max_len = 0
            for p in posts:
                plen = p[LENGTH]
                if plen is not None:
                    min_len = min(min_len, plen)
                    max_len = max(max_len, plen)

            # There were no lengths, just set min_len and max_len to 0
            if min_len == 0xffffffff:
                min_len = 0

        weight_bytes = b''
        if fmt.has_weights:
            weights = self._extract(posts, WEIGHT)  # type: List[float]
            weight_bytes = self.encode_weights(weights)

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

        # Pack the metadata into the header struct
        header = self.header.pack(
            self.MAGIC_NUMBER, len(posts), flags, min_len, max_len,
            len(id_bytes), len(len_bytes), len(weight_bytes), len(pos_bytes),
            len(char_bytes), len(pay_bytes)
        )

        # Join the header with all the byte strings
        return b''.join((header, id_bytes, len_bytes, weight_bytes, pos_bytes,
                         char_bytes, pay_bytes))

    @staticmethod
    def encode_docids(docids: Sequence[int]) -> bytes:
        if not docids:
            raise ValueError
        if any(n < 0 for n in docids):
            raise ValueError("Negative docid in %s" % docids)

        deltas = min_array(list(delta_encode(docids)))
        if not IS_LITTLE:
            deltas.byteswap()
        return deltas.typecode.encode("ascii") + array_tobytes(deltas)

    @staticmethod
    def decode_docids(src: bytes, offset: int, size: int) -> Sequence[int]:
        typecode = bytes(src[offset:offset + 1]).decode("ascii")
        deltas = array(typecode)
        array_frombytes(deltas, src[offset + 1: offset + size])
        if not IS_LITTLE:
            deltas.byteswap()
        return tuple(delta_decode(deltas))

    @staticmethod
    def encode_terms(terms: Sequence[bytes]) -> bytes:
        return BasicIO.encode_chunk_list(terms)

    @staticmethod
    def decode_terms(src: bytes, offset: int, size: int) -> Sequence[bytes]:
        return BasicIO.decode_chunk_list(src, offset, size)

    @staticmethod
    def encode_lengths(lengths: Sequence[int]) -> bytes:
        if any(not isinstance(n, int) or n < 0 or n > 255 for n in lengths):
            raise ValueError("Bad byte in %r" % lengths)
        return array_tobytes(array("B", lengths))

    @staticmethod
    def decode_lengths(src: bytes, offset: int, count: int) -> Sequence[int]:
        end = offset + count
        len_array = array("B")
        array_frombytes(len_array, src[offset:end])
        return len_array

    @staticmethod
    def encode_weights(weights: Sequence[float]) -> bytes:
        if not weights or any(not isinstance(w, (int, float)) for w in weights):
            raise ValueError("Bad weight in %r" % weights)

        if all(w == 1 for w in weights):
            return b"1"

        intweights = [int(w) for w in weights]
        if all(w == wi for w, wi in zip_(weights, intweights)):
            arr = min_array(intweights)
        else:
            arr = array("f", weights)
        if not IS_LITTLE:
            arr.byteswap()

        return arr.typecode.encode("ascii") + array_tobytes(arr)

    @staticmethod
    def decode_weights(src: bytes, offset: int, size: int, count: int
                       ) -> Sequence[float]:
        typecode = str(bytes(src[offset:offset + 1]).decode("ascii"))
        if typecode == "1":
            return array("f", (1.0 for _ in xrange(count)))

        arr = array(typecode)
        array_frombytes(arr, src[offset + 1: offset + size])
        if not IS_LITTLE:
            arr.byteswap()
        return arr

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
        for i in xrange(0, len(indices), 2):
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
        return b''.join(index + chunks)

    @staticmethod
    def decode_chunk_index(src: bytes, offset: int, size: int
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
        # Translate the local offsets to global offsets
        offsets = [h_end + off for off in off_array]

        # Load the lengths array
        len_array = array(lens_code)
        lens_end = off_end + len_array.itemsize * count
        array_frombytes(len_array, src[off_end: lens_end])
        if not IS_LITTLE:
            len_array.byteswap()

        return list(izip(offsets, len_array))

    @staticmethod
    def decode_chunk_list(src: bytes, offset: int, size: int
                          ) -> Sequence[bytes]:
        ix = BasicIO.decode_chunk_index(src, offset, size)
        return tuple(bytes(src[chunk_off:chunk_off + length])
                     for chunk_off, length in ix)


class BasicPostingReader(PostingReader):
    def __init__(self, fmt: Format, src: bytes, offset: int=0):
        super(BasicPostingReader, self).__init__(fmt, src, offset)

        header_struct = BasicIO.header

        # Unpack the header from the beginning of the byte range
        (magic, self._count, flags, self._min_len, self._max_len,
         self._ids_size, self._lens_size, self._weights_size,
         self._poses_size, self._chars_size, self._pays_size,
         ) = header_struct.unpack(src[offset: offset + header_struct.size])
        assert magic == BasicIO.MAGIC_NUMBER

        # Compute the offset of each section based on their sizes
        self._ids_offset = offset + header_struct.size
        self._lens_offset = self._ids_offset + self._ids_size
        self._weights_offset = self._lens_offset + self._lens_size
        self._poses_offset = self._weights_offset + self._weights_size
        self._chars_offset = self._poses_offset + self._poses_size
        self._pays_offset = self._chars_offset + self._chars_size
        self._end_offset = self._pays_offset + self._pays_size

        # Until we load a section, store None
        self._lens = None  # type: Sequence[int]
        self._weights = None  # type: Sequence[float]
        self._chunk_indexes = [None, None, None]

    def raw_bytes(self) -> bytes:
        return self._src[self._offset: self._end_offset]

    def can_copy_raw_to(self, fmt: Format):
        return self._format.can_copy_raw_to(fmt)

    def end_offset(self) -> int:
        return self._end_offset

    def _get_lens(self) -> Sequence[int]:
        if self._lens is None:
            self._lens = BasicIO.decode_lengths(self._src, self._lens_offset,
                                                self._lens_size)
        return self._lens

    def length(self, n: int):
        if n < 0 or n >= self._count:
            raise IndexError
        if not self._lens_size:
            raise UnsupportedFeature

        return self._get_lens()[n]

    def min_length(self):
        return self._min_len

    def max_length(self):
        return self._max_len

    def _get_weights(self) -> Sequence[float]:
        if self._weights is None:
            self._weights = BasicIO.decode_weights(
                self._src, self._weights_offset, self._weights_size, self._count
            )
        return self._weights

    def weight(self, n: int) -> float:
        if n < 0 or n >= self._count:
            raise IndexError
        if not self._weights_size:
            raise UnsupportedFeature
        elif self._weights_size == 1:
            return 1.0

        return self._get_weights()[n]

    def total_weight(self) -> float:
        if not self._weights_size:
            raise UnsupportedFeature
        return sum(self._get_weights())

    def max_weight(self):
        if not self._weights_size:
            raise UnsupportedFeature
        elif self._weights_size == 1:
            return 1.0

        return max(self._get_weights())

    def _chunk_offsets(self, n: int, offset: int, size: int,
                       ix_pos: int) -> Tuple[int, int]:
        if n < 0 or n >= self._count:
            raise IndexError
        if not size:
            raise UnsupportedFeature

        ix = self._chunk_indexes[ix_pos]
        if ix is None:
            ix = BasicIO.decode_chunk_index(self._src, offset, size)
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

        self._ids = BasicIO.decode_docids(self._src, self._ids_offset,
                                          self._ids_size)

    def __repr__(self):
        return "<%s %r>" % (type(self).__name__, list(self.postings()))

    def id(self, n: int) -> int:
        if n < 0 or n >= self._count:
            raise IndexError("%r/%s" % (n, self._count))

        return self._ids[n]

    def id_slice(self, start: int, end: int) -> Sequence[int]:
        return self._ids[start:end]

    def all_ids(self):
        return self._ids

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

        self._terms = BasicIO.decode_terms(self._src, self._ids_offset,
                                           self._ids_size)

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

