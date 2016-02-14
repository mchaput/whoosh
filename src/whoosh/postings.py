from abc import abstractmethod
from bisect import bisect_left
from collections import defaultdict, namedtuple
from operator import itemgetter
from struct import Struct, calcsize
from typing import (Any, Callable, Iterable, List, Optional,
                    Sequence, Tuple, Union, cast)

from array import array

from whoosh.ifaces import analysis
from whoosh.compat import array_tobytes, array_frombytes
from whoosh.compat import text_type, xrange, zip_
from whoosh.system import IS_LITTLE
from whoosh.util.numlists import min_array_code, delta_encode, delta_decode


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


# Basic implementation functions for encoding/decoding data to/from bytes.
# These are broken out from the BasicPostingIO object to make them easier to
# test.

def encode_docids(docids: Sequence[int]) -> bytes:
    if not docids:
        raise ValueError
    if any(n < 0 for n in docids):
        raise ValueError("Negative docid in %s" % docids)

    arr = min_array(list(delta_encode(docids)))
    if not IS_LITTLE:
        arr.byteswap()
    return arr.typecode.encode("ascii") + array_tobytes(arr)


def decode_docids(src: bytes, offset: int, size: int) -> Sequence[int]:
    typecode = bytes(src[offset:offset + 1]).decode("ascii")
    arr = array(typecode)
    array_frombytes(arr, src[offset + 1: offset + size])
    if not IS_LITTLE:
        arr.byteswap()

    return list(delta_decode(arr))


def encode_terms(terms: Sequence[bytes]) -> bytes:
    len_array = min_array([len(t) for t in terms])
    if not IS_LITTLE:
        len_array.byteswap()

    termbytes = b''.join(terms)
    return b''.join((len_array.typecode.encode("ascii"),
                     array_tobytes(len_array),
                     termbytes))


def decode_terms(src: bytes, offset: int, size: int,
                 count: int) -> Sequence[bytes]:
    lens_typecode = src[offset:offset + 1].decode("ascii")
    len_array = array(lens_typecode)
    start = offset + 1
    end = start + count * len_array.itemsize
    array_frombytes(len_array, src[start: end])
    if not IS_LITTLE:
        len_array.byteswap()

    base = end
    terms = []
    for length in len_array:
        terms.append(src[base:base + length])
        base += length
    return terms


def encode_lengths(lengths: Sequence[int]) -> bytes:
    if any(not isinstance(n, int) or n < 0 or n > 255 for n in lengths):
        raise ValueError("Bad byte in %r" % lengths)
    return array_tobytes(array("B", lengths))


def decode_lengths(src: bytes, offset: int, size: int) -> Sequence[int]:
    end = offset + size
    len_array = array("B")
    array_frombytes(len_array, src[offset:end])
    return len_array


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


def decode_weights(src: bytes, offset: int, size: int,
                   count: int) -> Sequence[float]:
    end = offset + size

    typecode = bytes(src[offset:offset + 1]).decode("ascii")
    if typecode == "1":
        return array("f", (1 for _ in xrange(count)))

    arr = array(typecode)
    array_frombytes(arr, src[offset + 1: end])

    if not IS_LITTLE:
        arr.byteswap()

    return arr


def encode_list_list(lens: List[int], data: List[int]) -> bytes:
    # This encodes a list of lists of numbers as bytes.
    # The first argument is the length of each sublist.
    # The second argument is the sublists smushed together into one big list.

    # Convert the lengths into an array
    len_array = min_array(lens)
    if not IS_LITTLE:
        len_array.byteswap()

    # Convert the big list into an array
    data_array = min_array(data)
    if not IS_LITTLE:
        data_array.byteswap()

    # Join the lengths typecode, the lengths, the data typecode, and the data
    return b''.join((len_array.typecode.encode("ascii"),
                     array_tobytes(len_array),
                     data_array.typecode.encode("ascii"),
                     array_tobytes(data_array)))


# A named tuple to hold info about the list list bytes structure
listinfo = namedtuple("listinfo", "offsets lengths data_typecode data_start")


def decode_list_list(src: bytes, offset: int, size: int,
                     count: int) -> listinfo:
    # This function decodes the structure of bytes created by encode_list_list,
    # and loads the lengths, but does NOT load the numbers. This is to allow
    # a reader to only load the numbers they need.

    len_typecode = bytes(src[offset: offset + 1]).decode("ascii")
    lengths = array(len_typecode)
    len_end = offset + 1 + calcsize(len_typecode) * count
    array_frombytes(lengths, src[offset + 1: len_end])
    if not IS_LITTLE:
        lengths.byteswap()

    data_typecode = bytes(src[len_end: len_end + 1]).decode("ascii")
    data_start = len_end + 1

    offsets = array("i")
    base = 0
    for length in lengths:
        offsets.append(base)
        base += length

    return listinfo(offsets, lengths, data_typecode, data_start)


def encode_positions(poslists: Sequence[Sequence[int]]) -> bytes:
    if not poslists:
        raise ValueError

    lens = []
    all_poses = []
    for ps in poslists:
        lens.append(len(ps))
        all_poses.extend(delta_encode(ps))
    return encode_list_list(lens, all_poses)


def decode_position_list(src: bytes, offset: int, size: int, n: int,
                         count: int, cache=None) -> Sequence[int]:
    # Try to get cached info, and if it's not available, generate it
    info = cache._positions_info if cache else None
    if info is None:
        info = decode_list_list(src, offset, size, count)
        if cache:
            cache._positions_info = info

    # Calculate where the nth position list starts
    itemsize = calcsize(info.data_typecode)
    start = info.data_start + info.offsets[n] * itemsize
    # Get the number of items in this particular list
    count = info.lengths[n]
    # Build a struct to unpack a list of this length
    s = Struct("<" + str(count) + info.data_typecode)
    # Calculate the end byte position
    end = start + s.size
    # Read and unpack the list
    dposes = s.unpack(src[start:end])
    # The positions are delta coded, decode and return them
    return list(delta_decode(dposes))


def encode_chars(charlists: Sequence[Sequence[Tuple[int, int]]]) -> bytes:
    if not charlists:
        raise ValueError

    lens = []
    all_indices = []
    for cs in charlists:
        lens.append(len(cs))

        base = 0
        for startchar, endchar in cs:
            if startchar < base:
                raise ValueError("Chars out of order: %s %s"
                                 % (base, startchar))
            if endchar < startchar:
                raise ValueError("Negative char range: %s %s"
                                 % (startchar, endchar))

            all_indices.append(startchar - base)
            all_indices.append(endchar - startchar)
            base = endchar

    return encode_list_list(lens, all_indices)


def decode_chars_list(src: bytes, offset: int, size: int, n: int,
                      count: int, cache=None) -> Sequence[Tuple[int, int]]:
    # Try to get cached info, and if it's not available, generate it
    info = cache._chars_info if cache else None
    if info is None:
        info = decode_list_list(src, offset, size, count)
        if cache:
            cache._chars_info = info

    # Calculate the start offset byte, multiplying by 2 because each item
    # in a list is TWO numbers (start and end)
    itemsize = calcsize(info.data_typecode)
    start = info.data_start + itemsize * info.offsets[n] * 2
    # Get the number of items in this particular list
    count = info.lengths[n]
    # Build a struct to unpack a list of this length
    s = Struct("<" + str(count * 2) + info.data_typecode)
    # Calculate the end byte position
    end = start + s.size
    # Read and unpack the list of character indices
    indices = s.unpack(src[start:end])

    # Unzip the linear list into pairs, and at the same time delta-decode
    # the numbers
    base = 0
    cs = []
    for i in xrange(0, len(indices), 2):
        start = base + indices[i]
        end = start + indices[i + 1]
        cs.append((start, end))
        base = end
    return cs


def encode_payloads(paylists: Sequence[Sequence[bytes]]) -> bytes:
    lens = []
    pay_lens = []
    pay_bytes = bytearray()
    for paylist in paylists:
        lens.append(len(paylist))
        pay_lens.extend([len(pay) for pay in paylist])
        pay_bytes += b''.join(paylist)

    list_bytes = encode_list_list(lens, pay_lens)
    return list_bytes + pay_bytes


def build_payload_index(src: bytes, offset: int, size: int,
                        count: int) -> Sequence[Tuple[int, Sequence[int]]]:
    # Get the info on the payloads; we don't need to cache it, so the
    # key argument is None
    info = decode_list_list(src, offset, size, count)
    data_start = info.data_start

    # This gets confusing because there are two types of lists here. What's in
    # info.lengths is the length of each LIST of payload lengths. To avoid
    # confusion, in this code we'll call those "widths". The numbers in the
    # sublist of the data is the length of each payload string. In this code
    # we'll call those "lengths".
    widths = info.lengths

    # The total number of payloads in the block is the sum of the widths
    total_width = sum(widths)

    # Create an array to hold all payload lengths
    all_lengths = array(info.data_typecode)

    # Compute the end of the list data/beginning of the actual payload strings
    payloads_start = data_start + total_width * all_lengths.itemsize

    # Load the full list of payload lengths
    array_frombytes(all_lengths, src[data_start:payloads_start])
    if not IS_LITTLE:
        all_lengths.byteswap()

    # Chunk the full lengths list based on the counts, and build an index of
    # (offset, [lengths]) for each item
    ix = []
    i = 0
    for width in widths:
        j = i + width
        lengths = all_lengths[i:j]
        ix.append((payloads_start, lengths))
        payloads_start += sum(lengths)
        i = j

    return ix


def decode_pays_list(src: bytes, offset: int, size: int, n: int, count: int,
                     cache=None) -> Sequence[bytes]:
    # Try to get cached index, and if it's not available, generate it
    ix = cache._payload_index if cache else None
    if ix is None:
        ix = build_payload_index(src, offset, size, count)
        if cache:
            cache._payload_index = ix

    # Find the offset and lengths for this item
    pos, lengths = ix[n]

    # Read the payloads from the file
    payloads = []
    for length in lengths:
        payloads.append(src[pos:pos + length])
        pos += length
    return payloads


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

    def postings(self, termbytes: bytes=None) -> Iterable[PostTuple]:
        """
        Generates a series posting tuples corresponding to the data in the
        reader.

        :param termbytes: set this as the termbytes for the postings.
        :return:
        """

        for i in xrange(len(self)):
            yield self.posting_at(i, termbytes)


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
        return type(self) is type(other)

    def __ne__(self, other):
        return not self == other

    @staticmethod
    def _extract(posts: Sequence[PostTuple], member: int):
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


class Format(object):
    """
    Base class of objects representing a format for storing postings in the
    database.
    """

    def __init__(self, has_lengths: bool=False, has_weights: bool=False,
                 has_positions: bool=False, has_chars: bool=False,
                 has_payloads: bool=False, io: PostingsIO=None,
                 boost=1.0):
        self._io = io or BasicPostingsIO()
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

    def doclist_to_bytes(self, posts: Sequence[PostTuple]) -> bytes:
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


# Basic implementation of postings IO

class BasicPostingsIO(PostingsIO):
    MAGIC_NUMBER = 51966

    # Encoded post list header
    # H - magic number
    # H - number of items in the block
    # H - flags
    # xx - unused
    # i - minimum field length of docs in the block, or 0xffffffff
    # i - maximum field length of docs in the block, or 0
    # i - length of IDs section
    # H - length of lengths section
    # H - length of weights section
    # H - length of poses section
    # H - length of chars section
    # i - length of payloads section
    header = Struct("<HHHxxiiiHHHHi")

    # Encoded single post header
    # B - flags
    # I - docid
    # H - termbytes length
    # B - field length (encoded as byte)
    post_header = Struct("<BIHB")

    def _posts_to_bytes(self, fmt: Format, posts: Sequence[PostTuple],
                        flags: int, id_bytes: bytes) -> bytes:
        len_bytes = b''
        if fmt.has_lengths:
            lengths = self._extract(posts, LENGTH)  # type: List[int]
            len_bytes = encode_lengths(lengths)
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
            weight_bytes = encode_weights(weights)

        pos_bytes = b''
        if fmt.has_positions:
            poslists = self._extract(posts, POSITIONS)  # type: List[PosList]
            pos_bytes = encode_positions(poslists)

        char_bytes = b''
        if fmt.has_chars:
            charlists = self._extract(posts, CHARS)  # type: List[CharList]
            char_bytes = encode_chars(charlists)

        pay_bytes = b''
        if fmt.has_payloads:
            paylists = self._extract(posts, PAYLOADS)  # type: List[PayList]
            pay_bytes = encode_payloads(paylists)

        # Pack the metadata into the header struct
        header = self.header.pack(self.MAGIC_NUMBER, len(posts), flags,
                                  min_len, max_len,
                                  len(id_bytes),
                                  len(len_bytes),
                                  len(weight_bytes),
                                  len(pos_bytes),
                                  len(char_bytes),
                                  len(pay_bytes))

        # Join the header with all the byte strings
        return b''.join((header, id_bytes, len_bytes, weight_bytes, pos_bytes,
                         char_bytes, pay_bytes))

    def doclist_to_bytes(self, fmt: Format,
                         posts: Sequence[PostTuple]) -> bytes:
        if not posts:
            raise ValueError("Empty document postings list")

        flags = 0
        id_bytes = encode_docids([p[DOCID] for p in posts])
        return self._posts_to_bytes(fmt, posts, flags, id_bytes)

    def vector_to_bytes(self, fmt: Format, posts: List[PostTuple]) -> bytes:
        if not posts:
            raise ValueError("Empty vector postings list")

        flags = 1
        id_bytes = encode_terms([p[TERMBYTES] for p in posts])
        return self._posts_to_bytes(fmt, posts, flags, id_bytes)

    def doclist_reader(self, fmt: Format, src: bytes,
                       offset: int=0) -> 'BasicDocListReader':
        return BasicDocListReader(fmt, src, offset)

    def vector_reader(self, fmt: Format, src: bytes,
                      offset: int=0) -> 'BasicVectorReader':
        return BasicVectorReader(fmt, src, offset)


class BasicPostingReader(PostingReader):
    def __init__(self, fmt: Format, src: bytes, offset: int=0):
        super(BasicPostingReader, self).__init__(fmt, src, offset)

        header_struct = BasicPostingsIO.header

        # Unpack the header from the beginning of the byte range
        (magic, self._count, flags, self._min_len, self._max_len,
         self._ids_size, self._lens_size, self._weights_size,
         self._poses_size, self._chars_size, self._pays_size,
         ) = header_struct.unpack(src[offset: offset + header_struct.size])
        assert magic == BasicPostingsIO.MAGIC_NUMBER

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
        self._positions_info = None  # type: listinfo
        self._chars_info = None  # type: listinfo
        self._payload_index = None  # type: Sequence[Tuple[int, Sequence[int]]

    def end_offset(self) -> int:
        return self._end_offset

    def _get_lens(self):
        if self._lens is None:
            self._lens = decode_lengths(self._src, self._lens_offset,
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

    def _get_weights(self):
        if self._weights is None:
            self._weights = decode_weights(self._src, self._weights_offset,
                                           self._weights_size, self._count)
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

    def positions(self, n: int) -> Sequence[int]:
        if n < 0 or n >= self._count:
            raise IndexError
        if not self._poses_size:
            raise UnsupportedFeature

        return decode_position_list(self._src, self._poses_offset,
                                    self._poses_size, n, self._count,
                                    cache=self)

    def chars(self, n: int) -> Sequence[Tuple[int, int]]:
        if n < 0 or n >= self._count:
            raise IndexError
        if not self._chars_size:
            raise UnsupportedFeature

        return decode_chars_list(self._src, self._chars_offset,
                                 self._chars_size, n, self._count,
                                 cache=self)

    def payloads(self, n: int) -> Sequence[bytes]:
        if n < 0 or n >= self._count:
            raise IndexError
        if not self._pays_size:
            raise UnsupportedFeature

        return decode_pays_list(self._src, self._pays_offset, self._pays_size,
                                n, self._count, cache=self)


class BasicDocListReader(BasicPostingReader, DocListReader):
    def __init__(self, fmt: Format, src: bytes, offset: int=0):
        super(BasicDocListReader, self).__init__(fmt, src, offset)

        self._ids = decode_docids(self._src, self._ids_offset, self._ids_size)

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


class BasicVectorReader(BasicPostingReader, VectorReader):
    def __init__(self, fmt: Format, src: bytes, offset: int=0):
        super(BasicVectorReader, self).__init__(fmt, src, offset)

        self._terms = decode_terms(self._src, self._ids_offset, self._ids_size,
                                   self._count)

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


# Term cache

# class TermCache(object):
#     def __init__(self, schema: 'fields.Schema', postlimit: int=1000000):
#         self.schema = schema
#         self.postlimit = postlimit
#
#         # {fieldname: {termbytes: [list of postings]}}
#         self._cache = {}  # type: Dict[str, Dict[bytes, List[PostTuple]]]
#         self._count = 0
#
#         self.key_flushes = 0
#         self.limit_flushes = 0
#
#     def __len__(self) -> int:
#         return self._count
#
#     def add_posting(self, fieldname, post: PostTuple):
#         cache = self._cache
#         if fieldname in cache:
#             fcache = cache[fieldname]
#         else:
#             fcache = cache[fieldname] = {}
#
#         self._count += 1
#         termbytes = post[TERMBYTES]
#         if termbytes in fcache:
#             ls = fcache[termbytes]
#         else:
#             fcache[termbytes] = ls = []
#
#         ls.append(post)
#
#     def add_postings(self, fieldname: str, posts: Sequence[PostTuple]):
#         cache = self._cache
#         if fieldname in cache:
#             fcache = cache[fieldname]
#         else:
#             fcache = cache[fieldname] = {}
#
#         self._count += len(posts)
#         for post in posts:
#             termbytes = post[TERMBYTES]
#             if termbytes in fcache:
#                 ls = fcache[termbytes]
#             else:
#                 fcache[termbytes] = ls = []
#
#             ls.append(post)
#
#     def term_postings(self, fieldname
#                       ) -> Iterable[Tuple[bytes, List[PostTuple]]]:
#         cache = self._cache[fieldname]
#         if fieldname not in cache:
#             return
#         fcache = cache[fieldname]
#         for termbytes in sorted(fcache):
#             yield termbytes, fcache[termbytes]
#
#     def write_to(self, writer: 'codec.FieldWriter'):
#         for fieldname in sorted(self._cache):
#             fieldobj = self.schema[fieldname]
#             writer.start_field(fieldname, fieldobj)
#             for termbytes, postings in self.term_postings(fieldname):
#                 writer.start_term(termbytes)
#                 writer.add_posting_list(postings)
#                 writer.finish_term()
#             writer.finish_field()
#
