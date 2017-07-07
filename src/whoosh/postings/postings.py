import logging
from abc import abstractmethod
from typing import Any, Iterable, List, Optional, Sequence, Tuple, Union

from whoosh.ifaces import analysis
from whoosh.compat import text_type
from whoosh.postings import postform
from whoosh.postings.ptuples import PostTuple, RawPost


if False:
    from whoosh.postings.postform import Format


logger = logging.getLogger(__name__)


# Exceptions

class UnsupportedFeature(Exception):
    pass


# Helper functions

def tokens(value: Union[Sequence, text_type], analyzer: 'analysis.Analyzer',
           kwargs: dict) -> 'Iterable[analysis.Token]':
    if isinstance(value, (tuple, list)):
        gen = analysis.entoken(value, **kwargs)
    else:
        gen = analyzer(value, **kwargs)
    return analysis.unstopped(gen)


# Interfaces

class PostingReader:
    def __init__(self, src: bytes, offset: int=0):
        self._src = src
        self._offset = offset
        self._count = 0

        self.has_lengths = False
        self.has_weights = False
        self.has_positions = False
        self.has_chars = False
        self.has_payloads = False

    def __len__(self) -> int:
        return self._count

    def supports(self, feature: str) -> bool:
        return getattr(self, "has_%s" % feature, False)

    def total_weight(self) -> float:
        # Sublclasses should replace this with something more efficient
        return sum(self.weight(i) for i in range(len(self)))

    @abstractmethod
    def can_copy_raw_to(self, to_io: 'PostingsIO',
                        to_fmt: 'postform.Format') -> bool:
        raise NotImplementedError

    @abstractmethod
    def raw_bytes(self) -> bytes:
        raise NotImplementedError

    @abstractmethod
    def size_in_bytes(self) -> int:
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
    def max_weight(self) -> float:
        raise NotImplementedError


class DocListReader(PostingReader):
    @abstractmethod
    def __len__(self):
        raise NotImplementedError

    @abstractmethod
    def id(self, n: int) -> int:
        raise NotImplementedError

    @abstractmethod
    def length(self, n: int) -> int:
        raise NotImplementedError

    @abstractmethod
    def min_length(self):
        raise NotImplementedError

    @abstractmethod
    def max_length(self):
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

        from whoosh.postings.ptuples import posting

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
    def __len__(self):
        raise NotImplementedError

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

        from whoosh.postings.ptuples import posting

        weight = self.weight(i) if self.has_weights else None
        poses = self.positions(i) if self.has_positions else None
        chars = self.chars(i) if self.has_chars else None
        pays = self.payloads(i) if self.has_payloads else None

        return posting(docid, termbytes=self.termbytes(i),
                       weight=weight, positions=poses, chars=chars,
                       payloads=pays)

    def postings(self, docid: int=None) -> Iterable[PostTuple]:
        from whoosh.postings.ptuples import posting

        has_weights = self.has_weights
        has_poses = self.has_positions
        has_chars = self.has_chars
        has_payloads = self.has_payloads

        for i in range(len(self)):
            yield posting(
                docid, termbytes=self.termbytes(i),
                weight=self.weight(i) if has_weights else None,
                positions=self.positions(i) if has_poses else None,
                chars=self.chars(i) if has_chars else None,
                payloads=self.payloads(i) if has_payloads else None,
            )


class PostingsIO:
    def __eq__(self, other):
        return type(self) is type(other) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not self == other

    @staticmethod
    def _extract(posts: Sequence[RawPost], member: int) -> Sequence:
        from whoosh.postings.ptuples import postfield_name

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

    def can_copy_raw_to(self, from_fmt: 'postform.Format',
                        other_io: 'PostingsIO',
                        to_fmt: 'postform.Format'
                        ) -> bool:
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
    def doclist_reader(self, bs: bytes, offset: int=0) -> DocListReader:
        raise NotImplementedError

    @abstractmethod
    def vector_reader(self, bs: bytes) -> VectorReader:
        raise NotImplementedError


class EmptyVectorReader(VectorReader):
    def __init__(self):
        pass

    def __len__(self):
        return 0

    def termbytes(self, n: int) -> bytes:
        raise IndexError("No items in this reader")

    def weight(self, n: int) -> float:
        raise IndexError("No items in this reader")

    def positions(self, n: int) -> List[int]:
        raise IndexError("No items in this reader")

    def chars(self, n: int) -> List[Tuple[int, int]]:
        raise IndexError("No items in this reader")

    def payloads(self, n: int) -> List[bytes]:
        raise IndexError("No items in this reader")

    def max_weight(self):
        return 0.0

    def raw_bytes(self) -> bytes:
        raise Exception("Virtual reader has no underlying bytes")

    def can_copy_raw_to(self, to_io: 'PostingsIO',
                        fmt: 'postform.Format') -> bool:
        raise Exception("Virtual reader cannot be copied")

    def size_in_bytes(self) -> int:
        raise Exception("Virtual reader has no size")


