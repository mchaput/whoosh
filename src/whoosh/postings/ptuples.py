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

from operator import itemgetter
from typing import Optional, Sequence, Tuple


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


def change_docid(post: PostTuple, newdocid: int) -> PostTuple:
    return newdocid, post[1], post[2], post[3], post[4], post[5], post[6]


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
