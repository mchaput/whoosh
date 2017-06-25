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

from collections import defaultdict
from typing import Any, Callable, Sequence, Tuple

from whoosh import analysis
from whoosh.postings import ptuples


class Format:
    """
    Base class of objects representing a set of options for storing postings in
    the backend.
    """

    def __init__(self, has_lengths: bool=False, has_weights: bool=False,
                 has_positions: bool=False, has_chars: bool=False,
                 has_payloads: bool=False, boost=1.0):
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

    def json_info(self) -> dict:
        return {
            "has_lengths": self.has_lengths,
            "has_weights": self.has_weights,
            "has_positions": self.has_positions,
            "has_chars": self.has_chars,
            "has_payloads": self.has_payloads,
        }

    def can_copy_raw_to(self, fmt: 'Format') -> bool:
        return (
            fmt.has_lengths == self.has_lengths and
            fmt.has_weights == self.has_weights and
            fmt.has_positions == self.has_positions and
            fmt.has_chars == self.has_chars and
            fmt.has_payloads == self.has_payloads
        )

    def supports(self, feature: str) -> bool:
        """
        Returns True if this format object supports the named information type:
        "lengths", "weights", "positions", "chars", or "payloads".

        :param feature: a string naming a posting feature to check.
        :rtype: bool
        """

        return getattr(self, "has_%s" % feature, False)

    def index(self, analyzer: 'analysis.Analyzer',
              to_bytes: Callable[[str], bytes],
              value: Any, docid: int=None, boost: float=1.0,
              **kwargs) -> Tuple[int, Sequence[ptuples.PostTuple]]:
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

        from whoosh.postings.postings import tokens
        posting = ptuples.posting

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


