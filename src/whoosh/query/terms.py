# Copyright 2007 Matt Chaput. All rights reserved.
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

from __future__ import division
import copy
import fnmatch
import re
import typing
from typing import Iterable, List, Tuple

from whoosh import collectors, searching
from whoosh.query import queries
from whoosh.matching import matchers
from whoosh.analysis import analysis
from whoosh.lang.morph_en import variations

# Typing imports
if typing.TYPE_CHECKING:
    from whoosh import reading, searching


__all__ = ("Term", "MultiTerm", "PatternQuery", "Prefix", "Wildcard", "Regex",
           "ExpandingTerm", "FuzzyTerm", "Variations", "PathListQuery")


@collectors.register("term")
class Term(queries.Query):
    """
    Matches documents containing the given term (fieldname+text pair).

    >>> Term("content", u"render")
    """

    __inittypes__ = dict(fieldname=str, text=str, boost=float)

    def __init__(self, fieldname, text, boost=1.0, minquality=None):
        super(Term, self).__init__(boost=boost)
        self.fieldname = fieldname
        self.text = text
        self.minquality = minquality

    def __eq__(self, other: 'Term'):
        return (other
                and self.__class__ is other.__class__
                and self.field() == other.field()
                and self.text == other.text
                and self.boost == other.boost)

    def __repr__(self):
        r = "%s(%r, %r" % (self.__class__.__name__, self.fieldname, self.text)
        if self.boost != 1.0:
            r += ", boost=%s" % self.boost
        r += ")"
        return r

    def __str__(self):
        text = self.text
        if isinstance(text, bytes):
            try:
                text = text.decode("ascii")
            except UnicodeDecodeError:
                text = repr(text)

        t = u"%s:%s" % (self.fieldname, text)
        if self.boost != 1:
            t += u"^%s" % str(self.boost)
        return t

    def __hash__(self):
        return hash(self.fieldname) ^ hash(self.text) ^ hash(self.boost)

    def set_text(self, text):
        self.text = text

    def has_terms(self) -> bool:
        return True

    def _terms(self, reader: 'reading.IndexReader'=None,
               phrases: bool=True) -> Iterable[Tuple[str, str]]:
        fieldname = self.field()
        if not fieldname:
            return

        text = self.text
        if reader and not isinstance(text, bytes):
            if (fieldname, text) not in reader:
                return
            fieldobj = reader.schema[fieldname]
            text = fieldobj.to_bytes(text)

        yield fieldname, text

    def _tokens(self, reader: 'reading.IndexReader'=None, phrases: bool=True,
                boost=1.0) -> 'Iterable[analysis.Token]':
        fieldname = self.field()
        if not fieldname:
            return

        yield analysis.Token(fieldname=fieldname, text=self.text,
                             boost=boost * self.boost,
                             range_start=self.startchar,
                             range_end=self.endchar, ranges=True)

    def estimate_size(self, ixreader: 'reading.IndexReader') -> int:
        fieldname = self.fieldname
        if fieldname not in ixreader.schema:
            return 0
        return ixreader.doc_frequency(fieldname, self.text)

    def matcher(self, searcher: 'searching.Searcher',
                context: 'searching.SearchContext'=None) -> 'matchers.Matcher':
        from whoosh.matching.wrappers import WrappingMatcher

        assert isinstance(searcher, searching.SearcherType)
        fieldname = self.fieldname
        if fieldname not in searcher.schema:
            return matchers.NullMatcher()
        field = searcher.schema[fieldname]
        include = context.include if context else None
        exclude = context.exclude if context else None

        text = self.text
        if not isinstance(text, bytes):
            try:
                text = field.to_bytes(text)
            except ValueError:
                return matchers.NullMatcher()

        if (self.fieldname, text) in searcher.reader():
            if context is None:
                w = searcher.weighting
            else:
                w = context.weighting

            m = searcher.matcher(fieldname, text, weighting=w,
                                 include=include, exclude=exclude)
            # if self.minquality:
            #     m.set_min_quality(self.minquality)
            if self.boost != 1.0:
                m = WrappingMatcher(m, boost=self.boost)
            return m
        else:
            return matchers.NullMatcher()


class MultiTerm(queries.Query):
    """
    Abstract base class for queries that operate on multiple terms in the
    same field.
    """

    def __init__(self, fieldname: str, text: str, boost: float=1.0):
        super(MultiTerm, self).__init__(boost=boost)
        self.fieldname = fieldname
        self.text = text
        self.constantscore = False

    def set_text(self, text):
        self.text = text

    def _btexts(self, ixreader):
        raise NotImplementedError(self.__class__.__name__)

    def _terms(self, reader: 'reading.IndexReader'=None,
               phrases: bool=True) -> Iterable[Tuple[str, str]]:
        fieldname = self.field()
        if reader and fieldname:
            for btext in self._btexts(reader):
                yield fieldname, btext

    def _tokens(self, reader: 'reading.IndexReader'=None, phrases: bool=True,
                boost=1.0) -> 'Iterable[analysis.Token]':
        fieldname = self.field()
        if not fieldname:
            return

        if reader:
            fieldobj = reader.schema[fieldname]
            texts = (fieldobj.from_bytes(tbytes) for tbytes
                     in self._btexts(reader))
        else:
            texts = [self.text]

        for text in texts:
            yield analysis.Token(fieldname=fieldname, text=text,
                                 boost=boost * self.boost,
                                 range_start=self.startchar,
                                 range_end=self.endchar,
                                 ranges=True)

    def estimate_size(self, ixreader: 'reading.IndexReader') -> int:
        fieldname = self.field()
        return sum(ixreader.doc_frequency(fieldname, btext)
                   for btext in self._btexts(ixreader))

    def estimate_min_size(self, ixreader):
        fieldname = self.field()
        return min(ixreader.doc_frequency(fieldname, text)
                   for text in self._btexts(ixreader))

    def simplify(self, reader: 'reading.IndexReader') -> queries.Query:
        from whoosh.query.compound import Or

        fieldname = self.field()

        qs = [Term(fieldname, tbytes, boost=self.boost)
              for tbytes in self._btexts(reader)]

        if not qs:
            q = queries.NullQuery()
        elif len(qs) == 1:
            q = qs[0]
        else:
            # Or the terms together
            q = Or(qs)
        return q

    def matcher(self, searcher, context=None) -> 'matchers.Matcher':
        if self.constantscore:
            # To tell the sub-query that score doesn't matter, set weighting
            # to None
            if context:
                context = context.set(weighting=None)
            else:
                context = searcher.context(weighting=None)
        sq = self.simplify(searcher.reader())
        return sq.matcher(searcher, context)


class PatternQuery(MultiTerm):
    """
    An intermediate base class for common methods of Prefix and Wildcard.
    """

    __inittypes__ = dict(fieldname=str, text=str, boost=float)

    def __init__(self, fieldname, text, boost=1.0, constantscore=True):
        super(PatternQuery, self).__init__(fieldname, text, boost=boost)
        self.constantscore = constantscore

    def __eq__(self, other):
        return (other and self.__class__ is other.__class__
                and self.fieldname == other.fieldname
                and self.text == other.text and self.boost == other.boost
                and self.constantscore == other.constantscore)

    def __repr__(self):
        r = "%s(%r, %r" % (self.__class__.__name__, self.fieldname, self.text)
        if self.boost != 1:
            r += ", boost=%s" % self.boost
        r += ")"
        return r

    def __hash__(self):
        return (hash(self.fieldname) ^ hash(self.text) ^ hash(self.boost)
                ^ hash(self.constantscore))

    def _get_pattern(self):
        raise NotImplementedError

    def _find_prefix(self, text):
        # Subclasses/instances should set the SPECIAL_CHARS attribute to a set
        # of characters that mark the end of the literal prefix
        specialchars = self.SPECIAL_CHARS
        i = 0
        for i, char in enumerate(text):
            if char in specialchars:
                break
        return text[:i]

    def _btexts(self, ixreader):
        field = ixreader.schema[self.fieldname]

        exp = re.compile(self._get_pattern())
        prefix = self._find_prefix(self.text)
        if prefix:
            candidates = ixreader.expand_prefix(self.fieldname, prefix)
        else:
            candidates = ixreader.lexicon(self.fieldname)

        from_bytes = field.from_bytes
        for btext in candidates:
            text = from_bytes(btext)
            if exp.match(text):
                yield btext


@collectors.register("prefix")
class Prefix(PatternQuery):
    """
    Matches documents that contain any terms that start with the given text.

    >>> # Match documents containing words starting with 'comp'
    >>> Prefix("content", u"comp")
    """

    def __str__(self):
        return "%s:%s*" % (self.fieldname, self.text)

    def _btexts(self, ixreader):
        return ixreader.expand_prefix(self.fieldname, self.text)

    def matcher(self, searcher, context=None) -> 'matchers.Matcher':
        if self.text == "":
            from whoosh.query import Every
            eq = Every(self.fieldname, boost=self.boost)
            return eq.matcher(searcher, context)
        else:
            return super(Prefix, self).matcher(searcher, context)


@collectors.register("wildcard")
class Wildcard(PatternQuery):
    """Matches documents that contain any terms that match a "glob" pattern.
    See the Python ``fnmatch`` module for information about globs.

    >>> Wildcard("content", u"in*f?x")
    """

    SPECIAL_CHARS = frozenset("*?[")

    def __str__(self):
        return "%s:%s" % (self.fieldname, self.text)

    def _get_pattern(self):
        return fnmatch.translate(self.text)

    def normalize(self):
        from whoosh.query import Every

        # If there are no wildcard characters in this "wildcard", turn it into
        # a simple Term
        text = self.text
        if text == "*":
            return Every(self.fieldname, boost=self.boost)
        if "*" not in text and "?" not in text:
            # If no wildcard chars, convert to a normal term.
            return Term(self.fieldname, self.text, boost=self.boost)
        elif ("?" not in text and text.endswith("*")
              and text.find("*") == len(text) - 1):
            # If the only wildcard char is an asterisk at the end, convert to a
            # Prefix query.
            return Prefix(self.fieldname, self.text[:-1], boost=self.boost)
        else:
            return self

    def matcher(self, searcher, context=None) -> 'matchers.Matcher':
        if self.text == "*":
            from whoosh.query import Every
            eq = Every(self.fieldname, boost=self.boost)
            return eq.matcher(searcher, context)
        else:
            return PatternQuery.matcher(self, searcher, context)

    # _btexts() implemented in PatternQuery


@collectors.register("regex")
class Regex(PatternQuery):
    """Matches documents that contain any terms that match a regular
    expression. See the Python ``re`` module for information about regular
    expressions.
    """

    SPECIAL_CHARS = frozenset("{}()[].?*+^$\\")

    def __str__(self):
        return '%s:r"%s"' % (self.fieldname, self.text)

    def _get_pattern(self):
        return self.text

    def _find_prefix(self, text):
        if "|" in text:
            return ""
        if text.startswith("^"):
            text = text[1:]
        elif text.startswith("\\A"):
            text = text[2:]

        prefix = PatternQuery._find_prefix(self, text)

        lp = len(prefix)
        if lp < len(text) and text[lp] in "*?":
            # we stripped something starting from * or ? - they both MAY mean
            # "0 times". As we had stripped starting from FIRST special char,
            # that implies there were only ordinary chars left of it. Thus,
            # the very last of them is not part of the real prefix:
            prefix = prefix[:-1]
        return prefix

    def matcher(self, searcher, context=None) -> 'matchers.Matcher':
        if self.text == ".*":
            from whoosh.query import Every
            eq = Every(self.fieldname, boost=self.boost)
            return eq.matcher(searcher, context)
        else:
            return PatternQuery.matcher(self, searcher, context)

    # _btexts() implemented in PatternQuery


class ExpandingTerm(MultiTerm):
    """
    Intermediate base class for queries such as FuzzyTerm and Variations
    that expand into multiple queries, but come from a single term.
    """

    def has_terms(self):
        return True

    def _terms(self, reader: 'reading.IndexReader'=None,
               phrases: bool=True) -> Iterable[Tuple[str, str]]:
        fieldname = self.field()
        if fieldname:
            if reader:
                for btext in self._btexts(reader):
                    yield fieldname, btext
            else:
                yield fieldname, self.text


@collectors.register("fuzzy_term")
class FuzzyTerm(ExpandingTerm):
    """
    Matches documents containing words similar to the given term.
    """

    __inittypes__ = dict(fieldname=str, text=str, boost=float,
                         maxdist=float, prefixlength=int)

    def __init__(self, fieldname, text, boost=1.0, maxdist=1,
                 prefixlength=1, constantscore=True):
        """
        :param fieldname: The name of the field to search.
        :param text: The text to search for.
        :param boost: A boost factor to apply to scores of documents matching
            this query.
        :param maxdist: The maximum edit distance from the given text.
        :param prefixlength: The matched terms must share this many initial
            characters with 'text'. For example, if text is "light" and
            prefixlength is 2, then only terms starting with "li" are checked
            for similarity.
        """

        super(FuzzyTerm, self).__init__(fieldname, text, boost=boost)
        self.maxdist = maxdist
        self.prefixlength = prefixlength
        self.constantscore = constantscore

    def __eq__(self, other):
        return (other and self.__class__ is other.__class__
                and self.fieldname == other.fieldname
                and self.text == other.text
                and self.maxdist == other.maxdist
                and self.prefixlength == other.prefixlength
                and self.boost == other.boost
                and self.constantscore == other.constantscore)

    def __repr__(self):
        r = "%s(%r, %r, boost=%f, maxdist=%d, prefixlength=%d)"
        return r % (self.__class__.__name__, self.fieldname, self.text,
                    self.boost, self.maxdist, self.prefixlength)

    def __str__(self):
        r = u"%s:%s" % (self.fieldname, self.text) + u"~"
        if self.maxdist > 1:
            r += u"%d" % self.maxdist
        if self.boost != 1.0:
            r += u"^%f" % self.boost
        return r

    def __hash__(self):
        return (hash(self.fieldname) ^ hash(self.text) ^ hash(self.boost)
                ^ hash(self.maxdist) ^ hash(self.prefixlength)
                ^ hash(self.constantscore))

    def _btexts(self, ixreader):
        return ixreader.terms_within(self.fieldname, self.text, self.maxdist,
                                     prefix=self.prefixlength)

    def replace(self, fieldname, oldtext, newtext):
        q = copy.copy(self)
        if q.fieldname == fieldname and q.text == oldtext:
            q.text = newtext
        return q


@collectors.register("variations")
class Variations(ExpandingTerm):
    """
    Query that automatically searches for morphological variations of the
    given word in the same field.
    """

    def __init__(self, fieldname, text, boost=1.0, constantscore=False):
        super(Variations, self).__init__(fieldname, text, boost=boost)
        self.constantscore = constantscore

    def __repr__(self):
        r = "%s(%r, %r" % (self.__class__.__name__, self.fieldname, self.text)
        if self.boost != 1:
            r += ", boost=%s" % self.boost
        r += ")"
        return r

    def __eq__(self, other):
        return (other and self.__class__ is other.__class__
                and self.fieldname == other.fieldname
                and self.text == other.text and self.boost == other.boost)

    def __hash__(self):
        return hash(self.fieldname) ^ hash(self.text) ^ hash(self.boost)

    def _btexts(self, ixreader):
        fieldname = self.fieldname
        to_bytes = ixreader.schema[fieldname].to_bytes
        for word in variations(self.text):
            try:
                btext = to_bytes(word)
            except ValueError:
                continue

            if (fieldname, btext) in ixreader:
                yield btext

    def __str__(self):
        return u"%s:<%s>" % (self.fieldname, self.text)

    def replace(self, fieldname, oldtext, newtext):
        q = copy.copy(self)
        if q.fieldname == fieldname and q.text == oldtext:
            q.text = newtext
        return q


class PathListQuery(queries.Query):
    """
    This is essentially a single-purpose query, to find all documents whose
    unique ID is in a list  of IDs, for bulk deletion. The twist is that the
    query matches docs not only by the exact IDs, but also docs with IDs that
    have a `#fragment` on the end, where except for the fragment the ID would
    be in the set. This is to match "sub-documents" derived from the original
    file, which should also be deleted.

    For example, if the pathlist is `["/a/b", "/c/d"]`, then this will delete
    docswith paths `/a/b` and `/c/d`, but also for example `/a/b#foo` and
    `/c/d#bar`.

    This query is not useful for scoring the results; it artificially sees the
    weight of each matching document as 1.0.
    """

    def __init__(self, fieldname: str, pathlist: Iterable[bytes]):
        """
        :param fieldname: The name of the field to search in.
        :param pathlist: A sequence of bytestrings or unicode strings to match.
            The query will match these strings as well as strings that also have
            an extra `#fragment` on the end.
        """
        self.fieldname = fieldname
        # Make sure the list is all bytestrings, and in sorted order
        self.pathlist = sorted((t.encode("utf8") if isinstance(t, str) else t)
                               for t in pathlist)
        self.boost = 1.0

    def has_terms(self):
        return True

    def terms(self, reader: 'reading.IndexReader'=None, phrases: bool=True
              ) -> Iterable[Tuple[str, str]]:
        fieldname = self.fieldname
        for term in self._paths(reader):
            yield fieldname, term

    def _paths(self, reader: 'reading.IndexReader'=None) -> List[bytes]:
        # Look at all the terms in the field and return a list of the terms that
        # match the given paths

        pathlist = self.pathlist
        found = []

        # Current place in the sorted list of paths to match
        n = 0
        # Current path to match
        current = pathlist[n]
        # Current path to match plus a hash mark, to check prefix matches
        prefix = current + b'#'
        # Term cursor to iterate through the field's terms
        cur = reader.cursor(self.fieldname)
        while n < len(pathlist) and cur.is_valid():
            # Current term
            tbytes = cur.termbytes()
            if tbytes < current:
                # We ahead of the current path, seek to the current path
                cur.seek(current)
            elif tbytes == current:
                # The current term matches the current path, record it as found
                found.append(tbytes)
                # Move to the next term (to check for prefix matches)
                cur.next()
            elif tbytes.startswith(prefix):
                # The current term has a prefix match with the current path
                found.append(tbytes)
                # Move to the next term (to check for more prefix matches)
                cur.next()
            else:
                # The current term is past the current path, move to the next
                # path in the pathlist
                n += 1
                if n < len(pathlist):
                    # Re-establish the current and prefix vars
                    current = pathlist[n]
                    prefix = current + b'#'
        return found

    def needs_spans(self):
        return False

    def estimate_size(self, reader: 'reading.IndexReader'):
        return len(self._paths(reader))

    def matcher(self, searcher, context) -> 'matchers.Matcher':
        from whoosh.matching import NullMatcher, ListMatcher

        fieldname = self.fieldname
        reader = searcher.reader()
        termlist = tuple(self._paths(reader))
        if termlist:
            # Look up each matching term and record the first doc ID (assumes
            # each value is unique!). This should be fast because unique doc IDs
            # are stored "inline" in the terminfo.
            docids = []
            for termbytes in termlist:
                m = reader.matcher(fieldname, termbytes)
                if m.is_active():
                    docids.append(m.id())
            docids.sort()
            return ListMatcher(docids)
        else:
            return NullMatcher()

