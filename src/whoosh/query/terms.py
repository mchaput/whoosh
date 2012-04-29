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
import copy, fnmatch, re
from collections import defaultdict

from whoosh import matching
from whoosh.analysis import Token
from whoosh.compat import bytes_type, text_type, u
from whoosh.lang.morph_en import variations
from whoosh.query import qcore


class Term(qcore.Query):
    """Matches documents containing the given term (fieldname+text pair).

    >>> Term("content", u"render")
    """

    __inittypes__ = dict(fieldname=str, text=text_type, boost=float)

    def __init__(self, fieldname, text, boost=1.0):
        self.fieldname = fieldname
        self.text = text
        self.boost = boost

    def __eq__(self, other):
        return (other
                and self.__class__ is other.__class__
                and self.fieldname == other.fieldname
                and self.text == other.text
                and self.boost == other.boost)

    def __repr__(self):
        r = "%s(%r, %r" % (self.__class__.__name__, self.fieldname, self.text)
        if self.boost != 1.0:
            r += ", boost=%s" % self.boost
        r += ")"
        return r

    def __unicode__(self):
        t = u("%s:%s") % (self.fieldname, self.text)
        if self.boost != 1:
            t += u("^") + text_type(self.boost)
        return t

    __str__ = __unicode__

    def __hash__(self):
        return hash(self.fieldname) ^ hash(self.text) ^ hash(self.boost)

    def has_terms(self):
        return True

    def tokens(self, boost=1.0):
        yield Token(fieldname=self.fieldname, text=self.text,
                    boost=boost * self.boost, startchar=self.startchar,
                    endchar=self.endchar, chars=True)

    def replace(self, fieldname, oldtext, newtext):
        q = copy.copy(self)
        if q.fieldname == fieldname and q.text == oldtext:
            q.text = newtext
        return q

    def estimate_size(self, ixreader):
        return ixreader.doc_frequency(self.fieldname, self.text)

    def matcher(self, searcher, weighting=None):
        text = self.text
        if self.fieldname not in searcher.schema:
            return matching.NullMatcher()
        # If someone created a query object with a non-text term,e.g.
        # query.Term("printed", True), be nice and convert it to text
        if not isinstance(text, (bytes_type, text_type)):
            field = searcher.schema[self.fieldname]
            text = field.to_text(text)

        if (self.fieldname, text) in searcher.reader():
            m = searcher.postings(self.fieldname, text, weighting=weighting)
            if self.boost != 1.0:
                m = matching.WrappingMatcher(m, boost=self.boost)
            return m
        else:
            return matching.NullMatcher()


class MultiTerm(qcore.Query):
    """Abstract base class for queries that operate on multiple terms in the
    same field.
    """

    TOO_MANY_CLAUSES = 1024
    constantscore = False

    def _words(self, ixreader):
        raise NotImplementedError

    def simplify(self, ixreader):
        existing = [Term(self.fieldname, word, boost=self.boost)
                    for word in sorted(set(self._words(ixreader)))]
        if len(existing) == 1:
            return existing[0]
        elif existing:
            from whoosh.query import Or
            return Or(existing)
        else:
            return qcore.NullQuery

    def estimate_size(self, ixreader):
        return sum(ixreader.doc_frequency(self.fieldname, text)
                   for text in self._words(ixreader))

    def estimate_min_size(self, ixreader):
        return min(ixreader.doc_frequency(self.fieldname, text)
                   for text in self._words(ixreader))

    def existing_terms(self, ixreader, termset=None, reverse=False,
                       phrases=True, expand=False):
        termset, test = self._existing_terms_helper(ixreader, termset, reverse)

        if not expand:
            return termset
        fieldname = self.field()
        if fieldname is None:
            return termset

        for word in self._words(ixreader):
            term = (fieldname, word)
            if test(term):
                termset.add(term)
        return termset

    def matcher(self, searcher, weighting=None):
        fieldname = self.fieldname
        constantscore = self.constantscore
        reader = searcher.reader()
        qs = [Term(fieldname, word) for word in self._words(reader)]
        if not qs:
            return matching.NullMatcher()

        if len(qs) == 1:
            # If there's only one term, just use it
            q = qs[0]
        elif constantscore or len(qs) > self.TOO_MANY_CLAUSES:
            # If there's so many clauses that an Or search would take forever,
            # trade memory for time and just find all the matching docs serve
            # them up as one or more ListMatchers
            fmt = searcher.schema[fieldname].format
            doc_to_values = defaultdict(list)
            doc_to_weights = defaultdict(float)
            for q in qs:
                m = q.matcher(searcher)
                while m.is_active():
                    docnum = m.id()
                    doc_to_values[docnum].append(m.value())
                    if not constantscore:
                        doc_to_weights[docnum] += m.weight()
                    m.next()

            docnums = sorted(doc_to_values.keys())
            # This is a list of lists of value strings -- ListMatcher will
            # actually do the work of combining multiple values if the user
            # asks for them
            values = [doc_to_values[docnum] for docnum in docnums]

            kwargs = {"values": values, "format": fmt}
            if constantscore:
                kwargs["all_weights"] = self.boost
            else:
                kwargs["weights"] = [doc_to_weights[docnum]
                                     for docnum in docnums]

            return matching.ListMatcher(docnums, **kwargs)
        else:
            # The default case: Or the terms together
            from whoosh.query import Or
            q = Or(qs)

        return q.matcher(searcher, weighting=weighting)


class PatternQuery(MultiTerm):
    """An intermediate base class for common methods of Prefix and Wildcard.
    """

    __inittypes__ = dict(fieldname=str, text=text_type, boost=float)

    def __init__(self, fieldname, text, boost=1.0, constantscore=True):
        self.fieldname = fieldname
        self.text = text
        self.boost = boost
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
        for i, char in enumerate(text):
            if char in specialchars:
                break
        return text[:i]

    def _words(self, ixreader):
        exp = re.compile(self._get_pattern())
        prefix = self._find_prefix(self.text)
        if prefix:
            candidates = ixreader.expand_prefix(self.fieldname, prefix)
        else:
            candidates = ixreader.lexicon(self.fieldname)

        for text in candidates:
            if exp.match(text):
                yield text


class Prefix(PatternQuery):
    """Matches documents that contain any terms that start with the given text.

    >>> # Match documents containing words starting with 'comp'
    >>> Prefix("content", u"comp")
    """

    def __unicode__(self):
        return "%s:%s*" % (self.fieldname, self.text)

    __str__ = __unicode__

    def _words(self, ixreader):
        return ixreader.expand_prefix(self.fieldname, self.text)


class Wildcard(PatternQuery):
    """Matches documents that contain any terms that match a "glob" pattern.
    See the Python ``fnmatch`` module for information about globs.

    >>> Wildcard("content", u"in*f?x")
    """

    SPECIAL_CHARS = frozenset("*?")

    def __unicode__(self):
        return "%s:%s" % (self.fieldname, self.text)

    __str__ = __unicode__

    def _get_pattern(self):
        return fnmatch.translate(self.text)

    def normalize(self):
        # If there are no wildcard characters in this "wildcard", turn it into
        # a simple Term
        text = self.text
        if text == "*":
            from whoosh.query import Every
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

    # _words() implemented in PatternQuery


class Regex(PatternQuery):
    """Matches documents that contain any terms that match a regular
    expression. See the Python ``re`` module for information about regular
    expressions.
    """

    SPECIAL_CHARS = frozenset("{}()[].?*+^$\\")

    def __unicode__(self):
        return '%s:r"%s"' % (self.fieldname, self.text)

    __str__ = __unicode__

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

    # _words() implemented in PatternQuery


class ExpandingTerm(MultiTerm):
    """Intermediate base class for queries such as FuzzyTerm and Variations
    that expand into multiple queries, but come from a single term.
    """

    def has_terms(self):
        return True

    def tokens(self, boost=1.0):
        yield Token(fieldname=self.fieldname, text=self.text,
                    boost=boost * self.boost, startchar=self.startchar,
                    endchar=self.endchar, chars=True)


class FuzzyTerm(ExpandingTerm):
    """Matches documents containing words similar to the given term.
    """

    __inittypes__ = dict(fieldname=str, text=text_type, boost=float,
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

        self.fieldname = fieldname
        self.text = text
        self.boost = boost
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

    def __unicode__(self):
        r = self.text + u("~")
        if self.maxdist > 1:
            r += u("%d") % self.maxdist
        if self.boost != 1.0:
            r += u("^%f") % self.boost
        return r

    __str__ = __unicode__

    def __hash__(self):
        return (hash(self.fieldname) ^ hash(self.text) ^ hash(self.boost)
                ^ hash(self.maxdist) ^ hash(self.prefixlength)
                ^ hash(self.constantscore))

    def _words(self, ixreader):
        return ixreader.terms_within(self.fieldname, self.text, self.maxdist,
                                     prefix=self.prefixlength)


class Variations(ExpandingTerm):
    """Query that automatically searches for morphological variations of the
    given word in the same field.
    """

    def __init__(self, fieldname, text, boost=1.0):
        self.fieldname = fieldname
        self.text = text
        self.boost = boost

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

    def _words(self, ixreader):
        fieldname = self.fieldname
        return [word for word in variations(self.text)
                if (fieldname, word) in ixreader]

    def __unicode__(self):
        return u("%s:<%s>") % (self.fieldname, self.text)

    __str__ = __unicode__

    def replace(self, fieldname, oldtext, newtext):
        q = copy.copy(self)
        if q.fieldname == fieldname and q.text == oldtext:
            q.text = newtext
        return q


