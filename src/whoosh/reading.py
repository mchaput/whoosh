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

"""This module contains classes that allow reading from an index.
"""

from bisect import bisect_right
from heapq import heapify, heapreplace, heappop, nlargest

from whoosh.util import ClosableMixin
from whoosh.matching import MultiMatcher


# Exceptions

class TermNotFound(Exception):
    pass


# Base class

class IndexReader(ClosableMixin):
    """Do not instantiate this object directly. Instead use Index.reader().
    """

    def is_atomic(self):
        return True

    def __contains__(self, term):
        """Returns True if the given term tuple (fieldname, text) is
        in this reader.
        """
        raise NotImplementedError

    def __iter__(self):
        """Yields (fieldname, text, docfreq, indexfreq) tuples for each term in
        the reader, in lexical order.
        """
        raise NotImplementedError

    def close(self):
        """Closes the open files associated with this reader.
        """
        
        pass

    def generation(self):
        """Returns the generation of the index being read, or -1 if the backend
        is not versioned.
        """
        
        return -1

    def iter_from(self, fieldname, text):
        """Yields (field_num, text, doc_freq, index_freq) tuples for all terms
        in the reader, starting at the given term.
        """
        raise NotImplementedError

    def expand_prefix(self, fieldname, prefix):
        """Yields terms in the given field that start with the given prefix.
        """

        for fn, t, _, _ in self.iter_from(fieldname, prefix):
            if fn != fieldname or not t.startswith(prefix):
                return
            yield t

    def all_terms(self):
        """Yields (fieldname, text) tuples for every term in the index.
        """

        for fn, t, _, _ in self:
            yield (fn, t)

    def iter_field(self, fieldname, prefix=''):
        """Yields (text, doc_freq, index_freq) tuples for all terms in the
        given field.
        """

        for fn, t, docfreq, freq in self.iter_from(fieldname, prefix):
            if fn != fieldname:
                return
            yield t, docfreq, freq

    def iter_prefix(self, fieldname, prefix):
        """Yields (field_num, text, doc_freq, index_freq) tuples for all terms
        in the given field with a certain prefix.
        """

        for fn, t, docfreq, colfreq in self.iter_from(fieldname, prefix):
            if fn != fieldname or not t.startswith(prefix):
                return
            yield (t, docfreq, colfreq)

    def lexicon(self, fieldname):
        """Yields all terms in the given field.
        """

        for t, _, _ in self.iter_field(fieldname):
            yield t

    def has_deletions(self):
        """Returns True if the underlying index/segment has deleted
        documents.
        """
        raise NotImplementedError

    def all_doc_ids(self):
        """Returns an iterator of all (undeleted) document IDs in the reader.
        """
        
        # This default implementation works for backends like filedb that use
        # a continuous 0-N range of numbers to address documents, but will need
        # to be overridden if a backend, e.g., looks up documents using
        # persistent ID strings.
        
        is_deleted = self.is_deleted
        return (docnum for docnum in xrange(self.doc_count_all())
                if not is_deleted(docnum))
        
    def is_deleted(self, docnum):
        """Returns True if the given document number is marked deleted.
        """
        raise NotImplementedError

    def stored_fields(self, docnum):
        """Returns the stored fields for the given document number.
        
        :param numerickeys: use field numbers as the dictionary keys instead of
            field names.
        """
        raise NotImplementedError

    def all_stored_fields(self):
        """Yields the stored fields for all documents.
        """
        
        for docnum in xrange(self.doc_count_all()):
            if not self.is_deleted(docnum):
                yield self.stored_fields(docnum)

    def doc_count_all(self):
        """Returns the total number of documents, DELETED OR UNDELETED,
        in this reader.
        """
        raise NotImplementedError

    def doc_count(self):
        """Returns the total number of UNDELETED documents in this reader.
        """
        raise NotImplementedError

    def doc_frequency(self, fieldname, text):
        """Returns how many documents the given term appears in.
        """
        raise NotImplementedError

    def frequency(self, fieldname, text):
        """Returns the total number of instances of the given term in the
        collection.
        """
        raise NotImplementedError

    def field_length(self, fieldname):
        """Returns the total number of terms in the given field. This is used
        by some scoring algorithms.
        """
        raise NotImplementedError

    def doc_field_length(self, docnum, fieldname, default=0):
        """Returns the number of terms in the given field in the given
        document. This is used by some scoring algorithms.
        """
        raise NotImplementedError

    def doc_field_lengths(self, docnum):
        """Returns an iterator of (fieldname, length) pairs for the given
        document. This is used internally.
        """
        
        for fieldname in self.schema.scorable_names():
            length = self.doc_field_length(docnum, fieldname)
            if length:
                yield (fieldname, length)
    
    def max_field_length(self, fieldname, default=0):
        """Returns the maximum length of the field across all documents.
        """
        raise NotImplementedError

    def first_id(self, fieldname, text):
        """Returns the first ID in the posting list for the given term. This
        may be optimized in certain backends.
        """
        
        p = self.postings(fieldname, text)
        if p.is_active():
            return p.id()
        raise TermNotFound((fieldname, text))

    def postings(self, fieldname, text, scorer=None):
        """Returns a :class:`~whoosh.matching.Matcher` for the postings of the
        given term.
        
        >>> pr = reader.postings("content", "render")
        >>> pr.skip_to(10)
        >>> pr.id
        12
        
        :param fieldname: the field name or field number of the term.
        :param text: the text of the term.
        :rtype: :class:`whoosh.matching.Matcher`
        """

        raise NotImplementedError

    def has_vector(self, docnum, fieldname):
        """Returns True if the given document has a term vector for the given
        field.
        """
        raise NotImplementedError

    def vector(self, docnum, fieldname):
        """Returns a :class:`~whoosh.matching.Matcher` object for the
        given term vector.
        
        >>> docnum = searcher.document_number(path=u'/a/b/c')
        >>> v = searcher.vector(docnum, "content")
        >>> v.all_as("frequency")
        [(u"apple", 3), (u"bear", 2), (u"cab", 2)]
        
        :param docnum: the document number of the document for which you want
            the term vector.
        :param fieldname: the field name or field number of the field for which
            you want the term vector.
        :rtype: :class:`whoosh.matching.Matcher`
        """
        raise NotImplementedError

    def vector_as(self, astype, docnum, fieldname):
        """Returns an iterator of (termtext, value) pairs for the terms in the
        given term vector. This is a convenient shortcut to calling vector()
        and using the Matcher object when all you want are the terms and/or
        values.
        
        >>> docnum = searcher.document_number(path=u'/a/b/c')
        >>> searcher.vector_as("frequency", docnum, "content")
        [(u"apple", 3), (u"bear", 2), (u"cab", 2)]
        
        :param docnum: the document number of the document for which you want
            the term vector.
        :param fieldname: the field name or field number of the field for which
            you want the term vector.
        :param astype: a string containing the name of the format you want the
            term vector's data in, for example "weights".
        """

        vec = self.vector(docnum, fieldname)
        if astype == "weight":
            while vec.is_active():
                yield (vec.id(), vec.weight())
                vec.next()
        else:
            format = self.schema[fieldname].format
            decoder = format.decoder(astype)
            while vec.is_active():
                yield (vec.id(), decoder(vec.value()))
                vec.next()

    def most_frequent_terms(self, fieldname, number=5, prefix=''):
        """Returns the top 'number' most frequent terms in the given field as a
        list of (frequency, text) tuples.
        """

        return nlargest(number, ((tf, token)
                                 for token, _, tf
                                 in self.iter_prefix(fieldname, prefix)))

    def most_distinctive_terms(self, fieldname, number=5, prefix=None):
        """Returns the top 'number' terms with the highest `tf*idf` scores as
        a list of (score, text) tuples.
        """

        return nlargest(number, ((tf * (1.0 / df), token)
                                 for token, df, tf
                                 in self.iter_prefix(fieldname, prefix)))
    
    def leaf_readers(self):
        """Returns a list of (IndexReader, docbase) pairs for the child readers
        of this reader if it is a composite reader, or None if this reader
        is atomic.
        """
        
        return False
    
    #
    
    def supports_caches(self):
        """Returns True if this reader supports the field cache protocol.
        """
        
        return False
    
    def sort_docs_by(self, fieldname, docnums, reverse=False):
        """Returns a version of `docnums` sorted by the value of a field or
        a set of fields in each document.
        
        :param fieldname: either the name of a field, or a tuple of field names
            to specify a multi-level sort.
        :param docnums: a sequence of document numbers to sort.
        :param reverse: if True, reverses the sort direction.
        """
        
        raise NotImplementedError
    
    def key_docs_by(self, fieldname, docnums, limit, reverse=False, offset=0):
        """Returns a sequence of `(sorting_key, docnum)` pairs for the
        document numbers in `docnum`.
        
        If `limit` is `None`, this method associates every document number with
        a sorting key but does not sort them. If `limit` is not `None`, this
        method returns a sorted list of at most `limit` pairs.
        
        This method is useful for sorting and faceting documents in different
        readers, by associating the sort key with the document number.
        
        :param fieldname: either the name of a field, or a tuple of field names
            to specify a multi-level sort.
        :param docnums: a sequence of document numbers to key.
        :param limit: if not `None`, only keys the first/last N documents.
        :param reverse: if True, reverses the sort direction (when limit is not
            `None`).
        :param offset: a number to add to the docnums returned.
        """
        
        raise NotImplementedError
    
    def group_docs_by(self, fieldname, docnums, groups, counts=False, offset=0):
        """Returns a dictionary mapping field values to items with that value
        in the given field(s).
        
        :param fieldname: either the name of a field, or a tuple of field names
            to specify a multi-level sort.
        :param docnums: a sequence of document numbers to group.
        :param counts: if True, return a dictionary of doc counts, instead of
            a dictionary of lists of docnums.
        :param offset: a number to add to the docnums returned.
        """
        
        gen = self.key_docs_by(fieldname, docnums, None, offset=offset)
        
        if counts:
            for key, docnum in gen:
                if key not in groups:
                    groups[key] = 0
                groups[key] += 1
        else:
            for key, docnum in gen:
                if key not in groups:
                    groups[key] = []
                groups[key].append(docnum)
                
    def define_facets(self, name, doclists, save=False):
        """Tells the reader to remember a set of facets under the given name.
        
        :param name: the name to use for the set of facets.
        :param doclists: a dictionary mapping facet names to lists of document
            IDs.
        :param save: whether to save caches (if any) to some form of permanent
            storage (i.e. disk) if possible. This keyword may be used or
            ignored in the backend.
        """
        
        raise NotImplementedError
    
    def set_caching_policy(self, *args, **kwargs):
        """Sets the field caching policy for this reader.
        """
        
        pass
        

# Fake IndexReader class for empty indexes

class EmptyReader(IndexReader):
    def __init__(self, schema):
        self.schema = schema

    def __contains__(self, term):
        return False
    
    def __iter__(self):
        return iter([])
    
    def iter_from(self, fieldname, text):
        return iter([])
    
    def iter_field(self, fieldname):
        return iter([])
    
    def iter_prefix(self, fieldname):
        return iter([])
    
    def lexicon(self, fieldname):
        return iter([])
    
    def has_deletions(self):
        return False
    
    def is_deleted(self, docnum):
        return False
    
    def stored_fields(self, docnum):
        raise KeyError("No document number %s" % docnum)
    
    def all_stored_fields(self):
        return iter([])
    
    def doc_count_all(self):
        return 0
    
    def doc_count(self):
        return 0
    
    def doc_frequency(self, fieldname, text):
        return 0
    
    def frequency(self, fieldname, text):
        return 0
    
    def field_length(self, fieldname):
        return 0

    def doc_field_length(self, docnum, fieldname, default=0):
        return default

    def doc_field_lengths(self, docnum):
        raise ValueError

    def max_field_length(self, fieldname, default=0):
        return 0

    def postings(self, fieldname, text, scorer=None):
        raise TermNotFound("%s:%r" % (fieldname, text))

    def has_vector(self, docnum, fieldname):
        return False

    def vector(self, docnum, fieldname):
        raise KeyError("No document number %s" % docnum)

    def most_frequent_terms(self, fieldname, number=5, prefix=''):
        return iter([])

    def most_distinctive_terms(self, fieldname, number=5, prefix=None):
        return iter([])
    

# Multisegment reader class

class MultiReader(IndexReader):
    """Do not instantiate this object directly. Instead use Index.reader().
    """

    def is_atomic(self):
        return False

    def __init__(self, readers, generation=-1):
        self.readers = readers
        self._gen = generation
        self.schema = None
        if readers:
            self.schema = readers[0].schema
        
        self.doc_offsets = []
        self.base = 0
        for r in self.readers:
            self.doc_offsets.append(self.base)
            self.base += r.doc_count_all()
        
        self.is_closed = False

    def __contains__(self, term):
        return any(r.__contains__(term) for r in self.readers)

    def __iter__(self):
        return self._merge_iters([iter(r) for r in self.readers])

    def _document_segment(self, docnum):
        return max(0, bisect_right(self.doc_offsets, docnum) - 1)

    def _segment_and_docnum(self, docnum):
        segmentnum = self._document_segment(docnum)
        offset = self.doc_offsets[segmentnum]
        return segmentnum, docnum - offset

    def _merge_iters(self, iterlist):
        # Merge-sorts terms coming from a list of
        # term iterators (IndexReader.__iter__() or
        # IndexReader.iter_from()).

        # Fill in the list with the head term from each iterator.

        current = []
        for it in iterlist:
            fnum, text, docfreq, termcount = it.next()
            current.append((fnum, text, docfreq, termcount, it))
        heapify(current)

        # Number of active iterators
        active = len(current)
        while active > 0:
            # Peek at the first term in the sorted list
            fnum, text = current[0][:2]
            docfreq = 0
            termcount = 0

            # Add together all terms matching the first term in the list.
            while current and current[0][0] == fnum and current[0][1] == text:
                docfreq += current[0][2]
                termcount += current[0][3]
                it = current[0][4]
                try:
                    fn, t, df, tc = it.next()
                    heapreplace(current, (fn, t, df, tc, it))
                except StopIteration:
                    heappop(current)
                    active -= 1

            # Yield the term with the summed doc frequency and term count.
            yield (fnum, text, docfreq, termcount)

    def add_reader(self, reader):
        self.readers.append(reader)
        self.doc_offsets.append(self.base)
        self.base += reader.doc_count_all()

    def close(self):
        for d in self.readers:
            d.close()
        self.is_closed = True

    def generation(self):
        return self._gen

    def iter_from(self, fieldname, text):
        return self._merge_iters([r.iter_from(fieldname, text)
                                  for r in self.readers])

    # expand_prefix
    # all_terms
    # iter_field
    # iter_prefix
    # lexicon

    def has_deletions(self):
        return any(r.has_deletions() for r in self.readers)

    def is_deleted(self, docnum):
        segmentnum, segmentdoc = self._segment_and_docnum(docnum)
        return self.readers[segmentnum].is_deleted(segmentdoc)

    def stored_fields(self, docnum):
        segmentnum, segmentdoc = self._segment_and_docnum(docnum)
        return self.readers[segmentnum].stored_fields(segmentdoc)

    def all_stored_fields(self):
        for reader in self.readers:
            for result in reader.all_stored_fields():
                yield result

    def doc_count_all(self):
        return sum(dr.doc_count_all() for dr in self.readers)

    def doc_count(self):
        return sum(dr.doc_count() for dr in self.readers)

    def field_length(self, fieldname):
        return sum(dr.field_length(fieldname) for dr in self.readers)

    def doc_field_length(self, docnum, fieldname, default=0):
        segmentnum, segmentdoc = self._segment_and_docnum(docnum)
        reader = self.readers[segmentnum]
        return reader.doc_field_length(segmentdoc, fieldname, default=default)
    
    # max_field_length

    def first_id(self, fieldname, text):
        for i, r in enumerate(self.readers):
            try:
                id = r.first_id(fieldname, text)
            except (KeyError, TermNotFound):
                pass
            else:
                if id is None:
                    raise TermNotFound((fieldname, text))
                else:
                    return self.doc_offsets[i] + id
        
        raise TermNotFound((fieldname, text))

    def postings(self, fieldname, text, scorer=None):
        postreaders = []
        docoffsets = []
        term = (fieldname, text)
        
        for i, r in enumerate(self.readers):
            if term in r:
                offset = self.doc_offsets[i]
                
                # Get a posting reader for the term and add it to the list
                pr = r.postings(fieldname, text, scorer=scorer)
                postreaders.append(pr)
                docoffsets.append(offset)
        
        if not postreaders:
            raise TermNotFound(fieldname, text)
        else:
            return MultiMatcher(postreaders, docoffsets)

    def has_vector(self, docnum, fieldname):
        segmentnum, segmentdoc = self._segment_and_docnum(docnum)
        return self.readers[segmentnum].has_vector(segmentdoc, fieldname)

    def vector(self, docnum, fieldname):
        segmentnum, segmentdoc = self._segment_and_docnum(docnum)
        return self.readers[segmentnum].vector(segmentdoc, fieldname)

    def vector_as(self, astype, docnum, fieldname):
        segmentnum, segmentdoc = self._segment_and_docnum(docnum)
        return self.readers[segmentnum].vector_as(astype, segmentdoc, fieldname)

    def format(self, fieldname):
        for r in self.readers:
            fmt = r.format(fieldname)
            if fmt is not None:
                return fmt

    def vector_format(self, fieldname):
        for r in self.readers:
            vfmt = r.vector_format(fieldname)
            if vfmt is not None:
                return vfmt

    def doc_frequency(self, fieldname, text):
        return sum(r.doc_frequency(fieldname, text) for r in self.readers)

    def frequency(self, fieldname, text):
        return sum(r.frequency(fieldname, text) for r in self.readers)

    # most_frequent_terms
    # most_distinctive_terms
    
    def leaf_readers(self):
        return zip(self.readers, self.doc_offsets)

    def set_caching_policy(self, *args, **kwargs):
        for r in self.readers:
            r.set_caching_policy(*args, **kwargs)

        















