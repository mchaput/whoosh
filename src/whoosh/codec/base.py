# Copyright 2011 Matt Chaput. All rights reserved.
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

"""
This module contains base classes/interfaces for "codec" objects.
"""

import random
from bisect import bisect_right

from whoosh.compat import xrange
from whoosh.filedb.compound import CompoundStorage
from whoosh.matching import Matcher
from whoosh.spans import Span


# Base classes

class Codec(object):
    # Per document value writer
    def per_document_writer(self, storage, segment):
        raise NotImplementedError

    # Inverted index writer
    def field_writer(self, storage, segment):
        raise NotImplementedError

    # Index readers

    def terms_reader(self, storage, segment):
        raise NotImplementedError

    def lengths_reader(self, storage, segment):
        raise NotImplementedError

    def vector_reader(self, storage, segment):
        raise NotImplementedError

    def stored_fields_reader(self, storage, segment):
        raise NotImplementedError

    def graph_reader(self, storage, segment):
        raise NotImplementedError

    # Columns

    def supports_columns(self):
        return False

    def columns_writer(self, storage, segment):
        raise NotImplementedError

    def columns_reader(self, storage, segment):
        raise NotImplementedError

    # Segments and generations

    def new_segment(self, storage, indexname):
        raise NotImplementedError

    def commit_toc(self, storage, indexname, schema, segments, generation):
        raise NotImplementedError


# Writer classes

class PerDocumentWriter(object):
    def start_doc(self, docnum):
        raise NotImplementedError

    def add_field(self, fieldname, fieldobj, value, length):
        raise NotImplementedError

    def add_vector_items(self, fieldname, fieldobj, items):
        raise NotImplementedError

    def add_vector_matcher(self, fieldname, fieldobj, vmatcher):
        def readitems():
            while vmatcher.is_active():
                text = vmatcher.id()
                weight = vmatcher.weight()
                valuestring = vmatcher.value()
                yield (text, None, weight, valuestring)
                vmatcher.next()
        self.add_vector_items(fieldname, fieldobj, readitems())

    def finish_doc(self):
        pass


class FieldWriter(object):
    def add_postings(self, schema, lengths, items):
        start_field = self.start_field
        start_term = self.start_term
        add = self.add
        finish_term = self.finish_term
        finish_field = self.finish_field

        # items = (fieldname, text, docnum, weight, valuestring) ...
        lastfn = None
        lasttext = None
        dfl = lengths.doc_field_length
        for fieldname, token, docnum, weight, value in items:
            # Items where docnum is None indicate words that should be added
            # to the spelling graph
            if docnum is None and (fieldname != lastfn or token != lasttext):
                # TODO: how to decode the token bytes?
                self.add_spell_word(fieldname, token.decode("utf8"))
                lastfn = fieldname
                lasttext = token
                continue

            # This comparison is so convoluted because Python 3 removed the
            # ability to compare a string to None
            if ((lastfn is not None and fieldname < lastfn)
                or (fieldname == lastfn and lasttext is not None
                    and token < lasttext)):
                raise Exception("Postings are out of order: %r:%s .. %r:%s" %
                                (lastfn, lasttext, fieldname, token))
            if fieldname != lastfn or token != lasttext:
                if lasttext is not None:
                    finish_term()
                if fieldname != lastfn:
                    if lastfn is not None:
                        finish_field()
                    start_field(fieldname, schema[fieldname])
                    lastfn = fieldname
                start_term(token)
                lasttext = token
            length = dfl(docnum, fieldname)
            add(docnum, weight, value, length)
        if lasttext is not None:
            finish_term()
            finish_field()

    def start_field(self, fieldname, fieldobj):
        raise NotImplementedError

    def start_term(self, text):
        raise NotImplementedError

    def add(self, docnum, weight, valuestring, length):
        raise NotImplementedError

    def add_spell_word(self, fieldname, text):
        raise NotImplementedError

    def finish_term(self):
        raise NotImplementedError

    def finish_field(self):
        pass

    def close(self):
        pass


# Reader classes

class TermsReader(object):
    def __contains__(self, term):
        raise NotImplementedError

    def terms(self):
        raise NotImplementedError

    def terms_from(self, fieldname, prefix):
        raise NotImplementedError

    def items(self):
        raise NotImplementedError

    def items_from(self, fieldname, prefix):
        raise NotImplementedError

    def terminfo(self, fieldname, text):
        raise NotImplementedError

    def frequency(self, fieldname, text):
        return self.terminfo(fieldname, text).weight()

    def doc_frequency(self, fieldname, text):
        return self.terminfo(fieldname, text).doc_frequency()

    def matcher(self, fieldname, text, format_, scorer=None):
        raise NotImplementedError

    def close(self):
        pass


class VectorReader(object):
    def __contains__(self, key):
        raise NotImplementedError

    def matcher(self, docnum, fieldname, format_):
        raise NotImplementedError


# Lengths

class LengthsReader(object):
    def doc_count_all(self):
        raise NotImplementedError

    def doc_field_length(self, docnum, fieldname, default=0):
        raise NotImplementedError

    def field_length(self, fieldname):
        raise NotImplementedError

    def min_field_length(self, fieldname):
        raise NotImplementedError

    def max_field_length(self, fieldname):
        raise NotImplementedError

    def close(self):
        pass


class MultiLengths(LengthsReader):
    def __init__(self, lengths, offset=0):
        self.lengths = []
        self.doc_offsets = []
        self._count = 0
        for lr in lengths:
            if lr.doc_count_all():
                self.lengths.append(lr)
                self.doc_offsets.append(self._count)
                self._count += lr.doc_count_all()
        self.is_closed = False

    def _document_reader(self, docnum):
        return max(0, bisect_right(self.doc_offsets, docnum) - 1)

    def _reader_and_docnum(self, docnum):
        lnum = self._document_reader(docnum)
        offset = self.doc_offsets[lnum]
        return lnum, docnum - offset

    def doc_count_all(self):
        return self._count

    def doc_field_length(self, docnum, fieldname, default=0):
        x, y = self._reader_and_docnum(docnum)
        return self.lengths[x].doc_field_length(y, fieldname, default=default)

    def min_field_length(self):
        return min(lr.min_field_length() for lr in self.lengths)

    def max_field_length(self):
        return max(lr.max_field_length() for lr in self.lengths)

    def close(self):
        for lr in self.lengths:
            lr.close()
        self.is_closed = True


# Stored fields

class StoredFieldsReader(object):
    def __iter__(self):
        raise NotImplementedError

    def __getitem__(self, docnum):
        raise NotImplementedError

    def cell(self, docnum, fieldname):
        fielddict = self.get(docnum)
        return fielddict.get(fieldname)

    def column(self, fieldname):
        for fielddict in self:
            yield fielddict.get(fieldname)

    def close(self):
        pass


# File posting matcher middleware

class FilePostingMatcher(Matcher):
    # Subclasses need to set
    #   self._term -- (fieldname, text) or None
    #   self.scorer -- a Scorer object or None
    #   self.format -- Format object for the posting values

    def __repr__(self):
        return "%s(%r, %r, %s)" % (self.__class__.__name__, str(self.postfile),
                                   self.term(), self.is_active())

    def term(self):
        return self._term

    def items_as(self, astype):
        decoder = self.format.decoder(astype)
        for id, value in self.all_items():
            yield (id, decoder(value))

    def supports(self, astype):
        return self.format.supports(astype)

    def value_as(self, astype):
        decoder = self.format.decoder(astype)
        return decoder(self.value())

    def spans(self):
        if self.supports("characters"):
            return [Span(pos, startchar=startchar, endchar=endchar)
                    for pos, startchar, endchar in self.value_as("characters")]
        elif self.supports("positions"):
            return [Span(pos) for pos in self.value_as("positions")]
        else:
            raise Exception("Field does not support positions (%r)"
                            % self._term)

    def supports_block_quality(self):
        return self.scorer and self.scorer.supports_block_quality()

    def max_quality(self):
        return self.scorer.max_quality

    def block_quality(self):
        return self.scorer.block_quality(self)


# Columns

class ColumnsWriter(object):
    def __init__(self, storage, segment):
        self._storage = storage
        self._segment = segment

    def add_field(self, fieldname, column):
        raise NotImplementedError

    def has_field(self, fieldname):
        raise NotImplementedError

    def add_doc_value(self, docnum, fieldname, value):
        raise NotImplementedError

    def close(self):
        pass


class ColumnsReader(object):
    def __init__(self, storage, segment):
        self._storage = storage
        self._segment = segment

    def has_column(self, fieldname):
        return False

    def reader(self, fieldname, column):
        raise NotImplementedError

    def close(self):
        pass


# Segment base class

class Segment(object):
    """Do not instantiate this object directly. It is used by the Index object
    to hold information about a segment. A list of objects of this class are
    pickled as part of the TOC file.
    
    The TOC file stores a minimal amount of information -- mostly a list of
    Segment objects. Segments are the real reverse indexes. Having multiple
    segments allows quick incremental indexing: just create a new segment for
    the new documents, and have the index overlay the new segment over previous
    ones for purposes of reading/search. "Optimizing" the index combines the
    contents of existing segments into one (removing any deleted documents
    along the way).
    """

    # These must be valid separate characters in CASE-INSENSTIVE filenames
    IDCHARS = "0123456789abcdefghijklmnopqrstuvwxyz"
    # Extension for compound segment files
    COMPOUND_EXT = ".seg"

    # self.indexname
    # self.segid

    @classmethod
    def _random_id(cls, size=12):
        return "".join(random.choice(cls.IDCHARS) for _ in xrange(size))

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__,
                            getattr(self, "segid", ""))

    def codec(self):
        raise NotImplementedError

    def segment_id(self):
        if hasattr(self, "name"):
            # Old segment class
            return self.name
        else:
            return "%s_%s" % (self.indexname, self.segid)

    def is_compound(self):
        if not hasattr(self, "compound"):
            return False
        return self.compound

    # File convenience methods

    def make_filename(self, ext):
        return "%s%s" % (self.segment_id(), ext)

    def list_files(self, storage):
        prefix = "%s." % self.segment_id()
        return [name for name in storage.list() if name.startswith(prefix)]

    def create_file(self, storage, ext, **kwargs):
        """Convenience method to create a new file in the given storage named
        with this segment's ID and the given extension. Any keyword arguments
        are passed to the storage's create_file method.
        """

        fname = self.make_filename(ext)
        return storage.create_file(fname, **kwargs)

    def open_file(self, storage, ext, **kwargs):
        """Convenience method to open a file in the given storage named with
        this segment's ID and the given extension. Any keyword arguments are
        passed to the storage's open_file method.
        """

        fname = self.make_filename(ext)
        return storage.open_file(fname, **kwargs)

    def create_compound_file(self, storage):
        segfiles = self.list_files(storage)
        assert not any(name.endswith(self.COMPOUND_EXT) for name in segfiles)
        cfile = self.create_file(storage, self.COMPOUND_EXT)
        CompoundStorage.assemble(cfile, storage, segfiles)
        for name in segfiles:
            storage.delete_file(name)

    def open_compound_file(self, storage):
        name = self.make_filename(self.COMPOUND_EXT)
        dbfile = storage.open_file(name)
        return CompoundStorage(dbfile, use_mmap=storage.supports_mmap)

    # Abstract methods dealing with document counts and deletions

    def doc_count_all(self):
        """
        Returns the total number of documents, DELETED OR UNDELETED, in this
        segment.
        """

        raise NotImplementedError

    def doc_count(self):
        """
        Returns the number of (undeleted) documents in this segment.
        """

        return self.doc_count_all() - self.deleted_count()

    def has_deletions(self):
        """
        Returns True if any documents in this segment are deleted.
        """

        return self.deleted_count() > 0

    def deleted_count(self):
        """
        Returns the total number of deleted documents in this segment.
        """

        raise NotImplementedError

    def delete_document(self, docnum, delete=True):
        """Deletes the given document number. The document is not actually
        removed from the index until it is optimized.

        :param docnum: The document number to delete.
        :param delete: If False, this undeletes a deleted document.
        """

        raise NotImplementedError

    def is_deleted(self, docnum):
        """
        Returns True if the given document number is deleted.
        """

        raise NotImplementedError













