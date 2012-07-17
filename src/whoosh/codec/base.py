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

from whoosh import columns
from whoosh.compat import abstractmethod, izip, xrange
from whoosh.filedb.compound import CompoundStorage
from whoosh.system import emptybytes


# Base classes

class Codec(object):
    length_stats = True

    # Per document value writer

    @abstractmethod
    def per_document_writer(self, storage, segment):
        raise NotImplementedError

    # Inverted index writer

    @abstractmethod
    def field_writer(self, storage, segment):
        raise NotImplementedError

    # Index readers

    @abstractmethod
    def terms_reader(self, storage, segment):
        raise NotImplementedError

    @abstractmethod
    def per_document_reader(self, storage, segment):
        raise NotImplementedError

    def supports_graph(self):
        return False

    # Don't need to override this if supports_graph() return False
    def graph_reader(self, storage, segment):
        raise NotImplementedError

    # Segments and generations

    @abstractmethod
    def new_segment(self, storage, indexname):
        raise NotImplementedError


# Writer classes

class PerDocumentWriter(object):
    @abstractmethod
    def start_doc(self, docnum):
        raise NotImplementedError

    @abstractmethod
    def add_field(self, fieldname, fieldobj, value, length):
        raise NotImplementedError

    @abstractmethod
    def add_column_value(self, fieldname, columnobj, value):
        raise NotImplementedError

    @abstractmethod
    def add_vector_items(self, fieldname, fieldobj, items):
        raise NotImplementedError

    def add_vector_matcher(self, fieldname, fieldobj, vmatcher):
        def readitems():
            while vmatcher.is_active():
                text = vmatcher.id()
                weight = vmatcher.weight()
                valuestring = vmatcher.value()
                yield (text, weight, valuestring)
                vmatcher.next()
        self.add_vector_items(fieldname, fieldobj, readitems())

    def finish_doc(self):
        pass

    def close(self):
        pass


class FieldWriter(object):
    def add_postings(self, schema, lengths, items):
        start_field = self.start_field
        start_term = self.start_term
        add = self.add
        finish_term = self.finish_term
        finish_field = self.finish_field

        if lengths:
            dfl = lengths.doc_field_length
        else:
            dfl = lambda docnum, fieldname: 0

        lastfn = None
        lasttext = None
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

            if value is None:
                value = emptybytes
            add(docnum, weight, value, length)
        if lasttext is not None:
            finish_term()
            finish_field()

    @abstractmethod
    def start_field(self, fieldname, fieldobj):
        raise NotImplementedError

    @abstractmethod
    def start_term(self, text):
        raise NotImplementedError

    @abstractmethod
    def add(self, docnum, weight, vbytes, length):
        raise NotImplementedError

    def add_spell_word(self, fieldname, text):
        raise NotImplementedError

    @abstractmethod
    def finish_term(self):
        raise NotImplementedError

    def finish_field(self):
        pass

    def close(self):
        pass


# Reader classes

class TermsReader(object):
    @abstractmethod
    def __contains__(self, term):
        raise NotImplementedError

    @abstractmethod
    def terms(self):
        raise NotImplementedError

    @abstractmethod
    def terms_from(self, fieldname, prefix):
        raise NotImplementedError

    @abstractmethod
    def items(self):
        raise NotImplementedError

    @abstractmethod
    def items_from(self, fieldname, prefix):
        raise NotImplementedError

    @abstractmethod
    def term_info(self, fieldname, text):
        raise NotImplementedError

    @abstractmethod
    def frequency(self, fieldname, text):
        return self.term_info(fieldname, text).weight()

    @abstractmethod
    def doc_frequency(self, fieldname, text):
        return self.term_info(fieldname, text).doc_frequency()

    @abstractmethod
    def matcher(self, fieldname, text, format_, scorer=None):
        raise NotImplementedError

    def close(self):
        pass


# Per-doc value reader

class PerDocumentReader(object):
    def close(self):
        pass

    @abstractmethod
    def doc_count(self):
        raise NotImplementedError

    @abstractmethod
    def doc_count_all(self):
        raise NotImplementedError

    # Deletions

    @abstractmethod
    def has_deletions(self):
        raise NotImplementedError

    @abstractmethod
    def is_deleted(self, docnum):
        raise NotImplementedError

    @abstractmethod
    def deleted_docs(self):
        raise NotImplementedError

    def all_doc_ids(self):
        """Returns an iterator of all (undeleted) document IDs in the reader.
        """

        is_deleted = self.is_deleted
        return (docnum for docnum in xrange(self.doc_count_all())
                if not is_deleted(docnum))

    def iter_docs(self):
        for docnum in self.all_doc_ids():
            yield docnum, self.stored_fields(docnum)

    # Columns

    def supports_columns(self):
        return False

    def has_column(self, fieldname):
        return False

    # Don't need to override this if supports_columns() returns False
    def column_reader(self, fieldname, column):
        raise NotImplementedError

    # Lengths

    @abstractmethod
    def doc_field_length(self, docnum, fieldname, default=0):
        raise NotImplementedError

    @abstractmethod
    def field_length(self, fieldname):
        raise NotImplementedError

    @abstractmethod
    def min_field_length(self, fieldname):
        raise NotImplementedError

    @abstractmethod
    def max_field_length(self, fieldname):
        raise NotImplementedError

    # Vectors

    def has_vector(self, docnum, fieldname):
        return False

    # Don't need to override this if has_vector() always returns False
    def vector(self, docnum, fieldname, format_):
        raise NotImplementedError

    # Stored

    @abstractmethod
    def stored_fields(self, docnum):
        raise NotImplementedError

    def all_stored_fields(self):
        # Must yield stored fields for deleted documents too
        for docnum in xrange(self.doc_count_all()):
            yield self.stored_fields(docnum)


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

    def __init__(self, indexname):
        self.indexname = indexname
        self.segid = self._random_id()

    @classmethod
    def _random_id(cls, size=12):
        return "".join(random.choice(cls.IDCHARS) for _ in xrange(size))

    def __repr__(self):
        return "<%s %s>" % (self.__class__.__name__,
                            self.segment_id())

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

    # Abstract methods

    @abstractmethod
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

    @abstractmethod
    def deleted_count(self):
        """
        Returns the total number of deleted documents in this segment.
        """

        raise NotImplementedError

    @abstractmethod
    def delete_document(self, docnum, delete=True):
        """Deletes the given document number. The document is not actually
        removed from the index until it is optimized.

        :param docnum: The document number to delete.
        :param delete: If False, this undeletes a deleted document.
        """

        raise NotImplementedError

    @abstractmethod
    def is_deleted(self, docnum):
        """
        Returns True if the given document number is deleted.
        """

        raise NotImplementedError

    def should_assemble(self):
        return True


# Multi per doc reader

class MultiPerDocumentReader(PerDocumentReader):
    def __init__(self, readers, offset=0):
        self._readers = readers

        self._doc_offsets = []
        self._doccount = 0
        for pdr in readers:
            self._doc_offsets.append(self._doccount)
            self._doccount += pdr.doc_count_all()

        self.is_closed = False

    def close(self):
        for r in self._readers:
            r.close()
        self.is_closed = True

    def doc_count_all(self):
        return self._doccount

    def doc_count(self):
        total = 0
        for r in self._readers:
            total += r.doc_count()
        return total

    def _document_reader(self, docnum):
        return max(0, bisect_right(self._doc_offsets, docnum) - 1)

    def _reader_and_docnum(self, docnum):
        rnum = self._document_reader(docnum)
        offset = self._doc_offsets[rnum]
        return rnum, docnum - offset

    # Deletions

    def has_deletions(self):
        return any(r.has_deletions() for r in self._readers)

    def is_deleted(self, docnum):
        x, y = self._reader_and_docnum(docnum)
        return self._readers[x].is_deleted(y)

    def deleted_docs(self):
        for r, offset in izip(self._readers, self._doc_offsets):
            for docnum in r.deleted_docs():
                yield docnum + offset

    def all_doc_ids(self):
        for r, offset in izip(self._readers, self._doc_offsets):
            for docnum in r.all_doc_ids():
                yield docnum + offset

    # Columns

    def has_column(self, fieldname):
        return any(r.has_column(fieldname) for r in self._readers)

    def column_reader(self, fieldname, column):
        if not self.has_column(fieldname):
            raise ValueError("No column %r" % (fieldname,))

        default = column.default_value()
        colreaders = []
        for r in self._readers:
            if r.has_column(fieldname):
                cr = r.column_reader(fieldname, column)
            else:
                cr = columns.EmptyColumnReader(default, r.doc_count_all())
            colreaders.append(cr)

        if len(colreaders) == 1:
            return colreaders[0]
        else:
            return columns.MultiColumnReader(colreaders)

    # Lengths

    def doc_field_length(self, docnum, fieldname, default=0):
        x, y = self._reader_and_docnum(docnum)
        return self._readers[x].doc_field_length(y, fieldname, default)

    def field_length(self, fieldname):
        total = 0
        for r in self._readers:
            total += r.field_length(fieldname)
        return total

    def min_field_length(self):
        return min(r.min_field_length() for r in self._readers)

    def max_field_length(self):
        return max(r.max_field_length() for r in self._readers)


# Extended base classes

class PerDocWriterWithColumns(PerDocumentWriter):
    # Requires attributes _storage, _segment, _docnum

    @abstractmethod
    def _has_column(self, fieldname):
        raise NotImplementedError

    @abstractmethod
    def _create_column(self, fieldname, column):
        raise NotImplementedError

    @abstractmethod
    def _get_column(self, fieldname):
        raise NotImplementedError

    def add_column_value(self, fieldname, column, value):
        if not self._has_column(fieldname):
            self._create_column(fieldname, column)
        self._get_column(fieldname).add(self._docnum, value)


class CodecWithGraph(Codec):
    FST_EXT = ".fst"  # FSA/FST graph file

    def supports_graph(self):
        return True

    def graph_reader(self, storage, segment):
        from whoosh.fst import GraphReader
        from whoosh.reading import NoGraphError

        filename = segment.make_filename(self.FST_EXT)
        if not storage.file_exists(filename):
            raise NoGraphError
        return GraphReader(storage.open_file(filename))


class FieldWriterWithGraph(FieldWriter):
    # Requires attributes _storage, _segment

    FST_EXT = CodecWithGraph.FST_EXT

    def _prep_graph(self):
        from whoosh.fst import GraphWriter

        gf = self._segment.create_file(self._storage, self.FST_EXT)
        self._gwriter = GraphWriter(gf)

    def _start_graph_field(self, fieldname, fieldobj):
        spelling = fieldobj.spelling
        separate = fieldobj.separate_spelling()
        self._needs_graph = spelling or separate
        self._auto_graph = spelling and not separate

        if self._needs_graph:
            if not hasattr(self, "_gwriter") or self._gwriter is None:
                self._prep_graph()
            self._gwriter.start_field(fieldname)

    def _insert_graph_token(self, key):
        if self._auto_graph:
            self.add_spell_word(self._fieldname, key)

    def add_spell_word(self, fieldname, token):
        assert fieldname == self._fieldname
        self._gwriter.insert(token)

    def _finish_graph_field(self):
        if self._needs_graph:
            self._gwriter.finish_field()

    def _close_graph(self):
        if hasattr(self, "_gwriter") and self._gwriter:
            self._gwriter.close()










