#===============================================================================
# Copyright 2007 Matt Chaput
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#===============================================================================

from array import array
from collections import defaultdict

from whoosh.fields import UnknownFieldError
from whoosh.store import LockError
from whoosh.writing import IndexWriter
from whoosh.filedb import postpool
from whoosh.support.filelock import try_for
from whoosh.filedb.fileindex import SegmentDeletionMixin, Segment, SegmentSet
from whoosh.filedb.filepostings import FilePostingWriter
from whoosh.filedb.filetables import (FileTableWriter, FileListWriter,
                                      FileRecordWriter, encode_termkey,
                                      encode_vectorkey, encode_terminfo,
                                      enpickle, packint)
from whoosh.util import fib


DOCLENGTH_TYPE = "H"
DOCLENGTH_LIMIT = 2 ** 16 - 1


# Merge policies

# A merge policy is a callable that takes the Index object, the SegmentWriter
# object, and the current SegmentSet (not including the segment being written),
# and returns an updated SegmentSet (not including the segment being written).

def NO_MERGE(ix, writer, segments):
    """This policy does not merge any existing segments.
    """
    return segments


def MERGE_SMALL(ix, writer, segments):
    """This policy merges small segments, where "small" is defined using a
    heuristic based on the fibonacci sequence.
    """

    from whoosh.filedb.filereading import SegmentReader
    newsegments = SegmentSet()
    sorted_segment_list = sorted((s.doc_count_all(), s) for s in segments)
    total_docs = 0
    for i, (count, seg) in enumerate(sorted_segment_list):
        if count > 0:
            total_docs += count
            if total_docs < fib(i + 5):
                writer.add_reader(SegmentReader(ix.storage, seg, ix.schema))
            else:
                newsegments.append(seg)
    return newsegments


def OPTIMIZE(ix, writer, segments):
    """This policy merges all existing segments.
    """

    from whoosh.filedb.filereading import SegmentReader
    for seg in segments:
        writer.add_reader(SegmentReader(ix.storage, seg, ix.schema))
    return SegmentSet()


# Convenience functions

def create_terms(storage, segment):
    termfile = storage.create_file(segment.term_filename)
    return FileTableWriter(termfile,
                           keycoder=encode_termkey,
                           valuecoder=encode_terminfo)

def create_storedfields(storage, segment):
    listfile = storage.create_file(segment.docs_filename)
    return FileListWriter(listfile, valuecoder=enpickle)

def create_vectors(storage, segment):
    vectorfile = storage.create_file(segment.vector_filename)
    return FileTableWriter(vectorfile, keycoder=encode_vectorkey,
                           valuecoder=packint)

def create_doclengths(storage, segment, fieldcount):
    recordformat = "!" + DOCLENGTH_TYPE * fieldcount
    recordfile = storage.create_file(segment.doclen_filename)
    return FileRecordWriter(recordfile, recordformat)


# Writing classes

class FileIndexWriter(SegmentDeletionMixin, IndexWriter):
    # This class is mostly a shell for SegmentWriter. It exists to handle
    # multiple SegmentWriters during merging/optimizing.

    def __init__(self, ix, postlimit=32 * 1024 * 1024, blocklimit=128,
                 timeout=0.0, delay=0.1):
        """
        :param ix: the Index object you want to write to.
        :param postlimit: Essentially controls the maximum amount of memory the
            indexer uses at a time, in bytes (the actual amount of memory used
            by the Python process will be much larger because of other
            overhead). The default (32MB) is a bit small. You may want to
            increase this value for very large collections, e.g.
            ``postlimit=256*1024*1024``.
        """

        self.lock = ix.storage.lock(ix.indexname + "_LOCK")
        if not try_for(self.lock.acquire, timeout=timeout, delay=delay):
            raise LockError("Index %s is already locked for writing")

        self.index = ix
        self.segments = ix.segments.copy()
        self.postlimit = postlimit
        self.blocklimit = blocklimit
        self._segment_writer = None
        self._searcher = ix.searcher()

    def _finish(self):
        self._close_reader()
        self.lock.release()
        self._segment_writer = None

    def segment_writer(self):
        """Returns the underlying SegmentWriter object.
        """

        if not self._segment_writer:
            self._segment_writer = SegmentWriter(self.index, self.postlimit,
                                                 self.blocklimit)
        return self._segment_writer

    def add_document(self, **fields):
        self.segment_writer().add_document(fields)

    def commit(self, mergetype=MERGE_SMALL):
        """Finishes writing and unlocks the index.
        
        :param mergetype: How to merge existing segments. One of
            :class:`whoosh.filedb.filewriting.NO_MERGE`,
            :class:`whoosh.filedb.filewriting.MERGE_SMALL`,
            or :class:`whoosh.filedb.filewriting.OPTIMIZE`.
        """

        self._close_reader()
        if self._segment_writer or mergetype is OPTIMIZE:
            self._merge_segments(mergetype)
        self.index.commit(self.segments)
        self._finish()

    def cancel(self):
        if self._segment_writer:
            self._segment_writer._close_all()
        self._finish()

    def _merge_segments(self, mergetype):
        sw = self.segment_writer()
        new_segments = mergetype(self.index, sw, self.segments)
        sw.close()
        new_segments.append(sw.segment())
        self.segments = new_segments


class SegmentWriter(object):
    """Do not instantiate this object directly; it is created by the
    IndexWriter object.
    
    Handles the actual writing of new documents to the index: writes stored
    fields, handles the posting pool, and writes out the term index.
    """

    def __init__(self, ix, postlimit, blocklimit, name=None):
        """
        :param ix: the Index object in which to write the new segment.
        :param postlimit: the maximum size for a run in the posting pool.
        :param blocklimit: the maximum number of postings in a posting block.
        :param name: the name of the segment.
        """

        self.index = ix
        self.schema = ix.schema
        self.storage = storage = ix.storage
        self.name = name or ix._next_segment_name()

        self.max_doc = 0

        self.pool = postpool.PostingPool(postlimit)

        # Create mappings of field numbers to the position of that field in the
        # lists of scorable and stored fields. For example, consider a schema
        # with fields (A, B, C, D, E, F). If B, D, and E are scorable, then the
        # list of scorable fields is (B, D, E). The _scorable_to_pos dictionary
        # would then map B -> 0, D -> 1, and E -> 2.
        self._scorable_to_pos = dict((fnum, i)
                                     for i, fnum
                                     in enumerate(self.schema.scorable_fields()))
        self._stored_to_pos = dict((fnum, i)
                                   for i, fnum
                                   in enumerate(self.schema.stored_fields()))

        # Create a temporary segment object just so we can access its
        # *_filename attributes (so if we want to change the naming convention,
        # we only have to do it in one place).
        tempseg = Segment(self.name, 0, 0, None)
        self.termtable = create_terms(storage, tempseg)
        self.docslist = create_storedfields(storage, tempseg)
        self.doclengths = None
        if self.schema.scorable_fields():
            self.doclengths = create_doclengths(storage, tempseg, len(self._scorable_to_pos))

        postfile = storage.create_file(tempseg.posts_filename)
        self.postwriter = FilePostingWriter(postfile, blocklimit=blocklimit)

        self.vectortable = None
        if self.schema.has_vectored_fields():
            # Table associating document fields with (postoffset, postcount)
            self.vectortable = create_vectors(storage, tempseg)
            vpostfile = storage.create_file(tempseg.vectorposts_filename)
            self.vpostwriter = FilePostingWriter(vpostfile, stringids=True)

        # Keep track of the total number of tokens (across all docs)
        # in each field
        self.field_length_totals = defaultdict(int)

    def segment(self):
        """Returns an index.Segment object for the segment being written."""
        return Segment(self.name, self.max_doc, dict(self.field_length_totals))

    def _close_all(self):
        self.termtable.close()
        self.postwriter.close()
        self.docslist.close()

        if self.doclengths:
            self.doclengths.close()

        if self.vectortable:
            self.vectortable.close()
            self.vpostwriter.close()

    def close(self):
        """Finishes writing the segment (flushes the posting pool out to disk)
        and closes all open files.
        """

        self._flush_pool()
        self._close_all()

    def add_reader(self, reader):
        """Adds the contents of another segment to this one. This is used to
        merge existing segments into the new one before deleting them.
        
        :param ix: The index.Index object containing the segment to merge.
        :param segment: The index.Segment object to merge into this one.
        """

        start_doc = self.max_doc
        has_deletions = reader.has_deletions()

        if has_deletions:
            doc_map = {}

        schema = self.schema
        name2num = schema.name_to_number
        stored_to_pos = self._stored_to_pos

        def storedkeyhelper(item):
            return stored_to_pos[name2num(item[0])]

        # Merge document info
        docnum = 0
        vectored_fieldnums = schema.vectored_fields()
        for docnum in xrange(reader.doc_count_all()):
            if not reader.is_deleted(docnum):
                # Copy the stored fields and field lengths from the reader
                # into this segment
                storeditems = reader.stored_fields(docnum).items()
                storedvalues = [v for k, v
                                in sorted(storeditems, key=storedkeyhelper)]
                self._add_doc_data(storedvalues,
                                   reader.doc_field_lengths(docnum))

                if has_deletions:
                    doc_map[docnum] = self.max_doc

                # Copy term vectors
                for fieldnum in vectored_fieldnums:
                    if reader.has_vector(docnum, fieldnum):
                        self._add_vector(fieldnum,
                                         reader.vector(docnum, fieldnum).items())

                self.max_doc += 1

        # Add field length totals
        for fieldnum in schema.scorable_fields():
            self.field_length_totals[fieldnum] += reader.field_length(fieldnum)

        # Merge terms
        current_fieldnum = None
        decoder = None
        for fieldnum, text, _, _ in reader:
            if fieldnum != current_fieldnum:
                current_fieldnum = fieldnum
                decoder = schema[fieldnum].format.decode_frequency

            postreader = reader.postings(fieldnum, text)
            for docnum, valuestring in postreader.all_items():
                if has_deletions:
                    newdoc = doc_map[docnum]
                else:
                    newdoc = start_doc + docnum

                # TODO: Is there a faster way to do this?
                freq = decoder(valuestring)
                self.pool.add_posting(fieldnum, text, newdoc, freq, valuestring)

    def add_document(self, fields):
        scorable_to_pos = self._scorable_to_pos
        stored_to_pos = self._stored_to_pos
        schema = self.schema

        # Sort the keys by their order in the schema
        fieldnames = [name for name in fields.keys()
                      if not name.startswith("_")]
        fieldnames.sort(key=schema.name_to_number)

        # Check if the caller gave us a bogus field
        for name in fieldnames:
            if name not in schema:
                raise UnknownFieldError("There is no field named %r" % name)

        # Create an array of counters to record the length of each field
        fieldlengths = array(DOCLENGTH_TYPE, [0] * len(scorable_to_pos))

        # Create a list (initially a list of Nones) in which we will put stored
        # field values as we get them. Why isn't this an empty list that we
        # append to? Because if the caller doesn't supply a value for a stored
        # field, we don't want to have a list in the wrong order/of the wrong
        # length.
        storedvalues = [None] * len(stored_to_pos)

        for name in fieldnames:
            value = fields.get(name)
            if value:
                fieldnum = schema.name_to_number(name)
                field = schema.field_by_number(fieldnum)

                # If the field is indexed, add the words in the value to the
                # index
                if field.indexed:
                    # Count of all terms in the value
                    count = 0
                    # Count of UNIQUE terms in the value
                    unique = 0

                    # TODO: Method for adding progressive field values, ie
                    # setting start_pos/start_char?
                    for w, freq, valuestring in field.index(value):
                        #assert w != ""
                        self.pool.add_posting(fieldnum, w, self.max_doc, freq,
                                              valuestring)
                        count += freq
                        unique += 1

                    if field.scorable:
                        # Add the term count to the total for this field
                        self.field_length_totals[fieldnum] += count
                        # Set the term count to the per-document field length
                        pos = scorable_to_pos[fieldnum]
                        fieldlengths[pos] = min(count, DOCLENGTH_LIMIT)

                # If the field is vectored, add the words in the value to the
                # vector table
                vector = field.vector
                if vector:
                    # TODO: Method for adding progressive field values, ie
                    # setting start_pos/start_char?
                    vlist = sorted((w, valuestring) for w, freq, valuestring
                                   in vector.word_values(value, mode="index"))
                    self._add_vector(fieldnum, vlist)

                # If the field is stored, put the value in storedvalues
                if field.stored:
                    # Caller can override the stored value by including a key
                    # _stored_<fieldname>
                    storedname = "_stored_" + name
                    if storedname in fields:
                        stored_value = fields[storedname]
                    else :
                        stored_value = value

                    storedvalues[stored_to_pos[fieldnum]] = stored_value

        self._add_doc_data(storedvalues, fieldlengths)
        self.max_doc += 1

    def _add_terms(self):
        pass

    def _add_doc_data(self, storedvalues, fieldlengths):
        self.docslist.append(storedvalues)
        if self.doclengths:
            self.doclengths.append(fieldlengths)

    def _add_vector(self, fieldnum, vlist):
        vpostwriter = self.vpostwriter
        vformat = self.schema[fieldnum].vector

        offset = vpostwriter.start(vformat)
        for text, valuestring in vlist:
            assert isinstance(text, unicode), "%r is not unicode" % text
            vpostwriter.write(text, valuestring)
        vpostwriter.finish()

        self.vectortable.add((self.max_doc, fieldnum), offset)

    def _flush_pool(self):
        # This method pulls postings out of the posting pool (built up as
        # documents are added) and writes them to the posting file. Each time
        # it encounters a posting for a new term, it writes the previous term
        # to the term index (by waiting to write the term entry, we can easily
        # count the document frequency and sum the terms by looking at the
        # postings).

        termtable = self.termtable
        postwriter = self.postwriter
        schema = self.schema

        current_fieldnum = None # Field number of the current term
        current_text = None # Text of the current term
        first = True
        current_freq = 0
        offset = None

        # Loop through the postings in the pool. Postings always come out of
        # the pool in (field number, lexical) order.
        for fieldnum, text, docnum, freq, valuestring in self.pool:
            # Is this the first time through, or is this a new term?
            if first or fieldnum > current_fieldnum or text > current_text:
                if first:
                    first = False
                else:
                    # This is a new term, so finish the postings and add the
                    # term to the term table
                    postcount = postwriter.finish()
                    termtable.add((current_fieldnum, current_text),
                                  (current_freq, offset, postcount))

                # Reset the post writer and the term variables
                current_fieldnum = fieldnum
                current_text = text
                current_freq = 0
                offset = postwriter.start(schema[fieldnum].format)

            elif (fieldnum < current_fieldnum
                  or (fieldnum == current_fieldnum and text < current_text)):
                # This should never happen!
                raise Exception("Postings are out of order: %s:%s .. %s:%s" %
                                (current_fieldnum, current_text, fieldnum, text))

            # Write a posting for this occurrence of the current term
            current_freq += freq
            postwriter.write(docnum, valuestring)

        # If there are still "uncommitted" postings at the end, finish them off
        if not first:
            postcount = postwriter.finish()
            termtable.add((current_fieldnum, current_text),
                          (current_freq, offset, postcount))






