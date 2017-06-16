import logging
import os
import pickle
from tempfile import mkstemp
from typing import Any

from whoosh import fields
from whoosh.ifaces import codecs, storage
from whoosh.writing.writing import IndexWriter, SegmentWriter
from whoosh.util import now

logger = logging.getLogger(__name__)


class SpoolingWriter(SegmentWriter):
    def __init__(self, segment):
        self.segment = segment
        self.count = 0

        fd, self.filepath = mkstemp(suffix=".pickle", prefix="multi")
        self._tempfile = os.fdopen(fd, "wb")

        self._arglist = []

    def start_document(self):
        self._arglist = []

    def index_field(self, fieldname: str, field: 'fields.FieldType',
                    value: Any, stored_val: Any, boost=1.0):
        self._arglist.append((fieldname, value, stored_val, boost))

    def finish_document(self):
        pickle.dump(self._arglist, self._tempfile, -1)

    def finish_segment(self):
        self._tempfile.close()
        return self.filepath, self.segment


class MultiWriter(IndexWriter):
    def __init__(self, *args, **kwargs):
        super(MultiWriter, self).__init__(*args, **kwargs)
        assert self.executor

    def _start_new_segment(self):
        segment = self.codec.new_segment(self.session)
        self.segwriter = SpoolingWriter(segment)

    def _implement_flush(self, merge, optimize, expunge_deleted):
        count = self.segwriter.count
        filepath, newsegment = self.segwriter.finish_segment()

        logger.info("Submitting parallel segment flush of %r to %r",
                    newsegment, self.executor)
        store = self.session.store
        if store.supports_multiproc_writing():
            # The storage supports recursive locks, so use a version of the
            # merge function that takes a recursive lock
            args = (
                batch_r_index,
                filepath, count, self.codec, store, self.schema, newsegment,
                self.session.read_key(), self.session.indexname
            )
        else:
            # We don't support multi-processing, but if this is a threading
            # executor we will try to pass the session between threads
            args = (
                batch_index,
                filepath, count, self.codec, self.session, self.schema,
                newsegment
            )

        future = self.executor.submit(*args)

        # Add a callback to complete adding the segment when the future finishes
        def multi_flush_callback(f):
            self.seglist.add_segment(f.result())
            self._maybe_merge(merge, optimize, expunge_deleted)
        future.add_done_callback(multi_flush_callback)


# Helper functions for indexing from a batch file in a different thread/process

def batch_r_index(batch_filename: str,
                  count: int,
                  codec: 'codecs.Codec',
                  store: 'storage.Storage',
                  schema: 'fields.Schema',
                  newsegment: 'codecs.Segment',
                  key: int,
                  indexname: str
                  ) -> 'codecs.Segment':
    session = store.recursive_write_open(key, indexname)
    return batch_index(batch_filename, count, codec, session, schema,
                       newsegment)


def batch_index(batch_filename: str,
                count: int,
                codec: 'codecs.Codec',
                session: 'storage.Session',
                schema: 'fields.Schema',
                newsegment: 'codecs.Segment',
                ) -> 'codecs.Segment':
    logger.info("Batching indexing file %s to %r", batch_filename, newsegment)
    t = now()

    segwriter = SegmentWriter(codec, session, newsegment, schema)

    # The batch file contains a series of pickled lists of arguments to
    # SegmentWriter.index_field
    with open(batch_filename, "rb") as f:
        for _ in range(count):
            arg_list = pickle.load(f)
            segwriter.start_document()
            for fieldname, value, stored_val, boost in arg_list:
                field = schema[fieldname]
                segwriter.index_field(fieldname, field, value, stored_val,
                                      boost)
            segwriter.finish_document()

    # Get the finished segment from the segment writer
    newsegment = segwriter.finish_segment()

    # Delete the used up batch file
    os.remove(batch_filename)

    logger.info("Batch indexed %s to %r in %0.06f s",
                batch_filename, newsegment, now() - t)

    return newsegment



