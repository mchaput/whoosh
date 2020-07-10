# Copyright 2019 Matt Chaput. All rights reserved.
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

import logging
import sys
import typing
from typing import Any, Dict, List, Optional, Sequence

from whoosh.util import now

# Typing imports
if typing.TYPE_CHECKING:
    from whoosh import fields, index, query
    from whoosh.codec import codecs


logger = logging.getLogger(__name__)


class Reporter:
    def __init__(self):
        self.index = None  # type: index.Index
        self.schema = None  # type: fields.Schema
        self.unique_field = None  # type: Optional[str]
        self.doc_count = 0  # type: int
        self.finished = False
        self._starttime = 0.0
        self._endtime = 0.0
        self._unames = []

    # Public interface -- these are the methods subclasses should override to
    # get desired behavior

    def start_indexing(self):
        pass

    def cleared_segments(self):
        pass

    def delete_by_query(self, q: 'query.Query'):
        pass

    def start_new_segment(self, segments: 'List[codecs.Segment]',
                          segment: 'codecs.Segment'):
        pass

    def start_document(self, key: Any):
        pass

    def indexing_field(self, fieldname: str, fieldobj: 'fields.FieldType',
                       value: Any):
        pass

    def finish_document(self):
        pass

    def finish_segment(self, segments: 'List[codecs.Segment]',
                       segment: 'codecs.Segment'):
        pass

    def start_merge(self, merge_id: str, merging: 'List[codecs.Segment]',
                    new_segment_id: str):
        pass

    def finish_merge(self, merge_id: str, merged: 'List[codecs.Segment]',
                     new_segment: 'codecs.Segment'):
        pass

    def committing(self, optimized=False):
        pass

    def finish_indexing(self, segments: 'List[codecs.Segment]'):
        pass

    # Helper methods

    def runtime(self):
        if self.finished:
            return self._endtime - self._starttime
        else:
            return now() - self._starttime

    # Internal interface -- writer calls these methods, which do any necessary
    # bookkeeping and then call the equivalent public method

    def _start_indexing(self, ix: 'index.Index', unique_field: str):
        self.index = ix
        self.schema = ix.schema
        self.unique_field = unique_field
        self.doc_count = 0
        self.finished = False

        self._starttime = now()
        self._endtime = None
        self._unames = [name for name, fieldobj in self.schema.items()
                        if fieldobj.unique]

        self.start_indexing()

    _cleared_segments = cleared_segments

    _delete_by_query = delete_by_query

    _start_new_segment = start_new_segment

    def _start_document(self, kwargs: Dict[str, Any]):
        key = None
        if self.unique_field in kwargs:
            key = kwargs[self.unique_field]
        else:
            for uname in self._unames:
                if uname in kwargs:
                    key = kwargs[uname]
                    break

        self.doc_count += 1
        self.start_document(key)

    _indexing_field = indexing_field

    def _finish_document(self):
        self.doc_count += 1
        self.finish_document()

    _finish_segment = finish_segment

    _start_merge = start_merge

    _finish_merge = finish_merge

    _committing = committing

    def _finish_indexing(self, segments: 'List[codecs.Segment]'):
        self._endtime = now()
        self.finished = True
        self.finish_indexing(segments)


class ChainReporter(Reporter):
    def __init__(self, reporters: 'Sequence[Reporter]'):
        super().__init__()
        self._reporters = reporters

    def _start_indexing(self, ix: 'index.Index', unique_field: str):
        super()._start_indexing(ix, unique_field)
        for r in self._reporters:
            r._start_indexing(ix, unique_field)

    def _cleared_segments(self):
        super()._cleared_segments()
        for r in self._reporters:
            r._cleared_segments()

    def _delete_by_query(self, q: 'query.Query'):
        super()._delete_by_query(q)
        for r in self._reporters:
            r._delete_by_query(q)

    def _start_new_segment(self, segments: 'List[codecs.Segment]',
                          segment: 'codecs.Segment'):
        super()._start_new_segment(segments, segment)
        for r in self._reporters:
            r._start_new_segment(segments, segment)

    def _start_document(self, kwargs: Dict[str, Any]):
        super()._start_document(kwargs)
        for r in self._reporters:
            r._start_document(kwargs)

    def _indexing_field(self, fieldname: str, fieldobj: 'fields.FieldType',
                       value: Any):
        super()._indexing_field(fieldname, fieldobj, value)
        for r in self._reporters:
            r._indexing_field(fieldname, fieldobj, value)

    def _finish_document(self):
        super()._finish_document()
        for r in self._reporters:
            r._finish_document()

    def _finish_segment(self, segments: 'List[codecs.Segment]',
                        segment: 'codecs.Segment'):
        super()._finish_segment(segments, segment)
        for r in self._reporters:
            r._finish_segment(segments, segment)

    def _start_merge(self, merge_id: str, merging: 'List[codecs.Segment]',
                     new_segment_id: str):
        super()._start_merge(merge_id, merging, new_segment_id)
        for r in self._reporters:
            r._start_merge(merge_id, merging, new_segment_id)

    def _finish_merge(self, merge_id: str, merged: 'List[codecs.Segment]',
                      new_segment: 'codecs.Segment'):
        super()._finish_merge(merge_id, merged, new_segment)
        for r in self._reporters:
            r._finish_merge(merge_id, merged, new_segment)

    def _committing(self, optimized=False):
        super()._committing(optimized)
        for r in self._reporters:
            r._committing(optimized)

    def _finish_indexing(self, segments: 'List[codecs.Segment]'):
        super()._finish_indexing(segments)
        for r in self._reporters:
            r._finish_indexing(segments)


class LoggingReporter(Reporter):
    def __init__(self, log=None):
        super().__init__()
        self.logger = log or logger

    def start_indexing(self):
        store = self.index.storage()
        self.logger.info("Starting indexing in %r", store)

    def cleared_segments(self):
        self.logger.warning("Cleared segments, index is now empty")

    def delete_by_query(self, q: 'query.Query'):
        self.logger.info("Deleting documents matching query %r", q)

    def start_new_segment(self, segments: 'List[codecs.Segment]',
                          segment: 'codecs.Segment'):
        self.logger.info("Starting new segment %r, current=%r",
                         segment, segments)

    def start_document(self, key: Any):
        self.logger.info("Starting document key=%r", key)

    def indexing_field(self, fieldname: str, fieldobj: 'fields.FieldType',
                       value: Any):
        self.logger.debug("Indexing %r in %s using %r",
                          value, fieldname, fieldobj)

    def finish_document(self):
        self.logger.info("Finished document")

    def finish_segment(self, segments: 'List[codecs.Segment]',
                       segment: 'codecs.Segment'):
        self.logger.info("Finished new segment %r, current=%r",
                         segment, segments)

    def start_merge(self, merge_id: str, merging: 'List[codecs.Segment]',
                    new_segment_id: str):
        self.logger.info("Merging segments %r (%s) info %s",
                         merging, merge_id, new_segment_id)

    def finish_merge(self, merge_id: str, merged: 'List[codecs.Segment]',
                     new_segment: 'codecs.Segment'):
        self.logger.info("Finished merging %r (%s) into %r",
                         merged, merge_id, new_segment)

    def committing(self, optimized=False):
        self.logger.info("Committing data to storage (optimized=%s)", optimized)

    def finish_indexing(self, segments: 'List[codecs.Segment]'):
        doc_count = self.doc_count
        runtime = self.runtime()
        dps = doc_count / runtime

        self.logger.info("Finished indexing %s docs in %s secs (%s docs/sec)",
                         doc_count, runtime, dps)


null_reporter = Reporter
default_reporter = Reporter



