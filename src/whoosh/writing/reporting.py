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

import sys
import typing
from typing import Any, Dict, List, Optional

from whoosh.util import now

# Typing imports
if typing.TYPE_CHECKING:
    from whoosh import fields, index, query
    from whoosh.codec import codecs


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


class StreamReporter(Reporter):
    def __init__(self, stream=None):
        super(StreamReporter, self).__init__()
        self.stream = stream or sys.stderr

    def start_indexing(self):
        store = self.index.storage()
        print("Started indexing in", store, file=self.stream)

    def cleared_segments(self):
        print("Cleared segments, index is empty", file=self.stream)

    def delete_by_query(self, q: 'query.Query'):
        print("Deleting documents matching", repr(q), file=self.stream)

    def start_new_segment(self, segments: 'List[codecs.Segment]',
                          segment: 'codecs.Segment'):
        print("Started new segment", segment.segment_id(), file=self.stream)

    def start_document(self, key: Any):
        print("Indexing document", key, file=self.stream)

    def indexing_field(self, fieldname: str, fieldobj: 'fields.FieldType',
                       value: Any):
        pass

    def finish_document(self):
        pass

    def finish_segment(self, segments: 'List[codecs.Segment]',
                       segment: 'codecs.Segment'):
        print("Finished segment", segment.segment_id(), file=self.stream)

    def start_merge(self, merge_id: str, merging: 'List[codecs.Segment]',
                    new_segment_id: str):
        print("Merging segments", merging, "as", new_segment_id,
              file=self.stream)

    def finish_merge(self, merge_id: str, merged: 'List[codecs.Segment]',
                     new_segment: 'codecs.Segment'):
        print("Merged segments", merged, "into", new_segment, file=self.stream)

    def committing(self, optimized=False):
        print("Committing new data to storage, optimized=", optimized,
              file=self.stream)

    def finish_indexing(self, segments: 'List[codecs.Segment]'):
        doc_count = self.doc_count
        runtime = self.runtime()
        dps = doc_count / runtime
        print("Finished indexing", doc_count, "in", runtime, "seconds,", dps,
              "docs/sec", file=self.stream)


null_reporter = Reporter
default_reporter = Reporter



