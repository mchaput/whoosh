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

from typing import Any, Dict, Sequence, Set

from whoosh import columns, fields
from whoosh.ifaces import codecs
from whoosh.postings import basic, postings, ptuples


class NullPerDocWriter(codecs.PerDocumentWriter):
    def __init__(self):
        self._io = basic.BasicIO()

    def postings_io(self) -> 'postings.PostingsIO':
        return self._io

    def start_doc(self, docnum: int):
        pass

    def add_field(self, fieldname: str, fieldobj: 'fields.FieldType',
                  value: Any, length: int):
        pass

    def add_column_value(self, fieldname: str, columnobj: 'columns.Column',
                         value: Any):
        pass

    def add_vector_postings(self, fieldname: str, fieldobj: 'fields.FieldType',
                            posts: 'Sequence[ptuples.PostTuple]'):
        pass

    def add_raw_vector(self, fieldname: str, data: bytes):
        pass


class NullPerDocReader(codecs.PerDocumentReader):
    def __init__(self, doc_count=1):
        self._doccount = doc_count

    def doc_count(self) -> int:
        return self._doccount

    def doc_count_all(self) -> int:
        return self._doccount

    def has_deletions(self) -> bool:
        return False

    def is_deleted(self, docnum: int) -> bool:
        return False

    def deleted_docs(self) -> Set[int]:
        return set()

    def all_doc_ids(self):
        return range(self._doccount)

    def doc_field_length(self, docnum: int, fieldname: str,
                         default: int=0) -> int:
        return 0

    def field_length(self, fieldname: str) -> int:
        return 0

    def min_field_length(self, fieldname: str) -> int:
        return 0

    def max_field_length(self, fieldname: str) -> int:
        return 0

    def stored_fields(self, docnum: int) -> Dict:
        return {}

