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

import typing
from typing import List

# Typing imports
if typing.TYPE_CHECKING:
    from whoosh import index
    from whoosh.codec import codecs


class Reporter:
    def start_indexing(self, ix: 'index.Index'):
        pass

    def start_new_segment(self, segments: 'List[codecs.Segment]', segid: str):
        pass

    def added_documents(self, count: int):
        pass

    def finish_segment(self, segments: 'List[codecs.Segment]', segid: str,
                       doccount: int, size: int, deleted: int):
        pass

    def start_merge(self, segments: 'List[codecs.Segment]',
                    segids: List[str]):
        pass

    def finish_merge(self, segments: 'List[codecs.Segment]',
                     old_segids: List[str], new_segids: List[str]):
        pass

    def finish_indexing(self, segments: 'List[codecs.Segment]'):
        pass


null_reporter = Reporter
default_reporter = Reporter



