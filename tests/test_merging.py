# Copyright 2015 Matt Chaput. All rights reserved.
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

from typing import Dict, Iterable, Sequence

from whoosh import fields, merging, writing
from whoosh.ifaces import codecs, readers, searchers
from whoosh.filedb.filestore import RamStorage
from whoosh.ifaces import matchers
from whoosh.ifaces import queries


# logger = logging.getLogger("whoosh.merging")
# logger.addHandler(logging.StreamHandler(sys.stdout))
# logger.setLevel(logging.DEBUG)


class FakeSegment(codecs.Segment):
    def __init__(self, segid: str, doc_count, size=0,
                 patterns: Dict[str, Sequence[int]]=None):
        self._id = segid
        self._dc = self._dca = doc_count
        self._size = size or self._dc * 8192

        self._deleted = set()
        self.patterns = patterns or {}

    def __repr__(self):
        megs = self._size / (1024 * 1024)
        return "<%s %d/%d %0.03f>" % (self._id, self._dc, self._dca, megs)

    def segment_id(self):
        return self._id

    def size(self):
        return self._size

    def doc_count(self):
        return self._dc

    def doc_count_all(self):
        return self._dca

    def delete_percent(self, n):
        if self._dc:
            self._dc -= int(self._dc * n / 100)

    def delete_document(self, docnum: int):
        self._deleted.add(docnum)
        self._dc -= 1

    def is_deleted(self, docnum: int) -> bool:
        return docnum in self._deleted


class FakeReader(readers.IndexReader):
    def __init__(self, segment: FakeSegment):
        self.segment = segment
        self.patterns = segment.patterns
        self.schema = None

    def matcher(self, fieldname: str, text: str) -> matchers.Matcher:
        return FakeMatcher(self.patterns[text])

    def doc_count(self) -> int:
        return self.segment.doc_count()

    def doc_count_all(self) -> int:
        return self.segment.doc_count_all()


class FakeMatcher(matchers.Matcher):
    def __init__(self, pattern: Sequence[int]):
        self._pattern = pattern

    def all_ids(self) -> Iterable[int]:
        return iter(self._pattern)


class FakeQuery(queries.Query):
    def __init__(self, text: str):
        self.text = text

    def __repr__(self):
        return "<%r>" % self.text

    def docs(self, s: 'searchers.Searcher', deleting: bool=False
             ) -> Iterable[int]:
        r = s.reader()
        return r.matcher("x", self.text).all_ids()


#

def test_simple_strategy():
    segs = []
    for i in range(15):
        segs.append(FakeSegment(hex(i), 100))
    segs[-1].delete_percent(20)

    tms = merging.TieredMergeStrategy()
    merges = tms.get_merges(segs, ())
    assert len(merges) == 1
    assert segs[-1] in merges[0]


def test_simulation():
    import random

    segs = []
    tms = merging.TieredMergeStrategy()

    for i in range(2000):
        for seg in segs:
            seg.delete_percent(random.randint(0, 5))
        segs.append(FakeSegment(hex(i), random.randint(1, 100)))
        # print("%d segs: %r" %
        #       (len(segs), sorted(segs, key=lambda x: x.size(), reverse=True)))

        merges = tms.get_merges(segs, ())
        for merge in merges:
            segids = set(s.segment_id() for s in merge.segments)
            segs = [s for s in segs if s.segment_id() not in segids]
            newseg = FakeSegment(merge.doc_count(), merge.after_size())
            # print("Merged", len(merge), "into", newseg)
            segs.append(newseg)


def test_too_big():
    segs = [FakeSegment("a", 1), FakeSegment("b", 1), FakeSegment("c", 1000000)]
    tms = merging.TieredMergeStrategy(per_tier=2)

    merges = tms.get_merges(segs, ())
    assert not merges


def _fake_merge(m: merging.Merge, newid: str,
                patterns: Dict[str, Sequence[int]]=None):
    dc = sum(s.doc_count() for s in m.segments)
    sz = sum(s.size() for s in m.segments)
    return FakeSegment(newid, dc, sz, patterns=patterns)


def _make_list(st):
    schema = fields.Schema(text=fields.Text)
    sl = writing.SegmentList(st, schema, [])
    # Replace the SegmentList's make_reader method so it returns our
    # FakeReader
    sl.make_reader = lambda seg: FakeReader(seg)

    initial = [
        FakeSegment("1", 1000, patterns={"a": [1, 2, 3], "b": [10, 20, 30]}),
        FakeSegment("2", 400, patterns={"a": [7, 8, 9], "b": [70, 80, 90]}),
        FakeSegment("3", 790, patterns={"a": [4, 5, 6], "b": [40, 50, 60]}),
        FakeSegment("4", 7458, patterns={"a": [6, 7], "b": [65, 75]}),
        FakeSegment("5", 3, patterns={"a": [0], "b": [2]}),
        FakeSegment("6", 800, patterns={"a": [3, 4, 5], "b": [35, 45, 55]}),
        FakeSegment("7", 67, patterns={"a": [11, 12, 13], "b": [21, 31, 41]}),
        FakeSegment("8", 80, patterns={"a": [15, 16, 17], "b": [51, 61, 71]}),
    ]

    for seg in initial:
        sl.add(seg)

    return initial, sl


def _check_merge(initial, sl, newseg):
    assert len(sl.segments) == 5
    assert sl.has_segment(newseg)
    assert len(sl.merging_ids()) == 0
    for i, seg in enumerate(initial):
        if i < 4:
            assert not sl.has_segment(seg)
        else:
            assert sl.has_segment(seg)

            for pat in seg.patterns.values():
                for docnum in pat:
                    assert sl.test_is_deleted(seg, docnum)


def test_integrate():
    with RamStorage() as st:
        initial, sl = _make_list(st)

        # Make a merge of the first 4 segments
        m = merging.Merge(sl.segments[:4])
        sl.add_merge(m)
        assert sl.merging_ids() == set([s.segment_id()
                                        for s in sl.segments[:4]])

        # While the merge is in progress, queue some deletions
        sl.delete_by_query(FakeQuery("a"))
        sl.delete_by_query(FakeQuery("b"))

        # Fake the result of the merge and integrate it into the list
        newseg = _fake_merge(m, "x", patterns={"a": [66, 77, 88], "b": [97]})
        sl.integrate(newseg, m.merge_id)

        _check_merge(initial, sl, newseg)


def test_threaded_integrate():
    from threading import Timer

    with RamStorage() as st:
        initial, sl = _make_list(st)

        # Make a merge of the first 4 segments
        m = merging.Merge(sl.segments[:4])
        sl.add_merge(m)
        assert sl.merging_ids() == set([s.segment_id()
                                        for s in sl.segments[:4]])

        newseg = _fake_merge(m, "x", patterns={"a": [66, 77, 88], "b": [97]})

        def _threaded_merge():
            sl.integrate(newseg, m.merge_id)

        t = Timer(0.5, _threaded_merge)
        t.start()

        # While the merge is in progress, queue some deletions
        sl.delete_by_query(FakeQuery("a"))
        sl.delete_by_query(FakeQuery("b"))

        t.join()
        _check_merge(initial, sl, newseg)


