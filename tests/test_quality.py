from __future__ import with_statement
import random

from whoosh import fields, matching, scoring
from whoosh.compat import b, u, xrange
from whoosh.filedb.filestore import RamStorage
from whoosh.util.numeric import length_to_byte, byte_to_length


def _discreet(length):
    return byte_to_length(length_to_byte(length))


def test_max_field_length():
    st = RamStorage()
    schema = fields.Schema(t=fields.TEXT)
    ix = st.create_index(schema)
    for i in xrange(1, 200, 7):
        w = ix.writer()
        w.add_document(t=u(" ").join(["word"] * i))
        w.commit()

        with ix.reader() as r:
            assert r.max_field_length("t") == _discreet(i)


def test_minmax_field_length():
    st = RamStorage()
    schema = fields.Schema(t=fields.TEXT)
    ix = st.create_index(schema)
    least = 999999
    most = 0
    for _ in xrange(1, 200, 7):
        w = ix.writer()
        count = random.randint(1, 100)
        least = min(count, least)
        most = max(count, most)
        w.add_document(t=u(" ").join(["word"] * count))
        w.commit()

        with ix.reader() as r:
            assert r.min_field_length("t") == _discreet(least)
            assert r.max_field_length("t") == _discreet(most)


def test_term_stats():
    schema = fields.Schema(t=fields.TEXT)
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(t=u("alfa bravo charlie delta echo"))
    w.add_document(t=u("bravo charlie delta echo foxtrot"))
    w.add_document(t=u("charlie delta echo foxtrot golf"))
    w.add_document(t=u("delta echo foxtrot"))
    w.add_document(t=u("echo foxtrot golf hotel india juliet"))
    w.add_document(t=u("foxtrot alfa alfa alfa"))
    w.commit()

    with ix.reader() as r:
        ti = r.term_info("t", u("alfa"))
        assert ti.weight() == 4.0
        assert ti.doc_frequency() == 2
        assert ti.min_length() == 4
        assert ti.max_length() == 5
        assert ti.max_weight() == 3.0

        assert r.term_info("t", u("echo")).min_length() == 3

        assert r.doc_field_length(3, "t") == 3
        assert r.min_field_length("t") == 3
        assert r.max_field_length("t") == 6

    w = ix.writer()
    w.add_document(t=u("alfa"))
    w.add_document(t=u("bravo charlie"))
    w.add_document(t=u("echo foxtrot tango bravo"))
    w.add_document(t=u("golf hotel"))
    w.add_document(t=u("india"))
    w.add_document(t=u("juliet alfa bravo charlie delta echo foxtrot"))
    w.commit(merge=False)

    with ix.reader() as r:
        ti = r.term_info("t", u("alfa"))
        assert ti.weight() == 6.0
        assert ti.doc_frequency() == 4
        assert ti.min_length() == 1
        assert ti.max_length() == 7
        assert ti.max_weight() == 3.0

        assert r.term_info("t", u("echo")).min_length() == 3

        assert r.min_field_length("t") == 1
        assert r.max_field_length("t") == 7


def test_min_max_id():
    schema = fields.Schema(id=fields.STORED, t=fields.TEXT)
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(id=0, t=u("alfa bravo charlie"))
    w.add_document(id=1, t=u("bravo charlie delta"))
    w.add_document(id=2, t=u("charlie delta echo"))
    w.add_document(id=3, t=u("delta echo foxtrot"))
    w.add_document(id=4, t=u("echo foxtrot golf"))
    w.commit()

    with ix.reader() as r:
        ti = r.term_info("t", u("delta"))
        assert ti.min_id() == 1
        assert ti.max_id() == 3

        ti = r.term_info("t", u("alfa"))
        assert ti.min_id() == 0
        assert ti.max_id() == 0

        ti = r.term_info("t", u("foxtrot"))
        assert ti.min_id() == 3
        assert ti.max_id() == 4

    w = ix.writer()
    w.add_document(id=5, t=u("foxtrot golf hotel"))
    w.add_document(id=6, t=u("golf hotel alfa"))
    w.add_document(id=7, t=u("hotel alfa bravo"))
    w.add_document(id=8, t=u("alfa bravo charlie"))
    w.commit(merge=False)

    with ix.reader() as r:
        ti = r.term_info("t", u("delta"))
        assert ti.min_id() == 1
        assert ti.max_id() == 3

        ti = r.term_info("t", u("alfa"))
        assert ti.min_id() == 0
        assert ti.max_id() == 8

        ti = r.term_info("t", u("foxtrot"))
        assert ti.min_id() == 3
        assert ti.max_id() == 5


def test_replacements():
    sc = scoring.WeightScorer(0.25)
    a = matching.ListMatcher([1, 2, 3], [0.25, 0.25, 0.25], scorer=sc)
    b = matching.ListMatcher([1, 2, 3], [0.25, 0.25, 0.25], scorer=sc)
    um = matching.UnionMatcher(a, b)

    a2 = a.replace(0.5)
    assert a2.__class__ == matching.NullMatcherClass

    um2 = um.replace(0.5)
    assert um2.__class__ == matching.IntersectionMatcher
    um2 = um.replace(0.6)
    assert um2.__class__ == matching.NullMatcherClass

    wm = matching.WrappingMatcher(um, boost=2.0)
    wm = wm.replace(0.5)
    assert wm.__class__ == matching.WrappingMatcher
    assert wm.boost == 2.0
    assert wm.child.__class__ == matching.IntersectionMatcher

    ls1 = matching.ListMatcher([1, 2, 3], [0.1, 0.1, 0.1],
                               scorer=scoring.WeightScorer(0.1))
    ls2 = matching.ListMatcher([1, 2, 3], [0.2, 0.2, 0.2],
                               scorer=scoring.WeightScorer(0.2))
    ls3 = matching.ListMatcher([1, 2, 3], [0.3, 0.3, 0.3],
                               scorer=scoring.WeightScorer(0.3))
    mm = matching.MultiMatcher([ls1, ls2, ls3], [0, 4, 8])
    mm = mm.replace(0.25)
    assert mm.current == 2

    dm = matching.DisjunctionMaxMatcher(ls1, ls2)
    dm = dm.replace(0.15)
    assert dm is ls2
