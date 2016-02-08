from __future__ import with_statement
import random

from whoosh import fields, matching, scoring
from whoosh.compat import xrange
from whoosh.util.testing import TempIndex


def test_max_field_length():
    schema = fields.Schema(t=fields.TEXT)
    with TempIndex(schema) as ix:
        for i in xrange(1, 200, 7):
            with ix.writer() as w:
                w.add_document(t=u" ".join(["word"] * i))

            with ix.reader() as r:
                assert r.max_field_length("t") == i


def test_minmax_field_length():
    schema = fields.Schema(t=fields.TEXT)
    with TempIndex(schema) as ix:
        least = 999999
        most = 0
        for _ in xrange(1, 200, 7):
            with ix.writer() as w:
                count = random.randint(1, 100)
                least = min(count, least)
                most = max(count, most)
                w.add_document(t=u" ".join(["word"] * count))

            with ix.reader() as r:
                assert r.min_field_length("t") == least
                assert r.max_field_length("t") == most


def test_term_stats():
    schema = fields.Schema(t=fields.TEXT)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(t=u"alfa bravo charlie delta echo")
            w.add_document(t=u"bravo charlie delta echo foxtrot")
            w.add_document(t=u"charlie delta echo foxtrot golf")
            w.add_document(t=u"delta echo foxtrot")
            w.add_document(t=u"echo foxtrot golf hotel india juliet")
            w.add_document(t=u"foxtrot alfa alfa alfa")

        with ix.reader() as r:
            ti = r.term_info("t", u"alfa")
            assert ti.weight() == 4.0
            assert ti.doc_frequency() == 2
            assert ti.min_length() == 4
            assert ti.max_length() == 5
            assert ti.max_weight() == 3.0

            assert r.term_info("t", u"echo").min_length() == 3

            assert r.doc_field_length(3, "t") == 3
            assert r.min_field_length("t") == 3
            assert r.max_field_length("t") == 6

        with ix.writer() as w:
            w.merge = False
            w.add_document(t=u"alfa")
            w.add_document(t=u"bravo charlie")
            w.add_document(t=u"echo foxtrot tango bravo")
            w.add_document(t=u"golf hotel")
            w.add_document(t=u"india")
            w.add_document(t=u"juliet alfa bravo charlie delta echo foxtrot")

        with ix.reader() as r:
            ti = r.term_info("t", u"alfa")
            assert ti.weight() == 6.0
            assert ti.doc_frequency() == 4
            assert ti.min_length() == 1
            assert ti.max_length() == 7
            assert ti.max_weight() == 3.0

            assert r.term_info("t", u"echo").min_length() == 3

            assert r.min_field_length("t") == 1
            assert r.max_field_length("t") == 7


def test_min_max_id():
    schema = fields.Schema(id=fields.STORED, t=fields.TEXT)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(id=0, t=u"alfa bravo charlie")
            w.add_document(id=1, t=u"bravo charlie delta")
            w.add_document(id=2, t=u"charlie delta echo")
            w.add_document(id=3, t=u"delta echo foxtrot")
            w.add_document(id=4, t=u"echo foxtrot golf")

        with ix.reader() as r:
            ti = r.term_info("t", u"delta")
            assert ti.min_id() == 1
            assert ti.max_id() == 3

            ti = r.term_info("t", u"alfa")
            assert ti.min_id() == 0
            assert ti.max_id() == 0

            ti = r.term_info("t", u"foxtrot")
            assert ti.min_id() == 3
            assert ti.max_id() == 4

        with ix.writer() as w:
            w.merge = False
            w.add_document(id=5, t=u"foxtrot golf hotel")
            w.add_document(id=6, t=u"golf hotel alfa")
            w.add_document(id=7, t=u"hotel alfa bravo")
            w.add_document(id=8, t=u"alfa bravo charlie")

        with ix.reader() as r:
            ti = r.term_info("t", u"delta")
            assert ti.min_id() == 1
            assert ti.max_id() == 3

            ti = r.term_info("t", u"alfa")
            assert ti.min_id() == 0
            assert ti.max_id() == 8

            ti = r.term_info("t", u"foxtrot")
            assert ti.min_id() == 3
            assert ti.max_id() == 5


def test_replacements():
    a = matching.ListMatcher([1, 2, 3], all_weights=0.25)
    b = matching.ListMatcher([1, 2, 3], all_weights=0.25)
    um = matching.UnionMatcher(a, b)

    a2 = a.replace(0.5)
    assert type(a2) is matching.NullMatcherClass

    um2 = um.replace(0.5)
    assert type(um2) is matching.IntersectionMatcher
    um2 = um.replace(0.6)
    assert type(um2) is matching.NullMatcherClass

    a = matching.ListMatcher([1, 2, 3], all_weights=0.25)
    wm = matching.WrappingMatcher(a, boost=2.0)
    wm = wm.replace(0.4)
    assert type(wm) is matching.WrappingMatcher
    assert wm.score() == 0.5

    ls1 = matching.ListMatcher([1, 2, 3], all_weights=0.1)
    ls2 = matching.ListMatcher([1, 2, 3], all_weights=0.2)
    ls3 = matching.ListMatcher([1, 2, 3], all_weights=0.3)
    mm = matching.MultiMatcher([ls1, ls2, ls3], [0, 4, 8])
    mm = mm.replace(0.25)
    assert mm._current == 2
    #
    # dm = matching.DisjunctionMaxMatcher(ls1, ls2)
    # dm = dm.replace(0.15)
    # assert dm is ls2
