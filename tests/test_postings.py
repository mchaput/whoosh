from __future__ import with_statement

from nose.tools import assert_equal  # @UnresolvedImport

from whoosh import analysis, fields
from whoosh.compat import xrange, u
from whoosh.codec import default_codec
from whoosh.formats import (Characters, CharacterBoosts, Existence, Frequency,
                            Positions, PositionBoosts)
from whoosh.support.testing import TempStorage


def _roundtrip(content, format_, astype, ana=None):
    with TempStorage("roundtrip") as st:
        codec = default_codec()
        seg = codec.new_segment(st, "")
        ana = ana or analysis.StandardAnalyzer()
        field = fields.FieldType(format=format_, analyzer=ana)

        fw = codec.field_writer(st, seg)
        fw.start_field("f1", field)
        for text, _, weight, valuestring in sorted(field.index(content)):
            fw.start_term(text)
            fw.add(0, weight, valuestring, None)
            fw.finish_term()
        fw.finish_field()
        fw.close()

        tr = codec.terms_reader(st, seg)
        ps = []
        for fieldname, text in tr.keys():
            m = tr.matcher(fieldname, text, format_)
            ps.append((text, m.value_as(astype)))
        tr.close()
        return ps


def test_existence_postings():
    content = u("alfa bravo charlie")
    assert_equal(_roundtrip(content, Existence(), "frequency"),
                 [("alfa", 1), ("bravo", 1), ("charlie", 1)])


def test_frequency_postings():
    content = u("alfa bravo charlie bravo alfa alfa")
    assert_equal(_roundtrip(content, Frequency(), "frequency"),
                 [("alfa", 3), ("bravo", 2), ("charlie", 1)])


def test_position_postings():
    content = u("alfa bravo charlie bravo alfa alfa")
    assert_equal(_roundtrip(content, Positions(), "positions"),
                 [("alfa", [0, 4, 5]), ("bravo", [1, 3]), ("charlie", [2])])
    assert_equal(_roundtrip(content, Positions(), "frequency"),
                 [("alfa", 3), ("bravo", 2), ("charlie", 1)])


def test_character_postings():
    content = u("alfa bravo charlie bravo alfa alfa")
    assert_equal(_roundtrip(content, Characters(), "characters"),
                 [("alfa", [(0, 0, 4), (4, 25, 29), (5, 30, 34)]),
                  ("bravo", [(1, 5, 10), (3, 19, 24)]),
                  ("charlie", [(2, 11, 18)])])
    assert_equal(_roundtrip(content, Characters(), "positions"),
                 [("alfa", [0, 4, 5]), ("bravo", [1, 3]), ("charlie", [2])])
    assert_equal(_roundtrip(content, Characters(), "frequency"),
                 [("alfa", 3), ("bravo", 2), ("charlie", 1)])


def test_posboost_postings():
    pbs = PositionBoosts()
    ana = analysis.RegexTokenizer(r"\S+") | analysis.DelimitedAttributeFilter()
    content = u("alfa^2 bravo^0.1 charlie^2 bravo^0.5 alfa alfa")
    assert_equal(_roundtrip(content, pbs, "position_boosts", ana),
                 [("alfa", [(0, 2), (4, 1), (5, 1)]),
                  ("bravo", [(1, 0.1), (3, 0.5)]),
                  ("charlie", [(2, 2)])])
    assert_equal(_roundtrip(content, pbs, "positions", ana),
                 [("alfa", [0, 4, 5]), ("bravo", [1, 3]), ("charlie", [2])])
    assert_equal(_roundtrip(content, pbs, "frequency", ana),
                 [("alfa", 3), ("bravo", 2), ("charlie", 1)])


def test_charboost_postings():
    cbs = CharacterBoosts()
    ana = analysis.RegexTokenizer(r"\S+") | analysis.DelimitedAttributeFilter()
    content = u("alfa^2 bravo^0.1 charlie^2 bravo^0.5 alfa alfa")
    assert_equal(_roundtrip(content, cbs, "character_boosts", ana),
                 [("alfa", [(0, 0, 4, 2), (4, 37, 41, 1), (5, 42, 46, 1)]),
                  ("bravo", [(1, 7, 12, 0.1), (3, 27, 32, 0.5)]),
                  ("charlie", [(2, 17, 24, 2)])])
    assert_equal(_roundtrip(content, cbs, "position_boosts", ana),
                 [("alfa", [(0, 2), (4, 1), (5, 1)]),
                  ("bravo", [(1, 0.1), (3, 0.5)]),
                  ("charlie", [(2, 2)])])
    assert_equal(_roundtrip(content, cbs, "characters", ana),
                 [("alfa", [(0, 0, 4), (4, 37, 41), (5, 42, 46)]),
                  ("bravo", [(1, 7, 12), (3, 27, 32)]),
                  ("charlie", [(2, 17, 24)])])
    assert_equal(_roundtrip(content, cbs, "positions", ana),
                 [("alfa", [0, 4, 5]), ("bravo", [1, 3]), ("charlie", [2])])
    assert_equal(_roundtrip(content, cbs, "frequency", ana),
                 [("alfa", 3), ("bravo", 2), ("charlie", 1)])
