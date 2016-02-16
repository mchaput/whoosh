from __future__ import with_statement
import random, time, threading

import pytest

from whoosh import fields, query, writing
from whoosh.ifaces import analysis
from whoosh.compat import xrange, text_type, permutations
from whoosh.util.testing import TempIndex


def test_no_stored():
    schema = fields.Schema(id=fields.ID, text=fields.TEXT)
    with TempIndex(schema, "nostored") as ix:
        domain = (u"alfa", u"bravo", u"charlie", u"delta", u"echo",
                  u"foxtrot", u"golf", u"hotel", u"india")

        with ix.writer() as w:
            for i in xrange(20):
                w.add_document(id=text_type(i),
                               text=u" ".join(random.sample(domain, 5)))

        with ix.reader() as r:
            assert (sorted([int(id) for id in r.lexicon("id")]) ==
                    list(range(20)))


# def test_asyncwriter():
#     schema = fields.Schema(id=fields.ID(stored=True), text=fields.TEXT)
#     with TempIndex(schema, "asyncwriter") as ix:
#         domain = (u"alfa", u"bravo", u"charlie", u"delta", u"echo",
#                   u"foxtrot", u"golf", u"hotel", u"india")
#
#         writers = []
#         # Simulate doing 20 (near-)simultaneous commits. If we weren't using
#         # AsyncWriter, at least some of these would fail because the first
#         # writer wouldn't be finished yet.
#         for i in xrange(20):
#             w = writing.AsyncWriter(ix)
#             writers.append(w)
#             w.add_document(id=text_type(i),
#                            text=u" ".join(random.sample(domain, 5)))
#             w.commit()
#
#         # Wait for all writers to finish before checking the results
#         for w in writers:
#             if w.running:
#                 w.join()
#
#         # Check whether all documents made it into the index.
#         with ix.reader() as r:
#             assert sorted([int(id) for id in r.lexicon("id")]) == list(range(20))


# def test_asyncwriter_no_stored():
#     schema = fields.Schema(id=fields.ID, text=fields.TEXT)
#     with TempIndex(schema, "asyncnostored") as ix:
#         domain = (u"alfa", u"bravo", u"charlie", u"delta", u"echo",
#                   u"foxtrot", u"golf", u"hotel", u"india")
#
#         writers = []
#         # Simulate doing 20 (near-)simultaneous commits. If we weren't using
#         # AsyncWriter, at least some of these would fail because the first
#         # writer wouldn't be finished yet.
#         for i in xrange(20):
#             w = writing.AsyncWriter(ix)
#             writers.append(w)
#             w.add_document(id=text_type(i),
#                            text=u" ".join(random.sample(domain, 5)))
#             w.commit()
#
#         # Wait for all writers to finish before checking the results
#         for w in writers:
#             if w.running:
#                 w.join()
#
#         # Check whether all documents made it into the index.
#         with ix.reader() as r:
#             assert sorted([int(id) for id in r.lexicon("id")]) == list(range(20))


def test_updates():
    schema = fields.Schema(id=fields.ID(unique=True, stored=True))
    with TempIndex(schema) as ix:
        for _ in xrange(10):
            with ix.writer() as w:
                w.update_document(id=u"a")
        assert ix.doc_count() == 1


# def test_buffered():
#     schema = fields.Schema(id=fields.ID, text=fields.TEXT)
#     with TempIndex(schema, "buffered") as ix:
#         domain = u"alfa bravo charlie delta echo foxtrot golf hotel india"
#         domain = domain.split()
#
#         w = writing.BufferedWriter(ix, period=None, limit=10,
#                                    commitargs={"merge": False})
#         for i in xrange(20):
#             w.add_document(id=text_type(i),
#                            text=u" ".join(random.sample(domain, 5)))
#         time.sleep(0.1)
#         w.close()
#
#         assert len(ix._segments()) == 2
#
#
# def test_buffered_search():
#     schema = fields.Schema(id=fields.STORED, text=fields.TEXT)
#     with TempIndex(schema, "bufferedsearch") as ix:
#         w = writing.BufferedWriter(ix, period=None, limit=5)
#         w.add_document(id=1, text=u"alfa bravo charlie")
#         w.add_document(id=2, text=u"bravo tango delta")
#         w.add_document(id=3, text=u"tango delta echo")
#         w.add_document(id=4, text=u"charlie delta echo")
#
#         with w.searcher() as s:
#             r = s.search(query.Term("text", u"tango"))
#             assert sorted([d["id"] for d in r]) == [2, 3]
#
#         w.add_document(id=5, text=u"foxtrot golf hotel")
#         w.add_document(id=6, text=u"india tango juliet")
#         w.add_document(id=7, text=u"tango kilo lima")
#         w.add_document(id=8, text=u"mike november echo")
#
#         with w.searcher() as s:
#             r = s.search(query.Term("text", u"tango"))
#             assert sorted([d["id"] for d in r]) == [2, 3, 6, 7]
#
#         w.close()
#
#
# def test_buffered_update():
#     schema = fields.Schema(id=fields.ID(stored=True, unique=True),
#                            payload=fields.STORED)
#     with TempIndex(schema, "bufferedupdate") as ix:
#         w = writing.BufferedWriter(ix, period=None, limit=5)
#         for i in xrange(10):
#             for char in u"abc":
#                 fs = dict(id=char, payload=text_type(i) + char)
#                 w.update_document(**fs)
#
#         with w.reader() as r:
#             sfs = [sf for _, sf in r.iter_docs()]
#             sfs = sorted(sfs, key=lambda x: x["id"])
#             assert sfs == [{'id': u'a', 'payload': u'9a'},
#                            {'id': u'b', 'payload': u'9b'},
#                            {'id': u'c', 'payload': u'9c'}]
#             assert r.doc_count() == 3
#
#         w.close()
#
#
# def test_buffered_threads():
#     domain = u"alfa bravo charlie delta".split()
#     schema = fields.Schema(name=fields.ID(unique=True, stored=True))
#     with TempIndex(schema, "buffthreads") as ix:
#         w = writing.BufferedWriter(ix, limit=10)
#
#         class SimWriter(threading.Thread):
#             def run(self):
#                 for _ in xrange(5):
#                     w.update_document(name=random.choice(domain))
#                     time.sleep(random.uniform(0.01, 0.1))
#
#         threads = [SimWriter() for _ in xrange(5)]
#         for thread in threads:
#             thread.start()
#         for thread in threads:
#             thread.join()
#         w.close()
#
#         with ix.reader() as r:
#             assert r.doc_count() == 4
#             names = sorted([d["name"] for d in r.all_stored_fields()])
#             assert names == domain


def test_fractional_weights():
    from whoosh import analysis

    ana = analysis.RegexTokenizer(r"\S+") | analysis.DelimitedAttributeFilter()

    # With Positions format
    schema = fields.Schema(f=fields.TEXT(analyzer=ana))
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(f=u"alfa^0.5 bravo^1.5 charlie^2.0 delta^1.5")

        with ix.searcher() as s:
            wts = []
            for word in s.reader().lexicon("f"):
                p = s.matcher("f", word)
                wts.append(p.weight())
            assert wts == [0.5, 1.5, 2.0, 1.5]

    # Try again with Frequency format
    schema = fields.Schema(f=fields.TEXT(analyzer=ana, phrase=False))
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(f=u"alfa^0.5 bravo^1.5 charlie^2.0 delta^1.5")

        with ix.searcher() as s:
            wts = []
            for word in s.reader().lexicon("f"):
                p = s.matcher("f", word)
                wts.append(p.weight())
            assert wts == [0.5, 1.5, 2.0, 1.5]


def test_cancel_delete():
    schema = fields.Schema(id=fields.ID(stored=True))
    # Single segment
    with TempIndex(schema, "canceldelete1") as ix:
        w = ix.writer()
        for char in u"ABCD":
            w.add_document(id=char)
        w.commit()

        with ix.reader() as r:
            assert not r.has_deletions()

        w = ix.writer()
        w.delete_by_term("id", "C")
        w.delete_by_term("id", "D")
        w.cancel()

        with ix.reader() as r:
            assert not r.has_deletions()
            assert not r.is_deleted(2)
            assert not r.is_deleted(3)

    # Multiple segments
    with TempIndex(schema, "canceldelete2") as ix:
        for char in u"ABCD":
            w = ix.writer()
            w.add_document(id=char)
            w.commit(merge=False)

        with ix.reader() as r:
            assert not r.has_deletions()

        w = ix.writer()
        w.delete_by_term("id", "C")
        w.delete_by_term("id", "D")
        w.cancel()

        with ix.reader() as r:
            assert not r.has_deletions()
            assert not r.is_deleted(2)
            assert not r.is_deleted(3)


def test_delete_nonexistant():
    from whoosh.writing import IndexingError

    schema = fields.Schema(id=fields.ID(stored=True))
    # Single segment
    with TempIndex(schema, "deletenon1") as ix:
        w = ix.writer()
        for char in u"ABC":
            w.add_document(id=char)
        w.commit()

        try:
            w = ix.writer()
            w.delete_by_term("fieldname", "E")
        finally:
            w.cancel()

    # Multiple segments
    with TempIndex(schema, "deletenon1") as ix:
        for char in u"ABC":
            w = ix.writer()
            w.add_document(id=char)
            w.commit(merge=False)

        try:
            w = ix.writer()
            w.delete_by_term("fieldname", "E")
        finally:
            w.cancel()


def test_add_field():
    schema = fields.Schema(a=fields.TEXT)
    with TempIndex(schema, "addfield") as ix:
        with ix.writer() as w:
            w.add_document(a=u"alfa bravo charlie")
        with ix.writer() as w:
            w.add_field("b", fields.ID(stored=True))
            w.add_field("c*", fields.ID(stored=True))
            assert "cat" in w.schema
            w.add_document(a=u"delta echo foxtrot", b=u"india", cat=u"juliet")

        with ix.searcher() as s:
            assert "cat" in s.schema
            assert s.reader().indexed_field_names() == ["a", "b", "cat"]
            assert ("cat", "juliet") in s.reader()
            fs = s.document(b=u"india")
            assert fs == {"b": "india", "cat": "juliet"}


def test_add_reader():
    schema = fields.Schema(i=fields.ID(stored=True, unique=True),
                           a=fields.TEXT(stored=True, spelling=True),
                           b=fields.TEXT(vector=True))
    with TempIndex(schema, "addreader") as ix:
        with ix.writer() as w:
            w.add_document(i=u"0", a=u"alfa bravo charlie delta",
                           b=u"able baker coxwell dog")
            w.add_document(i=u"1", a=u"bravo charlie delta echo",
                           b=u"elf fabio gong hiker")
            w.add_document(i=u"2", a=u"charlie delta echo foxtrot",
                           b=u"india joker king loopy")
            w.add_document(i=u"3", a=u"delta echo foxtrot golf",
                           b=u"mister noogie oompah pancake")

        with ix.reader() as r:
            m = r.matcher("a", "alfa")
            assert m.weight() == 1.0

        with ix.writer() as w:
            w.delete_by_term("i", "1")
            w.delete_by_term("i", "3")

        with ix.writer() as w:
            w.add_document(i=u"4", a=u"hotel india juliet kilo",
                           b=u"quick rhubarb soggy trap")
            w.add_document(i=u"5", a=u"india juliet kilo lima",
                           b=u"umber violet weird xray")
            w.optimize = True

        with ix.reader() as r:
            assert r.doc_count() == 4

            sfs = sorted([d for _, d in r.iter_docs()], key=lambda d: d["i"])
            assert sfs == [
                {"i": u"0", "a":  u"alfa bravo charlie delta"},
                {"i": u"2", "a": u"charlie delta echo foxtrot"},
                {"i": u"4", "a": u"hotel india juliet kilo"},
                {"i": u"5", "a": u"india juliet kilo lima"},
            ]

            aterms = " ".join(r.field_terms("a"))
            assert aterms == (
                "alfa bravo charlie delta echo foxtrot hotel india juliet kilo "
                "lima"
            )

            targets = {
                "0": ["able", "baker", "coxwell", "dog"],
                "2": ["india", "joker", "king", "loopy"],
                "4": ["quick", "rhubarb", "soggy", "trap"],
                "5": ["umber", "violet", "weird", "xray"],
            }
            for docnum, stored in r.iter_docs():
                v = r.vector(docnum, "b")
                terms = [v.termbytes(i).decode("utf8") for i in xrange(len(v))]
                target = targets[stored["i"]]
                assert terms == target


def test_add_reader_spelling():
    # Test whether add_spell_word() items get copied over in a merge
    from whoosh.analysis.analyzers import StemmingAnalyzer

    # Because b is stemming and spelled, it will use add_spell_word()
    ana = StemmingAnalyzer()
    schema = fields.Schema(a=fields.TEXT(analyzer=ana),
                           b=fields.TEXT(analyzer=ana, spelling=True))

    with TempIndex(schema, "addreadersp") as ix:
        with ix.writer() as w:
            w.add_document(a=u"rendering modeling",
                           b=u"rendering modeling")
            w.add_document(a=u"flying rolling",
                           b=u"flying rolling")

        with ix.writer() as w:
            w.add_document(a=u"writing eyeing",
                           b=u"writing eyeing")
            w.add_document(a=u"undoing indicating",
                           b=u"undoing indicating")
            w.optimize = True

        with ix.reader() as r:
            al = list(r.lexicon("a"))
            assert al == [b"eye", b"fli", b"indic", b"model", b"render",
                          b"roll", b"undo", b"write"]

            assert "spell_b" in r.schema
            sws = list(r.lexicon("spell_b"))
            assert sws == [b"eyeing", b"flying", b"indicating", b"modeling",
                           b"rendering", b"rolling",  b"undoing", b"writing"]

            assert not schema["a"].spelling
            assert list(r.terms_within("a", "undoink", 1)) == []
            assert list(r.terms_within("b", "undoink", 1)) == ["undoing"]


# def test_clear():
#     schema = fields.Schema(a=fields.KEYWORD)
#     with TempIndex(schema) as ix:
#         # Add some segments
#         with ix.writer() as w:
#             w.add_document(a=u"one two three")
#             w.merge = False
#         with ix.writer() as w:
#             w.add_document(a=u"two three four")
#             w.merge = False
#         with ix.writer() as w:
#             w.add_document(a=u"three four five")
#             w.merge = False
#
#         # Clear
#         with ix.writer() as w:
#             w.add_document(a=u"foo bar baz")
#             w.mergetype = writing.CLEAR
#
#         with ix.searcher() as s:
#             assert s.doc_count_all() == 1
#             assert (list(s.reader().lexicon("a")) ==
#                     [b"bar", b"baz", b"foo"])


def test_spellable_list():
    # Make sure a spellable field works with a list of pre-analyzed tokens
    from whoosh.analysis.analyzers import StemmingAnalyzer

    ana = StemmingAnalyzer()
    schema = fields.Schema(Location=fields.STORED,Lang=fields.STORED,
                           Title=fields.TEXT(spelling=True, analyzer=ana))
    with TempIndex(schema) as ix:
        doc = {'Location': '1000/123', 'Lang': 'E',
               'Title': ['Introduction', 'Numerical', 'Analysis']}

        with ix.writer() as w:
            w.add_document(**doc)


def test_long_term():
    schema = fields.Schema(text=fields.Text)
    text = u"boogabooga" * 70000

    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(text=text)

        with ix.searcher() as s:
            assert ("text", text) in s.reader()

            r = s.search(query.Term("text", text))
            assert r.scored_length() == 1


def test_long_lengths():
    from whoosh.postings import Format

    schema = fields.Schema(text=fields.Text)
    lengths = [256, 35000]
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            for length in lengths:
                w.add_document(text=u"alfa " * length)


def test_merge_lengths():
    # import logging
    # logger = logging.getLogger("whoosh")
    # logger.addHandler(logging.StreamHandler())
    # logger.setLevel(logging.INFO)

    schema = fields.Schema(text=fields.Text)
    words = u"alfa bravo charlie delta echo foxtrot hotel india foxtrot".split()
    with TempIndex(schema) as ix:
        count = 0
        for _ in xrange(5):
            with ix.writer() as w:
                for j in xrange(1000):
                    length = j % len(words) + 1
                    doc = " ".join(words[:length])
                    count += 1
                    w.add_document(text=u" ".join(doc))
                w.merge = False

        # Merge segments
        with ix.writer() as w:
            w.optimize = True



