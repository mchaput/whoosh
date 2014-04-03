from __future__ import with_statement
import random, time, threading

import pytest

from whoosh import analysis, fields, query, writing
from whoosh.compat import b, u, xrange, text_type
from whoosh.util.testing import TempIndex


def test_no_stored():
    schema = fields.Schema(id=fields.ID, text=fields.TEXT)
    with TempIndex(schema, "nostored") as ix:
        domain = (u"alfa", u"bravo", u"charlie", u"delta", u"echo",
                  u"foxtrot", u"golf", u"hotel", u"india")

        w = ix.writer()
        for i in xrange(20):
            w.add_document(id=text_type(i),
                           text=u" ".join(random.sample(domain, 5)))
        w.commit()

        with ix.reader() as r:
            assert sorted([int(id) for id in r.lexicon("id")]) == list(range(20))


def test_asyncwriter():
    schema = fields.Schema(id=fields.ID(stored=True), text=fields.TEXT)
    with TempIndex(schema, "asyncwriter") as ix:
        domain = (u"alfa", u"bravo", u"charlie", u"delta", u"echo",
                  u"foxtrot", u"golf", u"hotel", u"india")

        writers = []
        # Simulate doing 20 (near-)simultaneous commits. If we weren't using
        # AsyncWriter, at least some of these would fail because the first
        # writer wouldn't be finished yet.
        for i in xrange(20):
            w = writing.AsyncWriter(ix)
            writers.append(w)
            w.add_document(id=text_type(i),
                           text=u" ".join(random.sample(domain, 5)))
            w.commit()

        # Wait for all writers to finish before checking the results
        for w in writers:
            if w.running:
                w.join()

        # Check whether all documents made it into the index.
        with ix.reader() as r:
            assert sorted([int(id) for id in r.lexicon("id")]) == list(range(20))


def test_asyncwriter_no_stored():
    schema = fields.Schema(id=fields.ID, text=fields.TEXT)
    with TempIndex(schema, "asyncnostored") as ix:
        domain = (u"alfa", u"bravo", u"charlie", u"delta", u"echo",
                  u"foxtrot", u"golf", u"hotel", u"india")

        writers = []
        # Simulate doing 20 (near-)simultaneous commits. If we weren't using
        # AsyncWriter, at least some of these would fail because the first
        # writer wouldn't be finished yet.
        for i in xrange(20):
            w = writing.AsyncWriter(ix)
            writers.append(w)
            w.add_document(id=text_type(i),
                           text=u" ".join(random.sample(domain, 5)))
            w.commit()

        # Wait for all writers to finish before checking the results
        for w in writers:
            if w.running:
                w.join()

        # Check whether all documents made it into the index.
        with ix.reader() as r:
            assert sorted([int(id) for id in r.lexicon("id")]) == list(range(20))


def test_updates():
    schema = fields.Schema(id=fields.ID(unique=True, stored=True))
    with TempIndex(schema, "updates") as ix:
        for _ in xrange(10):
            with ix.writer() as w:
                w.update_document(id=u"a")

        with ix.reader() as r:
            assert r.doc_count() == 1
            assert list(r.all_stored_fields()) == [(9, {"id": "a"})]


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
#         class SimWriter(threading.Thread):
#             def run(self):
#                 for _ in xrange(5):
#                     w.update_document(name=random.choice(domain))
#                     time.sleep(random.uniform(0.01, 0.1))
#
#         w = writing.BufferedWriter(ix, limit=10)
#         threads = [SimWriter() for _ in xrange(5)]
#         for thread in threads:
#             thread.start()
#         for thread in threads:
#             thread.join()
#         w.close()
#
#         with ix.reader() as r:
#             assert r.doc_count() == 4
#             assert sorted([d["name"] for d in r.all_stored_fields()]) == domain


def test_fractional_weights():
    ana = analysis.RegexTokenizer(r"\S+") | analysis.DelimitedAttributeFilter()

    schema = fields.Schema(f=fields.TEXT(analyzer=ana))
    with TempIndex(schema, "fractweights") as ix:
        with ix.writer() as w:
            w.add_document(f=u"alfa^0.5 bravo^1.5 charlie^2.0 delta^1.5")

        with ix.searcher() as s:
            wts = []
            for word in s.lexicon("f"):
                p = s.matcher("f", word)
                wts.append(p.weight())
            assert wts == [0.5, 1.5, 2.0, 1.5]


def test_cancel_delete():
    schema = fields.Schema(id=fields.ID(stored=True))
    # Single segment
    with TempIndex(schema, "canceldelete1") as ix:
        with ix.writer() as w:
            for char in u"ABCD":
                w.add_document(id=char)

        w = ix.writer()
        w.delete_document(2)
        w.delete_document(3)
        w.cancel()

        with ix.reader() as r:
            assert not r.is_deleted(2)
            assert not r.is_deleted(3)


def test_add_field():
    schema = fields.Schema(a=fields.TEXT)
    with TempIndex(schema, "addfield") as ix:
        with ix.writer() as w:
            w.add_document(a=u"alfa bravo charlie")
        with ix.writer() as w:
            w.add_field("b", fields.ID(stored=True))
            w.add_field("c*", fields.ID(stored=True), glob=True)
            w.add_document(a=u"delta echo foxtrot", b=u"india", cat=u"juliet")

        with ix.searcher() as s:
            fs = s.document(b=u"india")
            assert fs == {"b": "india", "cat": "juliet"}


def test_add_reader():
    schema = fields.Schema(i=fields.ID(stored=True, unique=True),
                           a=fields.TEXT(stored=True),
                           b=fields.TEXT())
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

        with ix.writer() as w:
            w.delete_by_term("i", "1")
            w.delete_by_term("i", "3")

        with ix.writer() as w:
            w.add_document(i=u"4", a=u"hotel india juliet kilo",
                           b=u"quick rhubarb soggy trap")
            w.add_document(i=u"5", a=u"india juliet kilo lima",
                           b=u"umber violet weird xray")

        with ix.reader() as r:
            assert r.doc_count() == 4

            sfs = list(r.all_stored_fields())
            print(sfs)
            assert sfs == [
                (0, {"i": u"0", "a": u"alfa bravo charlie delta"}),
                (2, {"i": u"2", "a": u"charlie delta echo foxtrot"}),
                (4, {"i": u"4", "a": u"hotel india juliet kilo"}),
                (5, {"i": u"5", "a": u"india juliet kilo lima"}),
            ]

            target = b("alfa bravo charlie delta echo foxtrot hotel india "
                       "juliet kilo lima").split()
            # print(target)
            # print(list(r.lexicon("a")))
            assert list(r.lexicon("a")) == target


# def test_spellable_list():
#     # Make sure a spellable field works with a list of pre-analyzed tokens
#
#     ana = analysis.StemmingAnalyzer()
#     schema = fields.Schema(Location=fields.STORED,Lang=fields.STORED,
#                            Title=fields.TEXT(spelling=True, analyzer=ana))
#     ix = RamStorage().create_index(schema)
#
#     doc = {'Location': '1000/123', 'Lang': 'E',
#            'Title': ['Introduction', 'Numerical', 'Analysis']}
#
#     with ix.writer() as w:
#         w.add_document(**doc)


def test_zero_procs():
    schema = fields.Schema(text=fields.TEXT)
    with TempIndex(schema, "zeroprocs") as ix:
        with ix.writer(procs=0) as w:
            assert isinstance(w, writing.IndexWriter)

        with ix.writer(procs=1) as w:
            assert isinstance(w, writing.IndexWriter)


def test_tidy():
    c = 100
    data = sorted((random.choice("abc"), u(hex(i))) for i in xrange(c))

    schema = fields.Schema(a=fields.TEXT, b=fields.TEXT, c=fields.TEXT)
    with TempIndex(schema, "clean") as ix:
        with ix.writer() as w:
            for fname, tbytes in data:
                w.add_document(**{fname: tbytes})

        with ix.writer() as w:
            for i in xrange(c):
                w.delete_document(i)

        with ix.reader() as r:
            assert list(r.all_terms()) == []
            assert list(r.all_doc_ids()) == []


def test_delete_nonexistant():
    schema = fields.Schema(id=fields.ID(stored=True, sortable=True))
    with TempIndex(schema, "deletenonexist") as ix:
        with ix.writer() as w:
            w.add_document(id=u"foo")

        with ix.writer() as w:
            w.delete_document(100)

