from __future__ import with_statement
import random, threading, time

from whoosh import analysis, fields, formats, reading
from whoosh.compat import b, u, xrange
from whoosh.util.testing import TempIndex

_schema = fields.Schema(f1=fields.KEYWORD(stored=True), f2=fields.KEYWORD,
                        f3=fields.KEYWORD)


def _populate(ix):
    with ix.writer() as w:
        w.add_document(f1=u"A B C", f2=u"1 2 3", f3=u"X Y Z")
        w.add_document(f1=u"D E F", f2=u"4 5 6", f3=u"Q R S")
        w.add_document(f1=u"A E C", f2=u"1 4 6", f3=u"X Q S")
        w.add_document(f1=u"A A A", f2=u"2 3 5", f3=u"Y R Z")
        w.add_document(f1=u"A B", f2=u"1 2", f3=u"X Y")


def _stats(r):
    return [(fname, text, ti.doc_frequency(), ti.weight())
            for (fname, text), ti in r]


def _fstats(r):
    return [(text, ti.doc_frequency(), ti.weight())
            for text, ti in r]


def test_reader():
    target = [("f1", b'A', 4, 6), ("f1", b'B', 2, 2), ("f1", b'C', 2, 2),
              ("f1", b'D', 1, 1), ("f1", b'E', 2, 2), ("f1", b'F', 1, 1),
              ("f2", b'1', 3, 3), ("f2", b'2', 3, 3), ("f2", b'3', 2, 2),
              ("f2", b'4', 2, 2), ("f2", b'5', 2, 2), ("f2", b'6', 2, 2),
              ("f3", b'Q', 2, 2), ("f3", b'R', 2, 2), ("f3", b'S', 2, 2),
              ("f3", b'X', 3, 3), ("f3", b'Y', 3, 3), ("f3", b'Z', 2, 2)]
    target = sorted(target)

    stored = [{"f1": "A B C"}, {"f1": "D E F"}, {"f1": "A E C"},
              {"f1": "A A A"}, {"f1": "A B"}]

    with TempIndex(_schema, "reader") as ix:
        _populate(ix)

        with ix.reader() as r:
            assert list(r.all_stored_fields()) == list(enumerate(stored))
            assert sorted(_stats(r)) == target


def test_term_inspection():
    schema = fields.Schema(t=fields.TEXT(stored=True),
                           c=fields.TEXT)
    with TempIndex(schema, "termintrospect") as ix:
        with ix.writer() as w:
            w.add_document(t=u"My document",
                           c=u"AA AA BB BB CC AA AA AA BB BB CC DD EE EE")
            w.add_document(t=u"My other document",
                           c=u"AA AB BB CC EE EE AX AX DD")

        with ix.reader() as r:
            assert b" ".join(r.lexicon("c")) == b"aa ab ax bb cc dd ee"
            assert b" ".join(r.expand_prefix("c", "a")) == b"aa ab ax"
            assert list(r.all_terms()) == [
                ("c", b"aa"), ("c", b"ab"), ("c", b"ax"), ("c", b"bb"),
                ("c", b"cc"), ("c", b"dd"), ("c", b"ee"),
                ("t", b"document"), ("t", b"my"), ("t", b"other"),
            ]

            assert _fstats(r.iter_field("c")) == [
                (b'aa', 2, 6), (b'ab', 1, 1), (b'ax', 1, 2),
                (b'bb', 2, 5), (b'cc', 2, 3), (b'dd', 2, 2),
                (b'ee', 2, 4)
            ]
            assert _fstats(r.iter_field("c", prefix="c")) == [
                (b'cc', 2, 3)
            ]


def test_stored_fields():
    schema = fields.Schema(a=fields.ID(stored=True), b=fields.STORED,
                           c=fields.KEYWORD, d=fields.TEXT(stored=True))
    with TempIndex(schema, "storedfields") as ix:
        with ix.writer() as w:
            w.add_document(a=u"1", b="a", c=u"zulu", d=u"Alfa")
            w.add_document(a=u"2", b="b", c=u"yankee", d=u"Bravo")
            w.add_document(a=u"3", b="c", c=u"xray", d=u"Charlie")

        with ix.searcher() as s:
            assert s.stored_fields(0) == {"a": u"1", "b": "a", "d": u"Alfa"}
            assert s.stored_fields(2) == {"a": u"3", "b": "c", "d": u"Charlie"}

            assert s.document(a=u"1") == {"a": u"1", "b": "a", "d": u"Alfa"}
            assert s.document(a=u"2") == {"a": u"2", "b": "b", "d": u"Bravo"}


def test_stored_fields2():
    schema = fields.Schema(content=fields.TEXT(stored=True),
                           title=fields.TEXT(stored=True),
                           summary=fields.STORED,
                           path=fields.ID(stored=True))

    storedkeys = ["content", "path", "summary", "title"]
    assert storedkeys == schema.stored_names()

    with TempIndex(schema, "storedfields2") as ix:
        with ix.writer() as w:
            w.add_document(content=u"Content of this document.",
                           title=u"This is the title",
                           summary=u"This is the summary", path=u"/main")
            w.add_document(content=u"Second document.", title=u"Second title",
                           summary=u"Summary numero due", path=u"/second")
            w.add_document(content=u"Third document.", title=u"Title 3",
                           summary=u"Summary treo", path=u"/san")

        with ix.searcher() as s:
            doc = s.document(path="/main")
            assert doc is not None
            assert ([doc[k] for k in sorted(doc.keys())]
                    == ["Content of this document.", "/main",
                        "This is the summary", "This is the title"])


def test_all_stored_fields():
    # all_stored_fields() should yield all stored fields, even for deleted
    # documents

    schema = fields.Schema(a=fields.ID(stored=True), b=fields.STORED)
    with TempIndex(schema, "allstoredfields") as ix:
        with ix.writer() as w:
            w.add_document(a=u"alfa", b=u"bravo")
            w.add_document(a=u"apple", b=u"bear")
            w.add_document(a=u"alpaca", b=u"beagle")
            w.add_document(a=u"aim", b=u"box")

        with ix.writer() as w:
            w.delete_by_term("a", "apple")
            w.delete_by_term("a", "aim")

        with ix.searcher() as s:
            assert s.doc_count() == 2
            sfs = list((sf["a"], sf["b"]) for _, sf in s.all_stored_fields())
            assert sfs == [("alfa", "bravo"), ("alpaca", "beagle")]


def test_unique_id():
    schema = fields.Schema(path=fields.ID(unique=True, stored=True),
                           version=fields.STORED)
    with TempIndex(schema, "firstid") as ix:
        with ix.writer() as w:
            w.add_document(path=u"/a", version=0)
            w.add_document(path=u"/b", version=0)
            w.add_document(path=u"/c", version=0)

        with ix.reader() as r:
            docid = r.unique_id("path", u"/b")
            assert r.stored_fields(docid) == {"path": "/b", "version": 0}

        with ix.writer() as w:
            w.add_document(path=u"/a", version=1)
            w.add_document(path=u"/b", version=1)
            w.add_document(path=u"/d", version=0)

        with ix.reader() as r:
            docid = r.unique_id("path", u"/b")
            assert r.stored_fields(docid) == {"path": "/b", "version": 1}

            docid = r.unique_id("path", u"/d")
            assert r.stored_fields(docid) == {"path": "/d", "version": 0}


class RecoverReader(threading.Thread):
    def __init__(self, ix):
        threading.Thread.__init__(self)
        self.ix = ix

    def run(self):
        for _ in xrange(50):
            r = self.ix.reader()
            r.close()


class RecoverWriter(threading.Thread):
    domain = u"alfa bravo charlie deleta echo foxtrot golf hotel india"
    domain = domain.split()

    def __init__(self, ix):
        threading.Thread.__init__(self)
        self.ix = ix

    def run(self):
        for _ in xrange(10):
            w = self.ix.writer()
            w.add_document(text=random.sample(self.domain, 4))
            w.commit()
            time.sleep(0.01)


def test_delete_recovery():
    schema = fields.Schema(text=fields.TEXT)
    with TempIndex(schema, "delrecover") as ix:
        rw = RecoverWriter(ix)
        rr = RecoverReader(ix)
        rw.start()
        rr.start()
        rw.join()
        rr.join()


def test_nonexclusive_read():
    schema = fields.Schema(text=fields.TEXT)
    with TempIndex(schema, "readlock") as ix:
        for num in u"one two three four five".split():
            with ix.writer() as w:
                w.add_document(text=u"Test document %s" % num)

        def fn():
            for _ in xrange(5):
                r = ix.reader()
                assert list(r.field_terms("text")) == ["document", "five", "four", "one", "test", "three", "two"]
                r.close()

        ths = [threading.Thread(target=fn) for _ in xrange(5)]
        for th in ths:
            th.start()
        for th in ths:
            th.join()


def test_doc_count():
    schema = fields.Schema(id=fields.NUMERIC)
    with TempIndex(schema, "doccount") as ix:
        with ix.writer() as w:
            for i in xrange(10):
                w.add_document(id=i)

        with ix.reader() as r:
            assert r.doc_count() == 10

        with ix.writer() as w:
            w.delete_document(2)
            w.delete_document(4)
            w.delete_document(6)
            w.delete_document(8)

        with ix.reader() as r:
            assert r.doc_count() == 6

        with ix.writer() as w:
            for i in xrange(10, 15):
                w.add_document(id=i)

        with ix.reader() as r:
            assert r.doc_count() == 11

        with ix.writer() as w:
            w.delete_document(10)
            w.delete_document(12)
            w.delete_document(14)

        with ix.reader() as r:
            assert r.doc_count() == 8

