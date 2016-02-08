from __future__ import with_statement
import random, threading, time

from whoosh import fields, reading
from whoosh.compat import xrange
from whoosh.util.testing import TempIndex


_simple_schema = fields.Schema(
    f1=fields.KEYWORD(stored=True, scorable=True, lowercase=False),
    f2=fields.KEYWORD(scorable=True, lowercase=False),
    f3=fields.KEYWORD(scorable=True, lowercase=False)
)


def _stats(r):
    return [(fname, text, ti.doc_frequency(), ti.weight())
            for (fname, text), ti in r]


def test_readers():
    target = [("f1", b'A', 4, 6.0), ("f1", b'B', 2, 2.0), ("f1", b'C', 2, 2.0),
              ("f1", b'D', 1, 1.0), ("f1", b'E', 2, 2.0), ("f1", b'F', 1, 1.0),
              ("f2", b'1', 3, 3.0), ("f2", b'2', 3, 3.0), ("f2", b'3', 2, 2.0),
              ("f2", b'4', 2, 2.0), ("f2", b'5', 2, 2.0), ("f2", b'6', 2, 2.0),
              ("f3", b'Q', 2, 2.0), ("f3", b'R', 2, 2.0), ("f3", b'S', 2, 2.0),
              ("f3", b'X', 3, 3.0), ("f3", b'Y', 3, 3.0), ("f3", b'Z', 2, 2.0)]
    target = sorted(target)

    stored = [{"f1": "A B C"}, {"f1": "D E F"}, {"f1": "A E C"},
              {"f1": "A A A"}, {"f1": "A B"}]

    with TempIndex(_simple_schema) as ix:
        with ix.writer() as w:
            w.add_document(f1=u"A B C", f2=u"1 2 3", f3=u"X Y Z")
            w.add_document(f1=u"D E F", f2=u"4 5 6", f3=u"Q R S")
            w.add_document(f1=u"A E C", f2=u"1 4 6", f3=u"X Q S")
            w.add_document(f1=u"A A A", f2=u"2 3 5", f3=u"Y R Z")
            w.add_document(f1=u"A B", f2=u"1 2", f3=u"X Y")

        assert len(ix.segments()) == 1
        with ix.reader() as r:
            assert [d for _, d in r.iter_docs()] == stored
            assert sorted(_stats(r)) == target

    with TempIndex(_simple_schema) as ix:
        with ix.writer() as w:
            w.add_document(f1=u"A B C", f2=u"1 2 3", f3=u"X Y Z")
            w.add_document(f1=u"D E F", f2=u"4 5 6", f3=u"Q R S")

        with ix.writer() as w:
            w.merge = False
            w.add_document(f1=u"A E C", f2=u"1 4 6", f3=u"X Q S")
            w.add_document(f1=u"A A A", f2=u"2 3 5", f3=u"Y R Z")

        with ix.writer() as w:
            w.merge = False
            w.add_document(f1=u"A B", f2=u"1 2", f3=u"X Y")

        assert len(ix.segments()) == 3
        with ix.reader() as r:
            assert [d for _, d in r.iter_docs()] == stored
            assert sorted(_stats(r)) == target


def test_term_inspection():
    schema = fields.Schema(title=fields.TEXT(stored=True),
                           content=fields.TEXT)

    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(title=u"My document",
                           content=u"AA AA BB BB CC AA AA AA BB BB CC DD EE EE")
            w.add_document(title=u"My other document",
                           content=u"AA AB BB CC EE EE AX AX DD")

        with ix.reader() as r:
            assert " ".join(r.field_terms("content")) == "aa ab ax bb cc dd ee"

            swa = list(r.expand_prefix("content", "a"))
            assert swa == [b'aa', b'ab', b'ax']

            all_terms = [
                ('content', b'aa'), ('content', b'ab'),
                ('content', b'ax'), ('content', b'bb'),
                ('content', b'cc'), ('content', b'dd'),
                ('content', b'ee'), ('title', b'document'),
                ('title', b'my'), ('title', b'other')
            ]
            assert set(r.all_terms()) == set(all_terms)

            def _fstats(items):
                return [(text, ti.doc_frequency(), ti.weight())
                        for text, ti in items]

            # (text, doc_freq, index_freq)
            assert _fstats(r.iter_field("content")) == [
                (b'aa', 2, 6), (b'ab', 1, 1), (b'ax', 1, 2), (b'bb', 2, 5),
                (b'cc', 2, 3), (b'dd', 2, 2), (b'ee', 2, 4)
            ]
            assert _fstats(r.iter_field("content", prefix="c")) == [
                (b'cc', 2, 3),
            ]
            # assert list(r.most_frequent_terms("content")) == [
            #     (6, b'aa'), (5, b'bb'), (4, b'ee'), (3, b'cc'),
            #     (2, b'dd')
            # ]
            # assert list(r.most_frequent_terms("content", prefix="a")) == [
            #     (6, b'aa'), (2, b'ax'), (1, b'ab')
            # ]
            # assert list(r.most_distinctive_terms("content", 3)) == [
            #     (1.3862943611198906, b'ax'),
            #     (0.6931471805599453, b'ab'),
            #     (0.0, b'ee')
            # ]


def test_vector_postings():
    s = fields.Schema(id=fields.ID(stored=True, unique=True),
                      content=fields.TEXT(vector=True))
    with TempIndex(s) as ix:
        with ix.writer() as w:
            w.add_document(
                id=u'1',
                content=u'the quick brown fox jumped over the lazy dogs'
            )

        with ix.reader() as r:
            v = r.vector(0, "content")
            from_bytes = r.schema["content"].from_bytes
            items = [(from_bytes(tbytes), weight) for tbytes, weight
                     in v.terms_and_weights()]

            assert items == [(u'brown', 1.0), (u'dogs', 1.0), (u'fox', 1.0),
                             (u'jumped', 1.0), (u'lazy', 1.0), (u'over', 1.0),
                             (u'quick', 1.0)]


def test_stored_fields():
    s = fields.Schema(a=fields.ID(stored=True), b=fields.STORED,
                      c=fields.KEYWORD, d=fields.TEXT(stored=True))
    with TempIndex(s) as ix:
        with ix.writer() as w:
            w.add_document(a=u"1", b="a", c=u"zulu", d=u"Alfa")
            w.add_document(a=u"2", b="b", c=u"yankee", d=u"Bravo")
            w.add_document(a=u"3", b="c", c=u"xray", d=u"Charlie")

        with ix.searcher() as sr:
            assert sr.stored_fields(0) == {
                "a": u"1", "b": "a", "d": u"Alfa"
            }
            assert sr.stored_fields(2) == {
                "a": u"3", "b": "c", "d": u"Charlie"
            }

            assert sr.document(a=u"1") == {
                "a": u"1", "b": "a", "d": u"Alfa"
            }
            assert sr.document(a=u"2") == {
                "a": u"2", "b": "b", "d": u"Bravo"
            }


def test_stored_fields2():
    schema = fields.Schema(content=fields.TEXT(stored=True),
                           title=fields.TEXT(stored=True),
                           summary=fields.STORED,
                           path=fields.ID(stored=True))

    storedkeys = ["content", "path", "summary", "title"]
    assert storedkeys == schema.stored_names()

    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(content=u"Content of this document.",
                           title=u"This is the title",
                           summary=u"This is the summary", path=u"/main")
            w.add_document(content=u"Second document.",
                           title=u"Second title",
                           summary=u"Summary numero due", path=u"/second")
            w.add_document(content=u"Third document.", title=u"Title 3",
                           summary=u"Summary treo", path=u"/san")

        with ix.searcher() as s:
            doc = s.document(path="/main")
            assert doc is not None
            assert ([doc[k] for k in sorted(doc.keys())] ==
                    ["Content of this document.", "/main",
                     "This is the summary", "This is the title"])


def test_all_stored_fields():
    # all_stored_fields() should yield all stored fields, even for deleted
    # documents

    schema = fields.Schema(a=fields.ID(stored=True), b=fields.STORED)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(a=u"alfa", b=u"bravo")
            w.add_document(a=u"apple", b=u"bear")
            w.add_document(a=u"alpaca", b=u"beagle")
            w.add_document(a=u"aim", b=u"box")

        with ix.writer() as w:
            w.merge = False
            w.delete_by_term("a", "apple")
            w.delete_by_term("a", "aim")

        with ix.searcher() as s:
            assert s.doc_count_all() == 4
            assert s.doc_count() == 2
            sfs = [(d["a"], d["b"]) for _, d in s.reader().iter_docs()]
            assert sfs == [("alfa", "bravo"), ("alpaca", "beagle")]


def test_first_id():
    schema = fields.Schema(path=fields.ID(stored=True))
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(path=u"/a")
            w.add_document(path=u"/b")
            w.add_document(path=u"/c")

        with ix.reader() as r:
            docid = r.first_id("path", u"/b")
            assert r.stored_fields(docid) == {"path": "/b"}

    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(path=u"/a")
            w.add_document(path=u"/b")
            w.add_document(path=u"/c")

        with ix.writer() as w:
            w.merge = False
            w.add_document(path=u"/d")
            w.add_document(path=u"/e")
            w.add_document(path=u"/f")

        with ix.writer() as w:
            w.merge = False
            w.add_document(path=u"/g")
            w.add_document(path=u"/h")
            w.add_document(path=u"/i")

        with ix.reader() as r:
            assert r.__class__ == reading.MultiReader
            docid = r.first_id("path", u"/e")
            assert r.stored_fields(docid) == {"path": "/e"}


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
            w = ix.writer()
            w.add_document(text=u"Test document %s" % num)
            w.commit(merge=False)

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
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            for i in xrange(10):
                w.add_document(id=i)

        with ix.reader() as r:
            assert r.doc_count() == 10
            assert r.doc_count_all() == 10

        with ix.writer() as w:
            w.delete_by_term("id", 2)
            w.delete_by_term("id", 4)
            w.delete_by_term("id", 6)
            w.delete_by_term("id", 8)

        with ix.reader() as r:
            assert r.doc_count() == 6
            assert r.doc_count_all() == 10

        with ix.writer() as w:
            w.merge = False
            for i in xrange(10, 15):
                w.add_document(id=i)

        with ix.reader() as r:
            assert r.doc_count() == 11
            assert r.doc_count_all() == 15

        with ix.writer() as w:
            w.merge = False
            w.delete_by_term("id", 10)
            w.delete_by_term("id", 12)
            w.delete_by_term("id", 14)

        with ix.reader() as r:
            assert r.doc_count() == 8
            assert r.doc_count_all() == 15

        ix.optimize()
        with ix.reader() as r:
            assert r.doc_count() == 8
            assert r.doc_count_all() == 8


def test_cursor():
    schema = fields.Schema(text=fields.TEXT)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(text=u"papa quebec romeo sierra tango")
            w.add_document(text=u"foxtrot golf hotel india juliet")
            w.add_document(text=u"alfa bravo charlie delta echo")
            w.add_document(text=u"uniform victor whiskey x-ray")
            w.add_document(text=u"kilo lima mike november oskar")
            w.add_document(text=u"charlie alfa alfa bravo bravo bravo")

        with ix.reader() as r:
            cur = r.cursor("text")
            assert cur.text() == "alfa"
            cur.next()
            assert cur.text() == "bravo"

            cur.seek(b"inc")
            assert cur.text() == "india"

            cur.first()
            assert cur.text() == "alfa"

            cur.seek(b"zulu")
            # assert cur.text() is None
            assert not cur.is_valid()

            cur.seek(b"a")
            assert cur.text() == "alfa"
            assert cur.term_info().weight() == 3

            cur.next()
            assert cur.text() == "bravo"
            assert cur.term_info().weight() == 4

            cur.next()
            assert cur.text() == "charlie"
            assert cur.term_info().weight() == 2
