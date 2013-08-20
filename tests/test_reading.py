from __future__ import with_statement
import random, threading, time

from whoosh import analysis, fields, formats, reading
from whoosh.compat import b, u, xrange
from whoosh.reading import SegmentReader
from whoosh.filedb.filestore import RamStorage
from whoosh.util.testing import TempIndex


def _create_index():
    s = fields.Schema(f1=fields.KEYWORD(stored=True),
                      f2=fields.KEYWORD,
                      f3=fields.KEYWORD)
    st = RamStorage()
    ix = st.create_index(s)
    return ix


def _one_segment_index():
    ix = _create_index()
    w = ix.writer()
    w.add_document(f1=u("A B C"), f2=u("1 2 3"), f3=u("X Y Z"))
    w.add_document(f1=u("D E F"), f2=u("4 5 6"), f3=u("Q R S"))
    w.add_document(f1=u("A E C"), f2=u("1 4 6"), f3=u("X Q S"))
    w.add_document(f1=u("A A A"), f2=u("2 3 5"), f3=u("Y R Z"))
    w.add_document(f1=u("A B"), f2=u("1 2"), f3=u("X Y"))
    w.commit()

    return ix


def _multi_segment_index():
    ix = _create_index()
    w = ix.writer()
    w.add_document(f1=u("A B C"), f2=u("1 2 3"), f3=u("X Y Z"))
    w.add_document(f1=u("D E F"), f2=u("4 5 6"), f3=u("Q R S"))
    w.commit()

    w = ix.writer()
    w.add_document(f1=u("A E C"), f2=u("1 4 6"), f3=u("X Q S"))
    w.add_document(f1=u("A A A"), f2=u("2 3 5"), f3=u("Y R Z"))
    w.commit(merge=False)

    w = ix.writer()
    w.add_document(f1=u("A B"), f2=u("1 2"), f3=u("X Y"))
    w.commit(merge=False)

    return ix


def _stats(r):
    return [(fname, text, ti.doc_frequency(), ti.weight())
            for (fname, text), ti in r]


def _fstats(r):
    return [(text, ti.doc_frequency(), ti.weight())
            for text, ti in r]


def test_readers():
    target = [("f1", b('A'), 4, 6), ("f1", b('B'), 2, 2), ("f1", b('C'), 2, 2),
              ("f1", b('D'), 1, 1), ("f1", b('E'), 2, 2), ("f1", b('F'), 1, 1),
              ("f2", b('1'), 3, 3), ("f2", b('2'), 3, 3), ("f2", b('3'), 2, 2),
              ("f2", b('4'), 2, 2), ("f2", b('5'), 2, 2), ("f2", b('6'), 2, 2),
              ("f3", b('Q'), 2, 2), ("f3", b('R'), 2, 2), ("f3", b('S'), 2, 2),
              ("f3", b('X'), 3, 3), ("f3", b('Y'), 3, 3), ("f3", b('Z'), 2, 2)]
    target = sorted(target)

    stored = [{"f1": "A B C"}, {"f1": "D E F"}, {"f1": "A E C"},
              {"f1": "A A A"}, {"f1": "A B"}]

    def t(ix):
        r = ix.reader()
        assert list(r.all_stored_fields()) == stored
        assert sorted(_stats(r)) == target

    ix = _one_segment_index()
    assert len(ix._segments()) == 1
    t(ix)

    ix = _multi_segment_index()
    assert len(ix._segments()) == 3
    t(ix)


def test_term_inspection():
    schema = fields.Schema(title=fields.TEXT(stored=True),
                           content=fields.TEXT)
    st = RamStorage()
    ix = st.create_index(schema)
    writer = ix.writer()
    writer.add_document(title=u("My document"),
                        content=u("AA AA BB BB CC AA AA AA BB BB CC DD EE EE"))
    writer.add_document(title=u("My other document"),
                        content=u("AA AB BB CC EE EE AX AX DD"))
    writer.commit()

    reader = ix.reader()
    assert " ".join(reader.field_terms("content")) == "aa ab ax bb cc dd ee"
    assert list(reader.expand_prefix("content", "a")) == [b('aa'), b('ab'), b('ax')]
    assert set(reader.all_terms()) == set([('content', b('aa')), ('content', b('ab')),
                                           ('content', b('ax')), ('content', b('bb')),
                                           ('content', b('cc')), ('content', b('dd')),
                                           ('content', b('ee')), ('title', b('document')),
                                           ('title', b('my')), ('title', b('other'))])
    # (text, doc_freq, index_freq)
    assert _fstats(reader.iter_field("content")) == [(b('aa'), 2, 6), (b('ab'), 1, 1), (b('ax'), 1, 2),
                                                     (b('bb'), 2, 5), (b('cc'), 2, 3), (b('dd'), 2, 2),
                                                     (b('ee'), 2, 4)]
    assert _fstats(reader.iter_field("content", prefix="c")) == [(b('cc'), 2, 3), (b('dd'), 2, 2), (b('ee'), 2, 4)]
    assert list(reader.most_frequent_terms("content")) == [(6, b('aa')), (5, b('bb')), (4, b('ee')), (3, b('cc')), (2, b('dd'))]
    assert list(reader.most_frequent_terms("content", prefix="a")) == [(6, b('aa')), (2, b('ax')), (1, b('ab'))]
    assert list(reader.most_distinctive_terms("content", 3)) == [(1.3862943611198906, b('ax')), (0.6931471805599453, b('ab')), (0.0, b('ee'))]


def test_vector_postings():
    s = fields.Schema(id=fields.ID(stored=True, unique=True),
                      content=fields.TEXT(vector=formats.Positions()))
    st = RamStorage()
    ix = st.create_index(s)

    writer = ix.writer()
    writer.add_document(id=u('1'),
                        content=u('the quick brown fox jumped over the ' +
                                  'lazy dogs'))
    writer.commit()
    r = ix.reader()

    terms = list(r.vector_as("weight", 0, "content"))
    assert terms == [(u('brown'), 1.0), (u('dogs'), 1.0), (u('fox'), 1.0),
                     (u('jumped'), 1.0), (u('lazy'), 1.0),
                     (u('over'), 1.0), (u('quick'), 1.0)]


def test_stored_fields():
    s = fields.Schema(a=fields.ID(stored=True), b=fields.STORED,
                      c=fields.KEYWORD, d=fields.TEXT(stored=True))
    st = RamStorage()
    ix = st.create_index(s)

    writer = ix.writer()
    writer.add_document(a=u("1"), b="a", c=u("zulu"), d=u("Alfa"))
    writer.add_document(a=u("2"), b="b", c=u("yankee"), d=u("Bravo"))
    writer.add_document(a=u("3"), b="c", c=u("xray"), d=u("Charlie"))
    writer.commit()

    with ix.searcher() as sr:
        assert sr.stored_fields(0) == {"a": u("1"), "b": "a", "d": u("Alfa")}
        assert sr.stored_fields(2) == {"a": u("3"), "b": "c", "d": u("Charlie")}

        assert sr.document(a=u("1")) == {"a": u("1"), "b": "a", "d": u("Alfa")}
        assert sr.document(a=u("2")) == {"a": u("2"), "b": "b", "d": u("Bravo")}


def test_stored_fields2():
    schema = fields.Schema(content=fields.TEXT(stored=True),
                           title=fields.TEXT(stored=True),
                           summary=fields.STORED,
                           path=fields.ID(stored=True))

    storedkeys = ["content", "path", "summary", "title"]
    assert storedkeys == schema.stored_names()

    ix = RamStorage().create_index(schema)

    writer = ix.writer()
    writer.add_document(content=u("Content of this document."),
                        title=u("This is the title"),
                        summary=u("This is the summary"), path=u("/main"))
    writer.add_document(content=u("Second document."), title=u("Second title"),
                        summary=u("Summary numero due"), path=u("/second"))
    writer.add_document(content=u("Third document."), title=u("Title 3"),
                        summary=u("Summary treo"), path=u("/san"))
    writer.commit()

    with ix.searcher() as s:
        doc = s.document(path="/main")
        assert doc is not None
        assert ([doc[k] for k in sorted(doc.keys())]
                == ["Content of this document.", "/main",
                    "This is the summary", "This is the title"])

    ix.close()


def test_all_stored_fields():
    # all_stored_fields() should yield all stored fields, even for deleted
    # documents

    schema = fields.Schema(a=fields.ID(stored=True), b=fields.STORED)
    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        w.add_document(a=u("alfa"), b=u("bravo"))
        w.add_document(a=u("apple"), b=u("bear"))
        w.add_document(a=u("alpaca"), b=u("beagle"))
        w.add_document(a=u("aim"), b=u("box"))

    w = ix.writer()
    w.delete_by_term("a", "apple")
    w.delete_by_term("a", "aim")
    w.commit(merge=False)

    with ix.searcher() as s:
        assert s.doc_count_all() == 4
        assert s.doc_count() == 2
        sfs = list((sf["a"], sf["b"]) for sf in s.all_stored_fields())
        assert sfs == [("alfa", "bravo"), ("apple", "bear"),
                       ("alpaca", "beagle"), ("aim", "box")]


def test_first_id():
    schema = fields.Schema(path=fields.ID(stored=True))
    ix = RamStorage().create_index(schema)

    w = ix.writer()
    w.add_document(path=u("/a"))
    w.add_document(path=u("/b"))
    w.add_document(path=u("/c"))
    w.commit()

    r = ix.reader()
    docid = r.first_id("path", u("/b"))
    assert r.stored_fields(docid) == {"path": "/b"}

    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(path=u("/a"))
    w.add_document(path=u("/b"))
    w.add_document(path=u("/c"))
    w.commit(merge=False)

    w = ix.writer()
    w.add_document(path=u("/d"))
    w.add_document(path=u("/e"))
    w.add_document(path=u("/f"))
    w.commit(merge=False)

    w = ix.writer()
    w.add_document(path=u("/g"))
    w.add_document(path=u("/h"))
    w.add_document(path=u("/i"))
    w.commit(merge=False)

    r = ix.reader()
    assert r.__class__ == reading.MultiReader
    docid = r.first_id("path", u("/e"))
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
    domain = u("alfa bravo charlie deleta echo foxtrot golf hotel india")
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
        for num in u("one two three four five").split():
            w = ix.writer()
            w.add_document(text=u("Test document %s") % num)
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
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    for i in xrange(10):
        w.add_document(id=i)
    w.commit()

    r = ix.reader()
    assert r.doc_count() == 10
    assert r.doc_count_all() == 10

    w = ix.writer()
    w.delete_document(2)
    w.delete_document(4)
    w.delete_document(6)
    w.delete_document(8)
    w.commit()

    r = ix.reader()
    assert r.doc_count() == 6
    assert r.doc_count_all() == 10

    w = ix.writer()
    for i in xrange(10, 15):
        w.add_document(id=i)
    w.commit(merge=False)

    r = ix.reader()
    assert r.doc_count() == 11
    assert r.doc_count_all() == 15

    w = ix.writer()
    w.delete_document(10)
    w.delete_document(12)
    w.delete_document(14)
    w.commit(merge=False)

    r = ix.reader()
    assert r.doc_count() == 8
    assert r.doc_count_all() == 15

    ix.optimize()
    r = ix.reader()
    assert r.doc_count() == 8
    assert r.doc_count_all() == 8


def test_reader_subclasses():
    from whoosh.util.testing import check_abstract_methods

    check_abstract_methods(reading.IndexReader, SegmentReader)
    check_abstract_methods(reading.IndexReader, reading.MultiReader)
    check_abstract_methods(reading.IndexReader, reading.EmptyReader)
