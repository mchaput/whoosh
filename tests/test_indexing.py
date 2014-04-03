from __future__ import with_statement
import random
from collections import defaultdict
from datetime import datetime

import pytest

from whoosh import analysis, fields, index, qparser, query
from whoosh.codec import default_codec
from whoosh.compat import b, u, xrange, text_type, PY3, permutations
from whoosh.writing import IndexingError
from whoosh.util.numeric import length_to_byte, byte_to_length
from whoosh.util.testing import TempDir, TempDB, TempIndex


def test_creation():
    s = fields.Schema(content=fields.TEXT(phrase=True),
                      title=fields.TEXT(stored=True),
                      path=fields.ID(stored=True),
                      tags=fields.KEYWORD(stored=True),
                      quick=fields.NGRAM,
                      note=fields.STORED)

    with TempDB("creation") as db:
        ix = index.Index.create(db, default_codec(), s)

        with ix.writer() as w:
            w.add_document(title=u"First",
                           content=u"This is the first document",
                           path=u"/a", tags=u"first second third",
                           quick=u"First document",
                           note=u"This is the first document")
            w.add_document(content=u"Let's try this again", title=u"Second",
                           path=u"/b", tags=u"Uno Dos Tres",
                           quick=u"Second document",
                           note=u"This is the second document")


def test_empty_commit():
    s = fields.Schema(id=fields.ID(stored=True))
    with TempIndex(s, "emptycommit") as ix:
        w = ix.writer()
        w.add_document(id=u"1")
        w.add_document(id=u"2")
        w.add_document(id=u"3")
        w.commit()

        w = ix.writer()
        w.commit()


def test_version():
    from whoosh import __version__

    schema = fields.Schema(key=fields.KEYWORD)
    with TempDir("version") as dirpath:
        ix = index.create_in(dirpath, schema)
        assert ix.is_empty()

        v = ix.version()
        assert v == __version__

        with ix.writer() as w:
            w.add_document(key=u"alfa")

        assert not ix.is_empty()


def test_simple_indexing():
    schema = fields.Schema(text=fields.TEXT, id=fields.STORED)
    domain = (u"alfa", u"bravo", u"charlie", u"delta", u"echo",
              u"foxtrot", u"golf", u"hotel", u"india", u"juliet",
              u"kilo", u"lima", u"mike", u"november")
    docs = defaultdict(list)
    with TempIndex(schema, "simple") as ix:
        with ix.writer() as w:
            for i in xrange(100):
                smp = random.sample(domain, 5)
                for word in smp:
                    docs[word].append(i)
                w.add_document(text=u" ".join(smp), id=i)

        with ix.searcher() as s:
            for word in domain:
                rset = sorted([hit["id"] for hit
                               in s.search(query.Term("text", word),
                                           limit=None)])
                assert rset == docs[word]


def test_integrity():
    s = fields.Schema(name=fields.TEXT, value=fields.TEXT)
    with TempIndex(s, "integrity") as ix:
        with ix.writer() as w:
            w.add_document(name=u"Yellow brown",
                           value=u"Blue red green purple?")
            w.add_document(name=u"Alpha beta",
                           value=u"Gamma delta epsilon omega.")

        with ix.writer() as w:
            w.add_document(name=u"One two", value=u"Three four five.")

        with ix.reader() as r:
            assert r.doc_count() == 3
            target = b"alpha beta brown one two yellow".split()
            assert list(r.lexicon("name")) == target


def test_lengths():
    s = fields.Schema(f1=fields.KEYWORD(stored=True, scorable=True),
                      f2=fields.KEYWORD(stored=True, scorable=True))
    with TempIndex(s, "testlengths") as ix:
        w = ix.writer()
        items = u"ABCDEFG"
        from itertools import cycle, islice
        lengths = [10, 20, 2, 102, 45, 3, 420, 2]
        for length in lengths:
            w.add_document(f2=u" ".join(islice(cycle(items), length)))
        w.commit()

        with ix.reader() as r:
            for item in items:
                m = r.matcher("f2", item.encode("utf8"))
                while m.is_active():
                    i = m.id()
                    length = m.length()
                    assert length == byte_to_length(length_to_byte(lengths[i]))
                    m.next()


def test_many_lengths():
    domain = u"alfa bravo charlie delta echo".split()
    schema = fields.Schema(text=fields.TEXT)
    with TempIndex(schema, "manylens") as ix:
        with ix.writer() as w:
            for i, word in enumerate(domain):
                length = (i + 1) ** 6
                text = ("%s " % word) * length
                w.add_document(text=text)

        with ix.searcher() as s:
            for i, word in enumerate(domain):
                termbytes = word.encode("utf8")
                ti = s.term_info("text", termbytes)
                target = byte_to_length(length_to_byte((i + 1) ** 6))
                assert ti.min_length() == target
                assert ti.max_length() == target


def test_frequency_keyword():
    s = fields.Schema(content=fields.KEYWORD)
    with TempIndex(s, "keywordfreq") as ix:
        with ix.writer() as w:
            w.add_document(content=u"A B C D E")
            w.add_document(content=u"B B B B C D D")
            w.add_document(content=u"D E F")

        with ix.reader() as r:
            assert r.doc_frequency("content", b"B") == 2
            assert r.frequency("content", b"B") == 5
            assert r.doc_frequency("content", b"E") == 2
            assert r.frequency("content", b"E") == 2
            assert r.doc_frequency("content", b"A") == 1
            assert r.frequency("content", b"A") == 1
            assert r.doc_frequency("content", b"D") == 3
            assert r.frequency("content", b"D") == 4
            assert r.doc_frequency("content", b"F") == 1
            assert r.frequency("content", b"F") == 1
            assert r.doc_frequency("content", b"Z") == 0
            assert r.frequency("content", b"Z") == 0

            stats = [(fname, text, ti.doc_frequency(), ti.weight())
                     for (fname, text), ti in r]

            assert stats == [("content", b"A", 1, 1),
                             ("content", b"B", 2, 5),
                             ("content", b"C", 2, 2),
                             ("content", b"D", 3, 4),
                             ("content", b"E", 2, 2),
                             ("content", b"F", 1, 1)]


def test_frequency_text():
    s = fields.Schema(content=fields.KEYWORD)
    with TempIndex(s, "textfreq") as ix:
        with ix.writer() as w:
            w.add_document(content=u"alfa bravo charlie delta echo")
            w.add_document(content=u"bravo bravo bravo bravo charlie delta delta")
            w.add_document(content=u"delta echo foxtrot")

        with ix.reader() as r:
            assert r.doc_frequency("content", b"bravo") == 2
            assert r.frequency("content", b"bravo") == 5
            assert r.doc_frequency("content", b"echo") == 2
            assert r.frequency("content", b"echo") == 2
            assert r.doc_frequency("content", b"alfa") == 1
            assert r.frequency("content", b"alfa") == 1
            assert r.doc_frequency("content", b"delta") == 3
            assert r.frequency("content", b"delta") == 4
            assert r.doc_frequency("content", b"foxtrot") == 1
            assert r.frequency("content", b"foxtrot") == 1
            assert r.doc_frequency("content", b"zulu") == 0
            assert r.frequency("content", b"zulu") == 0

            stats = [(fname, text, ti.doc_frequency(), ti.weight())
                     for (fname, text), ti in r]

            assert stats == [("content", b"alfa", 1, 1),
                             ("content", b"bravo", 2, 5),
                             ("content", b"charlie", 2, 2),
                             ("content", b"delta", 3, 4),
                             ("content", b"echo", 2, 2),
                             ("content", b"foxtrot", 1, 1)]


def test_deletion():
    s = fields.Schema(key=fields.ID, name=fields.TEXT, value=fields.TEXT)
    with TempIndex(s, "deletion") as ix:
        w = ix.writer()
        w.add_document(key=u"A", name=u"Yellow brown",
                       value=u"Blue red green purple?")
        w.add_document(key=u"B", name=u"Alpha beta",
                       value=u"Gamma delta epsilon omega.")
        w.add_document(key=u"C", name=u"One two",
                       value=u"Three four five.")
        w.commit()

        with ix.writer() as w:
            assert w.delete_by_term("key", u"B") == 1

        with ix.reader() as r:
            assert r.doc_count() == 2

        with ix.writer() as w:
            w.add_document(key=u"A", name=u"Yellow brown",
                           value=u"Blue red green purple?")
            w.add_document(key=u"B", name=u"Alpha beta",
                           value=u"Gamma delta epsilon omega.")
            w.add_document(key=u"C", name=u"One two",
                           value=u"Three four five.")

        with ix.writer() as w:
            assert w.delete_by_term("key", u"B") == 1

        with ix.reader() as tr:
            assert ix.doc_count() == 4
            assert b" ".join(tr.lexicon("name")) == b"brown one two yellow"


def test_writer_reuse():
    s = fields.Schema(key=fields.ID)
    with TempIndex(s, "reuse") as ix:
        with ix.writer() as w:
            w.add_document(key=u"A")
            w.add_document(key=u"B")
            w.add_document(key=u"C")

        # You can't re-use a commited/canceled writer
        pytest.raises(ValueError, w.add_document, key=u"D")
        pytest.raises(ValueError, w.update_document, key=u"B")
        pytest.raises(ValueError, w.delete_document, 0)
        pytest.raises(ValueError, w.add_reader, None)
        pytest.raises(ValueError, w.add_field, "name", fields.ID)
        pytest.raises(ValueError, w.remove_field, "key")
        pytest.raises(ValueError, w.searcher)


def test_update():
    # Test update with multiple unique keys
    SAMPLE_DOCS = [{"id": u"test1", "path": u"/test/1",
                    "text": u"Hello"},
                   {"id": u"test2", "path": u"/test/2",
                    "text": u"There"},
                   {"id": u"test3", "path": u"/test/3",
                    "text": u"Reader"},
                   ]

    schema = fields.Schema(id=fields.ID(unique=True, stored=True),
                           path=fields.ID(unique=True, stored=True),
                           text=fields.TEXT)

    with TempIndex(schema, "update") as ix:
        with ix.writer() as w:
            for doc in SAMPLE_DOCS:
                w.add_document(**doc)

        with ix.writer() as w:
            w.update_document(id=u"test2", path=u"test/1",
                              text=u"Replacement")


def test_update2():
    schema = fields.Schema(key=fields.ID(unique=True, stored=True),
                           p=fields.ID(stored=True))
    with TempIndex(schema, "update2") as ix:
        nums = list(range(21))
        random.shuffle(nums)
        for i, n in enumerate(nums):
            w = ix.writer()
            w.update_document(key=text_type(n % 10), p=text_type(i))
            w.commit()

        with ix.searcher() as s:
            results = [d["key"] for _, d in s.all_stored_fields()]
            results = " ".join(sorted(results))
            assert results == "0 1 2 3 4 5 6 7 8 9"


def test_update_numeric():
    schema = fields.Schema(num=fields.NUMERIC(unique=True, stored=True),
                           text=fields.ID(stored=True))
    with TempIndex(schema, "updatenum") as ix:
        nums = list(range(5)) * 3
        random.shuffle(nums)
        for num in nums:
            with ix.writer() as w:
                w.update_document(num=num, text=text_type(num))

        with ix.searcher() as s:
            results = [d["text"] for _, d in s.all_stored_fields()]
            results = " ".join(sorted(results))
            assert results == "0 1 2 3 4"


def test_reindex():
    SAMPLE_DOCS = [
        {'id': u'test1',
         'text': u'This is a document. Awesome, is it not?'},
        {'id': u'test2', 'text': u'Another document. Astounding!'},
        {'id': u'test3',
         'text': u('A fascinating article on the behavior of domestic '
                   'steak knives.')},
    ]

    schema = fields.Schema(text=fields.TEXT(stored=True),
                           id=fields.ID(unique=True, stored=True))
    with TempIndex(schema, "reindex") as ix:
        def reindex():
            writer = ix.writer()
            for doc in SAMPLE_DOCS:
                writer.update_document(**doc)
            writer.commit()

        reindex()
        assert ix.doc_count() == 3
        reindex()
        assert ix.doc_count() == 3


def test_noscorables1():
    values = [u"alfa", u"bravo", u"charlie", u"delta", u"echo",
              u"foxtrot", u"golf", u"hotel", u"india", u"juliet",
              u"kilo", u"lima"]
    from random import choice, sample, randint

    times = 1000

    schema = fields.Schema(id=fields.ID, tags=fields.KEYWORD)
    with TempIndex(schema, "noscorables1") as ix:
        w = ix.writer()
        for _ in xrange(times):
            w.add_document(id=choice(values),
                           tags=u" ".join(sample(values, randint(2, 7))))
        w.commit()

        with ix.searcher() as s:
            s.search(query.Term("id", "bravo"))


def test_noscorables2():
    schema = fields.Schema(field=fields.ID)
    with TempIndex(schema, "noscorables2") as ix:
        writer = ix.writer()
        writer.add_document(field=u'foo')
        writer.commit()


def test_multi():
    schema = fields.Schema(id=fields.ID(stored=True),
                           content=fields.KEYWORD(stored=True))
    with TempIndex(schema, "multi") as ix:
        with ix.writer() as w:
            # Deleted 1
            w.add_document(id=u"1", content=u"alfa bravo charlie")
            # Deleted 1
            w.add_document(id=u"2", content=u"bravo charlie delta echo")
            # Deleted 2
            w.add_document(id=u"3", content=u"charlie delta echo foxtrot")
            w.commit()

        with ix.writer() as w:
            w.delete_by_term("id", "1")
            w.delete_by_term("id", "2")
            w.add_document(id=u"4", content=u"apple bear cherry donut")
            w.add_document(id=u"5", content=u"bear cherry donut eggs")
            # Deleted 2
            w.add_document(id=u"6", content=u"delta echo foxtrot golf")
            # no d
            w.add_document(id=u"7", content=u"echo foxtrot golf hotel")

        with ix.writer() as w:
            w.delete_by_term("id", "3")
            w.delete_by_term("id", "6")
            w.add_document(id=u"8", content=u"cherry donut eggs falafel")
            w.add_document(id=u"9", content=u"donut eggs falafel grape")
            w.add_document(id=u"A", content=u" foxtrot golf hotel india")

        with ix.searcher() as s:
            assert s.doc_count() == 6

            r = s.search(query.Prefix("content", b"d"), optimize=False)
            assert sorted([d["id"] for d in r]) == ["4", "5", "8", "9"]

            r = s.search(query.Prefix("content", b"d"))
            assert sorted([d["id"] for d in r]) == ["4", "5", "8", "9"]

            r = s.search(query.Prefix("content", b"d"), limit=None)
            assert sorted([d["id"] for d in r]) == ["4", "5", "8", "9"]


def test_deleteall():
    schema = fields.Schema(text=fields.TEXT)
    with TempIndex(schema, "deleteall") as ix:
        with ix.writer() as w:
            domain = u"alfa bravo charlie".split()
            for i, ls in enumerate(permutations(domain)):
                w.add_document(text=u" ".join(ls))

        with ix.reader() as r:
            doccount = r.doc_count()

        with ix.writer() as w:
            # This is just a test, don't use this method to delete all docs IRL!
            for docnum in xrange(doccount):
                w.delete_document(docnum)

        with ix.searcher() as s:
            assert s.doc_count() == 0
            r = s.search(query.Or([query.Term("text", u"alfa"),
                                   query.Term("text", u"bravo")]))
            assert r.total_length() == 0


def test_simple_stored():
    schema = fields.Schema(a=fields.ID(stored=True), b=fields.ID(stored=False))
    with TempIndex(schema, "simplestored") as ix:
        with ix.writer() as w:
            w.add_document(a=u"alfa", b=u"bravo")
        with ix.searcher() as s:
            sf = s.stored_fields(0)
            assert sf == {"a": "alfa"}


def test_single():
    schema = fields.Schema(id=fields.ID(stored=True), text=fields.TEXT)
    with TempIndex(schema, "single") as ix:
        w = ix.writer()
        w.add_document(id=u"1", text=u"alfa")
        w.commit()

        with ix.searcher() as s:
            assert ("text", b"alfa") in s.reader()
            assert list(s.documents(id="1")) == [{"id": "1"}]
            assert list(s.documents(text="alfa")) == [{"id": "1"}]
            assert list(s.all_stored_fields()) == [(0, {"id": "1"})]


def test_indentical_fields():
    schema = fields.Schema(id=fields.STORED,
                           f1=fields.TEXT, f2=fields.TEXT, f3=fields.TEXT)
    with TempIndex(schema, "identifields") as ix:
        w = ix.writer()
        w.add_document(id=1, f1=u"alfa", f2=u"alfa", f3=u"alfa")
        w.commit()

        with ix.searcher() as s:
            assert list(s.lexicon("f1")) == [b"alfa"]
            assert list(s.lexicon("f2")) == [b"alfa"]
            assert list(s.lexicon("f3")) == [b"alfa"]
            assert list(s.documents(f1="alfa")) == [{"id": 1}]
            assert list(s.documents(f2="alfa")) == [{"id": 1}]
            assert list(s.documents(f3="alfa")) == [{"id": 1}]


def test_multivalue():
    ana = analysis.StemmingAnalyzer()
    schema = fields.Schema(id=fields.STORED, date=fields.DATETIME,
                           num=fields.NUMERIC,
                           txt=fields.TEXT(analyzer=ana))
    with TempIndex(schema, "multival") as ix:
        with ix.writer() as w:
            w.add_document(id=1, date=datetime(2001, 1, 1), num=5)
            w.add_document(id=2,
                           date=[datetime(2002, 2, 2), datetime(2003, 3, 3)],
                           num=[1, 2, 3, 12])
            w.add_document(txt=u"a b c".split())

        with ix.reader() as r:
            assert ("num", 3) in r
            assert ("date", datetime(2003, 3, 3)) in r
            assert b" ".join(r.lexicon("txt")) == b"a b c"


def test_multi_language():
    # Analyzer for English
    ana_eng = analysis.StemmingAnalyzer()

    # analyzer for Pig Latin
    def stem_piglatin(w):
        if w.endswith("ay"):
            w = w[:-2]
        return w
    ana_pig = analysis.StemmingAnalyzer(stoplist=["nday", "roay"],
                                        stemfn=stem_piglatin)

    # Dictionary mapping languages to analyzers
    analyzers = {"eng": ana_eng, "pig": ana_pig}

    # Fake documents
    corpus = [(u"eng", u"Such stuff as dreams are made on"),
              (u"pig", u"Otay ebay, roay otnay otay ebay")]

    schema = fields.Schema(content=fields.TEXT(stored=True),
                           lang=fields.ID(stored=True))
    with TempIndex(schema, "multilang") as ix:
        with ix.writer() as w:
            for doclang, content in corpus:
                ana = analyzers[doclang]
                # "Pre-analyze" the field into token strings
                words = [token.text for token in ana(content)]
                # Note we store the original value but index the pre-analyzed
                # words
                w.add_document(lang=doclang, content=words,
                               _stored_content=content)

        with ix.searcher() as s:
            schema = s.schema

            # Modify the schema to fake the correct analyzer for the language
            # we're searching in
            schema["content"].analyzer = analyzers["eng"]

            qp = qparser.QueryParser("content", schema)
            q = qp.parse("dreaming")
            r = s.search(q)
            assert r.total_length() == 1
            assert r[0]["content"] == "Such stuff as dreams are made on"

            schema["content"].analyzer = analyzers["pig"]
            qp = qparser.QueryParser("content", schema)
            q = qp.parse("otnay")
            r = s.search(q)
            assert r.total_length() == 1
            assert r[0]["content"] == "Otay ebay, roay otnay otay ebay"


def test_doc_boost():
    schema = fields.Schema(id=fields.STORED, a=fields.TEXT, b=fields.TEXT)
    with TempIndex(schema, "docboost") as ix:
        with ix.writer() as w:
            w.add_document(id=0, a=u"alfa alfa alfa", b=u"bravo")  # 3.0
            w.add_document(id=1, a=u"alfa", b=u"bear", _a_boost=5.0)  # 5.0
            w.add_document(id=2, a=u"alfa alfa alfa alfa", _boost=0.5)  # 2.0

        with ix.searcher() as s:
            r = s.search(query.Term("a", "alfa"))
            assert [hit["id"] for hit in r] == [1, 0, 2]

        with ix.writer() as w:
            w.add_document(id=3, a=u"alfa", b=u"bottle")
            w.add_document(id=4, b=u"bravo", _b_boost=2.0)

        with ix.searcher() as s:
            r = s.search(query.Term("a", "alfa"))
            assert [hit["id"] for hit in r] == [1, 0, 3, 2]


def test_globfield_length_merge():
    # Issue 343

    schema = fields.Schema(title=fields.TEXT(stored=True),
                           path=fields.ID(stored=True))
    schema.add("*_text", fields.TEXT, glob=True)

    with TempIndex(schema, "globlenmerge") as ix:
        with ix.writer() as w:
            w.add_document(title=u"First document", path=u"/a",
                           content_text=u"This is the first document we've added!")

        with ix.writer() as w:
            w.add_document(title=u"Second document", path=u"/b",
                           content_text=u"The second document is even more interesting!")

        with ix.searcher() as s:
            docid = s.document_number(path="/a")
            m = s.matcher("content_text", "first")
            m.skip_to(docid)
            assert m.length() is not None

            qp = qparser.QueryParser("content", schema)
            q = qp.parse("content_text:document")
            r = s.search(q)
            paths = sorted(hit["path"] for hit in r)
            assert paths == ["/a", "/b"]


def test_index_decimals():
    from decimal import Decimal

    schema = fields.Schema(name=fields.KEYWORD(stored=True),
                           num=fields.NUMERIC(int))
    with TempIndex(schema, "decimals") as ix:
        with ix.writer() as w:
            with pytest.raises(TypeError):
                w.add_document(name=u"hello", num=Decimal("3.2"))

    schema = fields.Schema(name=fields.KEYWORD(stored=True),
                           num=fields.NUMERIC(Decimal, decimal_places=5))
    with TempIndex(schema, "decimals") as ix:
        with ix.writer() as w:
            w.add_document(name=u"hello", num=Decimal("3.2"))





