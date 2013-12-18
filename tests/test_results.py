from __future__ import with_statement

import pytest

from whoosh import analysis, fields, formats, highlight, qparser, query
from whoosh.codec.whoosh3 import W3Codec
from whoosh.compat import u, xrange, text_type, permutations
from whoosh.filedb.filestore import RamStorage
from whoosh.util.testing import TempStorage, TempIndex


def test_score_retrieval():
    schema = fields.Schema(title=fields.TEXT(stored=True),
                           content=fields.TEXT(stored=True))
    storage = RamStorage()
    ix = storage.create_index(schema)
    writer = ix.writer()
    writer.add_document(title=u("Miss Mary"),
                        content=u("Mary had a little white lamb its fleece"
                                  " was white as snow"))
    writer.add_document(title=u("Snow White"),
                        content=u("Snow white lived in the forest with seven"
                                  " dwarfs"))
    writer.commit()

    with ix.searcher() as s:
        results = s.search(query.Term("content", "white"))
        assert len(results) == 2
        assert results[0]['title'] == u("Miss Mary")
        assert results[1]['title'] == u("Snow White")
        assert results.score(0) is not None
        assert results.score(0) != 0
        assert results.score(0) != 1


def test_resultcopy():
    schema = fields.Schema(a=fields.TEXT(stored=True))
    st = RamStorage()
    ix = st.create_index(schema)

    w = ix.writer()
    w.add_document(a=u("alfa bravo charlie"))
    w.add_document(a=u("bravo charlie delta"))
    w.add_document(a=u("charlie delta echo"))
    w.add_document(a=u("delta echo foxtrot"))
    w.commit()

    with ix.searcher() as s:
        r = s.search(qparser.QueryParser("a", None).parse(u("charlie")))
        assert len(r) == 3
        rcopy = r.copy()
        assert r.top_n == rcopy.top_n


def test_resultslength():
    schema = fields.Schema(id=fields.ID(stored=True),
                           value=fields.TEXT)
    ix = RamStorage().create_index(schema)

    w = ix.writer()
    w.add_document(id=u("1"), value=u("alfa alfa alfa alfa alfa"))
    w.add_document(id=u("2"), value=u("alfa alfa alfa alfa"))
    w.add_document(id=u("3"), value=u("alfa alfa alfa"))
    w.add_document(id=u("4"), value=u("alfa alfa"))
    w.add_document(id=u("5"), value=u("alfa"))
    w.add_document(id=u("6"), value=u("bravo"))
    w.commit()

    with ix.searcher() as s:
        r = s.search(query.Term("value", u("alfa")), limit=3)
        assert len(r) == 5
        assert r.scored_length() == 3
        assert r[10:] == []


def test_combine():
    schema = fields.Schema(id=fields.ID(stored=True),
                           value=fields.TEXT)
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(id=u("1"), value=u("alfa bravo charlie all"))
    w.add_document(id=u("2"), value=u("bravo charlie echo all"))
    w.add_document(id=u("3"), value=u("charlie echo foxtrot all"))
    w.add_document(id=u("4"), value=u("echo foxtrot india all"))
    w.add_document(id=u("5"), value=u("foxtrot india juliet all"))
    w.add_document(id=u("6"), value=u("india juliet alfa all"))
    w.add_document(id=u("7"), value=u("juliet alfa bravo all"))
    w.add_document(id=u("8"), value=u("charlie charlie charlie all"))
    w.commit()

    with ix.searcher() as s:
        def idsof(r):
            return "".join(hit["id"] for hit in r)

        def check(r1, methodname, r2, ids):
            getattr(r1, methodname)(r2)
            assert idsof(r1) == ids

        def rfor(t):
            return s.search(query.Term("value", t))

        assert idsof(rfor(u("foxtrot"))) == "345"
        check(rfor(u("foxtrot")), "extend", rfor("charlie"), "345812")
        check(rfor(u("foxtrot")), "filter", rfor("juliet"), "5")
        check(rfor(u("charlie")), "filter", rfor("foxtrot"), "3")
        check(rfor(u("all")), "filter", rfor("foxtrot"), "345")
        check(rfor(u("all")), "upgrade", rfor("india"), "45612378")
        check(rfor(u("charlie")), "upgrade_and_extend", rfor("echo"), "23814")


def test_results_filter():
    schema = fields.Schema(id=fields.STORED, words=fields.KEYWORD(stored=True))
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(id="1", words=u("bravo top"))
    w.add_document(id="2", words=u("alfa top"))
    w.add_document(id="3", words=u("alfa top"))
    w.add_document(id="4", words=u("alfa bottom"))
    w.add_document(id="5", words=u("bravo bottom"))
    w.add_document(id="6", words=u("charlie bottom"))
    w.add_document(id="7", words=u("charlie bottom"))
    w.commit()

    with ix.searcher() as s:
        def check(r, target):
            result = "".join(s.stored_fields(d)["id"] for d in r.docs())
            assert result == target

        r = s.search(query.Term("words", u("alfa")))
        r.filter(s.search(query.Term("words", u("bottom"))))
        check(r, "4")


def test_extend_empty():
    schema = fields.Schema(id=fields.STORED, words=fields.KEYWORD)
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(id=1, words=u("alfa bravo charlie"))
    w.add_document(id=2, words=u("bravo charlie delta"))
    w.add_document(id=3, words=u("charlie delta echo"))
    w.add_document(id=4, words=u("delta echo foxtrot"))
    w.add_document(id=5, words=u("echo foxtrot golf"))
    w.commit()

    with ix.searcher() as s:
        # Get an empty results object
        r1 = s.search(query.Term("words", u("hotel")))
        # Copy it
        r1c = r1.copy()
        # Get a non-empty results object
        r2 = s.search(query.Term("words", u("delta")))
        # Copy it
        r2c = r2.copy()
        # Extend r1 with r2
        r1c.extend(r2c)
        assert [hit["id"] for hit in r1c] == [2, 3, 4]
        assert r1c.scored_length() == 3


def test_extend_filtered():
    schema = fields.Schema(id=fields.STORED, text=fields.TEXT(stored=True))
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(id=1, text=u("alfa bravo charlie"))
    w.add_document(id=2, text=u("bravo charlie delta"))
    w.add_document(id=3, text=u("juliet delta echo"))
    w.add_document(id=4, text=u("delta bravo alfa"))
    w.add_document(id=5, text=u("foxtrot sierra tango"))
    w.commit()

    hits = lambda result: [hit["id"] for hit in result]

    with ix.searcher() as s:
        r1 = s.search(query.Term("text", u("alfa")), filter=set([1, 4]))
        assert r1.allowed == set([1, 4])
        assert len(r1.top_n) == 0

        r2 = s.search(query.Term("text", u("bravo")))
        assert len(r2.top_n) == 3
        assert hits(r2) == [1, 2, 4]

        r3 = r1.copy()
        assert r3.allowed == set([1, 4])
        assert len(r3.top_n) == 0
        r3.extend(r2)
        assert len(r3.top_n) == 3
        assert hits(r3) == [1, 2, 4]


def test_pages():
    from whoosh.scoring import Frequency

    schema = fields.Schema(id=fields.ID(stored=True), c=fields.TEXT)
    ix = RamStorage().create_index(schema)

    w = ix.writer()
    w.add_document(id=u("1"), c=u("alfa alfa alfa alfa alfa alfa"))
    w.add_document(id=u("2"), c=u("alfa alfa alfa alfa alfa"))
    w.add_document(id=u("3"), c=u("alfa alfa alfa alfa"))
    w.add_document(id=u("4"), c=u("alfa alfa alfa"))
    w.add_document(id=u("5"), c=u("alfa alfa"))
    w.add_document(id=u("6"), c=u("alfa"))
    w.commit()

    with ix.searcher(weighting=Frequency) as s:
        q = query.Term("c", u("alfa"))
        r = s.search(q)
        assert [d["id"] for d in r] == ["1", "2", "3", "4", "5", "6"]
        r = s.search_page(q, 2, pagelen=2)
        assert [d["id"] for d in r] == ["3", "4"]

        r = s.search_page(q, 2, pagelen=4)
        assert r.total == 6
        assert r.pagenum == 2
        assert r.pagelen == 2


def test_pages_with_filter():
    from whoosh.scoring import Frequency

    schema = fields.Schema(id=fields.ID(stored=True),
                           type=fields.TEXT(),
                           c=fields.TEXT)
    ix = RamStorage().create_index(schema)

    w = ix.writer()
    w.add_document(id=u("1"), type=u("odd"), c=u("alfa alfa alfa alfa alfa alfa"))
    w.add_document(id=u("2"), type=u("even"), c=u("alfa alfa alfa alfa alfa"))
    w.add_document(id=u("3"), type=u("odd"), c=u("alfa alfa alfa alfa"))
    w.add_document(id=u("4"), type=u("even"), c=u("alfa alfa alfa"))
    w.add_document(id=u("5"), type=u("odd"), c=u("alfa alfa"))
    w.add_document(id=u("6"), type=u("even"), c=u("alfa"))
    w.commit()

    with ix.searcher(weighting=Frequency) as s:
        q = query.Term("c", u("alfa"))
        filterq = query.Term("type", u("even"))
        r = s.search(q, filter=filterq)
        assert [d["id"] for d in r] == ["2", "4", "6"]
        r = s.search_page(q, 2, pagelen=2, filter=filterq)
        assert [d["id"] for d in r] == ["6"]


def test_extra_slice():
    schema = fields.Schema(key=fields.ID(stored=True))
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    for char in u("abcdefghijklmnopqrstuvwxyz"):
        w.add_document(key=char)
    w.commit()

    with ix.searcher() as s:
        r = s.search(query.Every(), limit=5)
        assert r[6:7] == []


def test_page_counts():
    from whoosh.scoring import Frequency

    schema = fields.Schema(id=fields.ID(stored=True))
    st = RamStorage()
    ix = st.create_index(schema)

    w = ix.writer()
    for i in xrange(10):
        w.add_document(id=text_type(i))
    w.commit()

    with ix.searcher(weighting=Frequency) as s:
        q = query.Every("id")

        r = s.search(q)
        assert len(r) == 10

        with pytest.raises(ValueError):
            s.search_page(q, 0)

        r = s.search_page(q, 1, 5)
        assert len(r) == 10
        assert r.pagecount == 2

        r = s.search_page(q, 1, 5)
        assert len(r) == 10
        assert r.pagecount == 2

        r = s.search_page(q, 2, 5)
        assert len(r) == 10
        assert r.pagecount == 2
        assert r.pagenum == 2

        r = s.search_page(q, 1, 10)
        assert len(r) == 10
        assert r.pagecount == 1
        assert r.pagenum == 1


def test_resultspage():
    schema = fields.Schema(id=fields.STORED, content=fields.TEXT(stored=True))
    ix = RamStorage().create_index(schema)

    domain = ("alfa", "bravo", "bravo", "charlie", "delta")
    w = ix.writer()
    for i, lst in enumerate(permutations(domain, 3)):
        w.add_document(id=text_type(i), content=u(" ").join(lst))
    w.commit()

    with ix.searcher() as s:
        q = query.Term("content", u("bravo"))
        r = s.search(q, limit=10)
        tops = list(r)

        rp = s.search_page(q, 1, pagelen=5)
        assert rp.scored_length() == 5
        assert list(rp) == tops[0:5]
        assert rp[10:] == []

        rp = s.search_page(q, 2, pagelen=5)
        assert list(rp) == tops[5:10]

        rp = s.search_page(q, 1, pagelen=10)
        assert len(rp) == 54
        assert rp.pagecount == 6
        rp = s.search_page(q, 6, pagelen=10)
        assert len(list(rp)) == 4
        assert rp.is_last_page()

        with pytest.raises(ValueError):
            s.search_page(q, 0)
        assert s.search_page(q, 10).pagenum == 6

        rp = s.search_page(query.Term("content", "glonk"), 1)
        assert len(rp) == 0
        assert rp.is_last_page()


def test_highlight_setters():
    schema = fields.Schema(text=fields.TEXT)
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(text=u("Hello"))
    w.commit()

    r = ix.searcher().search(query.Term("text", "hello"))
    hl = highlight.Highlighter()
    ucf = highlight.UppercaseFormatter()
    r.highlighter = hl
    r.formatter = ucf
    assert hl.formatter is ucf


def test_snippets():
    ana = analysis.StemmingAnalyzer()
    schema = fields.Schema(text=fields.TEXT(stored=True, analyzer=ana))
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(text=u("Lay out the rough animation by creating the important poses where they occur on the timeline."))
    w.add_document(text=u("Set key frames on everything that's key-able. This is for control and predictability: you don't want to accidentally leave something un-keyed. This is also much faster than selecting the parameters to key."))
    w.add_document(text=u("Use constant (straight) or sometimes linear transitions between keyframes in the channel editor. This makes the character jump between poses."))
    w.add_document(text=u("Keying everything gives quick, immediate results. But it can become difficult to tweak the animation later, especially for complex characters."))
    w.add_document(text=u("Copy the current pose to create the next one: pose the character, key everything, then copy the keyframe in the playbar to another frame, and key everything at that frame."))
    w.commit()

    target = ["Set KEY frames on everything that's KEY-able",
              "Copy the current pose to create the next one: pose the character, KEY everything, then copy the keyframe in the playbar to another frame, and KEY everything at that frame",
              "KEYING everything gives quick, immediate results"]

    with ix.searcher() as s:
        qp = qparser.QueryParser("text", ix.schema)
        q = qp.parse(u("key"))
        r = s.search(q, terms=True)
        r.fragmenter = highlight.SentenceFragmenter()
        r.formatter = highlight.UppercaseFormatter()

        assert sorted([hit.highlights("text", top=1) for hit in r]) == sorted(target)


def test_keyterms():
    ana = analysis.StandardAnalyzer()
    vectorformat = formats.Frequency()
    schema = fields.Schema(path=fields.ID,
                           content=fields.TEXT(analyzer=ana,
                                               vector=vectorformat))
    st = RamStorage()
    ix = st.create_index(schema)
    w = ix.writer()
    w.add_document(path=u("a"), content=u("This is some generic content"))
    w.add_document(path=u("b"), content=u("This is some distinctive content"))
    w.commit()

    with ix.searcher() as s:
        docnum = s.document_number(path=u("b"))
        keyterms = list(s.key_terms([docnum], "content"))
        assert len(keyterms) > 0
        assert keyterms[0][0] == "distinctive"

        r = s.search(query.Term("path", u("b")))
        keyterms2 = list(r.key_terms("content"))
        assert len(keyterms2) > 0
        assert keyterms2[0][0] == "distinctive"


def test_lengths():
    schema = fields.Schema(id=fields.STORED, text=fields.TEXT)
    ix = RamStorage().create_index(schema)

    w = ix.writer()
    w.add_document(id=1, text=u("alfa bravo charlie delta echo"))
    w.add_document(id=2, text=u("bravo charlie delta echo foxtrot"))
    w.add_document(id=3, text=u("charlie needle echo foxtrot golf"))
    w.add_document(id=4, text=u("delta echo foxtrot golf hotel"))
    w.add_document(id=5, text=u("echo needle needle hotel india"))
    w.add_document(id=6, text=u("foxtrot golf hotel india juliet"))
    w.add_document(id=7, text=u("golf needle india juliet kilo"))
    w.add_document(id=8, text=u("hotel india juliet needle lima"))
    w.commit()

    with ix.searcher() as s:
        q = query.Or([query.Term("text", u("needle")), query.Term("text", u("charlie"))])
        r = s.search(q, limit=2)
        assert not r.has_exact_length()
        assert r.estimated_length() == 7
        assert r.estimated_min_length() == 3
        assert r.scored_length() == 2
        assert len(r) == 6


def test_lengths2():
    schema = fields.Schema(text=fields.TEXT(stored=True))
    ix = RamStorage().create_index(schema)
    count = 0
    for _ in xrange(3):
        w = ix.writer()
        for ls in permutations(u("alfa bravo charlie").split()):
            if "bravo" in ls and "charlie" in ls:
                count += 1
            w.add_document(text=u(" ").join(ls))
        w.commit(merge=False)

    with ix.searcher() as s:
        q = query.Or([query.Term("text", u("bravo")), query.Term("text", u("charlie"))])
        r = s.search(q, limit=None)
        assert len(r) == count

        r = s.search(q, limit=3)
        assert len(r) == count


def test_stability():
    schema = fields.Schema(text=fields.TEXT)
    ix = RamStorage().create_index(schema)
    domain = u("alfa bravo charlie delta").split()
    w = ix.writer()
    for ls in permutations(domain, 3):
        w.add_document(text=u(" ").join(ls))
    w.commit()

    with ix.searcher() as s:
        q = query.Term("text", u("bravo"))
        last = []
        for i in xrange(s.doc_frequency("text", u("bravo"))):
            # Only un-optimized results are stable
            r = s.search(q, limit=i + 1, optimize=False)
            docnums = [hit.docnum for hit in r]
            assert docnums[:-1] == last
            last = docnums


def test_terms():
    schema = fields.Schema(text=fields.TEXT(stored=True))
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(text=u("alfa sierra tango"))
    w.add_document(text=u("bravo charlie delta"))
    w.add_document(text=u("charlie delta echo"))
    w.add_document(text=u("delta echo foxtrot"))
    w.commit()

    qp = qparser.QueryParser("text", ix.schema)
    q = qp.parse(u("(bravo AND charlie) OR foxtrot OR missing"))
    r = ix.searcher().search(q, terms=True)

    fieldobj = schema["text"]

    def txts(tset):
        return sorted(fieldobj.from_bytes(t[1]) for t in tset)

    assert txts(r.matched_terms()) == ["bravo", "charlie", "foxtrot"]
    for hit in r:
        value = hit["text"]
        for txt in txts(hit.matched_terms()):
            assert txt in value


def test_hit_column():
    # Not stored
    schema = fields.Schema(text=fields.TEXT())
    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        w.add_document(text=u("alfa bravo charlie"))

    with ix.searcher() as s:
        r = s.search(query.Term("text", "alfa"))
        assert len(r) == 1
        hit = r[0]
        with pytest.raises(KeyError):
            _ = hit["text"]

    # With column
    schema = fields.Schema(text=fields.TEXT(sortable=True))
    ix = RamStorage().create_index(schema)
    with ix.writer(codec=W3Codec()) as w:
        w.add_document(text=u("alfa bravo charlie"))

    with ix.searcher() as s:
        r = s.search(query.Term("text", "alfa"))
        assert len(r) == 1
        hit = r[0]
        assert hit["text"] == u("alfa bravo charlie")


def test_closed_searcher():
    from whoosh.reading import ReaderClosed

    schema = fields.Schema(key=fields.KEYWORD(stored=True, sortable=True,
                                              spelling=True))

    with TempStorage() as st:
        ix = st.create_index(schema)
        with ix.writer() as w:
            w.add_document(key=u("alfa"))
            w.add_document(key=u("bravo"))
            w.add_document(key=u("charlie"))
            w.add_document(key=u("delta"))
            w.add_document(key=u("echo"))

        s = ix.searcher()
        r = s.search(query.TermRange("key", "b", "d"))
        s.close()
        assert s.is_closed
        with pytest.raises(ReaderClosed):
            assert r[0]["key"] == "bravo"
        with pytest.raises(ReaderClosed):
            s.reader().column_reader("key")
        with pytest.raises(ReaderClosed):
            s.reader().has_word_graph("key")
        with pytest.raises(ReaderClosed):
            s.suggest("key", "brovo")

        s = ix.searcher()
        r = s.search(query.TermRange("key", "b", "d"))
        assert r[0]
        assert r[0]["key"] == "bravo"
        c = s.reader().column_reader("key")
        assert c[1] == "bravo"
        assert s.reader().has_word_graph("key")
        assert s.suggest("key", "brovo") == ["bravo"]


def test_paged_highlights():
    schema = fields.Schema(text=fields.TEXT(stored=True))
    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        w.add_document(text=u("alfa bravo charlie delta echo foxtrot"))
        w.add_document(text=u("bravo charlie delta echo foxtrot golf"))
        w.add_document(text=u("charlie delta echo foxtrot golf hotel"))
        w.add_document(text=u("delta echo foxtrot golf hotel india"))
        w.add_document(text=u("echo foxtrot golf hotel india juliet"))
        w.add_document(text=u("foxtrot golf hotel india juliet kilo"))

    with ix.searcher() as s:
        q = query.Term("text", u("alfa"))
        page = s.search_page(q, 1, pagelen=3)

        page.results.fragmenter = highlight.WholeFragmenter()
        page.results.formatter = highlight.UppercaseFormatter()
        hi = page[0].highlights("text")
        assert hi == u("ALFA bravo charlie delta echo foxtrot")


def test_phrase_keywords():
    schema = fields.Schema(text=fields.TEXT(stored=True))
    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        w.add_document(text=u("alfa bravo charlie delta"))
        w.add_document(text=u("bravo charlie delta echo"))
        w.add_document(text=u("charlie delta echo foxtrot"))
        w.add_document(text=u("delta echo foxtrot alfa"))
        w.add_document(text=u("echo foxtrot alfa bravo"))

    with ix.searcher() as s:
        q = query.Phrase("text", u("alfa bravo").split())
        r = s.search(q)
        assert len(r) == 2
        kts = " ".join(t for t, score in r.key_terms("text"))
        assert kts == "alfa bravo charlie foxtrot delta"


def test_every_keywords():
    schema = fields.Schema(title=fields.TEXT, content=fields.TEXT(stored=True))
    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        w.add_document(title=u("alfa"), content=u("bravo"))
        w.add_document(title=u("charlie"), content=u("delta"))

    with ix.searcher() as s:
        q = qparser.QueryParser("content", ix.schema).parse("*")
        assert isinstance(q, query.Every)

        r = s.search(q, terms=True)
        assert len(r) == 2
        hit = r[0]
        assert hit["content"] == "bravo"
        assert hit.highlights("content") == ""


def test_filter_by_result():
    schema = fields.Schema(title=fields.TEXT(stored=True),
                           content=fields.TEXT(stored=True))

    with TempIndex(schema, "filter") as ix:
        words = u("foo bar baz qux barney").split()
        with ix.writer() as w:
            for x in xrange(100):
                t = u("even" if x % 2 == 0 else "odd")
                c = words[x % len(words)]
                w.add_document(title=t, content=c)

        with ix.searcher() as searcher:
            fq = query.Term("title", "even")
            filter_result = searcher.search(fq)
            assert filter_result.docset is None

            q = query.Term("content", "foo")

            # filter_result.docs()
            result = searcher.search(q, filter=filter_result)
            assert all(x["title"] == "even" and x["content"] == "foo"
                       for x in result)

