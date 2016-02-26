import random
from itertools import permutations

import pytest

from whoosh import postings
from whoosh import fields
from whoosh.codec import x1
from whoosh.ifaces import codecs, readers
from whoosh.util.testing import TempStorage


def test_terminfo():
    fmt = postings.Format(has_weights=True, has_positions=True)

    single = postings.posting(docid=100, length=1, weight=2.5,
                              positions=[1, 9, 30])
    posts = [fmt.condition_post(single)]

    infos = [
        x1.X1TermInfo(weight=3.0, df=3, minlength=7, maxlength=10,
                      maxweight=1.5, minid=12, maxid=70),
        x1.X1TermInfo(weight=2.5, df=1, minlength=25, maxlength=25,
                      maxweight=2.5, minid=100, maxid=100,
                      inlinebytes=fmt.doclist_to_bytes(posts))
    ]
    infos[0].offset = 10000

    ti = infos[0]
    assert ti.weight() == 3.0
    assert ti.doc_frequency() == 3
    assert ti.min_length() == 7
    assert ti.max_length() == 10
    assert ti.max_weight() == 1.5
    assert ti.min_id() == 12
    assert ti.max_id() == 70

    with TempStorage() as st:
        with st.create_file("test") as f:
            offsets = []
            for ti in infos:
                offsets.append(f.tell())
                f.write(ti.to_bytes())

        with st.map_file("test") as mm:
            ti = x1.X1TermInfo.from_bytes(mm, 0)
            assert ti.weight() == 3.0
            assert ti.doc_frequency() == 3
            assert ti.min_length() == 7
            assert ti.max_length() == 10
            assert ti.max_weight() == 1.5
            assert ti.min_id() == 12
            assert ti.max_id() == 70
            assert ti.offset == 10000

            ti = x1.X1TermInfo.from_bytes(mm, offsets[1])
            assert ti.weight() == 2.5
            assert ti.doc_frequency() == 1
            assert ti.min_length() == 25
            assert ti.max_length() == 25
            assert ti.max_weight() == 2.5
            assert ti.min_id() == 100
            assert ti.max_id() == 100
            assert ti.offset == -1
            assert ti.inlinebytes

            inline_post = ti.posting_reader(fmt).posting_at(0)
            assert inline_post == postings.posting(
                docid=100, weight=2.5, positions=(1, 9, 30)
            )


def test_list_stats():
    posts = [
        postings.posting(docid=75, length=7, weight=1.0),
        postings.posting(docid=85, length=5, weight=3.0),
        postings.posting(docid=95, length=12, weight=1.5),
    ]
    ti = x1.X1TermInfo(weight=3.0, df=3, minlength=7, maxlength=10,
                       maxweight=1.5, minid=12, maxid=70)
    ti.add_posting_list_stats(posts)
    assert ti.weight() == 8.5
    assert ti.doc_frequency() == 6
    assert ti.min_length() == 5
    assert ti.max_length() == 12
    assert ti.max_weight() == 3.0
    assert ti.min_id() == 12
    assert ti.max_id() == 95

    posts = [
        postings.posting(75),
        postings.posting(85),
        postings.posting(95),
    ]
    ti = x1.X1TermInfo(weight=3.0, df=3, minlength=7, maxlength=10,
                       maxweight=1.5, minid=12, maxid=70)
    ti.add_posting_list_stats(posts)
    assert ti.weight() == 3.0
    assert ti.doc_frequency() == 6
    assert ti.min_length() == 7
    assert ti.max_length() == 10
    assert ti.max_weight() == 1.5
    assert ti.min_id() == 12
    assert ti.max_id() == 95


def test_reader_stats():
    fmt = postings.Format(has_lengths=True, has_weights=True)

    posts = [
        postings.posting(docid=75, length=7, weight=1.0),
        postings.posting(docid=85, length=5, weight=3.0),
        postings.posting(docid=95, length=12, weight=1.5),
    ]
    r = fmt.doclist_reader(fmt.doclist_to_bytes(posts))
    ti = x1.X1TermInfo(weight=3.0, df=3, minlength=7, maxlength=10,
                       maxweight=1.5, minid=12, maxid=70)
    ti.add_posting_reader_stats(r)
    assert ti.weight() == 8.5
    assert ti.doc_frequency() == 6
    assert ti.min_length() == 5
    assert ti.max_length() == 12
    assert ti.max_weight() == 3.0
    assert ti.min_id() == 12
    assert ti.max_id() == 95

    posts = [
        postings.posting(75, length=7, weight=1.0),
        postings.posting(85, length=9, weight=1.0),
        postings.posting(95, length=10, weight=1.0),
    ]
    r = fmt.doclist_reader(fmt.doclist_to_bytes(posts))
    ti = x1.X1TermInfo(weight=3.0, df=3, minlength=7, maxlength=10,
                       maxweight=1.5, minid=12, maxid=70)
    ti.add_posting_reader_stats(r)
    assert ti.weight() == 6.0
    assert ti.doc_frequency() == 6
    assert ti.min_length() == 7
    assert ti.max_length() == 10
    assert ti.max_weight() == 1.5
    assert ti.min_id() == 12
    assert ti.max_id() == 95


def test_perdoc():
    field = fields.Text(sortable=True, vector=True, stored=True,
                        phrase=False)

    with TempStorage() as st:
        cdc = x1.X1Codec()
        sesh = st.open("test", writable=True)
        seg = cdc.new_segment(sesh)
        pdw = x1.X1PerDocWriter(sesh, seg)

        pdw.start_doc(0)
        pdw.add_field("a", field, u"alfa bravo charlie", 3)
        pdw.add_vector_postings("a", field, [
            postings.posting(termbytes=b"alfa", weight=1.5, length=1),
            postings.posting(termbytes=b"bravo", weight=1.0, length=1),
            postings.posting(termbytes=b"charlie", weight=3.0, length=1),
        ])
        pdw.add_column_value("a", field.column, b"abc def ghi")
        pdw.finish_doc()

        pdw.start_doc(1)
        pdw.add_field("a", field, u"delta echo foxtrot golf", 4)
        pdw.finish_doc()

        pdw.close()
        sesh.close()

        sesh = st.open("test")
        pdr = x1.X1PerDocReader(sesh, seg)
        assert pdr.doc_count() == 2
        assert list(pdr.all_doc_ids()) == [0, 1]
        assert pdr.doc_field_length(0, "a") == 3
        assert pdr.doc_field_length(1, "a") == 4
        assert pdr.field_length("a") == 7
        assert pdr.min_field_length("a") == 3
        assert pdr.max_field_length("a") == 4
        assert pdr.stored_fields(0) == {"a": u"alfa bravo charlie"}
        assert pdr.stored_fields(1) == {"a": u"delta echo foxtrot golf"}

        assert pdr.has_vector(0, "a")
        assert not pdr.has_vector(1, "a")
        vr = pdr.vector(0, "a", field.vector)
        assert isinstance(vr, postings.BasicVectorReader)
        assert vr.termbytes(0) == b"alfa"
        assert vr.weight(0) == 1.5
        assert vr.termbytes(2) == b"charlie"
        assert vr.weight(2) == 3.0

        assert pdr.has_column("a")
        cr = pdr.column_reader("a", field.column)
        assert cr[0] == b"abc def ghi"
        assert cr[1] == b""
        cr.close()
        sesh.close()


def test_terms():
    field = fields.Text(phrase=False)
    fnames = ("alfa", "bravo", "charlie")
    field_letters = ("abcde", "efghi", "ijklm")
    all_terms = []
    termlists = {}
    all_posts = {}

    for i, fname in enumerate(fnames):
        terms = sorted(''.join(lets).encode("ascii")
                       for lets in permutations(field_letters[i]))
        termlists[fname] = terms
        all_terms.extend((fname, t) for t in terms)

        all_posts[fname] = termposts = {}
        for j, term in enumerate(terms):
            termposts[term] = posts = []
            for k in range((i + j) // 2 + 1):
                posts.append(postings.posting(
                    docid=j + k, length=5, weight=i + k * 1.5,
                ))

    with TempStorage() as st:
        cdc = x1.X1Codec()
        sesh = st.open("test", writable=True)
        seg = cdc.new_segment(sesh)
        fw = x1.X1FieldWriter(sesh, seg)

        for i, fname in enumerate(fnames):
            fw.start_field(fname, field)

            for term in termlists[fname]:
                fw.start_term(term)
                fw.add_posting_list(all_posts[fname][term])
                fw.finish_term()
            fw.finish_field()
        fw.close()
        sesh.close()

        sesh = st.open("test")
        tr = x1.X1TermsReader(sesh, seg)
        assert tr.indexed_field_names() == ["alfa", "bravo", "charlie"]
        assert list(tr.terms()) == all_terms

        for fname in fnames:
            terms = termlists[fname]

            for i in range(0, len(terms) - 1, 3):
                for j in range(i, len(terms), 3):
                    start = terms[i]
                    end = terms[j]

                    ts = list(tr.term_range(fname, start, end))
                    target = terms[i:j]
                    assert ts == target

            for term in terms:
                posts = all_posts[fname][term]
                w = sum(p[postings.WEIGHT] for p in posts)
                ti = tr.term_info(fname, term)
                assert ti.weight() == w
                assert ti.max_weight() == max(p[postings.WEIGHT] for p in posts)
                assert ti.doc_frequency() == len(posts)
                assert ti.min_id() == posts[0][postings.DOCID]
                assert ti.max_id() == posts[-1][postings.DOCID]

                assert tr.weight(fname, term) == w
                assert tr.doc_frequency(fname, term) == len(posts)

        with pytest.raises(readers.TermNotFound):
            _ = tr.term_info("alfa", b"zzz")

        with pytest.raises(readers.TermNotFound):
            _ = tr.term_info("zzz", b"abcde")

        with pytest.raises(ValueError):
            list(tr.term_range("alfa", b'x', b'f'))

        items = list(tr.items())
        assert [t for t, _ in items] == all_terms
        for (fname, term), ti in items:
            posts = all_posts[fname][term]
            assert ti.weight() == sum(p[postings.WEIGHT] for p in posts)
            assert ti.max_weight() == max(p[postings.WEIGHT] for p in posts)
            assert ti.doc_frequency() == len(posts)
            assert ti.min_id() == posts[0][postings.DOCID]
            assert ti.max_id() == posts[-1][postings.DOCID]

        tr.close()
        sesh.close()


def test_cursor():
    letters = "abcdef"
    terms = sorted(''.join(lets).encode("ascii") for lets
                   in permutations(letters))
    field = fields.Text(phrase=False)

    with TempStorage() as st:
        cdc = x1.X1Codec()
        sesh = st.open("test", writable=True)
        seg = cdc.new_segment(sesh)
        fw = x1.X1FieldWriter(sesh, seg)
        fw.start_field("a", field)
        for i, term in enumerate(terms):
            fw.start_term(term)
            fw.add_posting_list([
                postings.posting(docid=i, length=1, weight=1.0)
            ])
            fw.finish_term()
        fw.finish_field()
        fw.close()
        sesh.close()

        # print([(name, st.file_length(name)) for name in st.list()])

        sesh = st.open("test")
        tr = x1.X1TermsReader(sesh, seg)
        cur = tr.cursor("a", field)
        assert list(cur) == terms

        cur.first()
        assert cur.termbytes() == terms[0]

        cef = None
        for term in terms:
            if term.startswith(b"cef"):
                cef = term
                break
        cur.seek(b"cef")
        assert cur.termbytes() == cef

        cur.seek(b"zzz")
        assert not cur.is_valid()
        with pytest.raises(codecs.InvalidCursor):
            cur.next()

        tr.close()
        sesh.close()


def test_matcher():
    from collections import defaultdict
    from whoosh import analysis

    ana = analysis.SimpleAnalyzer()
    field = fields.Text(analyzer=ana)

    words = u"alfa bravo charlie delta echo foxtrot golf hotel india juliet"
    domain = words.split()
    docs = [u"zebra"]
    for i in range(2000):
        words = []
        for j in range(i, i + (i % 100) + 1):
            words.append(domain[j % len(domain)])
        docs.append(" ".join(words))
    docs.append(u"kilo")

    # Analyzed versions of the docs
    anadocs = []
    for doc in docs:
        anadocs.append([t.text for t in ana(doc)])

    lookup = defaultdict(list)
    for i, doc in enumerate(docs):
        length, posts = field.index(doc, docid=i)
        for post in posts:
            lookup[post[postings.TERMBYTES]].append(post)

    with TempStorage() as st:
        cdc = x1.X1Codec()
        seg = x1.X1Segment(cdc, "test")
        sesh = st.open("test", writable=True)
        fw = x1.X1FieldWriter(sesh, seg, blocksize=128)
        fw.start_field("a", field)
        for tbytes in sorted(lookup):
            fw.start_term(tbytes)
            fw.add_posting_list(lookup[tbytes])
            fw.finish_term()
        fw.finish_field()
        fw.close()
        sesh.close()

        sesh = st.open("test")
        tr = cdc.terms_reader(sesh, seg)
        ts = list(lookup)
        random.shuffle(ts)
        for tbytes in ts:
            posts = lookup[tbytes]
            target = [p[postings.DOCID] for p in lookup[tbytes]]

            m = tr.matcher("a", tbytes, field.format)
            assert m.has_positions()
            assert not m.has_chars()
            allids = []
            i = 0
            while m.is_active():
                allids.append(m.id())

                assert m.weight() == posts[i][postings.WEIGHT]
                for pos in m.positions():
                    assert anadocs[m.id()][pos].encode("utf8") == tbytes

                m.next()
                i += 1
            assert allids == target

            assert len(allids) == tr.doc_frequency("a", tbytes)

            m = tr.matcher("a", tbytes, field.format)
            allids = list(m.all_ids())
            assert allids == target

            m = tr.matcher("a", tbytes, field.format)
            go = target[0 - len(target) // 3]
            m.skip_to(go)
            assert m.id() == go

        tr.close()
        sesh.close()





