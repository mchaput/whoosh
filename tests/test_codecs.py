from __future__ import with_statement
import random
from array import array

import pytest

from whoosh import analysis, fields, formats, index, matching, reading
from whoosh.compat import b, u, permutations, xrange
from whoosh.codec import default_codec
from whoosh.codec import codec
from whoosh.formats import Posting
from whoosh.util.testing import TempDB


def test_top_methods():
    with TempDB("topmeths") as db:
        txn = db.open(write=True, create=True)
        cdc = default_codec()
        fobj = fields.TEXT(sortable=True)

        assert isinstance(cdc.doc_writer(txn), codec.DocWriter)
        assert isinstance(cdc.doc_reader(txn), codec.DocReader)
        assert isinstance(cdc.term_reader(txn), codec.TermReader)
        assert isinstance(cdc.automata(txn, "f", fobj), codec.Automata)
        assert isinstance(cdc.column_writer(txn), codec.ColumnWriter)
        assert isinstance(cdc.column_reader(txn, "f", fobj), codec.ColumnReader)
        txn.cancel()


def test_write_read():
    form = formats.BasicFormat(True, True, True)
    ana = analysis.StandardAnalyzer()
    fobj = fields.FieldType(form, ana, scorable=True, stored=True)

    data = [
        (5, u"alfa bravo charlie"),
        (10, u"bravo charlie delta echo"),
        (12, u"charlie delta echo foxtrot charlie"),
        (30, u"delta echo echo golf"),
    ]

    with TempDB("writeread") as db:
        with db.open(write=True, create=True) as txn:
            cdc = default_codec()
            dw = cdc.doc_writer(txn)

            for docid, value in data:
                dw.start_doc(docid)
                length, gen = form.index(ana, fobj.to_bytes, value)
                dw.add_field("f", fobj, value, length)
                dw.add_field_postings("f", fobj, length, gen)
                dw.finish_doc()
            dw.close()

        terms = [("f", bs) for bs
                 in b"alfa bravo charlie delta echo foxtrot golf".split()]

        with db.open() as txn:
            tr = cdc.term_reader(txn)

            assert ("f", b"echo") in tr
            assert ("f", b"india") not in tr
            assert list(tr.terms_from("f", b"dx")) == b"echo foxtrot golf".split()
            assert list(tr.indexed_field_names()) == ["f"]

            ti = tr.term_info("f", b"echo")
            assert isinstance(ti, reading.TermInfo)
            assert ti.min_id() == 10
            assert ti.max_id() == 30
            assert ti.min_length() == 4
            assert ti.max_length() == 5
            assert ti.max_weight() == 2.0

            dr = cdc.doc_reader(txn)
            with pytest.raises(reading.NoStoredFields):
                dr.stored_fields(35)
            assert dr.is_deleted(2)
            assert not dr.is_deleted(5)
            assert dr.is_deleted(15)
            assert not dr.is_deleted(30)
            assert dr.is_deleted(100000)
            assert list(dr.all_doc_ids()) == [5, 10, 12, 30]
            # assert dr.field_length("f") == 16
            # assert dr.min_field_length("f") == 3
            # assert dr.max_field_length("f") == 5
            assert dr.stored_fields(5) == {"f": u"alfa bravo charlie"}
            assert dr.stored_fields(30) == {"f": u"delta echo echo golf"}
            assert list(dr.all_stored_fields()) == [(docid, {"f": v}) for docid, v in data]

            out = []
            for fieldname, termbytes in terms:
                cm = tr.matcher(fieldname, fobj, termbytes)
                info = []
                while cm.is_active():
                    info.append((cm.id(), cm.length(), cm.weight()))
                    cm.next()
                out.append((termbytes, info))
            assert out == [
                (b"alfa", [(5, 3, 1.0)]),
                (b"bravo", [(5, 3, 1.0), (10, 4, 1.0)]),
                (b"charlie", [(5, 3, 1.0), (10, 4, 1.0), (12, 5, 2.0)]),
                (b"delta", [(10, 4, 1.0), (12, 5, 1.0), (30, 4, 1.0)]),
                (b"echo", [(10, 4, 1.0), (12, 5, 1.0), (30, 4, 2.0)]),
                (b"foxtrot", [(12, 5, 1.0)]),
                (b"golf", [(30, 4, 1.0)]),
            ]


def test_indexed_field_names():
    # Create every 1, 2, and 3 letter permutation of a/b/c
    def names():
        for c1 in "abc":
            n1 = c1
            yield n1
            for c2 in "abc ":
                if c2 != " ":
                    n2 = n1 + c2
                    yield n2
                    for c3 in "abc ":
                        if c3 != " ":
                            n3 = n2 + c3
                            yield n3
    fnames = list(names())

    cdc = default_codec()
    with TempDB("fieldnames") as db:
        with db.open(write=True, create=True) as w:
            dw = cdc.doc_writer(w)

            times = 1000
            assert times > len(fnames)
            fobj = fields.KEYWORD()
            tbytes = b"x"

            for i in xrange(1000):
                fname = fnames[i % len(fnames)]
                dw.start_doc(i)
                dw.add_field(fname, fobj, None, 1)
                posts = [Posting(id=tbytes, weight=1.0)]
                dw.add_field_postings(fname, fobj, 1, posts)
                dw.finish_doc()
            dw.close()

        with db.open() as txn:
            tr = cdc.term_reader(txn)
            names = list(tr.indexed_field_names())
            assert names == fnames


def test_docmap():
    fieldobj = fields.KEYWORD(stored=True)
    base = 0
    docids = []
    times = 10000
    for _ in xrange(times):
        base += random.randint(1, 10)
        docids.append(base)
    docset = set(docids)

    def check(dr):
        output = list(dr.all_doc_ids())
        assert len(output) == len(docids)
        assert output == docids
        assert dr.doc_count() == len(docids)
        for i in xrange(docids[-1] + 1):
            assert dr.is_deleted(i) == (i not in docset)

    cdc = default_codec()
    with TempDB("docmap") as db:
        with db.open(write=True, create=True) as w:
            dw = cdc.doc_writer(w)
            for docid in docids:
                dw.start_doc(docid)
                dw.add_field("a", fieldobj, docid, docid)
                dw.finish_doc()
            dw.close()

        with db.open() as r:
            dr = cdc.doc_reader(r)
            check(dr)

        with db.open(write=True) as w:
            dw = cdc.doc_writer(w)
            deletetimes = times // 2
            for _ in xrange(deletetimes):
                docid = docset.pop()
                dw.delete(docid)
            docids = sorted(docset)
            dw.close()

        with db.open() as r:
            dr = cdc.doc_reader(r)
            check(dr)


def test_remove_field_terms():
    fnames = "abcd"
    fobj = fields.KEYWORD()
    times = 1000
    all_terms = [(fnames[i % len(fnames)], str(i).encode("ascii"))
                 for i in xrange(times)]
    all_terms.sort()

    cdc = default_codec()
    with TempDB("removefield") as db:
        with db.open(write=True, create=True) as w:
            dw = cdc.doc_writer(w)
            dw.start_doc(0)

            for fname in fnames:
                dw.add_field(fname, fobj, None, 1)
                posts = [Posting(id=tbytes, weight=1.0)
                         for fn, tbytes in all_terms
                         if fn == fname]
                dw.add_field_postings(fname, fobj, len(posts), posts)

            dw.finish_doc()
            dw.close()

        with db.open() as r:
            tr = cdc.term_reader(r)
            assert list(tr.terms()) == all_terms

        with db.open(write=True) as w:
            dw = cdc.doc_writer(w)
            dw.remove_field_terms("b")
            dw.close()

        with db.open() as r:
            tr = cdc.term_reader(r)
            result = [term for term in all_terms if term[0] != "b"]
            assert list(tr.terms()) == result


def test_add_matcher():
    # Create some fake terms for a single document
    fnames = "abcd"
    fobj = fields.KEYWORD()
    times = 100
    all_terms = [(fnames[i % len(fnames)], str(i).encode("ascii"))
                 for i in xrange(times)]
    all_terms.sort()

    # Create some fake data for a matcher we'll add
    data = [(1, 2, 3.0), (4, 5, 6.0), (7, 8, 9.0), (10, 11, 1.5)]
    posts = [Posting(id=docid, length=length, weight=weight)
             for docid, length, weight in data]
    lm = matching.ListMatcher(posts)

    cdc = default_codec()
    with TempDB("addmatcher") as db:
        # Write a single document containing the fake terms
        with db.open(write=True, create=True) as w:
            dw = cdc.doc_writer(w)
            dw.start_doc(0)

            for fname in fnames:
                dw.add_field(fname, fobj, None, 6)
                posts = [Posting(id=tbytes, weight=2.5, length=6)
                         for fn, tbytes in all_terms
                         if fn == fname]
                dw.add_field_postings(fname, fobj, 6, posts)

            dw.finish_doc()
            dw.close()

        # Pick one of the fake terms
        fieldname, termbytes = all_terms[len(all_terms) // 3]

        # Add the fake matcher's postings to the chosen term's
        with db.open(write=True) as w:
            dw = cdc.doc_writer(w)
            dw.add_matcher(fieldname, fobj, termbytes, lm)
            dw.close()

        # Open a matcher for the chosen term and check that its postings consist
        # of the first fake document, and then the data from the matcher we
        # added
        with db.open() as r:
            tr = cdc.term_reader(r)
            m = tr.matcher(fieldname, fobj, termbytes)
            result = [(v.id, v.length, v.weight) for v in m.all_values()]
            assert result == [(0, 6, 2.5)] + data


def test_postings():
    import re

    def ana(text, **kwargs):
        t = analysis.Token(positions=True, chars=True)
        for i, match in enumerate(re.finditer("[^ ]+", text)):
            t.text = match.group()
            t.boost = float(i + 1)
            t.pos = i
            t.startchar = match.start()
            t.endchar = match.end()
            t.payload = t.text[0].encode("ascii")
            yield t

    domain = u"alfa bravo charlie".split()
    form = formats.BasicFormat(lengths=True, weights=True, positions=True,
                               characters=True, payloads=True)
    fobj = fields.FieldType(form, ana)
    cdc = default_codec()
    with TempDB("cdcpostings") as db:
        for i, ls in enumerate(permutations(domain)):
            with db.open(write=True, create=True) as w:
                dw = cdc.doc_writer(w)
                value = " ".join(ls)
                length, posts = form.index(ana, fobj.to_bytes, value)
                posts = list(posts)
                dw.start_doc(i)
                dw.add_field("a", fobj, None, length)
                dw.add_field_postings("a", fobj, length, posts)
                dw.finish_doc()
                dw.close()

        result = {}
        with db.open() as r:
            tr = cdc.term_reader(r)
            for fieldname, termbytes in tr.terms():
                m = tr.matcher(fieldname, fobj, termbytes)
                result[termbytes] = list(m.all_values())


def test_doc_id_range():
    cdc = default_codec()
    fieldobj = fields.KEYWORD()
    with TempDB("docidrange") as db:
        with db.open(write=True, create=True) as w:
            dw = cdc.doc_writer(w)
            for docid in (1000, 2000, 3000, 4000, 5000, 6000):
                dw.start_doc(docid)
                dw.add_field("a", fieldobj, None, None)
                dw.finish_doc()
            dw.close()

        with db.open() as r:
            dr = cdc.doc_reader(r)
            assert dr.doc_id_range() == (1000, 6000)
            dr.close()

        with db.open(write=True) as w:
            dw = cdc.doc_writer(w)
            dw.delete(1000)
            dw.delete(2000)
            dw.delete(6000)
            dw.close()

        with db.open() as r:
            dr = cdc.doc_reader(r)
            assert dr.doc_id_range() == (3000, 5000)
            dr.close()


def test_field_length():
    fieldobj = fields.TEXT()
    cdc = default_codec()
    with TempDB("codecfieldlen") as db:
        with db.open(write=True, create=True) as w:
            dw = cdc.doc_writer(w)
            dw.start_doc(0)
            dw.add_field("a", fieldobj, None, 9)
            dw.add_field_postings("a", fieldobj, 9,
                                  [Posting(id=b"word", weight=9, length=9)])
            dw.finish_doc()

            dw.start_doc(1)
            dw.add_field("a", fieldobj, None, 4)
            dw.add_field_postings("a", fieldobj, 4,
                                  [Posting(id=b"word", weight=9, length=4)])
            dw.finish_doc()
            dw.close()

        with db.open() as r:
            dr = cdc.doc_reader(r)
            assert dr.min_field_length("a") == 4
            assert dr.max_field_length("a") == 9


def test_column_readwrite():
    values = b"alfa bravo charlie delta echo foxtrot".split()

    def hink(n):
        return n * 2 + 5

    fieldobj = fields.ID(sortable=True)
    assert fieldobj.column_type
    cdc = default_codec()
    with TempDB("codeccolumn") as db:
        with db.open(write=True, create=True) as w:
            cw = cdc.column_writer(w)
            for i, v in enumerate(values):
                cw.add_value("f", fieldobj, hink(i), v)
            cw.close()

            cr = cdc.column_reader(w, "f", fieldobj)
            for i, v in enumerate(values):
                assert cr[hink(i)] == v

        with db.open() as r:
            cr = cdc.column_reader(r, "f", fieldobj)
            for i, v in enumerate(values):
                assert cr[hink(i)] == v

        delete = (0, 2, 3)
        with db.open(write=True) as w:
            cw = cdc.column_writer(w)
            for i in delete:
                cw.remove_value("f", fieldobj, hink(i))
            cw.close()

        with db.open() as r:
            cr = cdc.column_reader(r, "f", fieldobj)
            for i, v in enumerate(values):
                vv = cr[hink(i)]
                if i in delete:
                    assert vv == fieldobj.column_type.default_value()
                else:
                    assert vv == v
