from __future__ import with_statement

from whoosh import fields
from whoosh.util.testing import TempIndex, TempStorage


def test_addfield():
    schema = fields.Schema(id=fields.ID(stored=True), content=fields.TEXT)
    with TempIndex(schema, "addfield") as ix:
        w = ix.writer()
        w.add_document(id=u"a", content=u"alfa")
        w.add_document(id=u"b", content=u"bravo")
        w.add_document(id=u"c", content=u"charlie")
        w.commit()

        ix.add_field("added", fields.KEYWORD(stored=True))

        w = ix.writer()
        w.add_document(id=u"d", content=u"delta", added=u"fourth")
        w.add_document(id=u"e", content=u"echo", added=u"fifth")
        w.commit(merge=False)

        with ix.searcher() as s:
            assert ("id", "d") in s.reader()
            assert s.document(id="d") == {"id": "d", "added": "fourth"}
            assert s.document(id="b") == {"id": "b"}


def test_addfield_spelling():
    schema = fields.Schema(id=fields.ID(stored=True), content=fields.TEXT)
    with TempIndex(schema, "addfield") as ix:
        w = ix.writer()
        w.add_document(id=u"a", content=u"alfa")
        w.add_document(id=u"b", content=u"bravo")
        w.add_document(id=u"c", content=u"charlie")
        w.commit()

        ix.add_field("added", fields.KEYWORD(stored=True))

        w = ix.writer()
        w.add_document(id=u"d", content=u"delta", added=u"fourth")
        w.add_document(id=u"e", content=u"echo", added=u"fifth")
        w.commit(merge=False)

        with ix.searcher() as s:
            assert s.document(id=u"d") == {"id": "d", "added": "fourth"}
            assert s.document(id=u"b") == {"id": "b"}


def test_removefield():
    schema = fields.Schema(id=fields.ID(stored=True),
                           content=fields.TEXT,
                           city=fields.KEYWORD(stored=True))
    with TempIndex(schema, "removefield") as ix:
        w = ix.writer()
        w.add_document(id=u"b", content=u"bravo", city=u"baghdad")
        w.add_document(id=u"c", content=u"charlie", city=u"cairo")
        w.add_document(id=u"d", content=u"delta", city=u"dakar")
        w.commit()

        with ix.searcher() as s:
            assert s.document(id=u"c") == {"id": "c", "city": "cairo"}

        w = ix.writer()
        w.remove_field("content")
        w.remove_field("city")
        w.commit()

        ixschema = ix.schema
        assert ixschema.names() == ["id"]
        assert ixschema.stored_names() == ["id"]

        with ix.searcher() as s:
            assert ("content", b"charlie") not in s.reader()
            assert s.document(id=u"c") == {"id": u"c"}


def test_optimize_away():
    schema = fields.Schema(id=fields.ID(stored=True),
                           content=fields.TEXT,
                           city=fields.KEYWORD(stored=True))
    with TempIndex(schema, "optimizeaway") as ix:
        w = ix.writer()
        w.add_document(id=u"b", content=u"bravo", city=u"baghdad")
        w.add_document(id=u"c", content=u"charlie", city=u"cairo")
        w.add_document(id=u"d", content=u"delta", city=u"dakar")
        w.commit()

        with ix.searcher() as s:
            assert s.document(id=u"c") == {"id": "c", "city": "cairo"}

        w = ix.writer()
        w.remove_field("content")
        w.remove_field("city")
        w.commit(optimize=True)

        with ix.searcher() as s:
            assert ("content", u"charlie") not in s.reader()
            assert s.document(id=u"c") == {"id": u"c"}


def test_dynamic_column():
    with TempStorage() as st:
        schema1 = fields.Schema(data=fields.Id(stored=True))
        with st.create_index(schema1) as ix:
            with ix.writer() as w:
                w.add_document(data="alfa")
                w.add_document(data="bravo")
                w.add_document(data="charlie")

        schema2 = fields.Schema(data=fields.Id(stored=False, column=True))
        with st.open_index(schema=schema2) as ix:
            assert ix.schema is schema2
            assert ix.schema["data"].column

            with ix.writer() as w:
                assert w.schema is schema2
                assert w.schema["data"].column

                w.add_document(data="delta")
                w.add_document(data="echo")
                w.add_document(data="foxtrot")

            with ix.reader() as r:
                assert ("data", "delta") in r

                cr = r.column_reader("data")
                assert cr[0] == ''
                assert cr[1] == ''
                assert cr[2] == ''
                assert cr[3] == 'delta'
                assert cr[4] == 'echo'
                assert cr[5] == 'foxtrot'


def test_dynamic_positions():
    from whoosh.query import Term

    with TempStorage() as st:
        schema1 = fields.Schema(data=fields.Text(phrase=False))
        with st.create_index(schema1) as ix:
            with ix.writer() as w:
                w.add_document(data="alfa bravo charlie delta")
                w.add_document(data="bravo charlie delta echo")
                w.add_document(data="charlie delta echo foxtrot")

        schema2 = fields.Schema(data=fields.Text(phrase=True))
        with st.open_index(schema=schema2) as ix:
            with ix.writer() as w:
                w.add_document(data="delta echo foxtrot")
                w.add_document(data="echo delta charlie")
                w.add_document(data="charlie bravo alfa")

            with ix.searcher() as s:
                m = Term("data", "charlie").matcher(s, s.context())
                out = []
                while m.is_active():
                    out.append((m.id(), [s.start for s in m.spans()]))
                    m.next()
                m.close()

                assert out == [
                    (0, []),
                    (1, []),
                    (2, []),
                    (4, [2]),
                    (5, [0])
                ]


def test_dynamic_chars():
    from whoosh.query import Term

    with TempStorage() as st:
        schema1 = fields.Schema(data=fields.Text(phrase=True))
        with st.create_index(schema1) as ix:
            with ix.writer() as w:
                w.add_document(data="alfa bravo charlie delta")
                w.add_document(data="bravo charlie delta echo")
                w.add_document(data="charlie delta echo foxtrot")

        schema2 = fields.Schema(data=fields.Text(phrase=True, chars=True))
        with st.open_index(schema=schema2) as ix:
            with ix.writer() as w:
                w.add_document(data="delta echo foxtrot")
                w.add_document(data="echo delta charlie")
                w.add_document(data="charlie bravo alfa")

            with ix.searcher() as s:
                m = Term("data", "charlie").matcher(s, s.context())
                out = []
                while m.is_active():
                    out.append((m.id(), [s.startchar for s in m.spans()]))
                    m.next()
                m.close()

                assert out == [
                    (0, [None]),
                    (1, [None]),
                    (2, [None]),
                    (4, [11]),
                    (5, [0])
                ]


def test_dynamic_remove_chars():
    from whoosh.query import Term

    with TempStorage() as st:
        schema1 = fields.Schema(data=fields.Text(phrase=True, chars=True))
        with st.create_index(schema1) as ix:
            with ix.writer() as w:
                w.add_document(data="alfa bravo charlie delta")
                w.add_document(data="bravo charlie delta echo")
                w.add_document(data="charlie delta echo foxtrot")

        schema2 = fields.Schema(data=fields.Text(phrase=True, chars=False))
        with st.open_index(schema=schema2) as ix:
            with ix.writer() as w:
                w.add_document(data="delta echo foxtrot")
                w.add_document(data="echo delta charlie")
                w.add_document(data="charlie bravo alfa")

            with ix.searcher() as s:
                ss = s.leaf_searchers()[0][0]
                print("ss=", ss)
                r = ss.reader()
                m = Term("data", "charlie").matcher(ss, ss.context())
                print("m=", m)
                print("format=", m._format)
                print("format.has_chars=", m._format.has_chars)
                while m.is_active():
                    print("id=", m.id())
                    print(m.posting())
                    print(m._posts.has_chars)
                    m.next()

                m = Term("data", "charlie").matcher(s, s.context())
                out = []
                while m.is_active():
                    out.append((m.id(), [s.startchar for s in m.spans()]))
                    m.next()
                m.close()

                print(out)
                assert out == [
                    (0, [11]),
                    (1, [6]),
                    (2, [0]),
                    (4, [None]),
                    (5, [None])
                ]


def test_dynamic_vector():
    from whoosh.ifaces import readers

    with TempStorage() as st:
        schema1 = fields.Schema(data=fields.Text(vector=False))
        with st.create_index(schema1) as ix:
            with ix.writer() as w:
                w.add_document(data="alfa bravo charlie delta")
                w.add_document(data="bravo charlie delta echo")
                w.add_document(data="charlie delta echo foxtrot")

        schema2 = fields.Schema(data=fields.Text(vector=True))
        with st.open_index(schema=schema2) as ix:
            with ix.writer() as w:
                w.add_document(data="delta echo foxtrot")
                w.add_document(data="echo delta charlie")
                w.add_document(data="charlie bravo alfa")

            with ix.reader() as r:
                for docnum in r.all_doc_ids():
                    try:
                        v = r.vector(docnum, "data")
                        print(docnum, v)
                    except readers.NoVectorError:
                        print(docnum, "-")
                        pass

    assert False

