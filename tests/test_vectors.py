from __future__ import with_statement

from whoosh import analysis, fields
from whoosh.util.testing import TempIndex


def test_single_term():
    schema = fields.Schema(text=fields.TEXT(vector=True))
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(text=u"TEST TEST TEST")

        with ix.searcher() as s:
            v = s.reader().vector(0, "text")
            assert len(v)


def test_vector_reading():
    schema = fields.Schema(title=fields.TEXT,
                           content=fields.TEXT(vector=True))

    with TempIndex(schema, "vectorreading") as ix:
        writer = ix.writer()
        writer.add_document(
            title=u"one",
            content=u"This is the story of the black hole story"
        )
        writer.commit()

        with ix.reader() as r:
            v = r.vector(0, "content")
            items = list(v.terms_and_weights())
            assert items == [(b'black', 1), (b'hole', 1), (b'story', 2)]


def test_vector_merge():
    schema = fields.Schema(title=fields.TEXT,
                           content=fields.TEXT(vector=True))

    with TempIndex(schema, "vectormerge") as ix:
        with ix.writer() as w:
            w.add_document(
                title=u"one",
                content=u"This is the story of the black hole story"
            )

        with ix.writer() as w:
            w.add_document(title=u"two",
                           content=u"You can read along in your book")
            w.merge = False

        with ix.searcher() as s:
            r = s.reader()

            docnum = s.document_number(title=u"one")
            v = r.vector(docnum, "content")
            items = list(v.terms_and_weights())
            assert items == [(b'black', 1), (b'hole', 1), (b'story', 2)]

            docnum = s.document_number(title=u"two")
            v = r.vector(docnum, "content")
            items = list(v.terms_and_weights())
            assert items == [(b'along', 1), (b'book', 1), (b'read', 1)]


def test_vector_unicode():
    cf = fields.TEXT(analyzer=analysis.RegexTokenizer(), vector=True)
    schema = fields.Schema(id=fields.NUMERIC, text=cf)

    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(id=0, text=u"\u13a0\u13a1\u13a2 \u13a3\u13a4\u13a5")
            w.add_document(id=1, text=u"\u13a6\u13a7\u13a8 \u13a9\u13aa\u13ab")

        with ix.writer() as w:
            w.add_document(id=2, text=u"\u13ac\u13ad\u13ae \u13af\u13b0\u13b1")
            w.add_document(id=3, text=u"\u13b2\u13b3\u13b4 \u13b5\u13b6\u13b7")

        with ix.searcher() as s:
            from_bytes = s.schema["text"].from_bytes

            docnum = s.document_number(id=2)
            v = s.reader().vector(docnum, "text")
            assert len(v) == 2

            assert from_bytes(v.termbytes(0)) == u"\u13ac\u13ad\u13ae"
            assert v.weight(0) == 1

            assert from_bytes(v.termbytes(1)) == u"\u13af\u13b0\u13b1"
            assert v.weight(1) == 1


def test_add_vectored_field():
    schema = fields.Schema(id=fields.ID(stored=True), f1=fields.TEXT)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(id=u"a", f1=u"Testing one two three")

        with ix.writer() as w:
            w.add_field("f2", fields.TEXT(vector=True))
            w.add_document(id=u"b", f2=u"Frosting four five six")

        with ix.searcher() as s:
            docnum1 = s.document_number(id="a")
            assert not s.reader().has_vector(docnum1, "f1")

            docnum2 = s.document_number(id="b")
            assert not s.reader().has_vector(docnum2, "f1")
            assert s.reader().has_vector(docnum2, "f2")
