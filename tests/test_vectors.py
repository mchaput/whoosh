from __future__ import with_statement

from whoosh import fields, formats
from whoosh.compat import u
from whoosh.filedb.filestore import RamStorage
from whoosh.util.testing import TempIndex


def test_single_term():
    schema = fields.Schema(text=fields.TEXT(vector=True))
    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        w.add_document(text=u("TEST TEST TEST"))
    with ix.searcher() as s:
        v = s.vector(0, "text")
        assert v.is_active()


def test_vector_reading():
    schema = fields.Schema(title=fields.TEXT,
                           content=fields.TEXT(vector=formats.Frequency()))

    with TempIndex(schema, "vectorreading") as ix:
        writer = ix.writer()
        writer.add_document(title=u("one"),
                            content=u("This is the story of the black " +
                                      "hole story"))
        writer.commit()

        with ix.reader() as r:
            assert list(r.vector_as("frequency", 0, "content")) == [(u('black'), 1), (u('hole'), 1), (u('story'), 2)]


def test_vector_merge():
    schema = fields.Schema(title=fields.TEXT,
                           content=fields.TEXT(vector=formats.Frequency()))

    with TempIndex(schema, "vectormerge") as ix:
        writer = ix.writer()
        writer.add_document(title=u("one"),
                            content=u("This is the story of the black hole " +
                                      "story"))
        writer.commit()

        writer = ix.writer()
        writer.add_document(title=u("two"),
                            content=u("You can read along in your book"))
        writer.commit()

        with ix.searcher() as s:
            r = s.reader()

            docnum = s.document_number(title=u("one"))
            vec = list(r.vector_as("frequency", docnum, "content"))
            assert vec == [(u('black'), 1), (u('hole'), 1), (u('story'), 2)]

            docnum = s.document_number(title=u("two"))

            vec = list(r.vector_as("frequency", docnum, "content"))
            assert vec == [(u('along'), 1), (u('book'), 1), (u('read'), 1)]


def test_vector_unicode():
    cf = fields.TEXT(vector=True)
    schema = fields.Schema(content=cf)
    ix = RamStorage().create_index(schema)

    writer = ix.writer()
    writer.add_document(content=u("\u13a0\u13a1\u13a2 \u13a3\u13a4\u13a5"))
    writer.add_document(content=u("\u13a6\u13a7\u13a8 \u13a9\u13aa\u13ab"))
    writer.commit()

    writer = ix.writer()
    writer.add_document(content=u("\u13ac\u13ad\u13ae \u13af\u13b0\u13b1"))
    writer.add_document(content=u("\u13b2\u13b3\u13b4 \u13b5\u13b6\u13b7"))
    writer.commit()

    with ix.reader() as r:
        vec = list(r.vector_as("frequency", 0, "content"))
        assert vec == [(u('\u13ac\u13ad\u13ae'), 1), (u('\u13af\u13b0\u13b1'), 1)]


def test_add_vectored_field():
    schema = fields.Schema(id=fields.ID(stored=True), f1=fields.TEXT)
    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        w.add_document(id=u("a"), f1=u("Testing one two three"))

    with ix.writer() as w:
        w.add_field("f2", fields.TEXT(vector=True))
        w.add_document(id=u("b"), f2=u("Frosting four five six"))

    with ix.searcher() as s:
        docnum1 = s.document_number(id="a")
        assert not s.has_vector(docnum1, "f1")

        docnum2 = s.document_number(id="b")
        assert not s.has_vector(docnum2, "f1")
        assert s.has_vector(docnum2, "f2")
