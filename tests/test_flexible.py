from __future__ import with_statement

from whoosh import fields
from whoosh.compat import u, b
from whoosh.util.testing import TempIndex


def test_addfield():
    schema = fields.Schema(id=fields.ID(stored=True), content=fields.TEXT)
    with TempIndex(schema, "addfield") as ix:
        w = ix.writer()
        w.add_document(id=u("a"), content=u("alfa"))
        w.add_document(id=u("b"), content=u("bravo"))
        w.add_document(id=u("c"), content=u("charlie"))
        w.commit()

        ix.add_field("added", fields.KEYWORD(stored=True))

        w = ix.writer()
        w.add_document(id=u("d"), content=u("delta"), added=u("fourth"))
        w.add_document(id=u("e"), content=u("echo"), added=u("fifth"))
        w.commit(merge=False)

        with ix.searcher() as s:
            assert ("id", "d") in s.reader()
            assert s.document(id="d") == {"id": "d", "added": "fourth"}
            assert s.document(id="b") == {"id": "b"}


def test_addfield_spelling():
    schema = fields.Schema(id=fields.ID(stored=True), content=fields.TEXT)
    with TempIndex(schema, "addfield") as ix:
        w = ix.writer()
        w.add_document(id=u("a"), content=u("alfa"))
        w.add_document(id=u("b"), content=u("bravo"))
        w.add_document(id=u("c"), content=u("charlie"))
        w.commit()

        ix.add_field("added", fields.KEYWORD(stored=True, spelling=True))

        w = ix.writer()
        w.add_document(id=u("d"), content=u("delta"), added=u("fourth"))
        w.add_document(id=u("e"), content=u("echo"), added=u("fifth"))
        w.commit(merge=False)

        with ix.searcher() as s:
            assert s.document(id=u("d")) == {"id": "d", "added": "fourth"}
            assert s.document(id=u("b")) == {"id": "b"}


def test_removefield():
    schema = fields.Schema(id=fields.ID(stored=True),
                           content=fields.TEXT,
                           city=fields.KEYWORD(stored=True))
    with TempIndex(schema, "removefield") as ix:
        w = ix.writer()
        w.add_document(id=u("b"), content=u("bravo"), city=u("baghdad"))
        w.add_document(id=u("c"), content=u("charlie"), city=u("cairo"))
        w.add_document(id=u("d"), content=u("delta"), city=u("dakar"))
        w.commit()

        with ix.searcher() as s:
            assert s.document(id=u("c")) == {"id": "c", "city": "cairo"}

        w = ix.writer()
        w.remove_field("content")
        w.remove_field("city")
        w.commit()

        ixschema = ix._current_schema()
        assert ixschema.names() == ["id"]
        assert ixschema.stored_names() == ["id"]

        with ix.searcher() as s:
            assert ("content", b("charlie")) not in s.reader()
            assert s.document(id=u("c")) == {"id": u("c")}


def test_optimize_away():
    schema = fields.Schema(id=fields.ID(stored=True),
                           content=fields.TEXT,
                           city=fields.KEYWORD(stored=True))
    with TempIndex(schema, "optimizeaway") as ix:
        w = ix.writer()
        w.add_document(id=u("b"), content=u("bravo"), city=u("baghdad"))
        w.add_document(id=u("c"), content=u("charlie"), city=u("cairo"))
        w.add_document(id=u("d"), content=u("delta"), city=u("dakar"))
        w.commit()

        with ix.searcher() as s:
            assert s.document(id=u("c")) == {"id": "c", "city": "cairo"}

        w = ix.writer()
        w.remove_field("content")
        w.remove_field("city")
        w.commit(optimize=True)

        with ix.searcher() as s:
            assert ("content", u("charlie")) not in s.reader()
            assert s.document(id=u("c")) == {"id": u("c")}


if __name__ == "__main__":
    test_addfield()
