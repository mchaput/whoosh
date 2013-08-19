from __future__ import with_statement

from whoosh import fields, qparser, query
from whoosh.compat import b, u
from whoosh.filedb.filestore import RamStorage


def test_add():
    schema = fields.Schema(id=fields.STORED, text=fields.TEXT)
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(id=1, text=u("alfa bravo charlie"))
    w.add_document(id=2, text=u("alfa bravo delta"))
    w.add_document(id=3, text=u("alfa charlie echo"))
    w.commit()

    with ix.searcher() as s:
        assert s.doc_frequency("text", u("charlie")) == 2
        r = s.search(query.Term("text", u("charlie")))
        assert [hit["id"] for hit in r] == [1, 3]
        assert len(r) == 2


def test_filter_that_matches_no_document():
    schema = fields.Schema(id=fields.STORED, text=fields.TEXT)
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(id=1, text=u("alfa bravo charlie"))
    w.add_document(id=2, text=u("alfa bravo delta"))
    w.commit()

    with ix.searcher() as s:
        r = s.search(
            query.Every(),
            filter=query.Term("text", u("echo")))
        assert [hit["id"] for hit in r] == []
        assert len(r) == 0




