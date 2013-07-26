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


def test_daterange_matched_terms():
    from whoosh.qparser import GtLtPlugin
    from datetime import datetime

    schema = fields.Schema(id=fields.KEYWORD(stored=True),
                           body=fields.TEXT,
                           num=fields.NUMERIC(stored=True,unique=True),
                           created=fields.DATETIME(stored=True))
    ix = RamStorage().create_index(schema)

    with ix.writer() as w:
        w.add_document(id=u"one", body=u"this and this", num='5',
                       created=datetime.now())
        w.add_document(id=u"three", body=u"that and that", num='7',
                       created=datetime.now())
        w.add_document(id=u"two", body=u"this and that", num='6',
                       created=datetime.now())

    with ix.searcher() as s:
        parser = qparser.QueryParser("body", ix.schema)
        parser.add_plugin(GtLtPlugin())
        q = parser.parse(u"created:>='2013-07-01'")
        r = s.search(q, terms=True)

        assert r.has_matched_terms()
        assert (r[0].matched_terms()
                == [("created", b("(\x00\x00\x00\x00\x00\x80\xe1\xa2"))])

