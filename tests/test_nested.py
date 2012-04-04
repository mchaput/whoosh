from __future__ import with_statement
from nose.tools import assert_equal  #@UnresolvedImport

from whoosh import fields, query, scoring
from whoosh.compat import u
from whoosh.filedb.filestore import RamStorage


def test_nested():
    schema = fields.Schema(name=fields.ID(stored=True), type=fields.ID,
                           part=fields.ID, price=fields.NUMERIC)
    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        with w.group():
            w.add_document(name=u("iPad"), type=u("product"))
            w.add_document(part=u("screen"), price=100)
            w.add_document(part=u("battery"), price=50)
            w.add_document(part=u("case"), price=20)

        with w.group():
            w.add_document(name=u("iPhone"), type=u("product"))
            w.add_document(part=u("screen"), price=60)
            w.add_document(part=u("battery"), price=30)
            w.add_document(part=u("case"), price=10)

        with w.group():
            w.add_document(name=u("Mac mini"), type=u("product"))
            w.add_document(part=u("hard drive"), price=50)
            w.add_document(part=u("case"), price=50)

    with ix.searcher() as s:
        price = s.schema["price"]

        pq = query.Term("type", "product")
        cq = query.Term("price", price.to_text(50))
        q = query.Nested(pq, cq)

        r = s.search(q)
        assert_equal(sorted([hit["name"] for hit in r]), ["Mac mini", "iPad"])


def test_scoring():
    schema = fields.Schema(kind=fields.ID,
                           name=fields.KEYWORD(scorable=True, stored=True))
    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        with w.group():
            w.add_document(kind=u("class"), name=u("Index"))
            w.add_document(kind=u("method"), name=u("add document"))
            w.add_document(kind=u("method"), name=u("add reader"))
            w.add_document(kind=u("method"), name=u("close"))
        with w.group():
            w.add_document(kind=u("class"), name=u("Accumulator"))
            w.add_document(kind=u("method"), name=u("add"))
            w.add_document(kind=u("method"), name=u("get result"))
        with w.group():
            w.add_document(kind=u("class"), name=u("Calculator"))
            w.add_document(kind=u("method"), name=u("add"))
            w.add_document(kind=u("method"), name=u("add all"))
            w.add_document(kind=u("method"), name=u("add some"))
            w.add_document(kind=u("method"), name=u("multiply"))
            w.add_document(kind=u("method"), name=u("close"))

    with ix.searcher() as s:
        q = query.Nested(query.Term("kind", "class"),
                         query.Term("name", "add"))
        r = s.search(q)
        assert_equal([hit["name"] for hit in r], ["Calculator", "Index",
                                                  "Accumulator"])


def test_deletion():
    schema = fields.Schema(kind=fields.ID,
                           name=fields.KEYWORD(scorable=True, stored=True))
    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        with w.group():
            w.add_document(kind=u("class"), name=u("Index"))
            w.add_document(kind=u("method"), name=u("add document"))
            w.add_document(kind=u("method"), name=u("add reader"))
            w.add_document(kind=u("method"), name=u("close"))
        with w.group():
            w.add_document(kind=u("class"), name=u("Accumulator"))
            w.add_document(kind=u("method"), name=u("add"))
            w.add_document(kind=u("method"), name=u("get result"))
        with w.group():
            w.add_document(kind=u("class"), name=u("Calculator"))
            w.add_document(kind=u("method"), name=u("add"))
            w.add_document(kind=u("method"), name=u("add all"))
            w.add_document(kind=u("method"), name=u("add some"))
            w.add_document(kind=u("method"), name=u("multiply"))
            w.add_document(kind=u("method"), name=u("close"))
        with w.group():
            w.add_document(kind=u("class"), name=u("Deleter"))
            w.add_document(kind=u("method"), name=u("add"))
            w.add_document(kind=u("method"), name=u("delete"))

    with ix.searcher() as s:
        q = query.Nested(query.Term("kind", "class"),
                         query.Term("name", "add"))

        r = s.search(q)
        assert_equal([hit["name"] for hit in r], ["Calculator", "Index",
                                                  "Accumulator", "Deleter"])

    with ix.writer() as w:
        w.delete_by_term("name", "Accumulator")
        w.delete_by_term("name", "Calculator")

    with ix.searcher() as s:
        pq = query.Term("kind", "class")
        assert_equal(len(list(pq.docs(s))), 2)
        q = query.Nested(pq, query.Term("name", "add"))
        r = s.search(q)
        assert_equal([hit["name"] for hit in r], ["Index", "Deleter"])


def test_all_parents_deleted():
    schema = fields.Schema(kind=fields.ID,
                           name=fields.KEYWORD(scorable=True, stored=True))
    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        with w.group():
            w.add_document(kind=u("class"), name=u("Index"))
            w.add_document(kind=u("method"), name=u("add document"))
            w.add_document(kind=u("method"), name=u("add reader"))
            w.add_document(kind=u("method"), name=u("close"))
        with w.group():
            w.add_document(kind=u("class"), name=u("Accumulator"))
            w.add_document(kind=u("method"), name=u("add"))
            w.add_document(kind=u("method"), name=u("get result"))
        with w.group():
            w.add_document(kind=u("class"), name=u("Calculator"))
            w.add_document(kind=u("method"), name=u("add"))
            w.add_document(kind=u("method"), name=u("add all"))
            w.add_document(kind=u("method"), name=u("add some"))
            w.add_document(kind=u("method"), name=u("multiply"))
            w.add_document(kind=u("method"), name=u("close"))
        with w.group():
            w.add_document(kind=u("class"), name=u("Deleter"))
            w.add_document(kind=u("method"), name=u("add"))
            w.add_document(kind=u("method"), name=u("delete"))

    with ix.writer() as w:
        w.delete_by_term("name", "Index")
        w.delete_by_term("name", "Accumulator")
        w.delete_by_term("name", "Calculator")
        w.delete_by_term("name", "Deleter")

    with ix.searcher() as s:
        q = query.Nested(query.Term("kind", "class"),
                         query.Term("name", "add"))
        r = s.search(q)
        assert r.is_empty()


def test_everything_is_a_parent():
    schema = fields.Schema(id=fields.STORED, kind=fields.ID,
                           name=fields.ID(stored=True))
    k = u("alfa")
    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        w.add_document(id=0, kind=k, name=u("one"))
        w.add_document(id=1, kind=k, name=u("two"))
        w.add_document(id=2, kind=k, name=u("three"))
        w.add_document(id=3, kind=k, name=u("four"))
        w.add_document(id=4, kind=k, name=u("one"))
        w.add_document(id=5, kind=k, name=u("two"))
        w.add_document(id=6, kind=k, name=u("three"))
        w.add_document(id=7, kind=k, name=u("four"))
        w.add_document(id=8, kind=k, name=u("one"))
        w.add_document(id=9, kind=k, name=u("two"))
        w.add_document(id=10, kind=k, name=u("three"))
        w.add_document(id=11, kind=k, name=u("four"))

    with ix.searcher() as s:
        pq = query.Term("kind", k)
        cq = query.Or([query.Term("name", "two"), query.Term("name", "four")])
        q = query.Nested(pq, cq)
        r = s.search(q)
        assert_equal([hit["id"] for hit in r], [1, 3, 5, 7, 9, 11])


def test_no_parents():
    schema = fields.Schema(id=fields.STORED, kind=fields.ID,
                           name=fields.ID(stored=True))
    k = u("alfa")
    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        w.add_document(id=0, kind=k, name=u("one"))
        w.add_document(id=1, kind=k, name=u("two"))
        w.add_document(id=2, kind=k, name=u("three"))
        w.add_document(id=3, kind=k, name=u("four"))
        w.add_document(id=4, kind=k, name=u("one"))
        w.add_document(id=5, kind=k, name=u("two"))
        w.add_document(id=6, kind=k, name=u("three"))
        w.add_document(id=7, kind=k, name=u("four"))
        w.add_document(id=8, kind=k, name=u("one"))
        w.add_document(id=9, kind=k, name=u("two"))
        w.add_document(id=10, kind=k, name=u("three"))
        w.add_document(id=11, kind=k, name=u("four"))

    with ix.searcher() as s:
        pq = query.Term("kind", "bravo")
        cq = query.Or([query.Term("name", "two"), query.Term("name", "four")])
        q = query.Nested(pq, cq)
        r = s.search(q)
        assert r.is_empty()


