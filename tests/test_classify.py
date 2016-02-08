from __future__ import with_statement

from whoosh import analysis, classify, fields, query
from whoosh.filedb.filestore import RamStorage
from whoosh.util.testing import TempIndex


domain = [
    u"A volume that is a signed distance field used for collision calculations.  The turbulence is damped near the collision object to prevent particles from passing through.",
    u"When particles cross the SDF boundary they have their velocities reversed according to the SDF normal and are pushed outside of the SDF.",
    u"The distance at which the particles start to slow down due to a collision object.",
    u"There are several different ways to update a particle system in response to an external velocity field. They are broadly categorized as Force, Velocity, and Position updates.",
    u"Instead of applying a force in the direction of the velocity field, the force is applied relative to the difference between the particle's velocity and the velocity field.  This effectively adds an implicit drag that causes the particles to match the velocity field.",
    u"In Velocity Blend mode, the amount to mix in the field velocity every timestep.",
    u"In Velocity Blend mode, the amount to add the curlnoise velocity to the particle's velocity.  This can be useful in addition to advectbyvolume to layer turbulence on a velocity field.",
]

text = u"How do I use a velocity field for particles"


def create_index():
    analyzer = analysis.StandardAnalyzer()
    schema = fields.Schema(path=fields.ID(stored=True),
                           content=fields.TEXT(analyzer=analyzer,
                                               vector=True))

    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        from string import ascii_lowercase
        for letter, content in zip(ascii_lowercase, domain):
            w.add_document(path=u"/%s" % letter, content=content)
    return ix


def test_add_text():
    ix = create_index()
    with ix.searcher() as s:
        more = classify.MoreLike(s, "content")
        more.add_text(text)
        words = [word for word, score in more.get_terms(3)]
        assert words == ["particles", "velocity", "field"]
    ix.close()


def test_keyterms():
    ix = create_index()
    with ix.searcher() as s:
        r = s.search(query.Term("path", "/a"))
        keys = r[0].key_terms("content", top=3)
        assert keys == [u"collision", u"calculations", u"damped"]
    ix.close()


def test_keyterms_from_text():
    ix = create_index()
    with ix.searcher() as s:
        more = classify.MoreLike(s, "content")
        more.add_text(text)
        words = [word for word, score in more.get_terms(3)]
        assert words == ["particles", "velocity", "field"]
    ix.close()


def test_more_like_this():
    docs = [
        (u"1", u"alfa bravo charlie delta echo foxtrot golf"),
        (u"2", u"delta echo foxtrot golf hotel india juliet"),
        (u"3", u"echo foxtrot golf hotel india juliet kilo"),
        (u"4", u"foxtrot golf hotel india juliet kilo lima"),
        (u"5", u"golf hotel india juliet kilo lima mike"),
        (u"6", u"foxtrot golf hotel india alfa bravo charlie"),
    ]

    def _check(schema, use_text=None):
        with TempIndex(schema) as ix:
            with ix.writer() as w:
                for idnum, text in docs:
                    w.add_document(id=idnum, text=text)

            with ix.searcher() as s:
                mlt = classify.MoreLike(s, "text", maxterms=5)
                docid = s.document_number(id=u"1")
                r = mlt.like_docid(docid, text=use_text)
                ids = [hit["id"] for hit in r]
                assert ids == ["6", "2", "3"]

    schema = fields.Schema(id=fields.ID(stored=True),
                           text=fields.TEXT(stored=True))
    _check(schema)

    ana = analysis.StandardAnalyzer()
    schema = fields.Schema(id=fields.ID(stored=True),
                           text=fields.TEXT(analyzer=ana, vector=True))
    _check(schema)

    schema = fields.Schema(id=fields.ID(stored=True), text=fields.TEXT)
    _check(schema, use_text=docs[0][1])


def test_more_like():
    schema = fields.Schema(id=fields.ID(stored=True),
                           text=fields.TEXT(stored=True))
    with TempIndex(schema, "morelike") as ix:
        with ix.writer() as w:
            w.add_document(id=u"1", text=u"alfa bravo charlie")
            w.add_document(id=u"2", text=u"bravo charlie delta")
            w.add_document(id=u"3", text=u"echo")
            w.add_document(id=u"4", text=u"delta echo foxtrot")
            w.add_document(id=u"5", text=u"echo echo echo")
            w.add_document(id=u"6", text=u"foxtrot golf hotel")
            w.add_document(id=u"7", text=u"golf hotel india")

        with ix.searcher() as s:
            docnum = s.document_number(id="3")
            r = classify.MoreLike(s, "text").like_docid(docnum)
            assert [hit["id"] for hit in r] == ["5", "4"]


def test_empty_more_like():
    schema = fields.Schema(text=fields.TEXT)
    with TempIndex(schema, "emptymore") as ix:
        with ix.searcher() as s:
            assert s.doc_count() == 0
            q = query.Term("a", u"b")
            r = s.search(q)
            assert r.scored_length() == 0
            assert r.key_terms("text") == []

            more = classify.MoreLike(s, "text")
            assert more.get_terms(5) == []




