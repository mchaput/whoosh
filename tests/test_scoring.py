import copy

from whoosh import analysis, fields, qparser, query, scoring
from whoosh.compat import text_type
from whoosh.ifaces import weights, searchers
from whoosh.util.testing import TempIndex


def test_missing_field_scoring():
    schema = fields.Schema(name=fields.TEXT(stored=True),
                           hobbies=fields.TEXT(stored=True))
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(name=u'Frank', hobbies=u'baseball, basketball')

        with ix.reader() as r:
            assert r.field_length("hobbies") == 2
            assert r.field_length("name") == 1

        with ix.writer() as w:
            w.add_document(name=u'Jonny')

        with ix.searcher() as s:
            assert s.field_length("hobbies") == 2
            assert s.field_length("name") == 2

            parser = qparser.MultifieldParser(['name', 'hobbies'], schema)
            q = parser.parse(u"baseball")
            result = s.search(q)
            assert len(result) == 1


def test_weighting():
    schema = fields.Schema(id=fields.ID(stored=True),
                           n_comments=fields.STORED)
    with TempIndex(schema) as ix:
        w = ix.writer()
        w.add_document(id=u"1", n_comments=5)
        w.add_document(id=u"2", n_comments=12)
        w.add_document(id=u"3", n_comments=2)
        w.add_document(id=u"4", n_comments=7)
        w.commit()

        # Fake Weighting implementation
        class CommentWeighting(weights.WeightingModel):
            def scorer(self, searcher, fieldname, text, qf=1):
                return self.CommentScorer(searcher.stored_fields)

            class CommentScorer(weights.Scorer):
                def __init__(self, stored_fields):
                    self.stored_fields = stored_fields

                def max_quality(self):
                    return 1.0

                def score(self, matcher):
                    sf = self.stored_fields(matcher.id())
                    ncomments = sf.get("n_comments", 0)
                    return ncomments

        with ix.searcher(weighting=CommentWeighting()) as s:
            q = query.TermRange("id", u"1", u"4", constantscore=False)

            r = s.search(q)
            ids = [fs["id"] for fs in r]
            assert ids == ["2", "4", "1", "3"]


def test_finalweighting():
    schema = fields.Schema(id=fields.ID(stored=True),
                           summary=fields.TEXT,
                           n_comments=fields.STORED)
    with TempIndex(schema) as ix:
        w = ix.writer()
        w.add_document(id=u"1", summary=u"alfa bravo", n_comments=5)
        w.add_document(id=u"2", summary=u"alfa", n_comments=12)
        w.add_document(id=u"3", summary=u"bravo", n_comments=2)
        w.add_document(id=u"4", summary=u"bravo bravo", n_comments=7)
        w.commit()

        class CommentWeighting(scoring.Frequency):
            use_final = True

            def final(self, searcher: 'searchers.Searcher', docnum: int,
                      score: float) -> float:
                ncomments = searcher.stored_fields(docnum).get("n_comments", 0)
                return ncomments

        with ix.searcher(weighting=CommentWeighting()) as s:
            q = qparser.QueryParser("summary", None).parse("alfa OR bravo")
            r = s.search(q)
            ids = [fs["id"] for fs in r]
            assert ["2", "4", "1", "3"] == ids


def test_final2():
    schema = fields.Schema(path=fields.STORED, text=fields.TEXT)

    class MyWeighting(scoring.BM25F):
        use_final = True

        def final(self, searcher, docnum, score):
            path = searcher.stored_fields(docnum)["path"]
            return score * ord(path[0])

    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(path=u"f", text=u"alfa")
            w.add_document(path=u"b", text=u"alfa")
            w.add_document(path=u"c", text=u"alfa")
            w.add_document(path=u"a", text=u"alfa")
            w.add_document(path=u"e", text=u"alfa")

        with ix.searcher(weighting=MyWeighting()) as s:
            q = query.Term("text", u"alfa")
            r = s.search(q)
            paths = [hit["path"] for hit in r]
            assert " ".join(paths) == "f e c b a"


def test_final3():
    schema = fields.Schema(path=fields.ID(sortable=True), text=fields.TEXT)

    class MyWeighting(scoring.BM25F):
        use_final = True

        def final(self, searcher, docnum, score):
            cr = searcher.reader().column_reader("path")
            path = cr[docnum]
            return score * ord(path[0])

    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(path=u"f", text=u"alfa")
            w.add_document(path=u"b", text=u"alfa")
            w.add_document(path=u"c", text=u"alfa")
            w.add_document(path=u"a", text=u"alfa")
            w.add_document(path=u"e", text=u"alfa")

        with ix.searcher(weighting=MyWeighting()) as s:
            assert s.reader().has_column("path")

            q = query.Term("text", u"alfa")
            r = s.search(q)
            paths = [hit["path"] for hit in r]
            assert " ".join(paths) == "f e c b a"


def test_fieldboost():
    schema = fields.Schema(id=fields.STORED, a=fields.TEXT, b=fields.TEXT)
    with TempIndex(schema) as ix:
        w = ix.writer()
        w.add_document(id=0, a=u"alfa bravo charlie", b=u"echo foxtrot india")
        w.add_document(id=1, a=u"delta bravo charlie", b=u"alfa alfa alfa")
        w.add_document(id=2, a=u"alfa alfa alfa", b=u"echo foxtrot india")
        w.add_document(id=3, a=u"alfa sierra romeo", b=u"alfa tango echo")
        w.add_document(id=4, a=u"bravo charlie delta", b=u"alfa foxtrot india")
        w.add_document(id=5, a=u"alfa alfa echo", b=u"tango tango tango")
        w.add_document(id=6, a=u"alfa bravo echo", b=u"alfa alfa tango")
        w.commit()

        def field_booster(fieldname, factor=2.0):
            """
            Returns a function which will boost the given field in a query tree.
            """

            def booster_fn(obj):
                if obj.is_leaf() and obj.field() == fieldname:
                    obj.boost *= factor
                    return obj
                else:
                    return obj
            return booster_fn

        with ix.searcher() as s:
            q = query.Or([query.Term("a", u"alfa"),
                          query.Term("b", u"alfa")])
            q = q.accept(field_booster("a", 100.0))
            assert text_type(q) == u"(a:alfa^100.0 OR b:alfa)"
            r = s.search(q)
            assert [hit["id"] for hit in r] == [2, 5, 6, 3, 0, 1, 4]


def test_scorer():
    schema = fields.Schema(key=fields.TEXT(stored=True))
    with TempIndex(schema) as ix:
        w = ix.writer()
        w.add_document(key=u"alfa alfa alfa")
        w.add_document(key=u"alfa alfa alfa alfa")
        w.add_document(key=u"alfa alfa")
        w.commit()
        w = ix.writer()
        w.add_document(key=u"alfa alfa alfa alfa alfa alfa")
        w.add_document(key=u"alfa")
        w.add_document(key=u"alfa alfa alfa alfa alfa")
        w.commit(merge=False)


def test_pos_scorer():
    ana = analysis.SimpleAnalyzer()
    schema = fields.Schema(id=fields.STORED, key=fields.TEXT(analyzer=ana))
    with TempIndex(schema) as ix:
        w = ix.writer()
        w.add_document(id=0, key=u"0 0 1 0 0 0")
        w.add_document(id=1, key=u"0 0 0 1 0 0")
        w.add_document(id=2, key=u"0 1 0 0 0 0")
        w.commit()
        w = ix.writer()
        w.add_document(id=3, key=u"0 0 0 0 0 1")
        w.add_document(id=4, key=u"1 0 0 0 0 0")
        w.add_document(id=5, key=u"0 0 0 0 1 0")
        w.commit(merge=False)

        def pos_score_fn(searcher, fieldname, text, matcher):
            poses = matcher.positions()
            return 1.0 / (poses[0] + 1)
        pos_weighting = scoring.FunctionWeighting(pos_score_fn)

        s = ix.searcher(weighting=pos_weighting)
        r = s.search(query.Term("key", "1"))
        assert [hit["id"] for hit in r] == [4, 2, 0, 1, 5, 3]


def test_score_length():
    schema = fields.Schema(a=fields.TEXT, b=fields.TEXT)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(a=u"alfa bravo charlie")
            w.add_document(b=u"delta echo foxtrot")
            w.add_document(a=u"golf hotel india")

        with ix.writer() as w:
            w.merge = False
            w.add_document(b=u"juliet kilo lima")
            # In the second segment, there is an "a" field here, but in the
            # corresponding document in the first segment, the field doesn't exist,
            # so if the scorer is getting segment offsets wrong, scoring this
            # document will error
            w.add_document(a=u"mike november oskar")
            w.add_document(b=u"papa quebec romeo")

        with ix.searcher() as s:
            assert not s.is_atomic()
            m = s.matcher("a", "mike")
            while m.is_active():
                docnum = m.id()
                score = m.score()
                m.next()

