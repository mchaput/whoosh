import unittest

from whoosh import fields, index, qparser, query, searching, scoring, store, writing
from whoosh.query import *

class TestReading(unittest.TestCase):
    def make_index(self):
        s = fields.Schema(key = fields.ID(stored = True),
                          name = fields.TEXT,
                          value = fields.TEXT)
        st = store.RamStorage()
        ix = index.Index(st, s, create = True)
        
        w = writing.IndexWriter(ix)
        w.add_document(key = u"A", name = u"Yellow brown", value = u"Blue red green render purple?")
        w.add_document(key = u"B", name = u"Alpha beta", value = u"Gamma delta epsilon omega.")
        w.add_document(key = u"C", name = u"One two", value = u"Three rendered four five.")
        w.add_document(key = u"D", name = u"Quick went", value = u"Every red town.")
        w.add_document(key = u"E", name = u"Yellow uptown", value = u"Interest rendering outer photo!")
        w.commit()
        
        return ix
    
    def _get_keys(self, stored_fields):
        return sorted([d.get("key") for d in stored_fields])
    
    def _docs(self, q, s):
        return self._get_keys([s.stored_fields(docnum) for docnum
                               in q.docs(s)])
    
    def _doc_scores(self, q, s, w):
        return self._get_keys([s.stored_fields(docnum) for docnum, score
                               in q.doc_scores(s, weighting = w)])
    
    def test_empty_index(self):
        schema = fields.Schema(key = fields.ID(stored=True), value = fields.TEXT)
        st = store.RamStorage()
        self.assertRaises(index.EmptyIndexError, index.Index, st, schema)
    
    def test_docs_method(self):
        ix = self.make_index()
        s = ix.searcher()
        
        self.assertEqual(self._get_keys(s.documents(name = "yellow")), [u"A", u"E"])
        self.assertEqual(self._get_keys(s.documents(value = "red")), [u"A", u"D"])
    
    def test_queries(self):
        ix = self.make_index()
        s = ix.searcher()
        
        tests = [
                 (Term("name", u"yellow"),
                  [u"A", u"E"]),
                 (Term("value", u"red"),
                  [u"A", u"D"]),
                 (Term("value", u"zeta"),
                  []),
                 (Require([Term("value", u"red"), Term("name", u"yellow")]),
                  [u"A"]),
                 (And([Term("value", u"red"), Term("name", u"yellow")]),
                  [u"A"]),
                 (Or([Term("value", u"red"), Term("name", u"yellow")]),
                  [u"A", u"D", u"E"]),
                 (Or([Term("value", u"red"), Term("name", u"yellow"), Not(Term("name", u"quick"))]),
                  [u"A", u"E"]),
                 (AndNot(Term("name", u"yellow"), Term("value", u"purple")),
                  [u"E"]),
                 (Variations("value", u"render"), [u"A", u"C", u"E"]),
                 (Or([Wildcard('value', u'*red*'), Wildcard('name', u'*yellow*')]),
                  [u"A", u"C", u"D", u"E"]),
                ]
        
        for query, result in tests:
            self.assertEqual(self._docs(query, s), result)
        
        for wcls in dir(scoring):
            if wcls is scoring.Weighting: continue
            if isinstance(wcls, scoring.Weighting):
                for query, result in tests:
                    self.assertEqual(self._doc_scores(query, s, wcls), result)
        
        for methodname in ("_docs", "_doc_scores"):
            method = getattr(self, methodname)

    def test_keyword_or(self):
        schema = fields.Schema(a=fields.ID(stored=True), b=fields.KEYWORD)
        st = store.RamStorage()
        ix = index.Index(st, schema, create = True)
        
        w = ix.writer()
        w.add_document(a=u"First", b=u"ccc ddd")
        w.add_document(a=u"Second", b=u"aaa ddd")
        w.add_document(a=u"Third", b=u"ccc eee")
        w.commit()
        
        qp = qparser.QueryParser("b", schema=schema)
        searcher = ix.searcher()
        qr = qp.parse("b:ccc OR b:eee")
        self.assertEqual(qr.__class__, query.Or)
        r = searcher.search(qr)
        self.assertEqual(len(r), 2)
        self.assertEqual(r[0]["a"], "Third")
        self.assertEqual(r[1]["a"], "First")

    def test_score_retrieval(self):
        schema = fields.Schema(title=fields.TEXT(stored=True),
                               content=fields.TEXT(stored=True))
        storage = store.RamStorage()
        ix = index.Index(storage, schema, create=True)
        writer = ix.writer()
        writer.add_document(title=u"Miss Mary",
                            content=u"Mary had a little white lamb its fleece was white as snow")
        writer.add_document(title=u"Snow White",
                            content=u"Snow white lived in the forrest with seven dwarfs")
        writer.commit()
        
        searcher = ix.searcher()
        results = searcher.search(Term("content", "white"))
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]['title'], u"Miss Mary")
        self.assertEqual(results[1]['title'], u"Snow White")
        self.assertNotEqual(results.score(0), None)
        self.assertNotEqual(results.score(0), 0)
        self.assertNotEqual(results.score(0), 1)

    def test_missing_field_scoring(self):
        schema = fields.Schema(name=fields.TEXT(stored=True), hobbies=fields.TEXT(stored=True))
        storage = store.RamStorage()
        idx = index.Index(storage, schema, create=True)
        writer = idx.writer() 
        writer.add_document(name=u'Frank', hobbies=u'baseball, basketball')
        writer.commit()
        self.assertEqual(idx.segments[0].field_length(0), 2) # hobbies
        self.assertEqual(idx.segments[0].field_length(1), 1) # name
        
        writer = idx.writer()
        writer.add_document(name=u'Jonny') 
        writer.commit()
        self.assertEqual(len(idx.segments), 1)
        self.assertEqual(idx.segments[0].field_length(0), 2) # hobbies
        self.assertEqual(idx.segments[0].field_length(1), 2) # name
        
        parser = qparser.MultifieldParser(['name', 'hobbies'], schema=schema)
        searcher = idx.searcher()
        result = searcher.search(parser.parse(u'baseball'))
        self.assertEqual(len(result), 1)


if __name__ == '__main__':
    unittest.main()
