import unittest
from os import mkdir
from os.path import exists
from shutil import rmtree

from whoosh import fields, index, qparser, query, store

class TestQueryParser(unittest.TestCase):
    def make_index(self, dirname, schema):
        if not exists(dirname):
            mkdir(dirname)
        st = store.FileStorage(dirname)
        ix = index.Index(st, schema, create = True)
        return ix
    
    def destroy_index(self, dirname):
        if exists(dirname):
            rmtree(dirname)
    
    def test_boost(self):
        qp = qparser.QueryParser("content")
        q = qp.parse("this^3 fn:that^0.5 5.67")
        self.assertEqual(q.subqueries[0].boost, 3.0)
        self.assertEqual(q.subqueries[1].boost, 0.5)
        self.assertEqual(q.subqueries[1].fieldname, "fn")
        self.assertEqual(q.subqueries[2].text, "5.67")
        
    def test_wildcard(self):
        qp = qparser.QueryParser("content")
        q = qp.parse("hello *the?e* ?star*s? test")
        self.assertEqual(len(q.subqueries), 4)
        self.assertNotEqual(q.subqueries[0].__class__.__name__, "Wildcard")
        self.assertEqual(q.subqueries[1].__class__.__name__, "Wildcard")
        self.assertEqual(q.subqueries[2].__class__.__name__, "Wildcard")
        self.assertNotEqual(q.subqueries[3].__class__.__name__, "Wildcard")
        self.assertEqual(q.subqueries[1].text, "*the?e*")
        self.assertEqual(q.subqueries[2].text, "?star*s?")

    def test_fieldname_underscores(self):
        s = fields.Schema(my_name=fields.ID(stored=True), my_value=fields.TEXT)
        ix = self.make_index("testindex", s)
        
        try:
            w = ix.writer()
            w.add_document(my_name=u"Green", my_value=u"It's not easy being green")
            w.add_document(my_name=u"Red", my_value=u"Hopping mad like a playground ball")
            w.commit()
            
            qp = qparser.QueryParser("my_value", schema=ix.schema)
            s = ix.searcher()
            r = s.search(qp.parse("my_name:Green"))
            self.assertEqual(r[0]['my_name'], "Green")
            s.close()
            ix.close()
        finally:
            self.destroy_index("testindex")
    
    def test_endstar(self):
        qp = qparser.QueryParser("text")
        q = qp.parse("word*")
        self.assertEqual(q.__class__.__name__, "Prefix")
        self.assertEqual(q.text, "word")
    
    def test_escaping(self):
        qp = qparser.QueryParser("text")
        
        #q = qp.parse(r'http\:example')
        #self.assertEqual(q.__class__, query.Term)
        #self.assertEqual(q.fieldname, "text")
        #self.assertEqual(q.text, "http:example")
        
        # The following test currently fails because
        # pyparsing swallows escaped whitespace for some
        # reason.
        
        #q = qp.parse(r'hello\ there')
        #self.assertEqual(q.__class__, query.Term)
        #self.assertEqual(q.fieldname, "text")
        #self.assertEqual(q.text, "hello there")


if __name__ == '__main__':
    unittest.main()
