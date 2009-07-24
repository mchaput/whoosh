import unittest
from os import mkdir
from os.path import exists
from shutil import rmtree

from whoosh import fields, index, qparser, query
from whoosh.filedb.filestore import FileStorage

class TestQueryParser(unittest.TestCase):
    def make_index(self, dirname, schema):
        if not exists(dirname):
            mkdir(dirname)
        st = FileStorage(dirname)
        ix = st.create_index(schema)
        return ix
    
    def destroy_index(self, dirname):
        if exists(dirname):
            rmtree(dirname)
    
    def test_boost(self):
        qp = qparser.QueryParser("content")
        q = qp.parse("this^3 fn:that^0.5 5.67")
        self.assertEqual(q[0].boost, 3.0)
        self.assertEqual(q[1].boost, 0.5)
        self.assertEqual(q[1].fieldname, "fn")
        self.assertEqual(q[2].text, "5.67")
        
    def test_wildcard(self):
        qp = qparser.QueryParser("content")
        q = qp.parse("hello *the?e* ?star*s? test")
        self.assertEqual(len(q), 4)
        self.assertNotEqual(q[0].__class__.__name__, "Wildcard")
        self.assertEqual(q[1].__class__.__name__, "Wildcard")
        self.assertEqual(q[2].__class__.__name__, "Wildcard")
        self.assertNotEqual(q[3].__class__.__name__, "Wildcard")
        self.assertEqual(q[1].text, "*the?e*")
        self.assertEqual(q[2].text, "?star*s?")

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
        
        q = qp.parse("first* second")
        self.assertEqual(q[0].__class__.__name__, "Prefix")
        self.assertEqual(q[0].text, "first")
    
    def test_escaping(self):
        qp = qparser.QueryParser("text")
        
        q = qp.parse(r'big\small')
        self.assertEqual(q.__class__, query.Term)
        self.assertEqual(q.text, "bigsmall")
        
        q = qp.parse(r'big\\small')
        self.assertEqual(q.__class__, query.Term)
        self.assertEqual(q.text, r'big\small')
        
        q = qp.parse(r'http\:example')
        self.assertEqual(q.__class__, query.Term)
        self.assertEqual(q.fieldname, "text")
        self.assertEqual(q.text, "http:example")
        
        q = qp.parse(r'hello\ there')
        self.assertEqual(q.__class__, query.Term)
        self.assertEqual(q.text, "hello there")
        
        q = qp.parse(r'start\.\.end')
        self.assertEqual(q.__class__, query.Term)
        self.assertEqual(q.text, "start..end")


if __name__ == '__main__':
    unittest.main()
