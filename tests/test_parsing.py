# coding=utf-8

import unittest
from os import mkdir
from os.path import exists
from shutil import rmtree

from whoosh import analysis, fields, index, qparser, query
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
            try:
                rmtree(dirname)
            except OSError:
                pass
    
    def test_fields(self):
        s = fields.Schema(content=fields.TEXT, title=fields.TEXT, id=fields.ID)
        
        qp = qparser.QueryParser("content", s)
        q = qp.parse(u"test")
        self.assertEqual(q.__class__, query.Term)
        self.assertEqual(q.fieldname, "content")
        self.assertEqual(q.text, "test")
        
        mq = qparser.MultifieldParser(("title", "content"), s)
        q = mq.parse(u"test")
        self.assertEqual(q.__class__, query.Or)
        self.assertEqual(q[0].__class__, query.Term)
        self.assertEqual(q[1].__class__, query.Term)
        self.assertEqual(q[0].fieldname, "title")
        self.assertEqual(q[1].fieldname, "content")
        self.assertEqual(q[0].text, "test")
        self.assertEqual(q[1].text, "test")
        
        q = mq.parse(u"title:test")
        self.assertEqual(q.__class__, query.Term)
        self.assertEqual(q.fieldname, "title")
        self.assertEqual(q.text, "test")
    
    def test_colonspace(self):
        s = fields.Schema(content=fields.TEXT, url=fields.ID)
        
        qp = qparser.QueryParser("content", s)
        q = qp.parse(u"url:test")
        self.assertEqual(q.__class__, query.Term)
        self.assertEqual(q.fieldname, "url")
        self.assertEqual(q.text, "test")
        
        qp = qparser.QueryParser("content", s)
        q = qp.parse(u"url: test")
        self.assertEqual(q.__class__, query.And)
        self.assertEqual(q[0].__class__, query.Term)
        self.assertEqual(q[1].__class__, query.Term)
        self.assertEqual(q[0].fieldname, "content")
        self.assertEqual(q[1].fieldname, "content")
        self.assertEqual(q[0].text, "url")
        self.assertEqual(q[1].text, "test")
    
    def test_andnot(self):
        qp = qparser.QueryParser("content")
        q = qp.parse(u"this ANDNOT that")
        self.assertEqual(q.__class__.__name__, "AndNot")
        self.assertEqual(q.positive.__class__.__name__, "Term")
        self.assertEqual(q.negative.__class__.__name__, "Term")
        self.assertEqual(q.positive.text, "this")
        self.assertEqual(q.negative.text, "that")
        
        q = qp.parse(u"foo ANDNOT bar baz")
        self.assertEqual(q.__class__.__name__, "And")
        self.assertEqual(len(q.subqueries), 2)
        self.assertEqual(q[0].__class__.__name__, "AndNot")
        self.assertEqual(q[1].__class__.__name__, "Term")
        
        q = qp.parse(u"foo fie ANDNOT bar baz")
        self.assertEqual(q.__class__.__name__, "And")
        self.assertEqual(len(q.subqueries), 3)
        self.assertEqual(q[0].__class__.__name__, "Term")
        self.assertEqual(q[1].__class__.__name__, "AndNot")
        self.assertEqual(q[2].__class__.__name__, "Term")
    
    def test_boost(self):
        qp = qparser.QueryParser("content")
        q = qp.parse(u"this^3 fn:that^0.5 5.67")
        self.assertEqual(q[0].boost, 3.0)
        self.assertEqual(q[1].boost, 0.5)
        self.assertEqual(q[1].fieldname, "fn")
        self.assertEqual(q[2].text, "5.67")
        
    def test_wildcard1(self):
        qp = qparser.QueryParser("content")
        q = qp.parse(u"hello *the?e* ?star*s? test")
        self.assertEqual(len(q.subqueries), 4)
        self.assertEqual(q[0].__class__.__name__, "Term")
        self.assertEqual(q[0].text, "hello")
        self.assertEqual(q[1].__class__.__name__, "Wildcard")
        self.assertEqual(q[1].text, "*the?e*")
        self.assertEqual(q[2].__class__.__name__, "Wildcard")
        self.assertEqual(q[2].text, "?star*s?")
        self.assertEqual(q[3].__class__.__name__, "Term")
        self.assertEqual(q[3].text, "test")
        
    def test_wildcard2(self):
        qp = qparser.QueryParser("content")
        q = qp.parse(u"*the?e*")
        self.assertEqual(q.__class__.__name__, "Wildcard")
        self.assertEqual(q.text, "*the?e*")
        
    def test_parse_fieldname_underscores(self):
        s = fields.Schema(my_name=fields.ID(stored=True), my_value=fields.TEXT)
        qp = qparser.QueryParser("my_value", schema=s)
        q = qp.parse(u"my_name:Green")
        self.assertEqual(q.__class__.__name__, "Term")
        self.assertEqual(q.fieldname, "my_name")
        self.assertEqual(q.text, "Green")
    
    def test_endstar(self):
        qp = qparser.QueryParser("text")
        q = qp.parse(u"word*")
        self.assertEqual(q.__class__.__name__, "Prefix")
        self.assertEqual(q.text, "word")
        
        q = qp.parse(u"first* second")
        self.assertEqual(q[0].__class__.__name__, "Prefix")
        self.assertEqual(q[0].text, "first")
    
    def test_escaping(self):
        qp = qparser.QueryParser("text")
        
        q = qp.parse(r'big\small')
        self.assertEqual(q.__class__, query.Term, q)
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
        
        q = qp.parse(r'\[start\ TO\ end\]')
        self.assertEqual(q.__class__, query.Term)
        self.assertEqual(q.text, "[start TO end]")
    
        schema = fields.Schema(text=fields.TEXT)
        qp = qparser.QueryParser("text")
        q = qp.parse(r"http\:\/\/www\.example\.com")
        self.assertEqual(q.__class__.__name__, "Term")
        self.assertEqual(q.text, "http://www.example.com")
        
        q = qp.parse(u"\\\\")
        self.assertEqual(q.__class__.__name__, "Term")
        self.assertEqual(q.text, "\\")
    
    def test_escaping_wildcards(self):
        qp = qparser.QueryParser("text")
        
        q = qp.parse(u"a*b*c?d")
        self.assertEqual(q.__class__, query.Wildcard)
        self.assertEqual(q.text, "a*b*c?d")
        
        q = qp.parse(u"a*b\\*c?d")
        self.assertEqual(q.__class__, query.Wildcard)
        self.assertEqual(q.text, "a*b*c?d")
        
        q = qp.parse(u"a*b\\\\*c?d")
        self.assertEqual(q.__class__, query.Wildcard)
        self.assertEqual(q.text, u'a*b\\*c?d')
        
        q = qp.parse(u"ab*")
        self.assertEqual(q.__class__, query.Prefix)
        self.assertEqual(q.text, u"ab")
        
        q = qp.parse(u"ab\\\\*")
        self.assertEqual(q.__class__, query.Wildcard)
        self.assertEqual(q.text, u"ab\\*")
        
    def test_phrase(self):
        qp = qparser.QueryParser("content")
        q = qp.parse('"alfa bravo" "charlie delta echo"^2.2 test:"foxtrot golf"')
        self.assertEqual(q[0].__class__.__name__, "Phrase")
        self.assertEqual(q[0].words, ["alfa", "bravo"])
        self.assertEqual(q[1].__class__.__name__, "Phrase")
        self.assertEqual(q[1].words, ["charlie", "delta", "echo"])
        self.assertEqual(q[1].boost, 2.2)
        self.assertEqual(q[2].__class__.__name__, "Phrase")
        self.assertEqual(q[2].words, ["foxtrot", "golf"])
        self.assertEqual(q[2].fieldname, "test")
        
    def test_weird_characters(self):
        qp = qparser.QueryParser("content")
        q = qp.parse(u".abcd@gmail.com")
        self.assertEqual(q.__class__.__name__, "Term")
        self.assertEqual(q.text, u".abcd@gmail.com")
        q = qp.parse(u"r*")
        self.assertEqual(q.__class__.__name__, "Prefix")
        self.assertEqual(q.text, u"r")
        q = qp.parse(u".")
        self.assertEqual(q.__class__.__name__, "Term")
        self.assertEqual(q.text, u".")
        q = qp.parse(u"?")
        self.assertEqual(q.__class__.__name__, "Wildcard")
        self.assertEqual(q.text, u"?")
        
    def test_euro_chars(self):
        schema = fields.Schema(text=fields.TEXT)
        qp = qparser.QueryParser("text", schema=schema)
        q = qp.parse(u"straße")
        self.assertEqual(q.__class__.__name__, "Term")
        self.assertEqual(q.text, u"straße")
    
    def test_star(self):
        schema = fields.Schema(text = fields.TEXT(stored=True))
        qp = qparser.QueryParser("text", schema=schema)
        q = qp.parse(u"*")
        self.assertEqual(q.__class__.__name__, "Every")
        
        q = qp.parse(u"*h?ll*")
        self.assertEqual(q.__class__.__name__, "Wildcard")
        self.assertEqual(q.text, u"*h?ll*")
        
        q = qp.parse(u"h?pe")
        self.assertEqual(q.__class__.__name__, "Wildcard")
        self.assertEqual(q.text, u"h?pe")
        
        q = qp.parse(u"*? blah")
        self.assertEqual(q.__class__.__name__, "And")
        self.assertEqual(q[0].__class__.__name__, "Wildcard")
        self.assertEqual(q[0].text, u"*?")
        self.assertEqual(q[1].__class__.__name__, "Term")
        self.assertEqual(q[1].text, u"blah")
        
        q = qp.parse(u"*ending")
        self.assertEqual(q.__class__.__name__, "Wildcard")
        self.assertEqual(q.text, u"*ending")
        
        q = qp.parse(u"*q")
        self.assertEqual(q.__class__.__name__, "Wildcard")
        self.assertEqual(q.text, u"*q")

    def test_range(self):
        schema = fields.Schema(name=fields.ID(stored=True), text = fields.TEXT(stored=True))
        qp = qparser.QueryParser("text", schema=schema)
        q = qp.parse(u"Ind* AND name:[d TO]")
        self.assertEqual(q.__class__.__name__, "And")
        self.assertEqual(q[0].__class__.__name__, "Prefix")
        self.assertEqual(q[1].__class__.__name__, "TermRange")
        self.assertEqual(q[0].text, "ind")
        self.assertEqual(q[1].start, "d")
        self.assertEqual(q[1].fieldname, "name")
        
        q = qp.parse(u"name:[d TO]")
        self.assertEqual(q.__class__.__name__, "TermRange")
        self.assertEqual(q.start, "d")
        self.assertEqual(q.fieldname, "name")
        
    def test_stopped(self):
        schema = fields.Schema(text = fields.TEXT)
        qp = qparser.QueryParser("text", schema=schema)
        q = qp.parse(u"a b")
        self.assertEqual(q, query.NullQuery)
        
    def test_analyzing_terms(self):
        schema = fields.Schema(text=fields.TEXT(analyzer=analysis.StemmingAnalyzer()))
        qp = qparser.QueryParser("text", schema=schema)
        q = qp.parse(u"Indexed!")
        self.assertEqual(q.__class__.__name__, "Term")
        self.assertEqual(q.text, "index")


if __name__ == '__main__':
    unittest.main()
