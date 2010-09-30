# coding=utf-8

import unittest

from whoosh import analysis, fields, qparser, query


class TestQueryParser(unittest.TestCase):
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
        
        q = qp.parse(u"url: test")
        self.assertEqual(q.__class__, query.And)
        self.assertEqual(q[0].__class__, query.Term)
        self.assertEqual(q[1].__class__, query.Term)
        self.assertEqual(q[0].fieldname, "content")
        self.assertEqual(q[1].fieldname, "content")
        self.assertEqual(q[0].text, "url")
        self.assertEqual(q[1].text, "test")
        
        q = qp.parse(u"url:")
        self.assertEqual(q.__class__, query.Term)
        self.assertEqual(q.fieldname, "content")
        self.assertEqual(q.text, "url")
    
    def test_andor(self):
        qp  = qparser.QueryParser("a")
        q = qp.parse("a AND b OR c AND d OR e AND f")
        self.assertEqual(unicode(q), u"(a:a AND (a:b OR (a:c AND (a:d OR (a:e AND a:f)))))")
    
    def test_andnot(self):
        qp = qparser.QueryParser("content")
        q = qp.parse(u"this ANDNOT that")
        self.assertEqual(q.__class__, query.AndNot)
        self.assertEqual(q.positive.__class__, query.Term)
        self.assertEqual(q.negative.__class__, query.Term)
        self.assertEqual(q.positive.text, "this")
        self.assertEqual(q.negative.text, "that")
        
        q = qp.parse(u"foo ANDNOT bar baz")
        self.assertEqual(q.__class__, query.And)
        self.assertEqual(len(q), 2)
        self.assertEqual(q[0].__class__, query.AndNot)
        self.assertEqual(q[1].__class__, query.Term)
        
        q = qp.parse(u"foo fie ANDNOT bar baz")
        self.assertEqual(q.__class__, query.And)
        self.assertEqual(len(q), 3)
        self.assertEqual(q[0].__class__, query.Term)
        self.assertEqual(q[1].__class__, query.AndNot)
        self.assertEqual(q[2].__class__, query.Term)
        
        q = qp.parse(u"a AND b ANDNOT c")
        self.assertEqual(q.__class__, query.And)
        self.assertEqual(len(q), 2)
        self.assertEqual(q[0].__class__, query.Term)
        self.assertEqual(q[0].text, "a")
        self.assertEqual(q[1].__class__, query.AndNot)
        self.assertEqual(q[1].positive.text, "b")
        self.assertEqual(q[1].negative.text, "c")
    
    def test_boost(self):
        qp = qparser.QueryParser("content")
        q = qp.parse(u"this^3 fn:that^0.5 5.67 hi^5x")
        self.assertEqual(q[0].boost, 3.0)
        self.assertEqual(q[1].boost, 0.5)
        self.assertEqual(q[1].fieldname, "fn")
        self.assertEqual(q[2].text, "5.67")
        self.assertEqual(q[3].text, "hi^5x")
        
        q = qp.parse("alfa (bravo OR charlie)^2.5 ^3")
        self.assertEqual(len(q), 3)
        self.assertEqual(q[0].boost, 1.0)
        self.assertEqual(q[1].boost, 2.5)
        self.assertEqual(q[2].text, "^3")
        
    def test_wildcard1(self):
        qp = qparser.QueryParser("content")
        q = qp.parse(u"hello *the?e* ?star*s? test")
        self.assertEqual(len(q), 4)
        self.assertEqual(q[0].__class__, query.Term)
        self.assertEqual(q[0].text, "hello")
        self.assertEqual(q[1].__class__, query.Wildcard)
        self.assertEqual(q[1].text, "*the?e*")
        self.assertEqual(q[2].__class__, query.Wildcard)
        self.assertEqual(q[2].text, "?star*s?")
        self.assertEqual(q[3].__class__, query.Term)
        self.assertEqual(q[3].text, "test")
        
    def test_wildcard2(self):
        qp = qparser.QueryParser("content")
        q = qp.parse(u"*the?e*")
        self.assertEqual(q.__class__, query.Wildcard)
        self.assertEqual(q.text, "*the?e*")
        
    def test_parse_fieldname_underscores(self):
        s = fields.Schema(my_name=fields.ID(stored=True), my_value=fields.TEXT)
        qp = qparser.QueryParser("my_value", schema=s)
        q = qp.parse(u"my_name:Green")
        self.assertEqual(q.__class__, query.Term)
        self.assertEqual(q.fieldname, "my_name")
        self.assertEqual(q.text, "Green")
    
    def test_endstar(self):
        qp = qparser.QueryParser("text")
        q = qp.parse(u"word*")
        self.assertEqual(q.__class__, query.Prefix)
        self.assertEqual(q.text, "word")
        
        q = qp.parse(u"first* second")
        self.assertEqual(q[0].__class__, query.Prefix)
        self.assertEqual(q[0].text, "first")
    
    def test_singlequotes(self):
        qp = qparser.QueryParser("text")
        q = qp.parse("hell's hot 'i stab at thee'")
        self.assertEqual(q.__class__.__name__, 'And')
        self.assertEqual(len(q), 3)
        self.assertEqual(q[0].__class__, query.Term)
        self.assertEqual(q[1].__class__, query.Term)
        self.assertEqual(q[2].__class__, query.Term)
        self.assertEqual(q[0].text, "hell's")
        self.assertEqual(q[1].text, "hot")
        self.assertEqual(q[2].text, "i stab at thee")
        
        q = qp.parse("alfa zulu:'bravo charlie' delta")
        self.assertEqual(q.__class__.__name__, 'And')
        self.assertEqual(len(q), 3)
        self.assertEqual(q[0].__class__, query.Term)
        self.assertEqual(q[1].__class__, query.Term)
        self.assertEqual(q[2].__class__, query.Term)
        self.assertEqual((q[0].fieldname, q[0].text), ("text", "alfa"))
        self.assertEqual((q[1].fieldname, q[1].text), ("zulu", "bravo charlie"))
        self.assertEqual((q[2].fieldname, q[2].text), ("text", "delta"))
    
#    def test_escaping(self):
#        qp = qparser.QueryParser("text")
#        
#        q = qp.parse(r'big\small')
#        self.assertEqual(q.__class__, query.Term, q)
#        self.assertEqual(q.text, "bigsmall")
#        
#        q = qp.parse(r'big\\small')
#        self.assertEqual(q.__class__, query.Term)
#        self.assertEqual(q.text, r'big\small')
#        
#        q = qp.parse(r'http\:example')
#        self.assertEqual(q.__class__, query.Term)
#        self.assertEqual(q.fieldname, "text")
#        self.assertEqual(q.text, "http:example")
#        
#        q = qp.parse(r'hello\ there')
#        self.assertEqual(q.__class__, query.Term)
#        self.assertEqual(q.text, "hello there")
#        
#        q = qp.parse(r'\[start\ TO\ end\]')
#        self.assertEqual(q.__class__, query.Term)
#        self.assertEqual(q.text, "[start TO end]")
#    
#        schema = fields.Schema(text=fields.TEXT)
#        qp = qparser.QueryParser("text")
#        q = qp.parse(r"http\:\/\/www\.example\.com")
#        self.assertEqual(q.__class__, query.Term)
#        self.assertEqual(q.text, "http://www.example.com")
#        
#        q = qp.parse(u"\\\\")
#        self.assertEqual(q.__class__, query.Term)
#        self.assertEqual(q.text, "\\")
    
#    def test_escaping_wildcards(self):
#        qp = qparser.QueryParser("text")
#        
#        q = qp.parse(u"a*b*c?d")
#        self.assertEqual(q.__class__, query.Wildcard)
#        self.assertEqual(q.text, "a*b*c?d")
#        
#        q = qp.parse(u"a*b\\*c?d")
#        self.assertEqual(q.__class__, query.Wildcard)
#        self.assertEqual(q.text, "a*b*c?d")
#        
#        q = qp.parse(u"a*b\\\\*c?d")
#        self.assertEqual(q.__class__, query.Wildcard)
#        self.assertEqual(q.text, u'a*b\\*c?d')
#        
#        q = qp.parse(u"ab*")
#        self.assertEqual(q.__class__, query.Prefix)
#        self.assertEqual(q.text, u"ab")
#        
#        q = qp.parse(u"ab\\\\*")
#        self.assertEqual(q.__class__, query.Wildcard)
#        self.assertEqual(q.text, u"ab\\*")
        
    def test_phrase(self):
        qp = qparser.QueryParser("content")
        q = qp.parse('"alfa bravo" "charlie delta echo"^2.2 test:"foxtrot golf"')
        self.assertEqual(q[0].__class__, query.Phrase)
        self.assertEqual(q[0].words, ["alfa", "bravo"])
        self.assertEqual(q[1].__class__, query.Phrase)
        self.assertEqual(q[1].words, ["charlie", "delta", "echo"])
        self.assertEqual(q[1].boost, 2.2)
        self.assertEqual(q[2].__class__, query.Phrase)
        self.assertEqual(q[2].words, ["foxtrot", "golf"])
        self.assertEqual(q[2].fieldname, "test")
        
    def test_weird_characters(self):
        qp = qparser.QueryParser("content")
        q = qp.parse(u".abcd@gmail.com")
        self.assertEqual(q.__class__, query.Term)
        self.assertEqual(q.text, u".abcd@gmail.com")
        q = qp.parse(u"r*")
        self.assertEqual(q.__class__, query.Prefix)
        self.assertEqual(q.text, u"r")
        q = qp.parse(u".")
        self.assertEqual(q.__class__, query.Term)
        self.assertEqual(q.text, u".")
        q = qp.parse(u"?")
        self.assertEqual(q.__class__, query.Wildcard)
        self.assertEqual(q.text, u"?")
        
    def test_euro_chars(self):
        schema = fields.Schema(text=fields.TEXT)
        qp = qparser.QueryParser("text", schema=schema)
        q = qp.parse(u"straße")
        self.assertEqual(q.__class__, query.Term)
        self.assertEqual(q.text, u"straße")
    
    def test_star(self):
        schema = fields.Schema(text = fields.TEXT(stored=True))
        qp = qparser.QueryParser("text", schema=schema)
        q = qp.parse(u"*")
        self.assertEqual(q.__class__, query.Every)
        
        q = qp.parse(u"*h?ll*")
        self.assertEqual(q.__class__, query.Wildcard)
        self.assertEqual(q.text, u"*h?ll*")
        
        q = qp.parse(u"h?pe")
        self.assertEqual(q.__class__, query.Wildcard)
        self.assertEqual(q.text, u"h?pe")
        
        q = qp.parse(u"*? blah")
        self.assertEqual(q.__class__, query.And)
        self.assertEqual(q[0].__class__, query.Wildcard)
        self.assertEqual(q[0].text, u"*?")
        self.assertEqual(q[1].__class__, query.Term)
        self.assertEqual(q[1].text, u"blah")
        
        q = qp.parse(u"*ending")
        self.assertEqual(q.__class__, query.Wildcard)
        self.assertEqual(q.text, u"*ending")
        
        q = qp.parse(u"*q")
        self.assertEqual(q.__class__, query.Wildcard)
        self.assertEqual(q.text, u"*q")

    def test_range(self):
        schema = fields.Schema(name=fields.ID(stored=True), text = fields.TEXT(stored=True))
        qp = qparser.QueryParser("text", schema=schema)
        
        q = qp.parse(u"[alfa to bravo}")
        self.assertEqual(q.__class__, query.TermRange)
        self.assertEqual(q.start, "alfa")
        self.assertEqual(q.end, "bravo")
        self.assertEqual(q.startexcl, False)
        self.assertEqual(q.endexcl, True)
        
        q = qp.parse(u"['hello there' to 'what ever']")
        self.assertEqual(q.__class__, query.TermRange)
        self.assertEqual(q.start, "hello there")
        self.assertEqual(q.end, "what ever")
        self.assertEqual(q.startexcl, False)
        self.assertEqual(q.endexcl, False)
        
        q = qp.parse(u"name:{'to' to 'b'}")
        self.assertEqual(q.__class__, query.TermRange)
        self.assertEqual(q.start, "to")
        self.assertEqual(q.end, "b")
        self.assertEqual(q.startexcl, True)
        self.assertEqual(q.endexcl, True)
        
        q = qp.parse(u"name:{'a' to 'to']")
        self.assertEqual(q.__class__, query.TermRange)
        self.assertEqual(q.start, "a")
        self.assertEqual(q.end, "to")
        self.assertEqual(q.startexcl, True)
        self.assertEqual(q.endexcl, False)
        
        q = qp.parse(u"name:[a to to]")
        self.assertEqual(q.__class__, query.TermRange)
        self.assertEqual(q.start, "a")
        self.assertEqual(q.end, "to")
        
        q = qp.parse(u"name:[to to b]")
        self.assertEqual(q.__class__, query.TermRange)
        self.assertEqual(q.start, "to")
        self.assertEqual(q.end, "b")
        
        q = qp.parse(u"[alfa to alfa]")
        self.assertEqual(q.__class__, query.Term)
        self.assertEqual(q.text, "alfa")
        
        q = qp.parse(u"Ind* AND name:[d TO]")
        self.assertEqual(q.__class__, query.And)
        self.assertEqual(q[0].__class__, query.Prefix)
        self.assertEqual(q[1].__class__, query.TermRange)
        self.assertEqual(q[0].text, "ind")
        self.assertEqual(q[1].start, "d")
        self.assertEqual(q[1].fieldname, "name")
        
        q = qp.parse(u"name:[d TO]")
        self.assertEqual(q.__class__, query.TermRange)
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
        self.assertEqual(q.__class__, query.Term)
        self.assertEqual(q.text, "index")
        
    def test_simple(self):
        parser = qparser.SimpleParser("x")
        q = parser.parse(u"alfa bravo charlie delta")
        self.assertEqual(unicode(q), u"(x:alfa OR x:bravo OR x:charlie OR x:delta)")
        
        q = parser.parse(u"alfa +bravo charlie delta")
        self.assertEqual(unicode(q), u"(x:bravo ANDMAYBE (x:alfa OR x:charlie OR x:delta))")
        
        q = parser.parse(u"alfa +bravo -charlie delta")
        self.assertEqual(unicode(q), u"(x:bravo ANDMAYBE (x:alfa OR x:delta)) ANDNOT x:charlie")
        
        q = parser.parse(u"- alfa +bravo + delta")
        self.assertEqual(unicode(q), u"(x:bravo AND x:delta) ANDNOT x:alfa")
    
    def test_dismax(self):
        parser = qparser.DisMaxParser({"body": 0.8, "title": 2.5})
        q = parser.parse(u"alfa bravo charlie")
        self.assertEqual(unicode(q), u"(DisMax(body:alfa^0.8 title:alfa^2.5) OR DisMax(body:bravo^0.8 title:bravo^2.5) OR DisMax(body:charlie^0.8 title:charlie^2.5))")
        
        q = parser.parse(u"alfa +bravo charlie")
        self.assertEqual(unicode(q), u"(DisMax(body:bravo^0.8 title:bravo^2.5) ANDMAYBE (DisMax(body:alfa^0.8 title:alfa^2.5) OR DisMax(body:charlie^0.8 title:charlie^2.5)))")
        
        q = parser.parse(u"alfa -bravo charlie")
        self.assertEqual(unicode(q), u"(DisMax(body:alfa^0.8 title:alfa^2.5) OR DisMax(body:charlie^0.8 title:charlie^2.5)) ANDNOT DisMax(body:bravo^0.8 title:bravo^2.5)")
        
        q = parser.parse(u"alfa -bravo +charlie")
        self.assertEqual(unicode(q), u"(DisMax(body:charlie^0.8 title:charlie^2.5) ANDMAYBE DisMax(body:alfa^0.8 title:alfa^2.5)) ANDNOT DisMax(body:bravo^0.8 title:bravo^2.5)")
    
    def test_many_clauses(self):
        parser = qparser.QueryParser("content")
        qs = "1" + (" OR 1" * 1000)
        parser.parse(qs)
        
        parser = qparser.QueryParser("content")
        parser.parse(qs)
        
    def test_roundtrip(self):
        parser = qparser.QueryParser("a")
        q = parser.parse(u"a OR ((b AND c AND d AND e) OR f OR g) ANDNOT h")
        self.assertEqual(unicode(q), u"(a:a OR ((a:b AND a:c AND a:d AND a:e) OR a:f OR a:g) ANDNOT a:h)")
        


if __name__ == '__main__':
    unittest.main()
