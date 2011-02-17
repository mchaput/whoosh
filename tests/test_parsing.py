# coding=utf-8

import unittest

from whoosh import analysis, fields, qparser, query


class TestQueryParser(unittest.TestCase):
    def test_empty_querystring(self):
        s = fields.Schema(content=fields.TEXT, title=fields.TEXT, id=fields.ID)
        qp = qparser.QueryParser("content", s)
        q = qp.parse(u"")
        self.assertEqual(q, query.NullQuery)
    
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
        
    def test_fieldname_chars(self):
        s = fields.Schema(abc123=fields.TEXT, nisbah=fields.KEYWORD)
        qp = qparser.QueryParser("content", s)
        fieldmap = {'nisbah': [u'\u0646\u0633\u0628\u0629'],
                    'abc123': ['xyz']}
        qp.add_plugin(qparser.FieldAliasPlugin(fieldmap))
        
        q = qp.parse(u"abc123:456")
        self.assertEqual(q.__class__, query.Term)
        self.assertEqual(q.fieldname, u'abc123')
        self.assertEqual(q.text, u'456')
        
        q = qp.parse(u"abc123:456 def")
        self.assertEqual(unicode(q), u"(abc123:456 AND content:def)")
        
        q = qp.parse(u'\u0646\u0633\u0628\u0629:\u0627\u0644\u0641\u0644\u0633\u0637\u064a\u0646\u064a')
        self.assertEqual(q.__class__, query.Term)
        self.assertEqual(q.fieldname, u'nisbah')
        self.assertEqual(q.text, u'\u0627\u0644\u0641\u0644\u0633\u0637\u064a\u0646\u064a')
        
        q = qp.parse(u"abc123 (xyz:123 OR qrs)")
        self.assertEqual(unicode(q), "(content:abc123 AND (abc123:123 OR content:qrs))")
    
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
        
        s = fields.Schema(foo=fields.KEYWORD)
        qp = qparser.QueryParser("foo", s)
        q = qp.parse(u"blah:")
        self.assertEqual(q.__class__, query.Term)
        self.assertEqual(q.fieldname, "foo")
        self.assertEqual(q.text, "blah:")
    
    def test_andor(self):
        qp  = qparser.QueryParser("a", None)
        q = qp.parse("a AND b OR c AND d OR e AND f")
        self.assertEqual(unicode(q), u"((a:a AND a:b) OR (a:c AND a:d) OR (a:e AND a:f))")
        
        q = qp.parse("aORb")
        self.assertEqual(q, query.Term("a", "aORb"))
        
        q = qp.parse("aOR b")
        self.assertEqual(q, query.And([query.Term("a", "aOR"), query.Term("a", "b")]))
        
        q = qp.parse("a ORb")
        self.assertEqual(q, query.And([query.Term("a", "a"), query.Term("a", "ORb")]))
        
        self.assertEqual(qp.parse("OR"), query.Term("a", "OR"))
    
    def test_andnot(self):
        qp = qparser.QueryParser("content", None)
        q = qp.parse(u"this ANDNOT that")
        self.assertEqual(q.__class__, query.AndNot)
        self.assertEqual(q.a.__class__, query.Term)
        self.assertEqual(q.b.__class__, query.Term)
        self.assertEqual(q.a.text, "this")
        self.assertEqual(q.b.text, "that")
        
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
        self.assertEqual(q.__class__, query.AndNot)
        self.assertEqual(unicode(q), "((content:a AND content:b) ANDNOT content:c)")
    
    def test_boost(self):
        qp = qparser.QueryParser("content", None)
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
        qp = qparser.QueryParser("content", None)
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
        qp = qparser.QueryParser("content", None)
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
        qp = qparser.QueryParser("text", None)
        q = qp.parse(u"word*")
        self.assertEqual(q.__class__, query.Prefix)
        self.assertEqual(q.text, "word")
        
        q = qp.parse(u"first* second")
        self.assertEqual(q[0].__class__, query.Prefix)
        self.assertEqual(q[0].text, "first")
    
    def test_singlequotes(self):
        qp = qparser.QueryParser("text", None)
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
        
        q = qp.parse("The rest 'is silence")
        self.assertEqual(q.__class__, query.And)
        self.assertEqual(len(q), 4)
        self.assertEqual([t.text for t in q.subqueries],
                         ["The", "rest", "'is" ,"silence"])
        
        q = qp.parse("I don't like W's stupid face")
        self.assertEqual(q.__class__, query.And)
        self.assertEqual(len(q), 6)
        self.assertEqual([t.text for t in q.subqueries],
                         ["I", "don't", "like" ,"W's", "stupid", "face"])
        
        q = qp.parse("I forgot the drinkin' in '98")
        self.assertEqual(q.__class__, query.And)
        self.assertEqual(len(q), 6)
        self.assertEqual([t.text for t in q.subqueries],
                         ["I", "forgot", "the" ,"drinkin'", "in", "'98"])
    
#    def test_escaping(self):
#        qp = qparser.QueryParser("text", None)
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
#        qp = qparser.QueryParser("text", None)
#        q = qp.parse(r"http\:\/\/www\.example\.com")
#        self.assertEqual(q.__class__, query.Term)
#        self.assertEqual(q.text, "http://www.example.com")
#        
#        q = qp.parse(u"\\\\")
#        self.assertEqual(q.__class__, query.Term)
#        self.assertEqual(q.text, "\\")
    
#    def test_escaping_wildcards(self):
#        qp = qparser.QueryParser("text", None)
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
        qp = qparser.QueryParser("content", None)
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
        qp = qparser.QueryParser("content", None)
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
        qp = qparser.QueryParser("text", schema)
        q = qp.parse(u"straße")
        self.assertEqual(q.__class__, query.Term)
        self.assertEqual(q.text, u"straße")
    
    def test_star(self):
        schema = fields.Schema(text = fields.TEXT(stored=True))
        qp = qparser.QueryParser("text", schema)
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
        qp = qparser.QueryParser("text", schema)
        
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
    
    def test_numeric_range(self):
        schema = fields.Schema(id=fields.STORED, number=fields.NUMERIC)
        qp = qparser.QueryParser("number", schema)
        
        teststart = 40
        testend = 100
        
        q = qp.parse("[%s to *]" % teststart)
        self.assertEqual(q, query.NullQuery)
        
        q = qp.parse("[%s to]" % teststart)
        self.assertEqual(q.__class__, query.NumericRange)
        self.assertEqual(q.start, teststart)
        self.assertEqual(q.end, None)
        
        q = qp.parse("[to %s]" % testend)
        self.assertEqual(q.__class__, query.NumericRange)
        self.assertEqual(q.start, None)
        self.assertEqual(q.end, testend)
        
        q = qp.parse("[%s to %s]" % (teststart, testend))
        self.assertEqual(q.__class__, query.NumericRange)
        self.assertEqual(q.start, teststart)
        self.assertEqual(q.end, testend)
        
    def test_regressions(self):
        qp = qparser.QueryParser("f", None)
        
        # From 0.3.18, these used to require escaping. Mostly good for
        # regression testing.
        self.assertEqual(qp.parse(u"re-inker"), query.Term("f", u"re-inker"))
        self.assertEqual(qp.parse(u"0.7 wire"),
                         query.And([query.Term("f", u"0.7"), query.Term("f", u"wire")]))
        self.assertEqual(qp.parse(u"daler-rowney pearl 'bell bronze'"),
                         query.And([query.Term("f", u"daler-rowney"),
                                    query.Term("f", u"pearl"),
                                    query.Term("f", u"bell bronze")]))
        
        q = qp.parse(u'22" BX')
        self.assertEqual(q, query.And([query.Term("f", u'22"'), query.Term("f", "BX")]))
        
    def test_empty_ranges(self):
        schema = fields.Schema(name=fields.TEXT, num=fields.NUMERIC,
                               date=fields.DATETIME)
        qp = qparser.QueryParser("text", schema)
        
        for fname in ("name", "date"):
            q = qp.parse("%s:[to]" % fname)
            self.assertEqual(q.__class__, query.Every)
    
    def test_empty_numeric_range(self):
        schema = fields.Schema(id=fields.ID, num=fields.NUMERIC)
        qp = qparser.QueryParser("num", schema)
        q = qp.parse("num:[to]")
        self.assertEqual(q.__class__, query.NumericRange)
        self.assertEqual(q.start, None)
        self.assertEqual(q.end, None)
    
    def test_nonexistant_fieldnames(self):
        # Need an analyzer that won't mangle a URL
        a = analysis.SimpleAnalyzer("\\S+")
        schema = fields.Schema(id=fields.ID, text=fields.TEXT(analyzer=a))
        
        qp = qparser.QueryParser("text", schema)
        q = qp.parse(u"id:/code http://localhost/")
        self.assertEqual(q.__class__, query.And)
        self.assertEqual(q[0].__class__, query.Term)
        self.assertEqual(q[0].fieldname, "id")
        self.assertEqual(q[0].text, "/code")
        self.assertEqual(q[1].__class__, query.Term)
        self.assertEqual(q[1].fieldname, "text")
        self.assertEqual(q[1].text, "http://localhost/")
    
    def test_stopped(self):
        schema = fields.Schema(text = fields.TEXT)
        qp = qparser.QueryParser("text", schema)
        q = qp.parse(u"a b")
        self.assertEqual(q, query.NullQuery)
        
    def test_analyzing_terms(self):
        schema = fields.Schema(text=fields.TEXT(analyzer=analysis.StemmingAnalyzer()))
        qp = qparser.QueryParser("text", schema)
        q = qp.parse(u"Indexed!")
        self.assertEqual(q.__class__, query.Term)
        self.assertEqual(q.text, "index")
        
    def test_simple(self):
        parser = qparser.SimpleParser("x", None)
        q = parser.parse(u"alfa bravo charlie delta")
        self.assertEqual(unicode(q), u"(x:alfa OR x:bravo OR x:charlie OR x:delta)")
        
        q = parser.parse(u"alfa +bravo charlie delta")
        self.assertEqual(unicode(q), u"(x:bravo ANDMAYBE (x:alfa OR x:charlie OR x:delta))")
        
        q = parser.parse(u"alfa +bravo -charlie delta")
        self.assertEqual(unicode(q), u"((x:bravo ANDMAYBE (x:alfa OR x:delta)) ANDNOT x:charlie)")
        
        q = parser.parse(u"- alfa +bravo + delta")
        self.assertEqual(unicode(q), u"((x:bravo AND x:delta) ANDNOT x:alfa)")
    
    def test_dismax(self):
        parser = qparser.DisMaxParser({"body": 0.8, "title": 2.5}, None)
        q = parser.parse(u"alfa bravo charlie")
        self.assertEqual(unicode(q), u"(DisMax(body:alfa^0.8 title:alfa^2.5) OR DisMax(body:bravo^0.8 title:bravo^2.5) OR DisMax(body:charlie^0.8 title:charlie^2.5))")
        
        q = parser.parse(u"alfa +bravo charlie")
        self.assertEqual(unicode(q), u"(DisMax(body:bravo^0.8 title:bravo^2.5) ANDMAYBE (DisMax(body:alfa^0.8 title:alfa^2.5) OR DisMax(body:charlie^0.8 title:charlie^2.5)))")
        
        q = parser.parse(u"alfa -bravo charlie")
        self.assertEqual(unicode(q), u"((DisMax(body:alfa^0.8 title:alfa^2.5) OR DisMax(body:charlie^0.8 title:charlie^2.5)) ANDNOT DisMax(body:bravo^0.8 title:bravo^2.5))")
        
        q = parser.parse(u"alfa -bravo +charlie")
        self.assertEqual(unicode(q), u"((DisMax(body:charlie^0.8 title:charlie^2.5) ANDMAYBE DisMax(body:alfa^0.8 title:alfa^2.5)) ANDNOT DisMax(body:bravo^0.8 title:bravo^2.5))")
    
    def test_many_clauses(self):
        parser = qparser.QueryParser("content", None)
        qs = "1" + (" OR 1" * 1000)
        parser.parse(qs)
        
        parser = qparser.QueryParser("content", None)
        parser.parse(qs)
    
    def test_roundtrip(self):
        parser = qparser.QueryParser("a", None)
        q = parser.parse(u"a OR ((b AND c AND d AND e) OR f OR g) ANDNOT h")
        self.assertEqual(unicode(q), u"((a:a OR (a:b AND a:c AND a:d AND a:e) OR a:f OR a:g) ANDNOT a:h)")
        
    def test_ngrams(self):
        schema = fields.Schema(grams=fields.NGRAM)
        parser = qparser.QueryParser('grams', schema)
        parser.remove_plugin_class(qparser.WhitespacePlugin)
        
        q = parser.parse(u"Hello There")
        self.assertEqual(q.__class__, query.And)
        self.assertEqual(len(q), 8)
        self.assertEqual([sq.text for sq in q], ["hell", "ello", "llo ", "lo t", "o th", " the", "ther", "here"])
        
    def test_ngramwords(self):
        schema = fields.Schema(grams=fields.NGRAMWORDS(queryor=True))
        parser = qparser.QueryParser('grams', schema)
        
        q = parser.parse(u"Hello Tom")
        self.assertEqual(q.__class__, query.And)
        self.assertEqual(q[0].__class__, query.Or)
        self.assertEqual(q[1].__class__, query.Term)
        self.assertEqual(q[0][0].text, "hell")
        self.assertEqual(q[0][1].text, "ello")
        self.assertEqual(q[1].text, "tom")

    def test_multitoken_words(self):
        textfield = fields.TEXT()
        textfield.multitoken_query = "or"
        schema = fields.Schema(text=textfield)
        parser = qparser.QueryParser('text', schema)
        qstring = u"chaw-bacon"
        
        texts = list(schema["text"].process_text(qstring))
        self.assertEqual(texts, ["chaw", "bacon"])
        
        q = parser.parse(qstring)
        self.assertEqual(q.__class__, query.Or)
        self.assertEqual(len(q), 2)
        self.assertEqual(q[0].__class__, query.Term)
        self.assertEqual(q[0].text, "chaw")
        self.assertEqual(q[1].__class__, query.Term)
        self.assertEqual(q[1].text, "bacon")
    
    def test_operators(self):
        qp = qparser.QueryParser("f", None)
        
        q = qp.parse("a AND b OR c AND d")
        self.assertEqual(unicode(q), "((f:a AND f:b) OR (f:c AND f:d))")
        
        q = qp.parse("a OR b OR c OR d")
        self.assertEqual(unicode(q), "(f:a OR f:b OR f:c OR f:d)")
        
        q = qp.parse("a ANDMAYBE b ANDNOT c REQUIRE d")
        self.assertEqual(unicode(q), "((f:a ANDMAYBE (f:b ANDNOT f:c)) REQUIRE f:d)")
    
    def test_associativity(self):
        left_andmaybe = (qparser.InfixOperator("ANDMAYBE", qparser.AndMaybeGroup, True), 0)
        right_andmaybe = (qparser.InfixOperator("ANDMAYBE", qparser.AndMaybeGroup, False), 0)
        not_ = (qparser.PrefixOperator("NOT", qparser.NotGroup), 0)
        
        def make_parser(*ops):
            parser = qparser.QueryParser("f", None)
            parser.replace_plugin(qparser.CompoundsPlugin(ops, clean=True))
            return parser
        
        p = make_parser(left_andmaybe)
        q = p.parse("a ANDMAYBE b ANDMAYBE c ANDMAYBE d")
        self.assertEqual(unicode(q), "(((f:a ANDMAYBE f:b) ANDMAYBE f:c) ANDMAYBE f:d)")
        
        p = make_parser(right_andmaybe)
        q = p.parse("a ANDMAYBE b ANDMAYBE c ANDMAYBE d")
        self.assertEqual(unicode(q), "(f:a ANDMAYBE (f:b ANDMAYBE (f:c ANDMAYBE f:d)))")
        
        p = make_parser(not_)
        q = p.parse("a NOT b NOT c NOT d", normalize=False)
        self.assertEqual(unicode(q), "(f:a AND NOT f:b AND NOT f:c AND NOT f:d)")
        
        p = make_parser(left_andmaybe)
        q = p.parse("(a ANDMAYBE b) ANDMAYBE (c ANDMAYBE d)")
        self.assertEqual(unicode(q), "((f:a ANDMAYBE f:b) ANDMAYBE (f:c ANDMAYBE f:d))")
        
        p = make_parser(right_andmaybe)
        q = p.parse("(a ANDMAYBE b) ANDMAYBE (c ANDMAYBE d)")
        self.assertEqual(unicode(q), "((f:a ANDMAYBE f:b) ANDMAYBE (f:c ANDMAYBE f:d))")
        
    def test_not_assoc(self):
        qp = qparser.QueryParser("text", None)
        q = qp.parse(u"a AND NOT b OR c")
        self.assertEqual(unicode(q), "((text:a AND NOT text:b) OR text:c)")
        
        qp = qparser.QueryParser("text", None)
        q = qp.parse(u"a NOT (b OR c)")
        self.assertEqual(unicode(q), "(text:a AND NOT (text:b OR text:c))")
        
    def test_fieldname_space(self):
        qp = qparser.QueryParser("a", None)
        q = qp.parse("Man Ray: a retrospective")
        self.assertEqual(unicode(q), "(a:Man AND a:Ray: AND a:a AND a:retrospective)")
        
    def test_fieldname_fieldname(self):
        qp = qparser.QueryParser("a", None)
        q = qp.parse("a:b:")
        self.assertEqual(q, query.Term("a", u"b:"))
        
    def test_paren_fieldname(self):
        schema = fields.Schema(kind=fields.ID, content=fields.TEXT) 
    
        qp = qparser.QueryParser("content", schema)
        q = qp.parse(u"(kind:1d565 OR kind:7c584) AND (stuff)")
        self.assertEqual(unicode(q), "((kind:1d565 OR kind:7c584) AND content:stuff)")

        q = qp.parse(u"kind:(1d565 OR 7c584) AND (stuff)")
        self.assertEqual(unicode(q), "((kind:1d565 OR kind:7c584) AND content:stuff)")


if __name__ == '__main__':
    unittest.main()
