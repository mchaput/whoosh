# coding=utf-8

from nose.tools import assert_equal

from whoosh import analysis, fields, qparser, query
from whoosh.qparser import dateparse


def test_empty_querystring():
    s = fields.Schema(content=fields.TEXT, title=fields.TEXT, id=fields.ID)
    qp = qparser.QueryParser("content", s)
    q = qp.parse(u"")
    assert_equal(q, query.NullQuery)

def test_fields():
    s = fields.Schema(content=fields.TEXT, title=fields.TEXT, id=fields.ID)
    qp = qparser.QueryParser("content", s)
    q = qp.parse(u"test")
    assert_equal(q.__class__, query.Term)
    assert_equal(q.fieldname, "content")
    assert_equal(q.text, "test")
    
    mq = qparser.MultifieldParser(("title", "content"), s)
    q = mq.parse(u"test")
    assert_equal(q.__class__, query.Or)
    assert_equal(q[0].__class__, query.Term)
    assert_equal(q[1].__class__, query.Term)
    assert_equal(q[0].fieldname, "title")
    assert_equal(q[1].fieldname, "content")
    assert_equal(q[0].text, "test")
    assert_equal(q[1].text, "test")
    
    q = mq.parse(u"title:test")
    assert_equal(q.__class__, query.Term)
    assert_equal(q.fieldname, "title")
    assert_equal(q.text, "test")

def test_multifield():
    schema = fields.Schema(content=fields.TEXT, title=fields.TEXT,
                           cat=fields.KEYWORD, date=fields.DATETIME)
    
    qs = u"time (cinema muza cat:place) OR (cinema muza cat:event)"
    qp = qparser.MultifieldParser(['content', 'title'], schema)
    q = qp.parse(qs)
    assert_equal(unicode(q), "((content:time OR title:time) AND (((content:cinema OR title:cinema) AND (content:muza OR title:muza) AND cat:place) OR ((content:cinema OR title:cinema) AND (content:muza OR title:muza) AND cat:event)))")
    
def test_fieldname_chars():
    s = fields.Schema(abc123=fields.TEXT, nisbah=fields.KEYWORD)
    qp = qparser.QueryParser("content", s)
    fieldmap = {'nisbah': [u'\u0646\u0633\u0628\u0629'],
                'abc123': ['xyz']}
    qp.add_plugin(qparser.FieldAliasPlugin(fieldmap))
    
    q = qp.parse(u"abc123:456")
    assert_equal(q.__class__, query.Term)
    assert_equal(q.fieldname, u'abc123')
    assert_equal(q.text, u'456')
    
    q = qp.parse(u"abc123:456 def")
    assert_equal(unicode(q), u"(abc123:456 AND content:def)")
    
    q = qp.parse(u'\u0646\u0633\u0628\u0629:\u0627\u0644\u0641\u0644\u0633\u0637\u064a\u0646\u064a')
    assert_equal(q.__class__, query.Term)
    assert_equal(q.fieldname, u'nisbah')
    assert_equal(q.text, u'\u0627\u0644\u0641\u0644\u0633\u0637\u064a\u0646\u064a')
    
    q = qp.parse(u"abc123 (xyz:123 OR qrs)")
    assert_equal(unicode(q), "(content:abc123 AND (abc123:123 OR content:qrs))")

def test_colonspace():
    s = fields.Schema(content=fields.TEXT, url=fields.ID)
    qp = qparser.QueryParser("content", s)
    q = qp.parse(u"url:test")
    assert_equal(q.__class__, query.Term)
    assert_equal(q.fieldname, "url")
    assert_equal(q.text, "test")
    
    q = qp.parse(u"url: test")
    assert q.__class__, query.And
    assert_equal(q[0].__class__, query.Term)
    assert_equal(q[1].__class__, query.Term)
    assert_equal(q[0].fieldname, "content")
    assert_equal(q[1].fieldname, "content")
    assert_equal(q[0].text, "url")
    assert_equal(q[1].text, "test")
    
    q = qp.parse(u"url:")
    assert_equal(q.__class__, query.Term)
    assert_equal(q.fieldname, "content")
    assert_equal(q.text, "url")
    
    s = fields.Schema(foo=fields.KEYWORD)
    qp = qparser.QueryParser("foo", s)
    q = qp.parse(u"blah:")
    assert_equal(q.__class__, query.Term)
    assert_equal(q.fieldname, "foo")
    assert_equal(q.text, "blah:")

def test_andor():
    qp  = qparser.QueryParser("a", None)
    q = qp.parse("a AND b OR c AND d OR e AND f")
    assert_equal(unicode(q), u"((a:a AND a:b) OR (a:c AND a:d) OR (a:e AND a:f))")
    
    q = qp.parse("aORb")
    assert_equal(q, query.Term("a", "aORb"))
    
    q = qp.parse("aOR b")
    assert_equal(q, query.And([query.Term("a", "aOR"), query.Term("a", "b")]))
    
    q = qp.parse("a ORb")
    assert_equal(q, query.And([query.Term("a", "a"), query.Term("a", "ORb")]))
    
    assert_equal(qp.parse("OR"), query.Term("a", "OR"))

def test_andnot():
    qp = qparser.QueryParser("content", None)
    q = qp.parse(u"this ANDNOT that")
    assert_equal(q.__class__, query.AndNot)
    assert_equal(q.a.__class__, query.Term)
    assert_equal(q.b.__class__, query.Term)
    assert_equal(q.a.text, "this")
    assert_equal(q.b.text, "that")
    
    q = qp.parse(u"foo ANDNOT bar baz")
    assert_equal(q.__class__, query.And)
    assert_equal(len(q), 2)
    assert_equal(q[0].__class__, query.AndNot)
    assert_equal(q[1].__class__, query.Term)
    
    q = qp.parse(u"foo fie ANDNOT bar baz")
    assert_equal(q.__class__, query.And)
    assert_equal(len(q), 3)
    assert_equal(q[0].__class__, query.Term)
    assert_equal(q[1].__class__, query.AndNot)
    assert_equal(q[2].__class__, query.Term)
    
    q = qp.parse(u"a AND b ANDNOT c")
    assert_equal(q.__class__, query.AndNot)
    assert_equal(unicode(q), "((content:a AND content:b) ANDNOT content:c)")

def test_boost():
    qp = qparser.QueryParser("content", None)
    q = qp.parse(u"this^3 fn:that^0.5 5.67 hi^5x")
    assert_equal(q[0].boost, 3.0)
    assert_equal(q[1].boost, 0.5)
    assert_equal(q[1].fieldname, "fn")
    assert_equal(q[2].text, "5.67")
    assert_equal(q[3].text, "hi^5x")
    
    q = qp.parse("alfa (bravo OR charlie)^2.5 ^3")
    assert_equal(len(q), 3)
    assert_equal(q[0].boost, 1.0)
    assert_equal(q[1].boost, 2.5)
    assert_equal(q[2].text, "^3")
    
def test_wildcard1():
    qp = qparser.QueryParser("content", None)
    q = qp.parse(u"hello *the?e* ?star*s? test")
    assert_equal(len(q), 4)
    assert_equal(q[0].__class__, query.Term)
    assert_equal(q[0].text, "hello")
    assert_equal(q[1].__class__, query.Wildcard)
    assert_equal(q[1].text, "*the?e*")
    assert_equal(q[2].__class__, query.Wildcard)
    assert_equal(q[2].text, "?star*s?")
    assert_equal(q[3].__class__, query.Term)
    assert_equal(q[3].text, "test")
    
def test_wildcard2():
    qp = qparser.QueryParser("content", None)
    q = qp.parse(u"*the?e*")
    assert_equal(q.__class__, query.Wildcard)
    assert_equal(q.text, "*the?e*")
    
def test_parse_fieldname_underscores():
    s = fields.Schema(my_name=fields.ID(stored=True), my_value=fields.TEXT)
    qp = qparser.QueryParser("my_value", schema=s)
    q = qp.parse(u"my_name:Green")
    assert_equal(q.__class__, query.Term)
    assert_equal(q.fieldname, "my_name")
    assert_equal(q.text, "Green")

def test_endstar():
    qp = qparser.QueryParser("text", None)
    q = qp.parse(u"word*")
    assert_equal(q.__class__, query.Prefix)
    assert_equal(q.text, "word")
    
    q = qp.parse(u"first* second")
    assert_equal(q[0].__class__, query.Prefix)
    assert_equal(q[0].text, "first")

def test_singlequotes():
    qp = qparser.QueryParser("text", None)
    q = qp.parse("hell's hot 'i stab at thee'")
    assert_equal(q.__class__.__name__, 'And')
    assert_equal(len(q), 3)
    assert_equal(q[0].__class__, query.Term)
    assert_equal(q[1].__class__, query.Term)
    assert_equal(q[2].__class__, query.Term)
    assert_equal(q[0].text, "hell's")
    assert_equal(q[1].text, "hot")
    assert_equal(q[2].text, "i stab at thee")
    
    q = qp.parse("alfa zulu:'bravo charlie' delta")
    assert_equal(q.__class__.__name__, 'And')
    assert_equal(len(q), 3)
    assert_equal(q[0].__class__, query.Term)
    assert_equal(q[1].__class__, query.Term)
    assert_equal(q[2].__class__, query.Term)
    assert_equal((q[0].fieldname, q[0].text), ("text", "alfa"))
    assert_equal((q[1].fieldname, q[1].text), ("zulu", "bravo charlie"))
    assert_equal((q[2].fieldname, q[2].text), ("text", "delta"))
    
    q = qp.parse("The rest 'is silence")
    assert_equal(q.__class__, query.And)
    assert_equal(len(q), 4)
    assert_equal([t.text for t in q.subqueries], ["The", "rest", "'is" ,"silence"])
    
    q = qp.parse("I don't like W's stupid face")
    assert_equal(q.__class__, query.And)
    assert_equal(len(q), 6)
    assert_equal([t.text for t in q.subqueries], ["I", "don't", "like" ,"W's", "stupid", "face"])
    
    q = qp.parse("I forgot the drinkin' in '98")
    assert_equal(q.__class__, query.And)
    assert_equal(len(q), 6)
    assert_equal([t.text for t in q.subqueries], ["I", "forgot", "the" ,"drinkin'", "in", "'98"])

#    def test_escaping():
#        qp = qparser.QueryParser("text", None)
#        
#        q = qp.parse(r'big\small')
#        assert q.__class__, query.Term, q)
#        assert q.text, "bigsmall")
#        
#        q = qp.parse(r'big\\small')
#        assert q.__class__, query.Term)
#        assert q.text, r'big\small')
#        
#        q = qp.parse(r'http\:example')
#        assert q.__class__, query.Term)
#        assert q.fieldname, "text")
#        assert q.text, "http:example")
#        
#        q = qp.parse(r'hello\ there')
#        assert q.__class__, query.Term)
#        assert q.text, "hello there")
#        
#        q = qp.parse(r'\[start\ TO\ end\]')
#        assert q.__class__, query.Term)
#        assert q.text, "[start TO end]")
#    
#        schema = fields.Schema(text=fields.TEXT)
#        qp = qparser.QueryParser("text", None)
#        q = qp.parse(r"http\:\/\/www\.example\.com")
#        assert q.__class__, query.Term)
#        assert q.text, "http://www.example.com")
#        
#        q = qp.parse(u"\\\\")
#        assert q.__class__, query.Term)
#        assert q.text, "\\")

#    def test_escaping_wildcards():
#        qp = qparser.QueryParser("text", None)
#        
#        q = qp.parse(u"a*b*c?d")
#        assert q.__class__, query.Wildcard)
#        assert q.text, "a*b*c?d")
#        
#        q = qp.parse(u"a*b\\*c?d")
#        assert q.__class__, query.Wildcard)
#        assert q.text, "a*b*c?d")
#        
#        q = qp.parse(u"a*b\\\\*c?d")
#        assert q.__class__, query.Wildcard)
#        assert q.text, u'a*b\\*c?d')
#        
#        q = qp.parse(u"ab*")
#        assert q.__class__, query.Prefix)
#        assert q.text, u"ab")
#        
#        q = qp.parse(u"ab\\\\*")
#        assert q.__class__, query.Wildcard)
#        assert q.text, u"ab\\*")
    
def test_phrase():
    qp = qparser.QueryParser("content", None)
    q = qp.parse('"alfa bravo" "charlie delta echo"^2.2 test:"foxtrot golf"')
    assert_equal(q[0].__class__, query.Phrase)
    assert_equal(q[0].words, ["alfa", "bravo"])
    assert_equal(q[1].__class__, query.Phrase)
    assert_equal(q[1].words, ["charlie", "delta", "echo"])
    assert_equal(q[1].boost, 2.2)
    assert_equal(q[2].__class__, query.Phrase)
    assert_equal(q[2].words, ["foxtrot", "golf"])
    assert_equal(q[2].fieldname, "test")
    
def test_weird_characters():
    qp = qparser.QueryParser("content", None)
    q = qp.parse(u".abcd@gmail.com")
    assert_equal(q.__class__, query.Term)
    assert_equal(q.text, u".abcd@gmail.com")
    q = qp.parse(u"r*")
    assert_equal(q.__class__, query.Prefix)
    assert_equal(q.text, u"r")
    q = qp.parse(u".")
    assert_equal(q.__class__, query.Term)
    assert_equal(q.text, u".")
    q = qp.parse(u"?")
    assert_equal(q.__class__, query.Wildcard)
    assert_equal(q.text, u"?")
    
def test_euro_chars():
    schema = fields.Schema(text=fields.TEXT)
    qp = qparser.QueryParser("text", schema)
    q = qp.parse(u"straße")
    assert_equal(q.__class__, query.Term)
    assert_equal(q.text, u"straße")

def test_star():
    schema = fields.Schema(text = fields.TEXT(stored=True))
    qp = qparser.QueryParser("text", schema)
    q = qp.parse(u"*")
    assert_equal(q.__class__, query.Every)

    q = qp.parse(u"*h?ll*")
    assert_equal(q.__class__, query.Wildcard)
    assert_equal(q.text, u"*h?ll*")
    
    q = qp.parse(u"h?pe")
    assert_equal(q.__class__, query.Wildcard)
    assert_equal(q.text, u"h?pe")
    
    q = qp.parse(u"*? blah")
    assert_equal(q.__class__, query.And)
    assert_equal(q[0].__class__, query.Wildcard)
    assert_equal(q[0].text, u"*?")
    assert_equal(q[1].__class__, query.Term)
    assert_equal(q[1].text, u"blah")
    
    q = qp.parse(u"*ending")
    assert_equal(q.__class__, query.Wildcard)
    assert_equal(q.text, u"*ending")
    
    q = qp.parse(u"*q")
    assert_equal(q.__class__, query.Wildcard)
    assert_equal(q.text, u"*q")

def test_range():
    schema = fields.Schema(name=fields.ID(stored=True), text = fields.TEXT(stored=True))
    qp = qparser.QueryParser("text", schema)
    
    q = qp.parse(u"[alfa to bravo}")
    assert_equal(q.__class__, query.TermRange)
    assert_equal(q.start, "alfa")
    assert_equal(q.end, "bravo")
    assert_equal(q.startexcl, False)
    assert_equal(q.endexcl, True)
    
    q = qp.parse(u"['hello there' to 'what ever']")
    assert_equal(q.__class__, query.TermRange)
    assert_equal(q.start, "hello there")
    assert_equal(q.end, "what ever")
    assert_equal(q.startexcl, False)
    assert_equal(q.endexcl, False)
    
    q = qp.parse(u"name:{'to' to 'b'}")
    assert_equal(q.__class__, query.TermRange)
    assert_equal(q.start, "to")
    assert_equal(q.end, "b")
    assert_equal(q.startexcl, True)
    assert_equal(q.endexcl, True)
    
    q = qp.parse(u"name:{'a' to 'to']")
    assert_equal(q.__class__, query.TermRange)
    assert_equal(q.start, "a")
    assert_equal(q.end, "to")
    assert_equal(q.startexcl, True)
    assert_equal(q.endexcl, False)
    
    q = qp.parse(u"name:[a to to]")
    assert_equal(q.__class__, query.TermRange)
    assert_equal(q.start, "a")
    assert_equal(q.end, "to")
    
    q = qp.parse(u"name:[to to b]")
    assert_equal(q.__class__, query.TermRange)
    assert_equal(q.start, "to")
    assert_equal(q.end, "b")
    
    q = qp.parse(u"[alfa to alfa]")
    assert_equal(q.__class__, query.Term)
    assert_equal(q.text, "alfa")
    
    q = qp.parse(u"Ind* AND name:[d TO]")
    assert_equal(q.__class__, query.And)
    assert_equal(q[0].__class__, query.Prefix)
    assert_equal(q[1].__class__, query.TermRange)
    assert_equal(q[0].text, "ind")
    assert_equal(q[1].start, "d")
    assert_equal(q[1].fieldname, "name")
    
    q = qp.parse(u"name:[d TO]")
    assert_equal(q.__class__, query.TermRange)
    assert_equal(q.start, "d")
    assert_equal(q.fieldname, "name")

def test_numeric_range():
    schema = fields.Schema(id=fields.STORED, number=fields.NUMERIC)
    qp = qparser.QueryParser("number", schema)
    
    teststart = 40
    testend = 100
    
    q = qp.parse("[%s to *]" % teststart)
    assert_equal(q, query.NullQuery)
    
    q = qp.parse("[%s to]" % teststart)
    assert_equal(q.__class__, query.NumericRange)
    assert_equal(q.start, teststart)
    assert_equal(q.end, None)
    
    q = qp.parse("[to %s]" % testend)
    assert_equal(q.__class__, query.NumericRange)
    assert_equal(q.start, None)
    assert_equal(q.end, testend)
    
    q = qp.parse("[%s to %s]" % (teststart, testend))
    assert_equal(q.__class__, query.NumericRange)
    assert_equal(q.start, teststart)
    assert_equal(q.end, testend)
    
def test_regressions():
    qp = qparser.QueryParser("f", None)
    
    # From 0.3.18, these used to require escaping. Mostly good for
    # regression testing.
    assert_equal(qp.parse(u"re-inker"), query.Term("f", u"re-inker"))
    assert_equal(qp.parse(u"0.7 wire"), query.And([query.Term("f", u"0.7"), query.Term("f", u"wire")]))
    assert (qp.parse(u"daler-rowney pearl 'bell bronze'")
            == query.And([query.Term("f", u"daler-rowney"),
                          query.Term("f", u"pearl"),
                          query.Term("f", u"bell bronze")]))
    
    q = qp.parse(u'22" BX')
    assert_equal(q, query.And([query.Term("f", u'22"'), query.Term("f", "BX")]))
    
def test_empty_ranges():
    schema = fields.Schema(name=fields.TEXT, num=fields.NUMERIC,
                           date=fields.DATETIME)
    qp = qparser.QueryParser("text", schema)
    
    for fname in ("name", "date"):
        q = qp.parse("%s:[to]" % fname)
        assert_equal(q.__class__, query.Every)

def test_empty_numeric_range():
    schema = fields.Schema(id=fields.ID, num=fields.NUMERIC)
    qp = qparser.QueryParser("num", schema)
    q = qp.parse("num:[to]")
    assert_equal(q.__class__, query.NumericRange)
    assert_equal(q.start, None)
    assert_equal(q.end, None)

def test_nonexistant_fieldnames():
    # Need an analyzer that won't mangle a URL
    a = analysis.SimpleAnalyzer("\\S+")
    schema = fields.Schema(id=fields.ID, text=fields.TEXT(analyzer=a))
    
    qp = qparser.QueryParser("text", schema)
    q = qp.parse(u"id:/code http://localhost/")
    assert_equal(q.__class__, query.And)
    assert_equal(q[0].__class__, query.Term)
    assert_equal(q[0].fieldname, "id")
    assert_equal(q[0].text, "/code")
    assert_equal(q[1].__class__, query.Term)
    assert_equal(q[1].fieldname, "text")
    assert_equal(q[1].text, "http://localhost/")

def test_stopped():
    schema = fields.Schema(text = fields.TEXT)
    qp = qparser.QueryParser("text", schema)
    q = qp.parse(u"a b")
    assert_equal(q, query.NullQuery)
    
def test_analyzing_terms():
    schema = fields.Schema(text=fields.TEXT(analyzer=analysis.StemmingAnalyzer()))
    qp = qparser.QueryParser("text", schema)
    q = qp.parse(u"Indexed!")
    assert_equal(q.__class__, query.Term)
    assert_equal(q.text, "index")
    
def test_simple():
    parser = qparser.SimpleParser("x", None)
    q = parser.parse(u"alfa bravo charlie delta")
    assert_equal(unicode(q), u"(x:alfa OR x:bravo OR x:charlie OR x:delta)")
    
    q = parser.parse(u"alfa +bravo charlie delta")
    assert_equal(unicode(q), u"(x:bravo ANDMAYBE (x:alfa OR x:charlie OR x:delta))")
    
    q = parser.parse(u"alfa +bravo -charlie delta")
    assert_equal(unicode(q), u"((x:bravo ANDMAYBE (x:alfa OR x:delta)) ANDNOT x:charlie)")
    
    q = parser.parse(u"- alfa +bravo + delta")
    assert_equal(unicode(q), u"((x:bravo AND x:delta) ANDNOT x:alfa)")

def test_dismax():
    parser = qparser.DisMaxParser({"body": 0.8, "title": 2.5}, None)
    q = parser.parse(u"alfa bravo charlie")
    assert_equal(unicode(q), u"(DisMax(body:alfa^0.8 title:alfa^2.5) OR DisMax(body:bravo^0.8 title:bravo^2.5) OR DisMax(body:charlie^0.8 title:charlie^2.5))")
    
    q = parser.parse(u"alfa +bravo charlie")
    assert_equal(unicode(q), u"(DisMax(body:bravo^0.8 title:bravo^2.5) ANDMAYBE (DisMax(body:alfa^0.8 title:alfa^2.5) OR DisMax(body:charlie^0.8 title:charlie^2.5)))")
    
    q = parser.parse(u"alfa -bravo charlie")
    assert_equal(unicode(q), u"((DisMax(body:alfa^0.8 title:alfa^2.5) OR DisMax(body:charlie^0.8 title:charlie^2.5)) ANDNOT DisMax(body:bravo^0.8 title:bravo^2.5))")
    
    q = parser.parse(u"alfa -bravo +charlie")
    assert_equal(unicode(q), u"((DisMax(body:charlie^0.8 title:charlie^2.5) ANDMAYBE DisMax(body:alfa^0.8 title:alfa^2.5)) ANDNOT DisMax(body:bravo^0.8 title:bravo^2.5))")

def test_many_clauses():
    qs = "1" + (" OR 1" * 1000)
    
    parser = qparser.QueryParser("content", None)
    parser.parse(qs)
    
def test_roundtrip():
    parser = qparser.QueryParser("a", None)
    q = parser.parse(u"a OR ((b AND c AND d AND e) OR f OR g) ANDNOT h")
    assert_equal(unicode(q), u"((a:a OR (a:b AND a:c AND a:d AND a:e) OR a:f OR a:g) ANDNOT a:h)")
    
def test_ngrams():
    schema = fields.Schema(grams=fields.NGRAM)
    parser = qparser.QueryParser('grams', schema)
    parser.remove_plugin_class(qparser.WhitespacePlugin)
    
    q = parser.parse(u"Hello There")
    assert_equal(q.__class__, query.And)
    assert_equal(len(q), 8)
    assert_equal([sq.text for sq in q], ["hell", "ello", "llo ", "lo t", "o th", " the", "ther", "here"])
    
def test_ngramwords():
    schema = fields.Schema(grams=fields.NGRAMWORDS(queryor=True))
    parser = qparser.QueryParser('grams', schema)
    
    q = parser.parse(u"Hello Tom")
    assert_equal(q.__class__, query.And)
    assert_equal(q[0].__class__, query.Or)
    assert_equal(q[1].__class__, query.Term)
    assert_equal(q[0][0].text, "hell")
    assert_equal(q[0][1].text, "ello")
    assert_equal(q[1].text, "tom")

def test_multitoken_words():
    textfield = fields.TEXT()
    textfield.multitoken_query = "or"
    schema = fields.Schema(text=textfield)
    parser = qparser.QueryParser('text', schema)
    qstring = u"chaw-bacon"
    
    texts = list(schema["text"].process_text(qstring))
    assert_equal(texts, ["chaw", "bacon"])
    
    q = parser.parse(qstring)
    assert_equal(q.__class__, query.Or)
    assert_equal(len(q), 2)
    assert_equal(q[0].__class__, query.Term)
    assert_equal(q[0].text, "chaw")
    assert_equal(q[1].__class__, query.Term)
    assert_equal(q[1].text, "bacon")

def test_operators():
    qp = qparser.QueryParser("f", None)
    
    q = qp.parse("a AND b OR c AND d")
    assert_equal(unicode(q), "((f:a AND f:b) OR (f:c AND f:d))")
    
    q = qp.parse("a OR b OR c OR d")
    assert_equal(unicode(q), "(f:a OR f:b OR f:c OR f:d)")
    
    q = qp.parse("a ANDMAYBE b ANDNOT c REQUIRE d")
    assert_equal(unicode(q), "((f:a ANDMAYBE (f:b ANDNOT f:c)) REQUIRE f:d)")

def test_associativity():
    left_andmaybe = (qparser.InfixOperator("ANDMAYBE", qparser.AndMaybeGroup, True), 0)
    right_andmaybe = (qparser.InfixOperator("ANDMAYBE", qparser.AndMaybeGroup, False), 0)
    not_ = (qparser.PrefixOperator("NOT", qparser.NotGroup), 0)
    
    def make_parser(*ops):
        parser = qparser.QueryParser("f", None)
        parser.replace_plugin(qparser.CompoundsPlugin(ops, clean=True))
        return parser
    
    p = make_parser(left_andmaybe)
    q = p.parse("a ANDMAYBE b ANDMAYBE c ANDMAYBE d")
    assert_equal(unicode(q), "(((f:a ANDMAYBE f:b) ANDMAYBE f:c) ANDMAYBE f:d)")
    
    p = make_parser(right_andmaybe)
    q = p.parse("a ANDMAYBE b ANDMAYBE c ANDMAYBE d")
    assert_equal(unicode(q), "(f:a ANDMAYBE (f:b ANDMAYBE (f:c ANDMAYBE f:d)))")
    
    p = make_parser(not_)
    q = p.parse("a NOT b NOT c NOT d", normalize=False)
    assert_equal(unicode(q), "(f:a AND NOT f:b AND NOT f:c AND NOT f:d)")
    
    p = make_parser(left_andmaybe)
    q = p.parse("(a ANDMAYBE b) ANDMAYBE (c ANDMAYBE d)")
    assert_equal(unicode(q), "((f:a ANDMAYBE f:b) ANDMAYBE (f:c ANDMAYBE f:d))")
    
    p = make_parser(right_andmaybe)
    q = p.parse("(a ANDMAYBE b) ANDMAYBE (c ANDMAYBE d)")
    assert_equal(unicode(q), "((f:a ANDMAYBE f:b) ANDMAYBE (f:c ANDMAYBE f:d))")
    
def test_not_assoc():
    qp = qparser.QueryParser("text", None)
    q = qp.parse(u"a AND NOT b OR c")
    assert_equal(unicode(q), "((text:a AND NOT text:b) OR text:c)")
    
    qp = qparser.QueryParser("text", None)
    q = qp.parse(u"a NOT (b OR c)")
    assert_equal(unicode(q), "(text:a AND NOT (text:b OR text:c))")
    
def test_fieldname_space():
    qp = qparser.QueryParser("a", None)
    q = qp.parse("Man Ray: a retrospective")
    assert_equal(unicode(q), "(a:Man AND a:Ray: AND a:a AND a:retrospective)")
    
def test_fieldname_fieldname():
    qp = qparser.QueryParser("a", None)
    q = qp.parse("a:b:")
    assert_equal(q, query.Term("a", u"b:"))
    
def test_paren_fieldname():
    schema = fields.Schema(kind=fields.ID, content=fields.TEXT) 

    qp = qparser.QueryParser("content", schema)
    q = qp.parse(u"(kind:1d565 OR kind:7c584) AND (stuff)")
    assert_equal(unicode(q), "((kind:1d565 OR kind:7c584) AND content:stuff)")

    q = qp.parse(u"kind:(1d565 OR 7c584) AND (stuff)")
    assert_equal(unicode(q), "((kind:1d565 OR kind:7c584) AND content:stuff)")

def test_star_paren():
    qp = qparser.QueryParser("content", None)
    q = qp.parse(u"(*john*) AND (title:blog)")
    
    assert_equal(q.__class__, query.And)
    assert_equal(q[0].__class__, query.Wildcard)
    assert_equal(q[1].__class__, query.Term)
    assert_equal(q[0].fieldname, "content")
    assert_equal(q[1].fieldname, "title")
    assert_equal(q[0].text, "*john*")
    assert_equal(q[1].text, "blog")

    





