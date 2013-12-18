import pytest

from whoosh import analysis, fields, query
from whoosh.compat import u, text_type
from whoosh.qparser import default
from whoosh.qparser import plugins


def test_whitespace():
    p = default.QueryParser("t", None, [plugins.WhitespacePlugin()])
    assert repr(p.tag("hello there amiga")) == "<AndGroup <None:'hello'>, < >, <None:'there'>, < >, <None:'amiga'>>"


def test_singlequotes():
    p = default.QueryParser("t", None, [plugins.WhitespacePlugin(),
                                        plugins.SingleQuotePlugin()])
    assert repr(p.process("a 'b c' d")) == "<AndGroup <None:'a'>, <None:'b c'>, <None:'d'>>"


def test_prefix():
    p = default.QueryParser("t", None, [plugins.WhitespacePlugin(),
                                        plugins.PrefixPlugin()])
    assert repr(p.process("a b* c")) == "<AndGroup <None:'a'>, <None:'b'*>, <None:'c'>>"


def test_range():
    p = default.QueryParser("t", None, [plugins.WhitespacePlugin(),
                                        plugins.RangePlugin()])
    ns = p.tag("a [b to c} d")
    assert repr(ns) == "<AndGroup <None:'a'>, < >, <None:['b' 'c'}>, < >, <None:'d'>>"

    assert repr(p.process("a {b to]")) == "<AndGroup <None:'a'>, <None:{'b' None]>>"
    assert repr(p.process("[to c] d")) == "<AndGroup <None:[None 'c']>, <None:'d'>>"
    assert repr(p.process("[to]")) == "<AndGroup <None:[None None]>>"


def test_sq_range():
    p = default.QueryParser("t", None, [plugins.WhitespacePlugin(),
                                        plugins.SingleQuotePlugin(),
                                        plugins.RangePlugin()])
    assert repr(p.process("['a b' to ']']")) == "<AndGroup <None:['a b' ']']>>"


def test_phrase():
    p = default.QueryParser("t", None, [plugins.WhitespacePlugin(),
                                        plugins.PhrasePlugin()])
    assert repr(p.process('a "b c"')) == "<AndGroup <None:'a'>, <None:PhraseNode 'b c'~1>>"
    assert repr(p.process('"b c" d')) == "<AndGroup <None:PhraseNode 'b c'~1>, <None:'d'>>"
    assert repr(p.process('"b c"')) == "<AndGroup <None:PhraseNode 'b c'~1>>"

    q = p.parse('alfa "bravo charlie"~2 delta')
    assert q[1].__class__ == query.Phrase
    assert q[1].words == ["bravo", "charlie"]
    assert q[1].slop == 2


def test_groups():
    p = default.QueryParser("t", None, [plugins.WhitespacePlugin(),
                                        plugins.GroupPlugin()])

    ns = p.process("a ((b c) d) e")
    assert repr(ns) == "<AndGroup <None:'a'>, <AndGroup <AndGroup <None:'b'>, <None:'c'>>, <None:'d'>>, <None:'e'>>"


def test_fieldnames():
    p = default.QueryParser("t", None, [plugins.WhitespacePlugin(),
                                        plugins.FieldsPlugin(),
                                        plugins.GroupPlugin()])
    ns = p.process("a:b c d:(e f:(g h)) i j:")
    assert repr(ns) == "<AndGroup <'a':'b'>, <None:'c'>, <AndGroup <'d':'e'>, <AndGroup <'f':'g'>, <'f':'h'>>>, <None:'i'>, <None:'j:'>>"
    assert repr(p.process("a:b:")) == "<AndGroup <'a':'b:'>>"


def test_operators():
    p = default.QueryParser("t", None, [plugins.WhitespacePlugin(),
                                        plugins.OperatorsPlugin()])
    ns = p.process("a OR b")
    assert repr(ns) == "<AndGroup <OrGroup <None:'a'>, <None:'b'>>>"


def test_boost():
    p = default.QueryParser("t", None, [plugins.WhitespacePlugin(),
                                        plugins.GroupPlugin(),
                                        plugins.BoostPlugin()])
    ns = p.tag("a^3")
    assert repr(ns) == "<AndGroup <None:'a'>, <^ 3.0>>"
    ns = p.filterize(ns)
    assert repr(ns) == "<AndGroup <None:'a' ^3.0>>"

    assert repr(p.process("a (b c)^2.5")) == "<AndGroup <None:'a'>, <AndGroup <None:'b'>, <None:'c'> ^2.5>>"
    assert repr(p.process("a (b c)^.5 d")) == "<AndGroup <None:'a'>, <AndGroup <None:'b'>, <None:'c'> ^0.5>, <None:'d'>>"
    assert repr(p.process("^2 a")) == "<AndGroup <None:'^2'>, <None:'a'>>"
    assert repr(p.process("a^2^3")) == "<AndGroup <None:'a^2' ^3.0>>"


#

def test_empty_querystring():
    s = fields.Schema(content=fields.TEXT, title=fields.TEXT, id=fields.ID)
    qp = default.QueryParser("content", s)
    q = qp.parse(u(""))
    assert q == query.NullQuery


def test_fields():
    s = fields.Schema(content=fields.TEXT, title=fields.TEXT, id=fields.ID)
    qp = default.QueryParser("content", s)
    q = qp.parse(u("test"))
    assert q.__class__ == query.Term
    assert q.fieldname == "content"
    assert q.text == "test"

    mq = default.MultifieldParser(("title", "content"), s)
    q = mq.parse(u("test"))
    assert q.__class__ == query.Or
    assert q[0].__class__ == query.Term
    assert q[1].__class__ == query.Term
    assert q[0].fieldname == "title"
    assert q[1].fieldname == "content"
    assert q[0].text == "test"
    assert q[1].text == "test"

    q = mq.parse(u("title:test"))
    assert q.__class__ == query.Term
    assert q.fieldname == "title"
    assert q.text == "test"


def test_multifield():
    schema = fields.Schema(content=fields.TEXT, title=fields.TEXT,
                           cat=fields.KEYWORD, date=fields.DATETIME)

    qs = u("a (b c cat:d) OR (b c cat:e)")
    qp = default.MultifieldParser(['x', 'y'], schema)

    q = qp.parse(qs)
    assert text_type(q) == "((x:a OR y:a) AND (((x:b OR y:b) AND (x:c OR y:c) AND cat:d) OR ((x:b OR y:b) AND (x:c OR y:c) AND cat:e)))"


def test_fieldname_chars():
    s = fields.Schema(abc123=fields.TEXT, nisbah=fields.KEYWORD)
    qp = default.QueryParser("content", s)
    fieldmap = {'nisbah': [u('\u0646\u0633\u0628\u0629')],
                'abc123': ['xyz']}
    qp.add_plugin(plugins.FieldAliasPlugin(fieldmap))

    q = qp.parse(u("abc123:456"))
    assert q.__class__ == query.Term
    assert q.fieldname == u('abc123')
    assert q.text == u('456')

    q = qp.parse(u("abc123:456 def"))
    assert text_type(q) == u("(abc123:456 AND content:def)")

    q = qp.parse(u('\u0646\u0633\u0628\u0629:\u0627\u0644\u0641\u0644\u0633'
                   '\u0637\u064a\u0646\u064a'))
    assert q.__class__ == query.Term
    assert q.fieldname == u('nisbah')
    assert q.text == u('\u0627\u0644\u0641\u0644\u0633\u0637\u064a\u0646\u064a')

    q = qp.parse(u("abc123 (xyz:123 OR qrs)"))
    assert text_type(q) == "(content:abc123 AND (abc123:123 OR content:qrs))"


def test_colonspace():
    s = fields.Schema(content=fields.TEXT, url=fields.ID)
    qp = default.QueryParser("content", s)
    q = qp.parse(u("url:test"))
    assert q.__class__ == query.Term
    assert q.fieldname == "url"
    assert q.text == "test"

    q = qp.parse(u("url: test"))
    assert q.__class__ == query.And
    assert q[0].__class__ == query.Term
    assert q[1].__class__ == query.Term
    assert q[0].fieldname == "content"
    assert q[1].fieldname == "content"
    assert q[0].text == "url"
    assert q[1].text == "test"

    q = qp.parse(u("url:"))
    assert q.__class__ == query.Term
    assert q.fieldname == "content"
    assert q.text == "url"

    s = fields.Schema(foo=fields.KEYWORD)
    qp = default.QueryParser("foo", s)
    q = qp.parse(u("blah:"))
    assert q.__class__ == query.Term
    assert q.fieldname == "foo"
    assert q.text == "blah:"


def test_andor():
    qp = default.QueryParser("a", None)
    q = qp.parse("a AND b OR c AND d OR e AND f")
    assert text_type(q) == "((a:a AND a:b) OR (a:c AND a:d) OR (a:e AND a:f))"

    q = qp.parse("aORb")
    assert q == query.Term("a", "aORb")

    q = qp.parse("aOR b")
    assert q == query.And([query.Term("a", "aOR"), query.Term("a", "b")])

    q = qp.parse("a ORb")
    assert q == query.And([query.Term("a", "a"), query.Term("a", "ORb")])

    assert qp.parse("OR") == query.Term("a", "OR")


def test_andnot():
    qp = default.QueryParser("content", None)
    q = qp.parse(u("this ANDNOT that"))
    assert q.__class__ == query.AndNot
    assert q.a.__class__ == query.Term
    assert q.b.__class__ == query.Term
    assert q.a.text == "this"
    assert q.b.text == "that"

    q = qp.parse(u("foo ANDNOT bar baz"))
    assert q.__class__ == query.And
    assert len(q) == 2
    assert q[0].__class__ == query.AndNot
    assert q[1].__class__ == query.Term

    q = qp.parse(u("foo fie ANDNOT bar baz"))
    assert q.__class__ == query.And
    assert len(q) == 3
    assert q[0].__class__ == query.Term
    assert q[1].__class__ == query.AndNot
    assert q[2].__class__ == query.Term

    q = qp.parse(u("a AND b ANDNOT c"))
    assert q.__class__ == query.AndNot
    assert text_type(q) == "((content:a AND content:b) ANDNOT content:c)"


def test_boost_query():
    qp = default.QueryParser("content", None)
    q = qp.parse(u("this^3 fn:that^0.5 5.67 hi^5x"))
    assert q[0].boost == 3.0
    assert q[1].boost == 0.5
    assert q[1].fieldname == "fn"
    assert q[2].text == "5.67"
    assert q[3].text == "hi^5x"

    q = qp.parse("alfa (bravo OR charlie)^2.5 ^3")
    assert len(q) == 3
    assert q[0].boost == 1.0
    assert q[1].boost == 2.5
    assert q[2].text == "^3"


def test_boosts():
    qp = default.QueryParser("t", None)
    q = qp.parse("alfa ((bravo^2)^3)^4 charlie")
    assert q.__unicode__() == "(t:alfa AND t:bravo^24.0 AND t:charlie)"


def test_wild():
    qp = default.QueryParser("t", None, [plugins.WhitespacePlugin(),
                                         plugins.WildcardPlugin()])
    assert repr(qp.process("a b*c? d")) == "<AndGroup <None:'a'>, <None:Wild 'b*c?'>, <None:'d'>>"
    assert repr(qp.process("a * ? d")) == "<AndGroup <None:'a'>, <None:Wild '*'>, <None:Wild '?'>, <None:'d'>>"

    #
    qp = default.QueryParser("content", None)
    q = qp.parse(u("hello *the?e* ?star*s? test"))
    assert len(q) == 4
    assert q[0].__class__ == query.Term
    assert q[0].text == "hello"
    assert q[1].__class__ == query.Wildcard
    assert q[1].text == "*the?e*"
    assert q[2].__class__ == query.Wildcard
    assert q[2].text == "?star*s?"
    assert q[3].__class__ == query.Term
    assert q[3].text == "test"

    #
    qp = default.QueryParser("content", None)
    q = qp.parse(u("*the?e*"))
    assert q.__class__ == query.Wildcard
    assert q.text == "*the?e*"


def test_parse_fieldname_underscores():
    s = fields.Schema(my_name=fields.ID(stored=True), my_value=fields.TEXT)
    qp = default.QueryParser("my_value", schema=s)
    q = qp.parse(u("my_name:Green"))
    assert q.__class__ == query.Term
    assert q.fieldname == "my_name"
    assert q.text == "Green"


def test_endstar():
    qp = default.QueryParser("text", None)
    q = qp.parse(u("word*"))
    assert q.__class__ == query.Prefix
    assert q.text == "word"

    q = qp.parse(u("first* second"))
    assert q[0].__class__ == query.Prefix
    assert q[0].text == "first"


def test_singlequotes_query():
    qp = default.QueryParser("text", None)
    q = qp.parse("hell's hot 'i stab at thee'")
    assert q.__class__.__name__ == 'And'
    assert len(q) == 3
    assert q[0].__class__ == query.Term
    assert q[1].__class__ == query.Term
    assert q[2].__class__ == query.Term
    assert q[0].text == "hell's"
    assert q[1].text == "hot"
    assert q[2].text == "i stab at thee"

    q = qp.parse("alfa zulu:'bravo charlie' delta")
    assert q.__class__.__name__ == 'And'
    assert len(q) == 3
    assert q[0].__class__ == query.Term
    assert q[1].__class__ == query.Term
    assert q[2].__class__ == query.Term
    assert (q[0].fieldname, q[0].text) == ("text", "alfa")
    assert (q[1].fieldname, q[1].text) == ("zulu", "bravo charlie")
    assert (q[2].fieldname, q[2].text) == ("text", "delta")

    q = qp.parse("The rest 'is silence")
    assert q.__class__ == query.And
    assert len(q) == 4
    assert [t.text for t in q.subqueries] == ["The", "rest", "'is", "silence"]

    q = qp.parse("I don't like W's stupid face")
    assert q.__class__ == query.And
    assert len(q) == 6
    assert [t.text for t in q.subqueries] == ["I", "don't", "like", "W's",
                                              "stupid", "face"]

    q = qp.parse("I forgot the drinkin' in '98")
    assert q.__class__ == query.And
    assert len(q) == 6
    assert [t.text for t in q.subqueries] == ["I", "forgot", "the", "drinkin'",
                                              "in", "'98"]

#    def test_escaping():
#        qp = default.QueryParser("text", None)
#
#        q = qp.parse(r'big\small')
#        assert q.__class__, query.Term, q)
#        assert q.text == "bigsmall"
#
#        q = qp.parse(r'big\\small')
#        assert q.__class__ == query.Term
#        assert q.text == r'big\small'
#
#        q = qp.parse(r'http\:example')
#        assert q.__class__ == query.Term
#        assert q.fieldname == "text"
#        assert q.text == "http:example"
#
#        q = qp.parse(r'hello\ there')
#        assert q.__class__ == query.Term
#        assert q.text == "hello there"
#
#        q = qp.parse(r'\[start\ TO\ end\]')
#        assert q.__class__ == query.Term
#        assert q.text == "[start TO end]"
#
#        schema = fields.Schema(text=fields.TEXT)
#        qp = default.QueryParser("text", None)
#        q = qp.parse(r"http\:\/\/www\.example\.com")
#        assert q.__class__ == query.Term
#        assert q.text == "http://www.example.com"
#
#        q = qp.parse(u("\u005c\u005c"))
#        assert q.__class__ == query.Term
#        assert q.text == "\\"

#    def test_escaping_wildcards():
#        qp = default.QueryParser("text", None)
#
#        q = qp.parse(u("a*b*c?d"))
#        assert q.__class__ == query.Wildcard
#        assert q.text == "a*b*c?d"
#
#        q = qp.parse(u("a*b\u005c*c?d"))
#        assert q.__class__ == query.Wildcard
#        assert q.text == "a*b*c?d"
#
#        q = qp.parse(u("a*b\u005c\u005c*c?d"))
#        assert q.__class__ == query.Wildcard
#        assert q.text, u('a*b\u005c*c?d'))
#
#        q = qp.parse(u("ab*"))
#        assert q.__class__ == query.Prefix
#        assert q.text, u("ab"))
#
#        q = qp.parse(u("ab\u005c\u005c*"))
#        assert q.__class__ == query.Wildcard
#        assert q.text, u("ab\u005c*"))


def test_phrase_phrase():
    qp = default.QueryParser("content", None)
    q = qp.parse('"alfa bravo" "charlie delta echo"^2.2 test:"foxtrot golf"')
    assert q[0].__class__ == query.Phrase
    assert q[0].words == ["alfa", "bravo"]
    assert q[1].__class__ == query.Phrase
    assert q[1].words == ["charlie", "delta", "echo"]
    assert q[1].boost == 2.2
    assert q[2].__class__ == query.Phrase
    assert q[2].words == ["foxtrot", "golf"]
    assert q[2].fieldname == "test"


def test_weird_characters():
    qp = default.QueryParser("content", None)
    q = qp.parse(u(".abcd@gmail.com"))
    assert q.__class__ == query.Term
    assert q.text == ".abcd@gmail.com"
    q = qp.parse(u("r*"))
    assert q.__class__ == query.Prefix
    assert q.text == "r"
    q = qp.parse(u("."))
    assert q.__class__ == query.Term
    assert q.text == "."
    q = qp.parse(u("?"))
    assert q.__class__ == query.Wildcard
    assert q.text == "?"


def test_euro_chars():
    schema = fields.Schema(text=fields.TEXT)
    qp = default.QueryParser("text", schema)
    q = qp.parse(u("stra\xdfe"))
    assert q.__class__ == query.Term
    assert q.text == u("stra\xdfe")


def test_star():
    schema = fields.Schema(text=fields.TEXT(stored=True))
    qp = default.QueryParser("text", schema)
    q = qp.parse(u("*"))
    assert q.__class__ == query.Every
    assert q.fieldname == "text"

    q = qp.parse(u("*h?ll*"))
    assert q.__class__ == query.Wildcard
    assert q.text == "*h?ll*"

    q = qp.parse(u("h?pe"))
    assert q.__class__ == query.Wildcard
    assert q.text == "h?pe"

    q = qp.parse(u("*? blah"))
    assert q.__class__ == query.And
    assert q[0].__class__ == query.Wildcard
    assert q[0].text == "*?"
    assert q[1].__class__ == query.Term
    assert q[1].text == "blah"

    q = qp.parse(u("*ending"))
    assert q.__class__ == query.Wildcard
    assert q.text == "*ending"

    q = qp.parse(u("*q"))
    assert q.__class__ == query.Wildcard
    assert q.text == "*q"


def test_star_field():
    schema = fields.Schema(text=fields.TEXT)
    qp = default.QueryParser("text", schema)

    q = qp.parse(u("*:*"))
    assert q.__class__ == query.Every
    assert q.fieldname is None

    # This gets parsed to a term with text="*:test" which is then analyzed down
    # to just "test"
    q = qp.parse(u("*:test"))
    assert q.__class__ == query.Term
    assert q.fieldname == "text"
    assert q.text == "test"


def test_range_query():
    schema = fields.Schema(name=fields.ID(stored=True),
                           text=fields.TEXT(stored=True))
    qp = default.QueryParser("text", schema)

    q = qp.parse(u("[alfa to bravo}"))
    assert q.__class__ == query.TermRange
    assert q.start == "alfa"
    assert q.end == "bravo"
    assert q.startexcl is False
    assert q.endexcl is True

    q = qp.parse(u("['hello there' to 'what ever']"))
    assert q.__class__ == query.TermRange
    assert q.start == "hello there"
    assert q.end == "what ever"
    assert q.startexcl is False
    assert q.endexcl is False

    q = qp.parse(u("name:{'to' to 'b'}"))
    assert q.__class__ == query.TermRange
    assert q.start == "to"
    assert q.end == "b"
    assert q.startexcl is True
    assert q.endexcl is True

    q = qp.parse(u("name:{'a' to 'to']"))
    assert q.__class__ == query.TermRange
    assert q.start == "a"
    assert q.end == "to"
    assert q.startexcl is True
    assert q.endexcl is False

    q = qp.parse(u("name:[a to to]"))
    assert q.__class__ == query.TermRange
    assert q.start == "a"
    assert q.end == "to"

    q = qp.parse(u("name:[to to b]"))
    assert q.__class__ == query.TermRange
    assert q.start == "to"
    assert q.end == "b"

    q = qp.parse(u("[alfa to alfa]"))
    assert q.__class__ == query.Term
    assert q.text == "alfa"

    q = qp.parse(u("Ind* AND name:[d TO]"))
    assert q.__class__ == query.And
    assert q[0].__class__ == query.Prefix
    assert q[1].__class__ == query.TermRange
    assert q[0].text == "ind"
    assert q[1].start == "d"
    assert q[1].fieldname == "name"

    q = qp.parse(u("name:[d TO]"))
    assert q.__class__ == query.TermRange
    assert q.start == "d"
    assert q.fieldname == "name"


def test_numeric_range():
    schema = fields.Schema(id=fields.STORED, number=fields.NUMERIC)
    qp = default.QueryParser("number", schema)

    teststart = 40
    testend = 100

    q = qp.parse("[%s to *]" % teststart)
    assert q == query.NullQuery

    q = qp.parse("[%s to]" % teststart)
    assert q.__class__ == query.NumericRange
    assert q.start == teststart
    assert q.end is None

    q = qp.parse("[to %s]" % testend)
    assert q.__class__ == query.NumericRange
    assert q.start is None
    assert q.end == testend

    q = qp.parse("[%s to %s]" % (teststart, testend))
    assert q.__class__ == query.NumericRange
    assert q.start == teststart
    assert q.end == testend


def test_regressions():
    qp = default.QueryParser("f", None)

    # From 0.3.18, these used to require escaping. Mostly good for
    # regression testing.
    assert qp.parse(u("re-inker")) == query.Term("f", "re-inker")
    assert qp.parse(u("0.7 wire")) == query.And([query.Term("f", "0.7"),
                                                 query.Term("f", "wire")])
    assert (qp.parse(u("daler-rowney pearl 'bell bronze'"))
            == query.And([query.Term("f", "daler-rowney"),
                          query.Term("f", "pearl"),
                          query.Term("f", "bell bronze")]))

    q = qp.parse(u('22" BX'))
    assert q, query.And([query.Term("f", '22"') == query.Term("f", "BX")])


def test_empty_ranges():
    schema = fields.Schema(name=fields.TEXT, num=fields.NUMERIC,
                           date=fields.DATETIME)
    qp = default.QueryParser("text", schema)

    for fname in ("name", "date"):
        q = qp.parse(u("%s:[to]") % fname)
        assert q.__class__ == query.Every


def test_empty_numeric_range():
    schema = fields.Schema(id=fields.ID, num=fields.NUMERIC)
    qp = default.QueryParser("num", schema)
    q = qp.parse("num:[to]")
    assert q.__class__ == query.NumericRange
    assert q.start is None
    assert q.end is None


def test_numrange_multi():
    schema = fields.Schema(text=fields.TEXT, start=fields.NUMERIC,
                           end=fields.NUMERIC)
    qp = default.QueryParser("text", schema)

    q = qp.parse("start:[2008 to]")
    assert q.__class__ == query.NumericRange
    assert q.fieldname == "start"
    assert q.start == 2008
    assert q.end is None

    q = qp.parse("start:[2011 to 2012]")
    assert q.__class__.__name__ == "NumericRange"
    assert q.fieldname == "start"
    assert q.start == 2011
    assert q.end == 2012

    q = qp.parse("start:[2008 to] AND end:[2011 to 2012]")
    assert q.__class__ == query.And
    assert q[0].__class__ == query.NumericRange
    assert q[1].__class__ == query.NumericRange
    assert q[0].start == 2008
    assert q[0].end is None
    assert q[1].start == 2011
    assert q[1].end == 2012


def test_nonexistant_fieldnames():
    # Need an analyzer that won't mangle a URL
    a = analysis.SimpleAnalyzer("\\S+")
    schema = fields.Schema(id=fields.ID, text=fields.TEXT(analyzer=a))

    qp = default.QueryParser("text", schema)
    q = qp.parse(u("id:/code http://localhost/"))
    assert q.__class__ == query.And
    assert q[0].__class__ == query.Term
    assert q[0].fieldname == "id"
    assert q[0].text == "/code"
    assert q[1].__class__ == query.Term
    assert q[1].fieldname == "text"
    assert q[1].text == "http://localhost/"


def test_stopped():
    schema = fields.Schema(text=fields.TEXT)
    qp = default.QueryParser("text", schema)
    q = qp.parse(u("a b"))
    assert q == query.NullQuery


def test_analyzing_terms():
    ana = analysis.StemmingAnalyzer()
    schema = fields.Schema(text=fields.TEXT(analyzer=ana))
    qp = default.QueryParser("text", schema)
    q = qp.parse(u("Indexed!"))
    assert q.__class__ == query.Term
    assert q.text == "index"


def test_simple_parsing():
    parser = default.SimpleParser("x", None)
    q = parser.parse(u("alfa bravo charlie delta"))
    assert text_type(q) == "(x:alfa OR x:bravo OR x:charlie OR x:delta)"

    q = parser.parse(u("alfa +bravo charlie delta"))
    assert text_type(q) == "(x:bravo ANDMAYBE (x:alfa OR x:charlie OR x:delta))"

    q = parser.parse(u("alfa +bravo -charlie delta"))
    assert text_type(q) == "((x:bravo ANDMAYBE (x:alfa OR x:delta)) ANDNOT x:charlie)"

    q = parser.parse(u("- alfa +bravo + delta"))
    assert text_type(q) == "((x:bravo AND x:delta) ANDNOT x:alfa)"


def test_dismax():
    parser = default.DisMaxParser({"body": 0.8, "title": 2.5}, None)
    q = parser.parse(u("alfa bravo charlie"))
    assert text_type(q) == "(DisMax(body:alfa^0.8 title:alfa^2.5) OR DisMax(body:bravo^0.8 title:bravo^2.5) OR DisMax(body:charlie^0.8 title:charlie^2.5))"

    q = parser.parse(u("alfa +bravo charlie"))
    assert text_type(q) == "(DisMax(body:bravo^0.8 title:bravo^2.5) ANDMAYBE (DisMax(body:alfa^0.8 title:alfa^2.5) OR DisMax(body:charlie^0.8 title:charlie^2.5)))"

    q = parser.parse(u("alfa -bravo charlie"))
    assert text_type(q) == "((DisMax(body:alfa^0.8 title:alfa^2.5) OR DisMax(body:charlie^0.8 title:charlie^2.5)) ANDNOT DisMax(body:bravo^0.8 title:bravo^2.5))"

    q = parser.parse(u("alfa -bravo +charlie"))
    assert text_type(q) == "((DisMax(body:charlie^0.8 title:charlie^2.5) ANDMAYBE DisMax(body:alfa^0.8 title:alfa^2.5)) ANDNOT DisMax(body:bravo^0.8 title:bravo^2.5))"


def test_many_clauses():
    qs = "1" + (" OR 1" * 1000)

    parser = default.QueryParser("content", None)
    parser.parse(qs)


def test_roundtrip():
    parser = default.QueryParser("a", None)
    q = parser.parse(u("a OR ((b AND c AND d AND e) OR f OR g) ANDNOT h"))
    assert text_type(q) == "((a:a OR (a:b AND a:c AND a:d AND a:e) OR a:f OR a:g) ANDNOT a:h)"


def test_ngrams():
    schema = fields.Schema(grams=fields.NGRAM)
    parser = default.QueryParser('grams', schema)
    parser.remove_plugin_class(plugins.WhitespacePlugin)

    q = parser.parse(u("Hello There"))
    assert q.__class__ == query.And
    assert len(q) == 8
    assert [sq.text for sq in q] == ["hell", "ello", "llo ", "lo t", "o th",
                                     " the", "ther", "here"]


def test_ngramwords():
    schema = fields.Schema(grams=fields.NGRAMWORDS(queryor=True))
    parser = default.QueryParser('grams', schema)

    q = parser.parse(u("Hello Tom"))
    assert q.__class__ == query.And
    assert q[0].__class__ == query.Or
    assert q[1].__class__ == query.Term
    assert q[0][0].text == "hell"
    assert q[0][1].text == "ello"
    assert q[1].text == "tom"


def test_multitoken_default():
    textfield = fields.TEXT()
    assert textfield.multitoken_query == "default"
    schema = fields.Schema(text=textfield)
    parser = default.QueryParser('text', schema)
    qstring = u("chaw-bacon")

    texts = list(schema["text"].process_text(qstring))
    assert texts == ["chaw", "bacon"]

    q = parser.parse(qstring)
    assert q.__class__ == query.And
    assert len(q) == 2
    assert q[0].__class__ == query.Term
    assert q[0].text == "chaw"
    assert q[1].__class__ == query.Term
    assert q[1].text == "bacon"


def test_multitoken_or():
    textfield = fields.TEXT()
    textfield.multitoken_query = "or"
    schema = fields.Schema(text=textfield)
    parser = default.QueryParser('text', schema)
    qstring = u("chaw-bacon")

    texts = list(schema["text"].process_text(qstring))
    assert texts == ["chaw", "bacon"]

    q = parser.parse(qstring)
    assert q.__class__ == query.Or
    assert len(q) == 2
    assert q[0].__class__ == query.Term
    assert q[0].text == "chaw"
    assert q[1].__class__ == query.Term
    assert q[1].text == "bacon"


def test_multitoken_phrase():
    textfield = fields.TEXT()
    textfield.multitoken_query = "phrase"
    schema = fields.Schema(text=textfield)
    parser = default.QueryParser("text", schema)
    qstring = u("chaw-bacon")

    texts = list(schema["text"].process_text(qstring))
    assert texts == ["chaw", "bacon"]

    q = parser.parse(qstring)
    assert q.__class__ == query.Phrase


def test_singlequote_multitoken():
    schema = fields.Schema(text=fields.TEXT(multitoken_query="or"))
    parser = default.QueryParser("text", schema)
    q = parser.parse(u("foo bar"))
    assert q.__unicode__() == "(text:foo AND text:bar)"

    q = parser.parse(u("'foo bar'"))  # single quotes
    assert q.__unicode__() == "(text:foo OR text:bar)"


def test_operator_queries():
    qp = default.QueryParser("f", None)

    q = qp.parse("a AND b OR c AND d")
    assert text_type(q) == "((f:a AND f:b) OR (f:c AND f:d))"

    q = qp.parse("a OR b OR c OR d")
    assert text_type(q) == "(f:a OR f:b OR f:c OR f:d)"

    q = qp.parse("a ANDMAYBE b ANDNOT c REQUIRE d")
    assert text_type(q) == "((f:a ANDMAYBE (f:b ANDNOT f:c)) REQUIRE f:d)"


#def test_associativity():
#    left_andmaybe = (syntax.InfixOperator("ANDMAYBE", syntax.AndMaybeGroup, True), 0)
#    right_andmaybe = (syntax.InfixOperator("ANDMAYBE", syntax.AndMaybeGroup, False), 0)
#    not_ = (syntax.PrefixOperator("NOT", syntax.NotGroup), 0)
#
#    def make_parser(*ops):
#        parser = default.QueryParser("f", None)
#        parser.replace_plugin(plugins.CompoundsPlugin(ops, clean=True))
#        return parser
#
#    p = make_parser(left_andmaybe)
#    q = p.parse("a ANDMAYBE b ANDMAYBE c ANDMAYBE d")
#    assert text_type(q), "(((f:a ANDMAYBE f:b) ANDMAYBE f:c) ANDMAYBE f:d)")
#
#    p = make_parser(right_andmaybe)
#    q = p.parse("a ANDMAYBE b ANDMAYBE c ANDMAYBE d")
#    assert text_type(q), "(f:a ANDMAYBE (f:b ANDMAYBE (f:c ANDMAYBE f:d)))")
#
#    p = make_parser(not_)
#    q = p.parse("a NOT b NOT c NOT d", normalize=False)
#    assert text_type(q), "(f:a AND NOT f:b AND NOT f:c AND NOT f:d)")
#
#    p = make_parser(left_andmaybe)
#    q = p.parse("(a ANDMAYBE b) ANDMAYBE (c ANDMAYBE d)")
#    assert text_type(q), "((f:a ANDMAYBE f:b) ANDMAYBE (f:c ANDMAYBE f:d))")
#
#    p = make_parser(right_andmaybe)
#    q = p.parse("(a ANDMAYBE b) ANDMAYBE (c ANDMAYBE d)")
#    assert text_type(q), "((f:a ANDMAYBE f:b) ANDMAYBE (f:c ANDMAYBE f:d))")


def test_not_assoc():
    qp = default.QueryParser("text", None)
    q = qp.parse(u("a AND NOT b OR c"))
    assert text_type(q) == "((text:a AND NOT text:b) OR text:c)"

    qp = default.QueryParser("text", None)
    q = qp.parse(u("a NOT (b OR c)"))
    assert text_type(q) == "(text:a AND NOT (text:b OR text:c))"


def test_fieldname_space():
    qp = default.QueryParser("a", None)
    q = qp.parse("Man Ray: a retrospective")
    assert text_type(q) == "(a:Man AND a:Ray: AND a:a AND a:retrospective)"


def test_fieldname_fieldname():
    qp = default.QueryParser("a", None)
    q = qp.parse("a:b:")
    assert q == query.Term("a", "b:")


def test_paren_fieldname():
    schema = fields.Schema(kind=fields.ID, content=fields.TEXT)

    qp = default.QueryParser("content", schema)
    q = qp.parse(u("(kind:1d565 OR kind:7c584) AND (stuff)"))
    assert text_type(q) == "((kind:1d565 OR kind:7c584) AND content:stuff)"

    q = qp.parse(u("kind:(1d565 OR 7c584) AND (stuff)"))
    assert text_type(q) == "((kind:1d565 OR kind:7c584) AND content:stuff)"


def test_star_paren():
    qp = default.QueryParser("content", None)
    q = qp.parse(u("(*john*) AND (title:blog)"))

    assert q.__class__ == query.And
    assert q[0].__class__ == query.Wildcard
    assert q[1].__class__ == query.Term
    assert q[0].fieldname == "content"
    assert q[1].fieldname == "title"
    assert q[0].text == "*john*"
    assert q[1].text == "blog"


def test_dash():
    ana = analysis.StandardAnalyzer("[^ \t\r\n()*?]+")
    schema = fields.Schema(title=fields.TEXT(analyzer=ana),
                           text=fields.TEXT(analyzer=ana),
                           time=fields.ID)
    qtext = u("*Ben-Hayden*")

    qp = default.QueryParser("text", schema)
    q = qp.parse(qtext)
    assert q.__class__ == query.Wildcard
    assert q.fieldname == "text"
    assert q.text == "*ben-hayden*"

    qp = default.MultifieldParser(["title", "text", "time"], schema)
    q = qp.parse(qtext)
    assert q.__unicode__() == "(title:*ben-hayden* OR text:*ben-hayden* OR time:*Ben-Hayden*)"


def test_bool_True():
    schema = fields.Schema(text=fields.TEXT, bool=fields.BOOLEAN)
    qp = default.QueryParser("text", schema)
    q = qp.parse("bool:True")
    assert q.__class__ == query.Term
    assert q.fieldname == "bool"
    assert q.text is True


def test_not_order():
    schema = fields.Schema(id=fields.STORED,
                           count=fields.KEYWORD(lowercase=True),
                           cats=fields.KEYWORD(lowercase=True))
    qp = default.QueryParser("count", schema)

    q1 = qp.parse(u("(NOT (count:0) AND cats:1)"))
    assert q1.__class__ == query.And
    assert q1[0].__class__ == query.Not
    assert q1[1].__class__ == query.Term
    assert q1.__unicode__() == '(NOT count:0 AND cats:1)'

    q2 = qp.parse(u("(cats:1 AND NOT (count:0))"))
    assert q2.__class__ == query.And
    assert q2[0].__class__ == query.Term
    assert q2[1].__class__ == query.Not
    assert q2.__unicode__() == '(cats:1 AND NOT count:0)'


def test_spacespace_and():
    qp = default.QueryParser("f", None)
    # one blank before/after AND
    q = qp.parse("A AND B")
    assert q.__class__ == query.And
    assert len(q) == 2
    assert q[0] == query.Term("f", "A")
    assert q[1] == query.Term("f", "B")

    # two blanks before AND
    q = qp.parse("A  AND B")
    assert q.__class__ == query.And
    assert len(q) == 2
    assert q[0] == query.Term("f", "A")
    assert q[1] == query.Term("f", "B")


def test_unicode_num():
    schema = fields.Schema(num=fields.NUMERIC)
    parser = default.QueryParser(u("num"), schema=schema)
    q = parser.parse(u("num:1"))

    _ = text_type(q)


def test_phrase_andmaybe():
    qp = default.QueryParser("f", None)

    q = qp.parse(u('Dahmen ANDMAYBE "Besov Spaces"'))
    assert isinstance(q, query.AndMaybe)
    assert q[0] == query.Term("f", u("Dahmen"))
    assert q[1] == query.Phrase("f", [u("Besov"), u("Spaces")])


def test_phrase_boost():
    qp = default.QueryParser("f", None)
    q = qp.parse(u('Dahmen ANDMAYBE "Besov Spaces"^9'))
    assert isinstance(q, query.AndMaybe)
    assert q[0] == query.Term("f", u("Dahmen"))
    assert q[1] == query.Phrase("f", [u("Besov"), u("Spaces")], boost=9)


def test_andmaybe_none():
    schema = fields.Schema(f=fields.TEXT, year=fields.NUMERIC)
    qp = default.QueryParser("f", schema)
    _ = qp.parse(u("Dahmen ANDMAYBE @year:[2000 TO]"))


def test_quoted_prefix():
    qp = default.QueryParser("f", None)

    expr = r"(^|(?<=[ (]))(?P<text>\w+|[*]):"
    qp.replace_plugin(plugins.FieldsPlugin(expr))

    q = qp.parse(u('foo url:http://apple.com:8080/bar* baz'))
    assert isinstance(q, query.And)
    assert q[0] == query.Term("f", "foo")
    assert q[1] == query.Prefix("url", "http://apple.com:8080/bar")
    assert q[2] == query.Term("f", "baz")
    assert len(q) == 3
