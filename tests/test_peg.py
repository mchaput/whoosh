import pytest

from whoosh.parsing import peg


def test_nomatch():
    e = peg.NoMatch()
    with pytest.raises(peg.ParseException):
        e.parse_string("foo")
    assert not e.matches("foo")


def test_literal():
    e = peg.Str("foo")
    at, v = e.parse("foo", 0, peg.Context(e))
    assert at == 3
    assert v == "foo"
    assert e.matches("barfoo", 3)


def test_ignore_case():
    e = peg.Str("foo", ignore_case=True)
    at, v = e.parse("FoOlS!", 0, peg.Context(e))
    assert at == 3
    assert v == "FoO"
    assert e.matches("barFOo", 3)


def test_regex():
    import re

    pat = "(?P<nums>[0-9]+)(?P<lets>[A-Fa-f]+)"

    e = peg.Regex(pat, re.DOTALL)
    ctx = peg.Context(e)
    at, v = e.parse("06fz", 0, ctx)
    assert at == 3
    assert v == "06f"
    assert ctx["nums"] == "06"
    assert ctx["lets"] == "f"

    e = peg.Regex(pat)
    ctx = peg.Context(e)
    at, v = e.parse("06fz", 0,ctx)
    assert at == 3
    assert v == "06f"
    assert ctx["nums"] == "06"
    assert ctx["lets"] == "f"


def test_quoted_string():
    e = peg.QuotedString('"', esc_char="\\")
    at, v = e.parse('Are you "really \\"really\\" into quoting?" she said.',
                    8, peg.Context(e))
    assert v == 'really \"really\" into quoting?'
    assert at == 41


def test_whitespace():
    e = peg.ws
    at, v = e.parse("    \tfoo", 0, peg.Context(e))
    assert at == 5
    assert v == "    \t"


def test_stringstart():
    e = peg.StringStart()
    assert e.matches("foo")
    assert not e.matches("foo", 1)


def test_stringend():
    e = peg.StringEnd()
    assert e.matches("foo", 3)
    assert not e.matches("foo", 2)


def test_wordstart():
    e = peg.WordStart()
    s = u"The éstuary"
    assert e.matches(s, 0)
    assert not e.matches(s, 1)
    assert e.matches(s, 4)
    assert not e.matches(s, len(s))


def test_wordend():
    e = peg.WordEnd()
    s = u"Somé Té"
    assert not e.matches(s, 0)
    assert e.matches(s, 4)
    assert not e.matches(s, 5)
    assert e.matches(s, len(s))


def test_seq():
    e = peg.Str("foo") + peg.Str("bar") + peg.Str("baz")
    assert isinstance(e, peg.Seq)
    at, v = e.parse("foobarbaz", 0, peg.Context(e))
    assert at == 9
    assert v == "baz"

    assert not e.matches("foobarbong")


def test_collect():
    e = peg.Collect([peg.Str("foo"), peg.Str("bar"), peg.Str("baz")])
    assert isinstance(e, peg.Collect)
    at, v = e.parse("foobarbaz", 0, peg.Context(e))
    assert at == 9
    assert v == ["foo", "bar", "baz"]

    assert not e.matches("foobarbong")


def test_or():
    e = peg.Str("foo") | peg.Str("ba") | peg.Str("B")
    assert isinstance(e, peg.Or)
    s = "foobarBaz"

    at, v = e.parse(s, 0, peg.Context(e))
    assert at == 3
    assert v == "foo"

    at, v = e.parse(s, 3, peg.Context(e))
    assert at == 5
    assert v == "ba"

    at, v = e.parse(s, 6, peg.Context(e))
    assert at == 7
    assert v == "B"


def test_bag():
    e = peg.Bag([peg.Str("foo"), peg.Str("bar"), peg.Str("baz")],
                seperator=peg.ws)
    at, v = e.parse("foo bar baz", 0, peg.Context(e))
    assert at == 11
    assert v == ["foo", "bar", "baz"]

    at, v = e.parse("baz bar foo", 0, peg.Context(e))
    assert at == 11
    assert v == ["baz", "bar", "foo"]

    at, v = e.parse("baz bar", 0, peg.Context(e))
    assert at == 7
    assert v == ["baz", "bar"]

    at, v = e.parse("baz bar quux", 0, peg.Context(e))
    assert at == 8
    assert v == ["baz", "bar"]

    at, v = e.parse("foo", 0, peg.Context(e))
    assert at == 3
    assert v == ["foo"]


def test_names():
    e = peg.Str("foo").set("lit")
    assert e.name == "lit"
    ctx = peg.Context(e)
    at, r = e.parse("foobarbaz", 0, ctx)
    assert at == 3
    assert ctx["lit"] == "foo"


def test_action():
    e = peg.Regex("[a-z]+").set("name") + peg.Do(lambda ctx: ctx["name"][-1])
    at, r = e.parse("fooBar", 0, peg.Context(e))
    assert at == 3
    assert r == "o"

    at = e.try_parse("fooBar", 0, peg.Context(e))
    assert at == 3

    e = peg.Regex("(?P<lhs>[a-z]+)[=](?P<rhs>[a-z]+)")
    e += peg.Do(lambda ctx: '%s%s' % (ctx["rhs"], ctx["lhs"]))
    at, r = e.parse("foo=bar", 0, peg.Context(e))
    assert at == 7
    assert r == "barfoo"

    context = peg.Context(e)
    context.env["x"] = 10
    e = peg.Regex("[0-9]+").set("num")
    e += peg.Do(lambda ctx: int(ctx["num"]) - ctx["x"])
    assert e.parse_string("100", context=context) == 90
    assert e.parse_string("15", context=context) == 5


def test_if():
    e = (
        peg.Str("foo") +
        peg.Regex("[0-9]+").set("num") +
        peg.If(lambda ctx: int(ctx["num"]) > 500) +
        peg.Str("bar")
    )
    assert not e.matches("foo1bar")
    assert not e.matches("foo100bar")
    assert not e.matches("foo500bar")
    assert e.matches("foo600bar")
    assert e.matches("foo1000bar")


def test_not():
    e = ~peg.Str("foo")
    assert isinstance(e, peg.Not)
    assert e.matches("bar")
    assert not e.matches("foo")

    at, r = e.parse("bar", 0, peg.Context(e))
    assert at == 0


def test_peek():
    e = peg.Peek(peg.Str("foo"))
    assert isinstance(e, peg.Peek)
    assert not e.matches("bar", 0)
    assert e.matches("foo", 0)

    at, r = e.parse("foo", 0, peg.Context(e))
    assert at == 0


def test_repeat():
    e = peg.Repeat(peg.Str("a"), 2, 5) + peg.Str("b")
    assert not e.matches("ab")
    assert e.matches("aab")
    assert e.matches("aaab")
    assert e.matches("aaaab")
    assert e.matches("aaaaab")
    assert not e.matches("aaaaaab")


def test_oneormore():
    e = peg.Str("a").plus()
    assert isinstance(e, peg.OneOrMore)
    assert not e.matches("b")
    assert e.matches("ab")
    assert e.matches("aab")
    assert e.matches("aaab")


def test_zeroormore():
    e = peg.Str("a").star()
    assert isinstance(e, peg.ZeroOrMore)
    assert e.matches("b")
    assert e.matches("ab")
    assert e.matches("aab")
    assert e.matches("aaab")


def test_optional():
    e = peg.Str("a").opt() + peg.Str("b")
    assert not e.matches("c")
    assert e.matches("b")
    assert e.matches("ab")
    assert not e.matches("aab")


def test_until():
    e = (
        peg.Str("[") +
        peg.Until(peg.Str("]"))("inner") +
        peg.Str("]")
    )
    assert not e.matches("foobar")
    at, v = e.parse("[foobar]", 0, peg.Context(e))
    assert at == 8

    e = peg.Collect([
        peg.Str("["),
        peg.Until(peg.Str("]"), include=True)("inner"),
    ])
    assert not e.matches("foobar")
    at, r = e.parse("[foobar]", 0, peg.Context(e))
    assert at == 8
    assert r[0] == "["
    assert r[1][0] == "foobar"
    assert r[1][1] == "]"


def test_forward():
    fwd = peg.Forward()
    e = fwd + peg.Str("bar")
    fwd.assign(peg.Str("foo"))

    assert e.matches("foobar")
    assert not e.matches("foo")
    assert not e.matches("bar")


# def test_combine():
#     e = peg.Combine(peg.Collect([
#         peg.Str("["),
#         peg.Str("^").opt(),
#         peg.Str("a").plus(),
#         peg.Str("]"),
#     ]))
#     at, v = e.parse("[^aaaa]", 0, peg.Context(e))
#     assert at == 7
#     assert v == "[^aaaa]"


# def test_call():
#     ec = peg.Call("lets", may_be_empty=False)
#     e1 = peg.Str("foo")("f") + ec
#     e2 = peg.Regex("[a-z]+", may_be_empty=False)("lets")
#     e3 = peg.Str("_")("u")
#     e = peg.Or([e1, e2, e3]).plus()
#     context = peg.Context(e)
#     e.register(context)
#     assert "f" in context.lookup
#     assert "lets" in context.lookup
#     assert "u" in context.lookup
#
#     assert ec.matches("abc", context=context)
#     assert e1.matches("fooabc", context=context)
#
#     assert e.matches("fooabc__xyz", context=context)


def test_get():
    e = (
        peg.Str("[") +
        peg.Regex("[A-Za-z]+").set("id") +
        peg.Str("]") +
        peg.Get("id")
    )
    v = e.parse_string("[zoom]")
    assert v == "zoom"

    e = (
        peg.Str("[") +
        peg.Regex("[A-Za-z]+")("id") +
        peg.Str("]") +
        peg.Get("ident")
    )
    with pytest.raises(peg.FatalError):
        v = e.parse_string("[zoom]")


def test_recursive():
    e = peg.Forward()
    num = peg.Regex("[0-9]+")
    primary = num | e
    e.assign(primary | (primary + e))

    with pytest.raises(peg.RecursiveGrammarError):
        e.validate()


def test_complex():
    word = peg.Regex("[^ \t\r\n()]+", may_be_empty=False)
    sq = peg.QuotedString("'", esc_char="\\")
    ex = sq | word
    assert not ex.may_be_empty

    exws = peg.ws.opt().hide() + ex.set("ex") + peg.ws.opt().hide()

    exs = exws.star()
    v = exs.parse_string("foo fi 'fooly food' fum")
    assert v == ["foo", "fi", "fooly food", "fum"]

    expr = peg.Forward(may_be_empty=False)
    group = peg.Str("(").hide() + expr.star() + peg.Str(")").hide()
    expr.assign(peg.ws.opt() + (group | exws))
    expr.validate()

    fieldspec = (
        peg.Regex("[^: \t\r\n]+", may_be_empty=False) +
        peg.Str(":").hide()
    )
    fielded = (
        peg.ws.opt() +
        fieldspec.set("fname") +
        expr.set("expr") +
        peg.ws.opt() +
        peg.Do(lambda ctx: (ctx["fname"], ctx["expr"]))
    )

    v = fielded.parse_string("    foo:bar")
    assert v == ("foo", "bar")

    gram = (fielded | expr).star()
    ctx = peg.Context(gram, debug=True)
    r = gram.parse_string("foo (fi fo) bar:fum ", ctx)
    assert r == ["foo", ["fi", "fo"], ("bar", "fum")]


def test_find_and_replace():
    word = peg.Regex("[^ \t\r\n]+", may_be_empty=False)("word")
    e = (peg.ws.hide() + word + peg.ws.hide()).plus()
    assert e.matches(" foobar  ")
    assert e.matches(" zoo   topia ")
    assert not e.matches("  ")
    assert e.find("word") is word

    e2 = e.replace("word", peg.Regex("[A-F]+"))
    assert not e2.matches(" foobar  ")
    assert e2.matches(" ABC  DEF")
    assert not e2.matches("  ")


def test_add_alt():
    lets = peg.Regex("[a-z]+", may_be_empty=False)("lets")


def test_brackets():
    e = (
        peg.Str("(") +
        peg.OneOrMore(
            peg.Not(peg.Peek(peg.Str(")"))) + peg.Regex(".", may_be_empty=False)
        ).set("items") +
        peg.Str(")") +
        peg.Get("items")
    )
    r = e.parse_string("(abc)")
    assert r == ["a", "b", "c"]
    r = e.parse_string("(abcdef)")
    assert r == ["a", "b", "c", "d", "e", "f"]
    assert not e.matches("abc")


def test_hidden():
    e = (
        peg.Str("a") +
        peg.Str("b").hide()
    )
    r = e.parse_string("ab")
    assert r == "a"


def test_stringuntil():
    e = (
        peg.Str("[") +
        peg.StringUntil("]").set("c") +
        peg.Str("]") +
        peg.Get("c")
    )
    r = e.parse_string("[abc]")
    assert r == "abc"

    e = peg.StringUntil(peg.Str(" "), esc_char="\\")
    r = e.parse_string("abc\\ def ghi")
    assert r == "abc def"


def test_stringuntil_esc():
    e = (
        peg.Str("[") +
        peg.StringUntil(peg.Str("]")).set("c") +
        peg.Str("]") +
        peg.Get("c")
    )
    r = e.parse_string("[a\\]b\\]c]")
    assert r == "a]b]c"



