import pytest

from whoosh import fields, query
from whoosh.parsing import peg
from whoosh.parsing import parsing, peg
from whoosh.parsing import plugins as plugs


default_schema = fields.Schema(text=fields.Text, title=fields.Text,
                               nums=fields.Numeric, times=fields.DateTime)
ws = plugs.WhitespacePlugin.ws
Vgroup = plugs.Vgroup


def test_get_plugin():
    p = parsing.QueryParser("content")
    ep = p.every_plugin
    assert isinstance(ep, plugs.EveryPlugin)


def test_empty_string():
    qst = ""
    p = parsing.QueryParser("content")
    qs = p.parse_to_list(qst)
    assert not qs

    assert isinstance(p.parse(qst), query.NullQuery)


def test_terms():
    # import logging
    # logging.basicConfig(level=logging.DEBUG)

    p = parsing.QueryParser("content")
    qs = p.parse(u"foo bar baz")
    assert qs == query.And([
        query.Term("content", "foo"),
        query.Term("content", "bar"),
        query.Term("content", "baz")
    ])


def test_esc():
    # import logging
    # logging.basicConfig(level=logging.DEBUG)

    p = parsing.QueryParser("content")
    qs = p.parse(u"foo\\ bar baz", normalize=False)
    assert qs == query.And([
        query.And([
            query.Term("content", "foo"),
            query.Term("content", "bar"),
        ]),
        query.Term("content", "baz")
    ])


def test_schema_terms():
    # import logging
    # logging.basicConfig(level=logging.DEBUG)

    p = parsing.QueryParser("text", default_schema)
    q = p.parse(u"FOO BAR BAZ!")
    assert q == query.And([
        query.Term("text", "foo"),
        query.Term("text", "bar"),
        query.Term("text", "baz")
    ])


def test_grouping():
    qst = "aa bb cc"
    p = parsing.QueryParser("t")
    qs = p.parse(qst)
    assert qs == query.And([
        query.Term("t", "aa"),
        query.Term("t", "bb"),
        query.Term("t", "cc")
    ])

    p = parsing.QueryParser("t", group=query.Or)
    qs = p.parse(qst)
    assert qs == query.Or([
        query.Term("t", "aa"),
        query.Term("t", "bb"),
        query.Term("t", "cc")
    ])


def test_empty_group():
    p = parsing.QueryParser("t")
    qs = p.parse("()")
    assert isinstance(qs, query.NullQuery)


def test_field_spec():
    qst = u"foo:bar"
    p = parsing.QueryParser("content")
    qs = p.parse_to_list(qst)
    assert qs == [query.Term("foo", "bar")]


def test_bare_field():
    qst = u"foo:"
    p = parsing.QueryParser("text", default_schema)
    qs = p.parse_to_list(qst)
    assert qs == [query.Term(None, "foo:")]


def test_field_ws():
    qst = u"foo: bar"
    p = parsing.QueryParser("text")
    qs = p.parse_to_list(qst)
    assert qs == [query.Term("foo", "bar")]


def test_mixed_fielded():
    # import logging
    # logging.basicConfig(level=logging.DEBUG)

    qp = parsing.QueryParser("text", default_schema)
    qs = qp.parse("alfa title:25 bravo")
    assert qs == query.And([
        query.Term("text", "alfa"),
        query.Term("title", "25"),
        query.Term("text", "bravo")
    ])


def test_group1():
    p = parsing.QueryParser("text")
    assert p.parse_to_list("(aaa)") == [
        Vgroup([query.Term(None, "aaa")])
    ]


def test_group2():
    p = parsing.QueryParser("text")
    assert p.parse_to_list("(aa bb)") == [
        Vgroup([query.Term(None, "aa"), ws, query.Term(None, "bb")])
    ]


def test_group3():
    p = parsing.QueryParser("text")
    assert p.parse_to_list("(aa bb cc)") == [
        Vgroup([
            query.Term(None, "aa"), ws,
            query.Term(None, "bb"), ws,
            query.Term(None, "cc")
        ])
    ]


def test_groups():
    qst = "(alfa OR bravo) charlie (delta OR echo OR foxtrot) golf"
    p = parsing.QueryParser("text")
    qs = p.parse(qst)
    # query.dump(qs)
    # print(repr(qs))
    assert qs == query.And([
        query.Or([
            query.Term("text", "alfa"),
            query.Term("text", "bravo")
        ]),
        query.Term("text", "charlie"),
        query.Or([
            query.Term("text", "delta"),
            query.Term("text", "echo"),
            query.Term("text", "foxtrot")
        ]),
        query.Term("text", "golf")
    ])


def test_simple_ops1():
    qst = "alfa bravo OR charlie"
    p = parsing.QueryParser("t")
    qs = p.parse(qst)
    # query.dump(qs)
    assert qs == query.Or([
        query.And([
            query.Term("t", "alfa"),
            query.Term("t", "bravo")
        ]),
        query.Term("t", "charlie")
    ])


def test_simple_ops2():
    qst = "alfa OR bravo OR charlie"
    p = parsing.QueryParser("t")
    qs = p.parse(qst)
    # query.dump(qs)
    assert qs == query.Or([
        query.Term("t", "alfa"),
        query.Term("t", "bravo"),
        query.Term("t", "charlie")
    ])


def test_simple_ops3():
    qst = "alfa OR NOT bravo OR charlie"
    p = parsing.QueryParser("t")
    qs = p.parse(qst)
    # query.dump(qs)
    assert qs == query.Or([
        query.Term("t", "alfa"),
        query.Not(query.Term("t", "bravo")),
        query.Term("t", "charlie")
    ])


def test_op_binding():
    qst = "alfa OR bravo ANDNOT charlie OR delta"
    p = parsing.QueryParser("t")
    qs = p.parse(qst)
    # query.dump(qs)
    assert query.Or([
        query.Term("t", "alfa"),
        query.AndNot(
            query.Term("t", "bravo"),
            query.Term("t", "charlie")
        ),
        query.Term("t", "delta")
    ])


def test_nested_groups():
    qst = "alfa OR (bravo AND (charlie OR delta) AND echo) OR foxtrot"
    p = parsing.QueryParser("t")
    qs = p.parse(qst)
    # query.dump(qs)
    assert qs == query.Or([
        query.Term("t", "alfa"),
        query.And([
            query.Term("t", "bravo"),
            query.Or([
                query.Term("t", "charlie"),
                query.Term("t", "delta")
            ]),
            query.Term("t", "echo")
        ]),
        query.Term("t", "foxtrot")
    ])


def test_fielded_group():
    qst = "foo:(bar baz)"
    p = parsing.QueryParser("text")
    qs = p.parse(qst)
    assert qs == query.And([query.Term("foo", "bar"), query.Term("foo", "baz")])


def test_ws():
    qst = "        "
    p = parsing.QueryParser("content")
    qs = p.parse_to_list(qst)
    assert qs
    assert qs[0] is ws


def test_single_quotes():
    import logging
    logging.basicConfig(level=logging.DEBUG)

    qst = "aa '---' bb"
    p = parsing.QueryParser("text")
    qs = p.parse(qst)
    assert qs == query.And([
        query.Term("text", "aa"),
        query.Term("text", "---"),
        query.Term("text", "bb")
    ])


def test_prefix():
    # import logging
    # logging.basicConfig(level=logging.DEBUG)

    p = parsing.QueryParser("text", default_schema)
    ctx = p.context("text")
    syn = list(plugs.PrefixPlugin().syntaxes(p))[0][0]  # type: peg.Expr
    qst = "alfa*"
    at, value = syn.parse(qst, 0, ctx)
    assert value == query.Prefix(None, "alfa")
    qst = "alfa* bravo"
    at, value = syn.parse(qst, 0, ctx)
    assert value == query.Prefix(None, "alfa")
    qst = "b* bravo"
    at, value = syn.parse(qst, 0, ctx)
    assert value == query.Prefix(None, "b")

    p.remove_plugin_class(plugs.WildcardPlugin)
    p.add_plugin(plugs.PrefixPlugin())

    qst = "aa b* cc"
    qs = p.parse(qst, normalize=False)
    # query.dump(qs)
    assert qs == query.And([
        query.Term("text", "aa"),
        query.Prefix("text", "b"),
        query.Term("text", "cc")
    ])

    qst = "aa bb cc*"
    qs = p.parse(qst, normalize=False)
    # query.dump(qs)
    assert qs == query.And([
        query.Term("text", "aa"),
        query.Term("text", "bb"),
        query.Prefix("text", "cc")
    ])


def test_wildcards():
    # import logging
    # logging.basicConfig(level=logging.DEBUG)

    p = parsing.QueryParser("text", default_schema)
    wcp = plugs.WildcardPlugin()
    syn = list(wcp.syntaxes(p))[0][0]
    at, value = syn.parse("b*d?e", 0, p.context())
    assert at == 5
    assert value == query.Wildcard(None, "b*d?e")

    qs = p.parse("aa b*d?e cc", normalize=False)
    # query.dump(qs)
    assert qs == query.And([
        query.Term("text", "aa"),
        query.Wildcard("text", "b*d?e"),
        query.Term("text", "cc")
    ])


def test_wildcards2():
    qst = "a???b"
    p = parsing.QueryParser("text", default_schema)
    qs = p.parse(qst)
    assert qs == query.Wildcard("text", "a???b")


def test_regex():
    qst = 'aa r"(a|b)+" cc'
    p = parsing.QueryParser("text")
    p.add_plugin(plugs.RegexPlugin())
    qs = p.parse(qst, normalize=False)
    assert qs == query.And([
        query.Term("text", "aa"),
        query.Regex("text", "(a|b)+"),
        query.Term("text", "cc")
    ])


def test_fuzzy():
    import logging
    logging.basicConfig(level=logging.DEBUG)

    qst = "DESPIT~2"
    p = parsing.QueryParser("text", default_schema)
    qs = p.parse(qst, normalize=False)
    assert isinstance(qs, query.And)
    q = qs[0]
    assert isinstance(q, query.FuzzyTerm)
    assert q.field() == "text"
    assert q.query_text() == "despit"
    assert q.maxdist == 2


def test_phrase():
    p = parsing.QueryParser("text", default_schema)
    qst = 'Really "big deal" huh'
    qs = p.parse(qst)
    assert qs == query.And([
        query.Term("text", 'really'),
        query.Phrase("text", ['big', 'deal']),
        query.Term("text", 'huh')
    ])

    qs = p.parse(qst)
    assert qs == query.And([query.Term("text", "really"),
                            query.Phrase("text", ["big", "deal"]),
                            query.Term("text", "huh")])


def test_phrase_slop():
    qst = 'alfa "bravo charlie"~3 delta'
    p = parsing.QueryParser("text", default_schema)
    qs = p.parse(qst)
    # query.dump(qs)
    assert qs == query.And([
        query.Term("text", "alfa"),
        query.Phrase("text", ["bravo", "charlie"], slop=3),
        query.Term("text", "delta")
    ])


def test_sequence():
    # import logging
    # logging.basicConfig(level=logging.DEBUG)

    qst = "aaa <b* foo c*d?e??f*> ddd"
    p = parsing.QueryParser("text", default_schema)
    sp = plugs.SequencePlugin(start="<", end=">")
    p.add_plugin(sp)

    ctx = p.context("text")
    syn = list(sp.syntaxes(p))[0][0]
    at, value = syn.parse(qst, 4, ctx)
    # query.dump(value)
    assert value == query.Sequence([
        query.Wildcard(None, "b*"),
        ws,
        query.Term(None, "foo"),
        ws,
        query.Wildcard(None, "c*d?e??f*")
    ])

    qs = p.parse(qst, normalize=False)
    # query.dump(qs)
    assert qs == query.And([
        query.Term("text", "aaa"),
        query.Sequence([
            query.Wildcard("text", "b*"),
            query.Term("text", "foo"),
            query.Wildcard("text", "c*d?e??f*")
        ]),
        query.Term("text", "ddd")
    ])


def test_range():
    # import logging
    # logging.basicConfig(level=logging.DEBUG)

    p = parsing.QueryParser("text", default_schema)

    qs = p.parse("aa [b to c] dd", normalize=False)
    # query.dump(qs)
    assert qs == query.And([
        query.Term("text", "aa"),
        query.Range("text", "b", "c", False, False),
        query.Term("text", "dd")
    ])

    qs = p.parse("aa [b to c} dd", normalize=False)
    # query.dump(qs)
    assert qs == query.And([
        query.Term("text", "aa"),
        query.Range("text", "b", "c", False, True),
        query.Term("text", "dd")
    ])

    qs = p.parse("aa [to c} dd", normalize=False)
    assert qs == query.And([
        query.Term("text", "aa"),
        query.Range("text", None, "c", False, True),
        query.Term("text", "dd")
    ])

    qs = p.parse("aa [b to} dd", normalize=False)
    assert qs == query.And([
        query.Term("text", "aa"),
        query.Range("text", "b", None, False, True),
        query.Term("text", "dd")
    ])

    qs = p.parse("aa {b to] dd", normalize=False)
    assert qs == query.And([
        query.Term("text", "aa"),
        query.Range("text", "b", None, True, False),
        query.Term("text", "dd")
    ])


def test_weird_range_values():
    p = parsing.QueryParser("text", default_schema)

    qs = p.parse("['to' to ']']", normalize=False)
    assert qs == query.Range("text", "to", "]", False, False)

    qs = p.parse("[to to TO]", normalize=False)
    assert qs == query.Range("text", "to", "to", False, False)

    qs = p.parse("[to to]", normalize=False)
    assert qs == query.Range("text", None, "to", False, False)

    qs = p.parse("[\\to to]", normalize=False)
    assert qs == query.Range("text", "to", None, False, False)

    qs = p.parse("[\\to to \\]]", normalize=False)
    assert qs == query.Range("text", "to", "]", False, False)

    qs = p.parse("[aa bb to cc dd]", normalize=False)
    assert qs == query.Range("text", "aa bb", "cc dd", False, False)

    qs = p.parse("['a b' to c\\ d]", normalize=False)
    assert qs == query.Range("text", "a b", "c d", False, False)

    qs = p.parse("[AND to OR]", normalize=False)
    assert qs == query.Range("text", "and", "or", False, False)

    qst = '["foo bar" to "baz"]'
    qs = p.parse(qst, normalize=False)
    assert qs == query.Range("text", "foo bar", "baz", False, False)


def test_num_range():
    p = parsing.QueryParser("text", default_schema)
    qst = "nums:[100 to 200]"
    qs = p.parse(qst, normalize=False)
    # query.dump(qs)
    assert qs == query.NumericRange('nums', 100, 200, False, False)


def test_boost():
    p = parsing.QueryParser("text", default_schema)
    qst = "foo bar^3 baz"
    qs = p.parse(qst, normalize=False)
    assert qs == query.And([
        query.Term("text", "foo"),
        query.Term("text", "bar", boost=3.0),
        query.Term("text", "baz")
    ])


def test_field_alias():
    qst = "bar:alfa baz:beta"

    fa = plugs.FieldAliasPlugin({
        "foo": ["bar", "baz"],
    })
    p = parsing.QueryParser("content")
    p.add_plugin(fa)

    qs = p.parse_to_list(qst)
    assert qs == [query.Term("foo", "alfa"), ws, query.Term("foo", "beta")]


def test_copy_field():
    p = parsing.QueryParser("text")
    p.add_plugin(plugs.CopyFieldPlugin({"text": "title"}))
    qs = p.parse("foo bar")
    assert qs == query.And([
        query.Term("title", "foo"),
        query.Term("text", "foo"),
        query.Term("title", "bar"),
        query.Term("text", "bar")
    ])


def test_copy_field_grouping():
    p = parsing.QueryParser("text")
    cfp = plugs.CopyFieldPlugin({"text": "title"}, group=query.Or)
    assert cfp.group is not None
    p.add_plugin(cfp)
    qs = p.parse("foo bar")
    assert qs == query.And([
        query.Or([
            query.Term("text", "foo"),
            query.Term("title", "foo"),
        ]),
        query.Or([
            query.Term("text", "bar"),
            query.Term("title", "bar")
        ])
    ])


def test_multifield():
    p = parsing.QueryParser("text")
    p.add_plugin(plugs.MultifieldPlugin(["nums", "title"]))
    qs = p.parse("foo bar")
    # query.dump(qs)
    assert qs == query.And([
        query.Or([
            query.Term("nums", "foo"),
            query.Term("title", "foo"),
        ]),
        query.Or([
            query.Term("nums", "bar"),
            query.Term("title", "bar")
        ])
    ])


def test_simple_ops():
    p = parsing.QueryParser("text")
    qs = p.parse("aa OR bb OR cc")
    query.Or([
        query.Term("text", "aa"),
        query.Term("text", "bb"),
        query.Term("text", "cc")
    ])


def test_simple_infix():
    p = parsing.QueryParser("t")
    qst = "ff REQUIRE gg"
    qs = p.parse(qst)
    # query.dump(qs)
    assert qs == query.Require(
        query.Term("t", "ff"),
        query.Term("t", "gg")
    )


def test_standard_ops():
    # and, or, andnot, andmaybe, not, require
    p = parsing.QueryParser("t")
    qst = "(aa AND bb OR cc ANDNOT dd) NOT ee OR (ff REQUIRE gg)"
    qs = p.parse(qst)
    # query.dump(qs)
    assert qs == query.Or([
        query.And([
            query.AndNot(
                query.Or([
                    query.And([
                        query.Term("t", "aa"),
                        query.Term("t", "bb")
                    ]),
                    query.Term("t", "cc"),
                ]),
                query.Term("t", "dd")
            ),
            query.Not(
                query.Term("t", "ee")
            )
        ]),
        query.Require(
            query.Term("t", "ff"),
            query.Term("t", "gg")
        )
    ])

    # assert qs == query.And([
    #     query.AndNot(
    #         query.Or([
    #             query.And([
    #                 query.Term("t", "aa"),
    #                 query.Term("t", "bb")
    #             ]),
    #             query.Term("t", "cc")
    #         ]),
    #         query.Term("t", "dd")
    #     ),
    #     query.Or([
    #         query.Not(query.Term("t", "ee")),
    #         query.Require(
    #             query.Term("t", "ff"),
    #             query.Term("t", "gg")
    #         )
    #     ])
    # ])


def test_plusminus():
    p = parsing.QueryParser("t")
    p.add_plugin(plugs.PlusMinusPlugin())
    qst = "alfa +bravo charlie -delta foxtrot"
    qs = p.parse(qst)
    assert qs == query.AndNot(
        query.AndMaybe(
            query.Term("t", "bravo"),
            query.Or([
                query.Term("t", "alfa"),
                query.Term("t", "charlie"),
                query.Term("t", "foxtrot")
            ])
        ),
        query.Term("t", "delta")
    )


def test_stop_parsed():
    # import logging
    # logging.basicConfig(level=logging.DEBUG)

    p = parsing.QueryParser("t", default_schema)
    p.add_plugin(plugs.GtLtPlugin())
    pm = plugs.PlusMinusPlugin()
    p.add_plugin(pm)
    qst = "alfa (+bravo nums:<100) charlie"
    ls = p.parse_to_list(qst)
    assert ls == [
        query.Term(None, "alfa"), ws,
        Vgroup([
            pm.PlusMinus("+", True),
            query.Term(None, "bravo"), ws,
            query.Range("nums", None, '100', False, True)
        ]), ws,
        query.Term(None, "charlie")
    ]


def test_gtlt():
    # import logging
    # logging.basicConfig(level=logging.DEBUG)

    p = parsing.QueryParser("t", default_schema)
    p.add_plugin(plugs.GtLtPlugin())
    qst = "alfa >=bravo nums:<900 charlie"
    qs = p.parse(qst, normalize=False)
    # query.dump(qs)
    assert qs == query.And([
        query.Term("t", "alfa"),
        query.Range("t", "bravo", None, False, False),
        query.NumericRange("nums", None, 900, False, True),
        query.Term("t", "charlie")
    ])


def test_every():
    p = parsing.QueryParser("t", default_schema)
    assert p.has_plugin("every")
    qst = "alfa *:* bravo"
    qs = p.parse(qst, normalize=False)
    assert qs == query.And([
        query.Term("t", "alfa"),
        query.Every(),
        query.Term("t", "bravo")
    ])


def test_function_emptyargs():
    p = parsing.QueryParser("t", default_schema)
    fp = plugs.FunctionPlugin(
        "foo",
        lambda q: query.Require(q, query.Term("text", "bar"))
    )
    p.add_plugin(fp)
    qst = "alfa #foo[] bravo charlie"

    qs = p.parse(qst, normalize=False)
    assert qs == query.And([
        query.Term("t", "alfa"),
        query.Require(
            query.Term("t", "bravo"),
            query.Term("text", "bar"),
        ),
        query.Term("t", "charlie")
    ])


def test_function_noargs():
    p = parsing.QueryParser("t", default_schema)
    fp = plugs.FunctionPlugin(
        "foo",
        lambda q: query.Require(q, query.Term("text", "bar"))
    )
    p.add_plugin(fp)
    qst = "alfa #foo bravo charlie"

    qs = p.parse(qst, normalize=False)
    assert qs == query.And([
        query.Term("t", "alfa"),
        query.Require(
            query.Term("t", "bravo"),
            query.Term("text", "bar"),
        ),
        query.Term("t", "charlie")
    ])


def test_function_withargs():
    p = parsing.QueryParser("t", default_schema)
    fp = plugs.FunctionPlugin(
        "foo",
        lambda boost, q: query.Require(q, query.Term("text", "bar").set_boost(boost))
    )
    p.add_plugin(fp)
    qst = "alfa #foo[100] bravo charlie"

    qs = p.parse(qst, normalize=False)
    assert qs == query.And([
        query.Term("t", "alfa"),
        query.Require(
            query.Term("t", "bravo"),
            query.Term("text", "bar", boost=100),
        ),
        query.Term("t", "charlie")
    ])


def test_function_take_group():
    p = parsing.QueryParser("t", default_schema)
    fp = plugs.FunctionPlugin(
        "foo",
        lambda boost, q: query.Or([qq.set_boost(boost) for qq in q.children()])
    )
    p.add_plugin(fp)
    qst = "alfa #foo[100](bravo charlie)"

    qs = p.parse(qst, normalize=False)
    assert qs == query.And([
        query.Term("t", "alfa"),
        query.Or([
            query.Term("t", "bravo", boost=100),
            query.Term("t", "charlie", boost=100)
        ])
    ])


def test_pseudo_field():
    p = parsing.QueryParser("t")
    pfp = plugs.PseudoFieldPlugin(
        "high",
        lambda q: query.Term("magic", q.query_text(), boost=5)
    )
    p.add_plugin(pfp)
    qst = "text:alfa high:bravo nums:900"
    qs = p.parse(qst, normalize=False)
    # query.dump(qs)
    assert qs == query.And([
        query.Term("text", "alfa"),
        query.Term("magic", "bravo", boost=5),
        query.Term("nums", "900")
    ])


def test_numeric():
    qp = parsing.QueryParser("nums", default_schema)
    q = qp.parse(u"[10 to *]")
    # query.dump(q)
    assert isinstance(q, query.ErrorQuery)
    assert q.startchar == 0
    assert q.endchar == 9

    q = qp.parse(u"[to 400]")
    assert q.__class__ is query.NumericRange
    assert q.start is None
    assert q.end == 400

    q = qp.parse(u"[10 to]")
    assert q.__class__ is query.NumericRange
    assert q.start == 10
    assert q.end is None

    q = qp.parse(u"[10 to 400]")
    assert q.__class__ is query.NumericRange
    assert q.start == 10
    assert q.end == 400


def test_self_parsing():
    qp = parsing.QueryParser("nums", default_schema)
    qs = qp.parse("times:200608011201 nums:1000")
    target = query.And([
        query.NumericRange("times", 63290030460000000, 63290030519999999),
        query.Term("nums", default_schema["nums"].to_bytes("1000"))
    ])
    # query.diff(qs, target)
    assert qs == target


def test_self_parsing_mixed():
    qp = parsing.QueryParser("text", default_schema)
    qs = qp.parse("alfa nums:5 bravo")
    target = query.And([
        query.Term("text", "alfa"),
        query.Term("nums", default_schema["nums"].to_bytes("5")),
        query.Term("text", "bravo"),
    ])
    # query.diff(qs, target)
    assert qs == target


def test_custom_parsing():
    qp = parsing.QueryParser("text", default_schema)

    stars = peg.Regex("[*]{1,5}")
    expr = peg.Apply(stars, lambda s: query.Term("nums", len(s)))
    qp.set_field_expr("nums", expr)

    qs = qp.parse("alfa nums:*** bravo")
    assert qs == query.And([
        query.Term("text", "alfa"),
        query.Term("nums", 3),
        query.Term("text", "bravo")
    ])


def test_parse_relation_query():
    from whoosh.query.joins import RelationQuery

    schema = fields.Schema(
        id=fields.NUMERIC,
        type=fields.ID(stored=True),
        title=fields.TEXT(stored=True),
        artist=fields.TEXT(stored=True),
        parent=fields.NUMERIC,
    )

    qp = parsing.QueryParser("title", schema)
    rp = plugs.RelationPlugin()
    qp.add_plugin(rp)
    s = "RELATE id IN (type:album artist:bowie) TO parent IN type:song"

    expr = list(rp.syntaxes(qp))[0][0]
    at, qs = expr.parse(s, 0, qp.context())
    assert qs

    qs = qp.parse(s)
    # query.dump(qs)
    assert qs == RelationQuery("id", query.And([
        query.Term("type", "album"),
        query.Term("artist", "bowie")
    ]), "parent", query.Term("type", "song"))


def test_parse_nested_relation():
    from whoosh import analysis
    from whoosh.query.joins import RelationQuery

    sa = analysis.SimpleAnalyzer()
    schema = fields.Schema(
        id=fields.Numeric(unique=True, stored=True, column=True, signed=False),
        name=fields.Text(stored=True, analyzer=sa),
        artist=fields.Text(stored=True, analyzer=sa),
        type=fields.Id(stored=True),
        parent=fields.Numeric(stored=True, column=True, signed=False),
        sales=fields.Numeric(column=True),
    )

    qp = parsing.QueryParser("name", schema)
    qp.add_plugin(plugs.RelationPlugin())

    # Songs with sibling songs with "bridge" in the title
    # relation 1: song -> album
    q1 = RelationQuery("parent", query.And([query.Term("type", "song"),
                                            query.Term("name", "bridge")]),
                       "id", query.Term("type", "album"))
    # relation 2: album(s) from q1 -> songs
    q2 = RelationQuery("id", q1,
                       "parent", query.Term("type", "song"))

    uq = """
    RELATE id IN (
    RELATE parent IN (type:song name:bridge) TO id IN type:album
    ) TO parent IN type:song
    """

    q = qp.parse(uq)
    # query.dump(q)
    assert q == q2




