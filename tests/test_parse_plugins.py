from __future__ import with_statement
import inspect
from datetime import datetime

from whoosh import analysis, fields, formats, qparser, query
from whoosh.compat import u, text_type, xrange
from whoosh.filedb.filestore import RamStorage
from whoosh.qparser import dateparse, default, plugins, syntax
from whoosh.util.times import adatetime


def _plugin_classes(ignore):
    # Get all the subclasses of Plugin in whoosh.qparser.plugins
    return [c for _, c in inspect.getmembers(plugins, inspect.isclass)
            if plugins.Plugin in c.__bases__ and c not in ignore]


def test_combos():
    qs = ('w:a "hi there"^4.2 AND x:b^2.3 OR c AND (y:d OR e) ' +
          '(apple ANDNOT bear)^2.3')

    init_args = {plugins.MultifieldPlugin: (["content", "title"],
                                            {"content": 1.0, "title": 1.2}),
                 plugins.FieldAliasPlugin: ({"content": ("text", "body")},),
                 plugins.CopyFieldPlugin: ({"name": "phone"},),
                 plugins.PseudoFieldPlugin: ({"name": lambda x: x}),
                 }

    pis = _plugin_classes(())
    for i, plugin in enumerate(pis):
        try:
            pis[i] = plugin(*init_args.get(plugin, ()))
        except TypeError:
            raise TypeError("Error instantiating %s" % plugin)

    count = 0
    for i, first in enumerate(pis):
        for j in xrange(len(pis)):
            if i == j:
                continue
            plist = [p for p in pis[:j] if p is not first] + [first]
            qp = qparser.QueryParser("text", None, plugins=plist)
            qp.parse(qs)
            count += 1


def test_field_alias():
    qp = qparser.QueryParser("content", None)
    qp.add_plugin(plugins.FieldAliasPlugin({"title": ("article", "caption")}))
    q = qp.parse("alfa title:bravo article:charlie caption:delta")
    assert text_type(q) == u("(content:alfa AND title:bravo AND title:charlie AND title:delta)")


def test_dateparser():
    schema = fields.Schema(text=fields.TEXT, date=fields.DATETIME)
    qp = default.QueryParser("text", schema)

    errs = []

    def cb(arg):
        errs.append(arg)

    basedate = datetime(2010, 9, 20, 15, 16, 6, 454000)
    qp.add_plugin(dateparse.DateParserPlugin(basedate, callback=cb))

    q = qp.parse(u("hello date:'last tuesday'"))
    assert q.__class__ == query.And
    assert q[1].__class__ == query.DateRange
    assert q[1].startdate == adatetime(2010, 9, 14).floor()
    assert q[1].enddate == adatetime(2010, 9, 14).ceil()

    q = qp.parse(u("date:'3am to 5pm'"))
    assert q.__class__ == query.DateRange
    assert q.startdate == adatetime(2010, 9, 20, 3).floor()
    assert q.enddate == adatetime(2010, 9, 20, 17).ceil()

    q = qp.parse(u("date:blah"))
    assert q == query.NullQuery
    assert errs[0] == "blah"

    q = qp.parse(u("hello date:blarg"))
    assert q.__unicode__() == "(text:hello AND <_NullQuery>)"
    assert q[1].error == "blarg"
    assert errs[1] == "blarg"

    q = qp.parse(u("hello date:20055x10"))
    assert q.__unicode__() == "(text:hello AND <_NullQuery>)"
    assert q[1].error == "20055x10"
    assert errs[2] == "20055x10"

    q = qp.parse(u("hello date:'2005 19 32'"))
    assert q.__unicode__() == "(text:hello AND <_NullQuery>)"
    assert q[1].error == "2005 19 32"
    assert errs[3] == "2005 19 32"

    q = qp.parse(u("date:'march 24 to dec 12'"))
    assert q.__class__ == query.DateRange
    assert q.startdate == adatetime(2010, 3, 24).floor()
    assert q.enddate == adatetime(2010, 12, 12).ceil()

    q = qp.parse(u("date:('30 june' OR '10 july') quick"))
    assert q.__class__ == query.And
    assert len(q) == 2
    assert q[0].__class__ == query.Or
    assert q[0][0].__class__ == query.DateRange
    assert q[0][1].__class__ == query.DateRange


def test_date_range():
    schema = fields.Schema(text=fields.TEXT, date=fields.DATETIME)
    qp = qparser.QueryParser("text", schema)
    basedate = datetime(2010, 9, 20, 15, 16, 6, 454000)
    qp.add_plugin(dateparse.DateParserPlugin(basedate))

    q = qp.parse(u("date:['30 march' to 'next wednesday']"))
    assert q.__class__ == query.DateRange
    assert q.startdate == adatetime(2010, 3, 30).floor()
    assert q.enddate == adatetime(2010, 9, 22).ceil()

    q = qp.parse(u("date:[to 'next wednesday']"))
    assert q.__class__ == query.DateRange
    assert q.startdate is None
    assert q.enddate == adatetime(2010, 9, 22).ceil()

    q = qp.parse(u("date:['30 march' to]"))
    assert q.__class__ == query.DateRange
    assert q.startdate == adatetime(2010, 3, 30).floor()
    assert q.enddate is None

    q = qp.parse(u("date:[30 march to next wednesday]"))
    assert q.__class__ == query.DateRange
    assert q.startdate == adatetime(2010, 3, 30).floor()
    assert q.enddate == adatetime(2010, 9, 22).ceil()

    q = qp.parse(u("date:[to next wednesday]"))
    assert q.__class__ == query.DateRange
    assert q.startdate is None
    assert q.enddate == adatetime(2010, 9, 22).ceil()

    q = qp.parse(u("date:[30 march to]"))
    assert q.__class__ == query.DateRange
    assert q.startdate == adatetime(2010, 3, 30).floor()
    assert q.enddate is None


def test_daterange_multi():
    schema = fields.Schema(text=fields.TEXT, start=fields.DATETIME,
                           end=fields.DATETIME)
    qp = qparser.QueryParser("text", schema)
    basedate = datetime(2010, 9, 20, 15, 16, 6, 454000)
    qp.add_plugin(dateparse.DateParserPlugin(basedate))

    q = qp.parse("start:[2008 to] AND end:[2011 to 2011]")
    assert q.__class__ == query.And
    assert q[0].__class__ == query.DateRange
    assert q[1].__class__ == query.DateRange
    assert q[0].startdate == adatetime(2008).floor()
    assert q[0].enddate is None
    assert q[1].startdate == adatetime(2011).floor()
    assert q[1].enddate == adatetime(2011).ceil()


def test_daterange_empty_field():
    schema = fields.Schema(test=fields.DATETIME)
    ix = RamStorage().create_index(schema)

    writer = ix.writer()
    writer.add_document(test=None)
    writer.commit()

    with ix.searcher() as s:
        q = query.DateRange("test", datetime.fromtimestamp(0),
                            datetime.today())
        r = s.search(q)
        assert len(r) == 0


def test_free_dates():
    a = analysis.StandardAnalyzer(stoplist=None)
    schema = fields.Schema(text=fields.TEXT(analyzer=a), date=fields.DATETIME)
    qp = qparser.QueryParser("text", schema)
    basedate = datetime(2010, 9, 20, 15, 16, 6, 454000)
    qp.add_plugin(dateparse.DateParserPlugin(basedate, free=True))

    q = qp.parse(u("hello date:last tuesday"))
    assert q.__class__ == query.And
    assert len(q) == 2
    assert q[0].__class__ == query.Term
    assert q[0].text == "hello"
    assert q[1].__class__ == query.DateRange
    assert q[1].startdate == adatetime(2010, 9, 14).floor()
    assert q[1].enddate == adatetime(2010, 9, 14).ceil()

    q = qp.parse(u("date:mar 29 1972 hello"))
    assert q.__class__ == query.And
    assert len(q) == 2
    assert q[0].__class__ == query.DateRange
    assert q[0].startdate == adatetime(1972, 3, 29).floor()
    assert q[0].enddate == adatetime(1972, 3, 29).ceil()
    assert q[1].__class__ == query.Term
    assert q[1].text == "hello"

    q = qp.parse(u("date:2005 march 2"))
    assert q.__class__ == query.DateRange
    assert q.startdate == adatetime(2005, 3, 2).floor()
    assert q.enddate == adatetime(2005, 3, 2).ceil()

    q = qp.parse(u("date:'2005' march 2"))
    assert q.__class__ == query.And
    assert len(q) == 3
    assert q[0].__class__ == query.DateRange
    assert q[0].startdate == adatetime(2005).floor()
    assert q[0].enddate == adatetime(2005).ceil()
    assert q[1].__class__ == query.Term
    assert q[1].fieldname == "text"
    assert q[1].text == "march"

    q = qp.parse(u("date:march 24 to dec 12"))
    assert q.__class__ == query.DateRange
    assert q.startdate == adatetime(2010, 3, 24).floor()
    assert q.enddate == adatetime(2010, 12, 12).ceil()

    q = qp.parse(u("date:5:10pm"))
    assert q.__class__ == query.DateRange
    assert q.startdate == adatetime(2010, 9, 20, 17, 10).floor()
    assert q.enddate == adatetime(2010, 9, 20, 17, 10).ceil()

    q = qp.parse(u("(date:30 june OR date:10 july) quick"))
    assert q.__class__ == query.And
    assert len(q) == 2
    assert q[0].__class__ == query.Or
    assert q[0][0].__class__ == query.DateRange
    assert q[0][1].__class__ == query.DateRange


def test_prefix_plugin():
    schema = fields.Schema(id=fields.ID, text=fields.TEXT)
    ix = RamStorage().create_index(schema)

    w = ix.writer()
    w.add_document(id=u("1"), text=u("alfa"))
    w.add_document(id=u("2"), text=u("bravo"))
    w.add_document(id=u("3"), text=u("buono"))
    w.commit()

    with ix.searcher() as s:
        qp = qparser.QueryParser("text", schema)
        qp.remove_plugin_class(plugins.WildcardPlugin)
        qp.add_plugin(plugins.PrefixPlugin)

        q = qp.parse(u("b*"))
        r = s.search(q, limit=None)
        assert len(r) == 2

        q = qp.parse(u("br*"))
        r = s.search(q, limit=None)
        assert len(r) == 1


def test_custom_tokens():
    qp = qparser.QueryParser("text", None)
    qp.remove_plugin_class(plugins.OperatorsPlugin)

    cp = plugins.OperatorsPlugin(And="&", Or="\\|", AndNot="&!", AndMaybe="&~",
                                 Not="-")
    qp.add_plugin(cp)

    q = qp.parse("this | that")
    assert q.__class__ == query.Or
    assert q[0].__class__ == query.Term
    assert q[0].text == "this"
    assert q[1].__class__ == query.Term
    assert q[1].text == "that"

    q = qp.parse("this&!that")
    assert q.__class__ == query.AndNot
    assert q.a.__class__ == query.Term
    assert q.a.text == "this"
    assert q.b.__class__ == query.Term
    assert q.b.text == "that"

    q = qp.parse("alfa -bravo NOT charlie")
    assert len(q) == 4
    assert q[1].__class__ == query.Not
    assert q[1].query.text == "bravo"
    assert q[2].text == "NOT"


def test_copyfield():
    qp = qparser.QueryParser("a", None)
    qp.add_plugin(plugins.CopyFieldPlugin({"b": "c"}, None))
    assert text_type(qp.parse("hello b:matt")) == "(a:hello AND b:matt AND c:matt)"

    qp = qparser.QueryParser("a", None)
    qp.add_plugin(plugins.CopyFieldPlugin({"b": "c"}, syntax.AndMaybeGroup))
    assert text_type(qp.parse("hello b:matt")) == "(a:hello AND (b:matt ANDMAYBE c:matt))"

    qp = qparser.QueryParser("a", None)
    qp.add_plugin(plugins.CopyFieldPlugin({"b": "c"}, syntax.RequireGroup))
    assert text_type(qp.parse("hello (there OR b:matt)")) == "(a:hello AND (a:there OR (b:matt REQUIRE c:matt)))"

    qp = qparser.QueryParser("a", None)
    qp.add_plugin(plugins.CopyFieldPlugin({"a": "c"}, syntax.OrGroup))
    assert text_type(qp.parse("hello there")) == "((a:hello OR c:hello) AND (a:there OR c:there))"

    qp = qparser.QueryParser("a", None)
    qp.add_plugin(plugins.CopyFieldPlugin({"b": "c"}, mirror=True))
    assert text_type(qp.parse("hello c:matt")) == "(a:hello AND (c:matt OR b:matt))"

    qp = qparser.QueryParser("a", None)
    qp.add_plugin(plugins.CopyFieldPlugin({"c": "a"}, mirror=True))
    assert text_type(qp.parse("hello c:matt")) == "((a:hello OR c:hello) AND (c:matt OR a:matt))"

    ana = analysis.RegexAnalyzer(r"\w+") | analysis.DoubleMetaphoneFilter()
    fmt = formats.Frequency()
    schema = fields.Schema(name=fields.KEYWORD,
                           name_phone=fields.FieldType(fmt, ana,
                                                       multitoken_query="or"))
    qp = qparser.QueryParser("name", schema)
    qp.add_plugin(plugins.CopyFieldPlugin({"name": "name_phone"}))
    assert text_type(qp.parse(u("spruce view"))) == "((name:spruce OR name_phone:SPRS) AND (name:view OR name_phone:F OR name_phone:FF))"


def test_gtlt():
    schema = fields.Schema(a=fields.KEYWORD, b=fields.NUMERIC,
                           c=fields.KEYWORD,
                           d=fields.NUMERIC(float), e=fields.DATETIME)
    qp = qparser.QueryParser("a", schema)
    qp.add_plugin(plugins.GtLtPlugin())
    qp.add_plugin(dateparse.DateParserPlugin())

    q = qp.parse(u("a:hello b:>100 c:<=z there"))
    assert q.__class__ == query.And
    assert len(q) == 4
    assert q[0] == query.Term("a", "hello")
    assert q[1] == query.NumericRange("b", 100, None, startexcl=True)
    assert q[2] == query.TermRange("c", None, 'z')
    assert q[3] == query.Term("a", "there")

    q = qp.parse(u("hello e:>'29 mar 2001' there"))
    assert q.__class__ == query.And
    assert len(q) == 3
    assert q[0] == query.Term("a", "hello")
    # As of this writing, date ranges don't support startexcl/endexcl
    assert q[1] == query.DateRange("e", datetime(2001, 3, 29, 0, 0), None)
    assert q[2] == query.Term("a", "there")

    q = qp.parse(u("a:> alfa c:<= bravo"))
    assert text_type(q) == "(a:a: AND a:alfa AND a:c: AND a:bravo)"

    qp.remove_plugin_class(plugins.FieldsPlugin)
    qp.remove_plugin_class(plugins.RangePlugin)
    q = qp.parse(u("hello a:>500 there"))
    assert text_type(q) == "(a:hello AND a:a: AND a:500 AND a:there)"


def test_regex():
    schema = fields.Schema(a=fields.KEYWORD, b=fields.TEXT)
    qp = qparser.QueryParser("a", schema)
    qp.add_plugin(plugins.RegexPlugin())

    q = qp.parse(u("a:foo-bar b:foo-bar"))
    assert q.__unicode__() == '(a:foo-bar AND b:foo AND b:bar)'

    q = qp.parse(u('a:r"foo-bar" b:r"foo-bar"'))
    assert q.__unicode__() == '(a:r"foo-bar" AND b:r"foo-bar")'


def test_pseudofield():
    schema = fields.Schema(a=fields.KEYWORD, b=fields.TEXT)

    def regex_maker(node):
        if node.has_text:
            node = qparser.RegexPlugin.RegexNode(node.text)
            node.set_fieldname("content")
            return node

    qp = qparser.QueryParser("a", schema)
    qp.add_plugin(qparser.PseudoFieldPlugin({"regex": regex_maker}))
    q = qp.parse(u("alfa regex:br.vo"))
    assert q.__unicode__() == '(a:alfa AND content:r"br.vo")'

    def rev_text(node):
        if node.has_text:
            # Create a word node for the reversed text
            revtext = node.text[::-1]  # Reverse the text
            rnode = qparser.WordNode(revtext)
            # Duplicate the original node's start and end char
            rnode.set_range(node.startchar, node.endchar)

            # Put the original node and the reversed node in an OrGroup
            group = qparser.OrGroup([node, rnode])

            # Need to set the fieldname here because the PseudoFieldPlugin
            # removes the field name syntax
            group.set_fieldname("reverse")

            return group

    qp = qparser.QueryParser("content", schema)
    qp.add_plugin(qparser.PseudoFieldPlugin({"reverse": rev_text}))
    q = qp.parse(u("alfa reverse:bravo"))
    assert q.__unicode__() == '(content:alfa AND (reverse:bravo OR reverse:ovarb))'


def test_fuzzy_plugin():
    ana = analysis.StandardAnalyzer("\\S+")
    schema = fields.Schema(f=fields.TEXT(analyzer=ana))
    qp = default.QueryParser("f", schema)
    qp.add_plugin(plugins.FuzzyTermPlugin())

    q = qp.parse("bob~")
    assert q.__class__ == query.FuzzyTerm
    assert q.field() == "f"
    assert q.text == "bob"
    assert q.maxdist == 1

    q = qp.parse("Alfa Bravo~ Charlie")
    assert q.__class__ == query.And
    assert q[0].__class__ == query.Term
    assert q[0].text == "alfa"
    assert q[1].__class__ == query.FuzzyTerm
    assert q[1].field() == "f"
    assert q[1].text == "bravo"
    assert q[1].maxdist == 1
    assert q[2].__class__ == query.Term
    assert q[2].text == "charlie"

    q = qp.parse("Alfa Bravo~2 Charlie")
    assert q.__class__ == query.And
    assert q[0].__class__ == query.Term
    assert q[0].text == "alfa"
    assert q[1].__class__ == query.FuzzyTerm
    assert q[1].field() == "f"
    assert q[1].text == "bravo"
    assert q[1].maxdist == 2
    assert q[2].__class__ == query.Term
    assert q[2].text == "charlie"

    q = qp.parse("alfa ~2 bravo")
    assert q.__class__ == query.And
    assert q[0].__class__ == query.Term
    assert q[0].text == "alfa"
    assert q[1].__class__ == query.Term
    assert q[1].text == "~2"
    assert q[2].__class__ == query.Term
    assert q[2].text == "bravo"

    qp = default.QueryParser("f", None)
    q = qp.parse("'bob~'")
    assert q.__class__ == query.Term
    assert q.field() == "f"
    assert q.text == "bob~"


def test_fuzzy_prefix():
    from whoosh import scoring

    schema = fields.Schema(title=fields.TEXT(stored=True),
                           content=fields.TEXT(spelling=True))

    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        # Match -> first
        w.add_document(title=u("First"),
                       content=u("This is the first document we've added!"))
        # No match
        w.add_document(title=u("Second"),
                       content=u("The second one is even more interesting! filst"))
        # Match -> first
        w.add_document(title=u("Third"),
                       content=u("The world first line we've added!"))
        # Match -> zeroth
        w.add_document(title=u("Fourth"),
                       content=u("The second one is alaways comes after zeroth!"))
        # Match -> fire is within 2 edits (transpose + delete) of first
        w.add_document(title=u("Fifth"),
                       content=u("The fire is beautiful"))

    from whoosh.qparser import QueryParser, FuzzyTermPlugin #, BoundedFuzzyTermPlugin
    parser = QueryParser("content", ix.schema)
    parser.add_plugin(FuzzyTermPlugin())
    q = parser.parse("first~2/3 OR zeroth", debug=False)

    assert isinstance(q, query.Or)
    ft = q[0]
    assert isinstance(ft, query.FuzzyTerm)
    assert ft.maxdist == 2
    assert ft.prefixlength == 3

    with ix.searcher(weighting=scoring.TF_IDF()) as searcher:
        results = searcher.search(q)
        assert len(results) == 4
        assert " ".join(hit["title"] for hit in results) == "Fourth First Third Fifth"


def test_function_plugin():
    class FakeQuery(query.Query):
        def __init__(self, children, *args, **kwargs):
            self.children = children
            self.args = args
            self.kwargs = kwargs
            self.fieldname = None

        def __hash__(self):
            return hash(tuple(self.children)) ^ hash(self.args)

        def __unicode__(self):
            qs = "|".join(str(q) for q in self.children)
            args = ",".join(self.args)
            kwargs = ",".join(sorted("%s:%s" % item for item in self.kwargs.items()))
            return u("<%s %s %s>") % (qs, args, kwargs)

        __str__ = __unicode__

    def fuzzy(qs, prefix=0, maxdist=2):
        prefix = int(prefix)
        maxdist = int(maxdist)
        return query.FuzzyTerm(qs[0].fieldname, qs[0].text,
                               prefixlength=prefix, maxdist=maxdist)

    fp = plugins.FunctionPlugin({"foo": FakeQuery, "fuzzy": fuzzy})
    qp = default.QueryParser("f", None)
    qp.add_plugin(fp)

    def check(qstring, target):
        q = qp.parse(u(qstring), normalize=False)
        assert str(q) == target

    check("alfa #foo charlie delta",
          "(f:alfa AND <  > AND f:charlie AND f:delta)")

    check("alfa #foo(charlie delta) echo",
          "(f:alfa AND <f:charlie|f:delta  > AND f:echo)")

    check("alfa #foo(charlie AND delta) echo",
          "(f:alfa AND <(f:charlie AND f:delta)  > AND f:echo)")

    check("alfa #foo[a] charlie delta",
          "(f:alfa AND < a > AND f:charlie AND f:delta)")

    check("alfa #foo[a, b](charlie delta) echo",
          "(f:alfa AND <f:charlie|f:delta a,b > AND f:echo)")

    check("alfa #foo[a,b,c=d](charlie AND delta) echo",
          "(f:alfa AND <(f:charlie AND f:delta) a,b c:d> AND f:echo)")

    check("alfa #foo[a,b,c=d]() (charlie AND delta)",
          "(f:alfa AND < a,b c:d> AND ((f:charlie AND f:delta)))")

    check("alfa #foo[a=1,b=2](charlie AND delta)^2.0 echo",
          "(f:alfa AND <(f:charlie AND f:delta)  a:1,b:2,boost:2.0> AND f:echo)")

    check("alfa #fuzzy[maxdist=2](bravo) charlie",
          "(f:alfa AND f:bravo~2 AND f:charlie)")


def test_sequence_plugin():
    qp = default.QueryParser("f", None)
    qp.remove_plugin_class(plugins.PhrasePlugin)
    qp.add_plugin(plugins.FuzzyTermPlugin())
    qp.add_plugin(plugins.SequencePlugin())

    q = qp.parse(u('alfa "bravo charlie~2 (delta OR echo)" foxtrot'))
    assert q.__unicode__() == "(f:alfa AND (f:bravo NEAR f:charlie~2 NEAR (f:delta OR f:echo)) AND f:foxtrot)"
    assert q[1].__class__ == query.Sequence

    q = qp.parse(u('alfa "bravo charlie~2 d?lt*'))
    assert q[0].text == "alfa"
    assert q[1].text == "bravo"
    assert q[2].__class__ == query.FuzzyTerm
    assert q[3].__class__ == query.Wildcard

    q = qp.parse(u('alfa "bravo charlie~2" d?lt* "[a TO z] [0 TO 9]" echo'))
    assert q.__unicode__() == "(f:alfa AND (f:bravo NEAR f:charlie~2) AND f:d?lt* AND (f:[a TO z] NEAR f:[0 TO 9]) AND f:echo)"
    assert q[0].text == "alfa"
    assert q[1].__class__ == query.Sequence
    assert q[2].__class__ == query.Wildcard
    assert q[3].__class__ == query.Sequence
    assert q[3][0].__class__ == query.TermRange
    assert q[3][1].__class__ == query.TermRange
    assert q[4].text == "echo"

    q = qp.parse(u('alfa "bravo charlie~3"~2 delta'))
    assert q[1].__class__ == query.Sequence
    assert q[1].slop == 2
    assert q[1][1].__class__ == query.FuzzyTerm
    assert q[1][1].maxdist == 3


def test_sequence_andmaybe():
    qp = default.QueryParser("f", None)
    qp.remove_plugin_class(plugins.PhrasePlugin)
    qp.add_plugins([plugins.FuzzyTermPlugin(), plugins.SequencePlugin()])

    q = qp.parse(u('Dahmen ANDMAYBE "Besov Spaces"'))
    assert isinstance(q, query.AndMaybe)
    assert q[0] == query.Term("f", u("Dahmen"))
    assert q[1] == query.Sequence([query.Term("f", u("Besov")),
                                   query.Term("f", u("Spaces"))])

