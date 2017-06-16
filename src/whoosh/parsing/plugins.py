import ast
import inspect
import operator
import sys
from functools import wraps
from typing import Callable, Dict, Iterable, List, Sequence, Tuple

from whoosh import query
from whoosh.compat import text_type
from whoosh.parsing import parsing, peg
from whoosh.util.text import rcompile


# Typing aliases

Syntaxes = 'Iterable[Tuple[peg.Element, int]]'
FilterType = 'Callable[[Sequence[query.Query]], Sequence[query.Query]]'
Filters = 'Iterable[Tuple[FilterType, int]]'
Queries = 'Sequence[query.Query]'
GroupFnType = 'Callable[[List[query.Query]], query.Query]'


# Decorators

def syntax(priority: int=0):
    def _wrapper(method):
        method.is_syntax = True
        method.priority = priority
        return method
    return _wrapper


def qfilter(priority: int=0):
    def _wrapper(method):
        method.is_qfilter = True
        method.priority = priority
        return method
    return _wrapper


def analysis_filter(qtype, priority: int=190):
    def _wrapper(method):
        @wraps(method)
        def _analysis_filter(self, parser: 'parsing.QueryParser',
                             qs: query.Query) -> query.Query:
            if isinstance(qs, qtype) and not getattr(qs, "analyzed"):
                qs = method(self, parser, qs)
                qs.analyzed = True
            elif not qs.is_leaf():
                chs = [_analysis_filter(self, parser, subq)
                       for subq in qs.children()]
                qs.set_children(chs)
            return qs
        _analysis_filter.is_qfilter = True
        _analysis_filter.priority = priority
        return _analysis_filter
    return _wrapper


# Plugin classes

class Plugin:
    name = None
    expression = None

    def __repr__(self):
        return "<%s %r>" % (type(self).__name__, self.name)

    def modify_context(self, context: 'peg.Context'):
        """
        This is called on each plugin by the parser when it creates a new PEG
        context, to give the plugins a chance to modify it or add their own
        information.

        A plugin must modify the given context object in-place, rather than
        returning a new context object.
        """

        pass

    def syntaxes(self, parser: 'parsing.QueryParser') -> Syntaxes:
        for name, value in self.__class__.__dict__.items():
            if hasattr(value, "is_syntax") and value.is_syntax:
                yield getattr(self, name)(parser), value.priority

    def filters(self, parser: 'parsing.QueryParser') -> Filters:
        mro = inspect.getmro(self.__class__)
        for cls in mro:
            for name, value in cls.__dict__.items():
                if hasattr(value, "is_qfilter") and value.is_qfilter:
                    yield getattr(self, name), value.priority


class WhitespacePlugin(Plugin):
    name = "whitespace"
    ws = query.NullQuery("WS")

    # Run after other exprs
    @syntax(1000)
    def find_whitespace(self, parser: 'parsing.QueryParser'):
        return (peg.ws + peg.Do(lambda ctx: self.ws)).named("ws")

    # Run before other filters, so they don't see whitespace
    @qfilter(-100)
    def remove_whitespace(self, parser: 'parsing.QueryParser', qs: query.Query
                          ) -> query.Query:
        if not qs.is_leaf():
            qs = qs.with_children([self.remove_whitespace(parser, q)
                                   for q in qs.children()
                                   if q is not self.ws])
        return qs


class FieldsPlugin(Plugin):
    name = "fields"

    def __init__(self, pattern="(?P<fname>[^ \t\r\n:]+): ?"):
        self.pattern = pattern

    @syntax()
    def find_fieldspec(self, parser: 'parsing.QueryParser'):
        schema = parser.schema

        return (
            peg.Regex(self.pattern) +
            peg.If(lambda ctx: not schema or ctx["fname"] in schema) +
            peg.Not(peg.stringend) +
            peg.Parsed(parser, self.name, field_from="fname")
        ).named("fields")

    @qfilter(100)
    def fill_fieldnames(self, parser: 'parsing.QueryParser', qs: query.Query
                        ) -> query.Query:
        if qs.is_leaf():
            if not qs.field():
                qs = qs.with_fieldname(parser.fieldname)
        else:
            qs.set_children([self.fill_fieldnames(parser, subq)
                             for subq in qs.children()])
        return qs


class SingleQuotePlugin(Plugin):
    name = "single_quotes"

    @syntax()
    def find_single_quotes(self, parser: 'parsing.QueryParser'):
        def parse_inside(ctx: peg.Context):
            text = ctx["sq_string"]
            q = parser.term_query(ctx.fieldname, text
                                  ).set_extent(*ctx["extent"])
            q.analyzed = False
            return q

        return (
            peg.QuotedString("'", esc_char="\\").set_ext().set("sq_string") +
            peg.Do(parse_inside)
        ).named("singlequote")


class PrefixPlugin(Plugin):
    name = "prefix"
    star = query.NullQuery("*")

    def __init__(self, star_expr: 'peg.Expr'=None):
        self.star_expr = star_expr or peg.Str("*")

    @syntax(-2)
    def find_prefixes(self, parser: 'parsing.QueryParser'):
        def make_prefix(ctx: 'peg.Context') -> 'query.Query':
            # Pull the prefix text out of the context and make a Prefix query
            # from it
            fieldname = ctx.fieldname
            q = query.Prefix(fieldname, ctx["pre"]).set_extent(*ctx["extent"])
            q.analyzed = False
            return q

        starexpr = self.star_expr
        textexpr = peg.StringUntil(peg.ws | starexpr, esc_char=parser.esc_char)

        return (
            (
                peg.wordstart +
                textexpr +
                starexpr.hide()
            ).set_ext().set("pre") +
            peg.Do(make_prefix)
        ).named("prefix")

    # Run after FieldsPlugin and before TermsPlugin
    @analysis_filter(query.Prefix, 190)
    def analyze_prefixes(self, parser: 'parsing.QueryParser', qs: query.Prefix
                         ) -> query.Query:
        text = parser.first_token(qs.field(), qs.text, tokenize=False,
                                  removestops=False)
        return qs.with_text(text)


class WildcardPlugin(Plugin):
    name = "wildcard"

    # \u055E = Armenian question mark
    # \u061F = Arabic question mark
    # \u1367 = Ethiopic question mark
    def __init__(self, stars: str=u"*", qmarks: str=u"?\u055E\u061F\u1367"):
        self.stars = stars
        self.qmarks = qmarks

    @syntax(-1)
    def find_wildcards(self, parser: 'parsing.QueryParser'):
        def make_wildcard(ctx):
            fieldname = ctx.fieldname
            text = "".join(ctx["reps"]) + ctx.get("last", "")
            return query.Wildcard(fieldname, text)

        wildexpr = peg.Regex("[%s%s]+" % (self.stars, self.qmarks),
                             may_be_empty=False)
        textexpr = peg.StringUntil(wildexpr, esc_char=parser.esc_char,
                                   matches_end=False, add_context_expr=True,
                                   may_be_empty=True)
        lastexpr = peg.StringUntil(peg.NoMatch(), esc_char=parser.esc_char,
                                   matches_end=True, add_context_expr=True,
                                   may_be_empty=False)

        return peg.Guard("__wildcard",
            peg.OneOrMore(
                peg.TokenStart(parser) +
                textexpr.set("txt") +
                wildexpr.set("wild") +
                peg.Do(lambda ctx: ctx["txt"] + ctx["wild"])
            ).set("reps") +
            lastexpr.set("last").opt() +
            peg.Do(make_wildcard)
        ).named("wildcard")


class RegexPlugin(Plugin):
    name = "regex"

    def __init__(self, start: str='r"', end: str='"'):
        self.start = start
        self.end = end

    # Run before phrase expr
    @syntax(-1)
    def find_regexes(self, parser: 'parsing.QueryParser'):
        quoted = peg.QuotedString(self.start, self.end, esc_char="\\")

        def make_regex(ctx: peg.Context):
            q = query.Regex(ctx.fieldname, ctx["pat"])
            q.set_extent(*ctx["extent"])
            return q

        return (
            quoted.set("pat").set_ext() +
            peg.Do(make_regex)
        ).named("regex")


class FuzzyTermPlugin(Plugin):
    name = "fuzzy_term"

    @syntax()
    def find_fuzzy_terms(self, parser: 'parsing.QueryParser'):
        # This expr finds "fuzziness specification" syntax and leaves behind
        # a "Fuzziness" marker. Then, the filter pass finds Fuzziness markers
        # and applies them to the previous query.
        def make_fuzziness(ctx: peg.Context):
            maxdist = int(ctx["maxdist"][0]) if ctx["maxdist"] else 1
            prefixlen = int(ctx["prefixlen"][0]) if ctx["prefixlen"] else 0
            return self.Fuzziness(maxdist, prefixlen)

        return (
            peg.Not(peg.Follows(peg.ws, 1)) +
            peg.Str("~") +
            peg.Regex("[0-9]").opt().set("maxdist") +
            peg.Optional(
                peg.Str("/") +
                peg.Regex("[1-9][0-9]*")
            ).set("prefixlen") +
            peg.Do(make_fuzziness)
        ).named("fuzzy")

    @qfilter(190)
    def convert_fuzzies(self, parser: 'parsing.QueryParser', qs: query.Query
                        ) -> query.Query:
        if qs.is_leaf():
            return qs

        newkids = []
        for q in qs.children():
            if isinstance(q, self.Fuzziness) and newkids:
                last = newkids[-1]
                if isinstance(last, query.Term):
                    fieldname = last.field()
                    text = last.query_text()

                    if parser.schema and fieldname in parser.schema:
                        text = parser.first_token(fieldname, text,
                                                  tokenize=False,
                                                  removestops=False)

                    fuzz = query.FuzzyTerm(
                        fieldname, text, boost=last.boost,
                        maxdist=q.maxdist, prefixlength=q.prefixlen
                    ).set_extent(last.startchar, last.endchar)

                    newkids[-1] = fuzz
                    continue
            newkids.append(q)
        qs = qs.with_children(newkids)
        return qs

    class Fuzziness(query.NullQuery):
        def __init__(self, maxdist: int, prefixlen: int):
            super(FuzzyTermPlugin.Fuzziness, self).__init__()
            self.maxdist = maxdist
            self.prefixlen = prefixlen

        def __repr__(self):
            return "<Fuzz %r/%r>" % (self.maxdist, self.prefixlen)


class GroupPlugin(Plugin):
    name = "group"

    def __init__(self, start: str="(", end: str=")"):
        self.start = start
        self.end = end

    @syntax()
    def find_brackets(self, parser: 'parsing.QueryParser'):
        start = peg.Str(self.start).named("openparen")
        end = peg.Str(self.end).named("closeparen")

        # Instead of immediately replacing the syntax with the parser's default
        # group, we'll drop in a generic "Group" query, and replace it with a
        # filter later. If we put in the default group here, the parser could
        # incorrectly remove the group when it coalesced nested groups of the
        # same type (e.g. And inside an And) if a parsing filter calls
        # query.merge_subqueries() (which the OperatorsPlugin does).

        return (
            start +
            peg.ZeroOrMore(
                peg.Not(peg.Peek(end)) + peg.Parsed(parser, "group", end)
            ).set("items") +
            end +
            peg.Do(lambda ctx: Vgroup(ctx["items"]))
        ).named("group")

    # Run after everything else
    @qfilter(9999)
    def replace_groups(self, parser: 'parsing.QueryParser', qs: query.Query
                       ) -> query.Query:
        # Replace generic "Group" queries with the parser's default group

        if isinstance(qs, Vgroup):
            qs = parser.make_group(qs.subqueries)
        if not qs.is_leaf():
            qs = qs.with_children([self.replace_groups(parser, q)
                                   for q in qs.children()])
        return qs


class Vgroup(query.compound.CompoundQuery):
    joint = ";"

    def __init__(self, subqueries):
        super(Vgroup, self).__init__(subqueries)

    def with_children(self, newkids: 'List[query.Query]'):
        return self.__class__(newkids)


class PhrasePlugin(Plugin):
    name = "phrase"

    # Expression used to find words if a schema isn't available
    wordexpr = rcompile(r'\S+')

    def __init__(self, start: str='"', end: str='"', esc: str="\\"):
        self.start = start
        self.end = end
        self.esc = esc

    @syntax()
    def find_phrases(self, parser: 'parsing.QueryParser'):
        def make_phrase(ctx: peg.Context):
            slop = int(ctx["slop"][0]) if ctx["slop"] else 0

            # Leave the text unanalyzed (the filter will do it later); this
            # is required to allow MultifieldPlugin to add queries in other
            # fields before analysis in a single field
            q = query.Phrase(ctx.fieldname, [], slop=slop)
            q.phrase_text = ctx["text"]
            q.analyzed = False
            q.set_extent(*ctx["inext"])

            return q

        quoted = peg.QuotedString(self.start, self.end, esc_char=self.esc,
                                  inner_extent="inext")

        return (
            quoted.set("text").set_ext() +
            peg.Optional(
                peg.Str("~") +
                peg.Regex("[0-9]+", may_be_empty=False)
            ).set("slop") +
            peg.Do(make_phrase)
        ).named("phrase")

    # Run after FieldsPlugin and before TermsPlugin
    @analysis_filter(query.Phrase, 190)
    def analyze_phrases(self, parser: 'parsing.QueryParser', qs: query.Phrase
                        ) -> query.Query:
        text = qs.phrase_text
        tokens = parser.text_to_tokens(qs.field(), text, startchar=qs.startchar)
        qs.words = [t[0] for t in tokens]
        qs.char_ranges = [(tk[1], tk[2]) for tk in tokens]
        return qs


class SequencePlugin(Plugin):
    name = "sequence"

    def __init__(self, start="<", end=">"):
        self.start = start
        self.end = end

    @syntax(-100)
    def find_sequences(self, parser: 'parsing.QueryParser'):
        start = peg.Str(self.start)
        end = peg.Str(self.end)

        def make_sequence(ctx: peg.Context):
            slop = int(ctx["slop"][0]) if ctx["slop"] else 0
            q = query.Sequence(ctx["items"], slop=slop)
            return q

        return (
            start +
            peg.OneOrMore(
                peg.Not(peg.Peek(end)) +
                peg.Parsed(parser, self.name, end_expr=end)
            ).set("items") +
            end +
            peg.Optional(
                peg.Str("~") +
                peg.Regex("[0-9]+", may_be_empty=False)
            ).set("slop") +
            peg.Do(make_sequence)
        ).named("sequence")


class RangePlugin(Plugin):
    name = "range"

    @syntax(1)
    def find_ranges(self, parser: 'parsing.QueryParser'):
        ws = peg.ws
        # Open is either '{' or '[' followed by (hidden) whitespace
        open = (peg.Str("{") | peg.Str("[")) + ws.opt().hide()
        # Close is either '}' or ']', preceded by (discarded) whitespace
        close = ws.opt() + (peg.Str("}") | peg.Str("]"))
        # in between the lower and upper limit is the word "to"
        to = peg.Str("to", ignore_case=True)
        single_quoted = peg.QuotedString("'", esc_char=parser.esc_char)
        dbl_quoted = peg.QuotedString('"', esc_char=parser.esc_char)

        start_expr = (
            single_quoted | dbl_quoted |
            peg.StringUntil((ws + to), esc_char=parser.esc_char,
                            may_be_empty=False)
        ).named("rangestart").set("start")

        end_expr = (
            single_quoted | dbl_quoted |
            peg.StringUntil(close, esc_char=parser.esc_char,
                            may_be_empty=False)
        ).named("rangeend").set("end")

        # The lower or upper limit may be optional, but not both. These rules
        # cover the three cases.
        # 1. lower limit followed by "to" -- [a TO]
        open_end = (
            start_expr +
            ws + to + peg.Peek(close) +
            peg.Do(lambda ctx: (ctx["start"], None))
        ).named("range_open_end")
        # 2. "to" followed by upper limit -- [TO z]
        open_start = (
             to + ws +
             end_expr +
             peg.Do(lambda ctx: (None, ctx["end"]))
        ).named("range_open_start")
        # 3. lower, "to" upper -- [a TO z]
        dbl_end = (
            start_expr +
            ws + to + ws +
            end_expr +
            peg.Do(lambda ctx: (ctx["start"], ctx["end"]))
        ).named("closed_range")
        # Wrap the three possibilities in an Or rule
        body = (dbl_end | open_start | open_end).named("range_body")

        def make_range(ctx: peg.Context):
            fieldname = ctx.fieldname
            startchar = ctx["openext"][0]
            endchar = ctx["closeext"][1]

            start, end = ctx["body"]

            # What kind of open and close brackets were used?
            startexcl = ctx["open"] == "{"
            endexcl = ctx["close"] == "}"

            q = query.Range(fieldname, start, end, startexcl, endexcl)
            q.set_extent(startchar, endchar)
            q.analyzed = False

            return q

        return (
            open.set("open").set_ext("openext") +
            body.set("body") +
            close.set("close").set_ext("closeext") +
            peg.Do(make_range)
        ).named("range")

    @analysis_filter((query.Range, query.TermRange), 190)
    def analyze_ranges(self, parser: 'parsing.QueryParser', qs: query.Range,
                       ) -> query.Query:
        fieldname = qs.field()
        start = qs.start
        end = qs.end
        if start is not None:
            qs.start = parser.first_token(fieldname, start, tokenize=False,
                                          removestops=False)
        if end is not None:
            qs.end = parser.first_token(fieldname, end, tokenize=False,
                                        removestops=False)

        schema = parser.schema
        if schema and fieldname in schema and schema[fieldname].self_parsing():
            field = schema[fieldname]
            try:
                qs = field.parse_range(
                    fieldname, start, end, qs.startexcl, qs.endexcl
                ).set_extent(qs.startchar, qs.endchar)
            except query.QueryParserError:
                qs = query.ErrorQuery(sys.exc_info()[1], qs)

        return qs


class BoostPlugin(Plugin):
    name = "boost"
    pattern = "\\^(?P<boost>[0-9]*(\\.[0-9]+)?)($|(?=[ \t\r\n)]))"

    @syntax()
    def find_boost(self, parser: 'parsing.QueryParser'):
        # This syntax just leaves a marker with the boost amount; then the
        # filter finds the markers and applies the boost to the previous query
        def make_boost(ctx: peg.Context):
            scale = float(ctx["boost"])
            return self.Boost(scale)

        return (
            peg.Regex(self.pattern) +
            peg.Do(make_boost)
        ).named("boost")

    @qfilter(510)
    def apply_boost(self, parser: 'parsing.QueryParser', qs: query.Query
                    ) -> query.Query:
        if qs.is_leaf():
            return qs

        newkids = []
        for subq in qs.children():
            if isinstance(subq, self.Boost) and newkids:
                boost = newkids[-1].boost * subq.scale
                newkids[-1].set_boost(boost)
            else:
                newkids.append(subq)
        return qs.with_children(newkids)

    class Boost(query.Term):
        def __init__(self, scale):
            super(BoostPlugin.Boost, self).__init__(None, scale)
            self.scale = scale


class FieldAliasPlugin(Plugin):
    name = "field_alias"

    def __init__(self, fieldmap: Dict[str, Sequence[text_type]]):
        self.fieldmap = fieldmap

    @syntax(-1)
    def find_fields_with_aliasing(self, parser: 'parsing.QueryParser'):
        schema = parser.schema
        fields_plugin = parser.plugin("fields")  # type: FieldsPlugin

        lookup = {}
        for k, vs in self.fieldmap.items():
            for v in vs:
                lookup[v] = k

        def known_fieldname(ctx: peg.Context):
            fname = ctx["fname"]
            if fname not in lookup:
                return False
            if schema:
                return lookup[fname] in schema
            else:
                return True

        return (
            peg.Regex(fields_plugin.pattern) +
            peg.If(known_fieldname) +
            peg.Do(lambda ctx: lookup[ctx["fname"]]).set("aname") +
            peg.Parsed(parser, self.name, field_from="aname")
        ).named("fieldalias")


class CopyFieldPlugin(Plugin):
    name = "copyfield"

    def __init__(self, fieldmap: Dict[str, str],
                 group: GroupFnType=None,
                 two_way=False):
        """
        :param fieldmap: a dictionary mapping names of fields to copy to the
            names of the destination fields.
        :param group: the type of group to create in place of the original
            token. You can specify ``group=None`` to put the copied node
            "inline" next to the original node instead of in a new group.
        :param two_way: if True, the plugin copies both ways, so if the user
            specifies a query in the 'toname' field, it will be copied to
            the 'fromname' field.
        """

        self.fieldmap = fieldmap
        self.group = group
        if two_way:
            # Add in reversed mappings
            fieldmap.update(dict((v, k) for k, v in fieldmap.items()))

    # Run after FieldsPlugin fills in missing field names
    @qfilter(150)
    def copy_fields(self, parser: 'parsing.QueryParser', qs: query.Query
                    ) -> query.Query:
        fieldmap = self.fieldmap
        group = self.group

        if qs.is_leaf():
            # Handle the case where the current query is a leaf
            if qs.field() in fieldmap:
                # Create a copy with the new field name
                newq = qs.with_fieldname(fieldmap[qs.field()])
                if group:
                    qs = group([qs, newq])
                else:
                    qs = parser.make_group([qs, newq])

        elif group is not None:
            # If we're grouping, we can recursively call the above
            qs = qs.with_children([self.copy_fields(parser, q)
                                     for q in qs.children()])

        else:
            # If group is None, we need to put the copied queries inline,
            # so we need to treat it specially at this level
            newkids = []
            for subq in qs.children():
                if subq.is_leaf():
                    fieldname = subq.field()
                    if fieldname in fieldmap:
                        newname = fieldmap[fieldname]
                        newkids.append(subq.with_fieldname(newname))
                else:
                    subq = subq.with_children([self.copy_fields(parser, q)
                                              for q in subq.children()])
                newkids.append(subq)
            qs = qs.with_children(newkids)

        return qs


class MultifieldPlugin(Plugin):
    name = "multifield"

    def __init__(self, fieldnames: Sequence[str],
                 fieldboosts: Dict[str, float]=None,
                 group: GroupFnType=query.Or):
        self.fieldnames = fieldnames
        self.boosts = fieldboosts or {}
        self.group = group

    # Run before FieldsPlugin fills in missing field names
    @qfilter(90)
    def do_multifield(self, parser: 'parsing.QueryParser', qs: query.Query
                      ) -> query.Query:
        group = self.group

        if qs.is_leaf():
            if not qs.field():
                qs = group([qs.with_fieldname(fn) for fn in self.fieldnames])
        else:
            qs.set_children([self.do_multifield(parser, subq)
                             for subq in qs.children()])
        return qs


class OperatorPlugin(Plugin):
    syntax_priority = 0

    def __init__(self, name: str, pattern: str, fn: GroupFnType,
                 priority: int=0, leftward: bool=False, greedy_left=True,
                 greedy_right=True):
        self.name = name
        self.pattern = pattern
        self.fn = fn
        self.priority = priority
        self.leftward = leftward
        self.greedy_left = greedy_left
        self.greedy_right = greedy_right

        self.opclass = type(name.capitalize() + 'Op', (query.Term,), {})

    def __repr__(self):
        return "<%s %s>" % (type(self).__name__, self.name)

    def syntaxes(self, parser: 'parsing.QueryParser') -> Syntaxes:
        e = (
            peg.Regex(self.pattern).set("text") +
            peg.Do(lambda ctx: self.opclass(ctx.fieldname, ctx["text"]))
        ).named(self.name)
        return [(e, self.syntax_priority)]

    def filters(self, parser: 'parsing.QueryParser'):
        return [(self.do_operator, self.priority)]

    def do_operator(self, parser: 'parsing.QueryParser', qs: query.Query
                    ) -> query.Query:
        if qs.is_leaf():
            return qs

        # Make a copy of the query children
        qlist = list(qs.children())
        if not qlist:
            return qs

        if self.leftward:
            # Collect from right to left
            i = len(qlist) - 1
            delta = -1
            check = lambda i: i >= 0
        else:
            # Collect from left to right
            i = 0
            delta = 1
            check = lambda i: i < len(qlist)

        changed = False
        while check(i):
            subq = qlist[i]
            if isinstance(subq, self.opclass):
                qlist, i = self.replace_op(parser, qs, qlist, i)
                changed = True
                continue
            else:
                i += delta

        if changed:
            # Merge sub-queries... this seems wasteful, but we need to do
            # this before recursing below so the tree doesn't get crazy deep
            qs = qs.with_children(qlist).merge_subqueries()

        # Make a new parent from the result of recursing on the new children
        qs = qs.with_children([self.do_operator(parser, q)
                               for q in qs.children()])

        return qs

    @staticmethod
    def _list_to_query(parent, qlist):
        if len(qlist) == 1:
            return qlist[0]
        else:
            return parent.with_children(qlist)

    def replace_op(self, parser: 'parsing.QueryParser', qtype, qlist: List,
                   pos: int) -> Tuple[List[query.Query], int]:
        raise NotImplementedError


class PrefixOpPlugin(OperatorPlugin):
    def __init__(self, name: str, pattern: str, fn: GroupFnType,
                 priority: int=-1, leftward: bool=False,
                 greedy: bool=False):
        super(PrefixOpPlugin, self).__init__(name, pattern, fn,
                                             priority=priority,
                                             leftward=leftward,
                                             greedy_right=greedy)

    def replace_op(self, parser: 'parsing.QueryParser', parent,
                   qlist: List[query.Query],
                   pos: int) -> Tuple[List[query.Query], int]:
        del qlist[pos]
        if pos < len(qlist):
            if self.greedy_right:
                right = self._list_to_query(parent, qlist[pos:])
                qlist = qlist[:pos] + [right]
            else:
                qlist[pos] = self.fn(qlist[pos])

            if self.leftward:
                pos -= 1

        return qlist, pos


class InfixOpPlugin(OperatorPlugin):
    def replace_op(self, parser: 'parsing.QueryParser', parent,
                   qlist: List[query.Query],
                   pos: int) -> Tuple[List[query.Query], int]:
        if not (0 < pos < len(qlist) - 1):
            del qlist[pos]
            if self.leftward:
                pos -= 1
            return qlist, pos

        if self.greedy_left:
            before = []
            left = self._list_to_query(parent, qlist[:pos])
            newpos = 0
        else:
            before = qlist[:pos]
            left = qlist[pos - 1]
            newpos = pos - 1

        if self.greedy_right:
            right = self._list_to_query(parent, qlist[pos + 1:])
            after = []
        else:
            right = qlist[pos + 1]
            after = qlist[pos + 1:]

        qlist = before + [self.fn([left, right])] + after
        return qlist, newpos


class PostfixOpPlugin(OperatorPlugin):
    def __init__(self, name: str, pattern: str, fn: GroupFnType,
                 priority: int=-1, leftward: bool=True,
                 greedy: bool=False):
        super(PostfixOpPlugin, self).__init__(name, pattern, fn,
                                              priority=priority,
                                              leftward=leftward,
                                              greedy_left=greedy)

    def replace_op(self, parser: 'parsing.QueryParser', parent,
                   qlist: List[query.Query],
                   pos: int) -> Tuple[List[query.Query], int]:
        del qlist[pos]
        if pos == 0:
            return qlist, 0

        if self.greedy_left:
            left = self._list_to_query(parent, qlist[:pos])
            qlist = [self.fn(left)] + qlist[pos:]
        else:
            qlist[pos - 1] = self.fn(qlist[pos - 1])
        return qlist, pos


class PlusMinusPlugin(Plugin):
    name = "plus_minus"

    def __init__(self, plus="(^|(?<=(\s|[()])))\\+",
                 minus="(^|(?<=(\s|[()])))-"):
        self.plus = plus
        self.minus = minus

    @syntax()
    def find_plus(self, parser: 'parsing.QueryParser') -> 'peg.Expr':
        return (peg.Regex(self.plus).set("text") +
                peg.Do(lambda ctx: self.PlusMinus(ctx["text"], True))
                ).named("plus")

    @syntax()
    def find_minus(self, parser: 'parsing.QueryParser'):
        return (peg.Regex(self.minus).set("text") +
                peg.Do(lambda ctx: self.PlusMinus(ctx["text"], False))
                ).named("minus")

    @qfilter(510)
    def do_plusminus(self, parser: 'parsing.QueryParser', qs: query.Query
                     ) -> query.Query:
        if qs.is_leaf():
            return qs

        required = []
        optional = []
        banned = []

        next_list = optional
        for subq in qs.children():
            if isinstance(subq, self.PlusMinus):
                if subq.is_plus:
                    next_list = required
                else:
                    next_list = banned
            else:
                next_list.append(self.do_plusminus(parser, subq))
                next_list = optional

        qs = query.Or(optional)
        if required:
            qs = query.AndMaybe(query.And(required), qs)
        if banned:
            qs = query.AndNot(qs, query.Or(banned))
        return qs

    class PlusMinus(query.Term):
        def __init__(self, text: str, is_plus: bool):
            super(PlusMinusPlugin.PlusMinus, self).__init__(None, text)
            self.is_plus = is_plus

        def __eq__(self, other):
            return (
                type(self) is type(other) and
                self.text == other.text and
                self.is_plus == other.is_plus
            )


class GtLtPlugin(Plugin):
    name = "gt_lt"

    @syntax()
    def find_gtlt(self, parser: 'parsing.QueryParser'):
        def make_rel(ctx: peg.Context):
            fname = ctx.fieldname
            rel = ctx["rel"]
            q = ctx["subj"]  # type: query.Query
            if q.is_leaf():
                value = q.query_text()
                if rel == "<":
                    q = query.Range(fname, None, value, False, True)
                elif rel == ">":
                    q = query.Range(fname, value, None, True, False)
                elif rel in ("<=", "=<"):
                    q = query.Range(fname, None, value, False, False)
                elif rel in (">=", "=>"):
                    q = query.Range(fname, value, None, False, False)
                q.analyzed = False
            return q

        return (
            peg.Regex(r"(?P<rel>(<=|>=|<|>|=<|=>))") +
            peg.Parsed(parser, self.name).set("subj") +
            peg.Do(make_rel)
        ).named("gtlt")


class EveryPlugin(Plugin):
    name = "every"
    pattern = "*:*"

    @syntax(-3)
    def find_every(self, parser: 'parsing.QueryParser'):
        return (
            peg.Str(self.pattern) +
            peg.Do(lambda ctx: query.Every("*"))
        ).named("every")


class FunctionPlugin(Plugin):
    name = "function"
    name_pattern = "\w+"

    def __init__(self, name: str, fn, fn_name: str=None, takes_query: bool=True):
        self.name = name
        self.fn = fn
        self.fn_name = fn_name or "#" + name
        self.takes_query = takes_query

    @syntax()
    def find_functions(self, parser: 'parsing.QueryParser'):
        from ast import literal_eval

        value = peg.Apply(peg.Or([
            peg.QuotedString('"', esc_char="\\"),
            peg.QuotedString("'", esc_char="\\"),
            peg.Regex("[0-9]+([.][0-9]+)?", may_be_empty=False),
        ]), literal_eval)

        args = peg.Optional(
            peg.Str("[") +
            peg.ZeroOrMore(
                value +
                peg.Hidden(
                    peg.ws.opt() +
                    peg.Str(",").opt() +
                    peg.ws.opt()
                )
            ).set("values") +
            peg.Str("]") +
            peg.Get("values")
        )

        def apply_function(ctx: peg.Context):
            if ctx["args"]:
                args = list(ctx["args"][0])
            else:
                args = []

            if self.takes_query:
                args.append(ctx["took"])

            return self.fn(*args)

        seq = (
            peg.wordstart +
            peg.Str(self.fn_name) +
            args.set("args") +
            peg.ws.opt()
        )
        if self.takes_query:
            seq += peg.Parsed(parser, self.name).set("took")
        seq += peg.Do(apply_function)
        return seq("function")


class PseudoFieldPlugin(Plugin):
    name = "pseudo_field"

    def __init__(self, name: str, fn: Callable[[query.Query], query.Query],
                 field_name: str=None):
        self.name = name
        self.fieldname = field_name or name
        self.fn = fn

    # Run before the fields expr
    @syntax(-2)
    def find_fields_with_aliasing(self, parser: 'parsing.QueryParser'
                                  ) -> 'peg.Expr':
        fields_plugin = parser.plugin("fields")  # type: FieldsPlugin

        def apply_pseudo(ctx: peg.Context):
            return self.fn(ctx["subj"])

        return (
            peg.Regex(fields_plugin.pattern) +
            peg.If(lambda ctx: ctx["fname"] == self.fieldname) +
            peg.Parsed(parser, self.name).set("subj") +
            peg.Do(apply_pseudo)
        ).named("pseudofield")


class StoredQueryPlugin(Plugin):
    name = "stored_query"

    def __init__(self, queries: 'Dict[str, query.Query]',
                 ignore_case: bool=False):
        self.queries = queries
        self.ignore_case = ignore_case

    @syntax()
    def find_joins(self, parser: 'parsing.QueryParser') -> 'peg.Expr':
        import re

        queries = self.queries
        pattern = re.compile("|".join(re.escape(name) for name in queries))

        return (
            peg.Regex(pattern, ignore_case=self.ignore_case).set("name") +
            peg.If(lambda ctx: ctx["name"] in queries) +
            peg.Do(lambda ctx: queries[ctx["name"]])
        )


class RelationPlugin(Plugin):
    name = "relation"

    def __init__(self, relate_keyword="RELATE", in_keyword="IN",
                 to_keyword="TO", ignore_case=False):
        self.relate_keyword = relate_keyword
        self.in_keyword = in_keyword
        self.to_keyword = to_keyword
        self.ignore_case = ignore_case

    def _kw(self, string):
        return (
            peg.ws.opt() +
            peg.Str(string, ignore_case=self.ignore_case) +
            peg.ws.opt()
        ).hide()

    @syntax()
    def find_joins(self, parser: 'parsing.QueryParser') -> 'peg.Expr':
        def make_relation(ctx: peg.Context):
            from whoosh.query.joins import RelationQuery

            return RelationQuery(ctx["left_key"], ctx["left_query"],
                                 ctx["right_key"], ctx["right_query"])

        keyname_expr = peg.Regex("[A-Za-z_][A-Za-z0-9_]*")

        relate_expr = self._kw(self.relate_keyword).hide()
        in_expr = self._kw(self.in_keyword).hide()
        to_expr = self._kw(self.to_keyword).hide()

        # RELATE id IN type:album artist:bowie TO album_id IN type:song
        return (
            relate_expr +  # RELATE
            keyname_expr.set("left_key") +  # <field name>
            in_expr +  # IN
            peg.Parsed(parser, "leftq", to_expr).set("left_query") +  # <query>
            to_expr +  # TO
            keyname_expr.set("right_key") +  # <field_name>
            in_expr +  # IN
            peg.Parsed(parser, "rightq").set("right_query") +  # <query>
            peg.Do(make_relation)
        )




