import logging
import sys
from datetime import datetime
from typing import Callable, Dict, List, Optional, Sequence, Tuple

from whoosh import fields, query
from whoosh.ifaces import queries
from whoosh.parsing import peg
from whoosh.parsing import plugins as plugs
from whoosh.util.text import rcompile


logger = logging.getLogger(__name__)


# List of plugins included automatically in the default QueryParser
DEFAULT_PLUGINS = (
    # plugs.EscapePlugin(),
    plugs.WhitespacePlugin(),
    plugs.FieldsPlugin(),
    plugs.SingleQuotePlugin(),
    plugs.WildcardPlugin(),
    plugs.PhrasePlugin(),
    plugs.RangePlugin(),
    plugs.GroupPlugin(),

    plugs.InfixOpPlugin("andnot", r"(?<=\s)ANDNOT(?=\s)",
                        lambda qs: query.AndNot(*qs)),
    plugs.InfixOpPlugin("require", r"(^|(?<=\s))REQUIRE(?=\s)",
                        lambda qs: query.Require(*qs)),
    plugs.InfixOpPlugin("andmaybe", r"(?<=\s)ANDMAYBE(?=\s)",
                        lambda qs: query.AndMaybe(*qs)),
    plugs.PrefixOpPlugin("not", r"(^|(?<=(\s|[()])))NOT(?=\s)",
                         query.Not, priority=-1),
    plugs.InfixOpPlugin("or", r"(?<=\s)OR(?=\s)", query.Or, leftward=False),
    plugs.InfixOpPlugin("and", r"(?<=\s)AND(?=\s)", query.And, leftward=False),

    plugs.EveryPlugin(),
    plugs.BoostPlugin(),
    plugs.FuzzyTermPlugin(),
)


class QueryParser:
    """
    Parses query strings into trees of `whoosh.query.Query` objects.
    """

    # Fallback regular expression to use when the parser must tokenize a string
    # without a `whoosh.fields.FieldType` object
    word_expr = rcompile(r"\S+")

    def __init__(self, fieldname: str,
                 schema: fields.Schema=None,
                 termclass: type=query.Term,
                 group: type=query.And,
                 plugins: 'List[plugs.Plugin]'=None,
                 esc_char: str="\\",
                 base_datetime: datetime=None):
        self.fieldname = fieldname
        self.schema = schema
        self.group = group
        self.termclass = termclass
        self.esc_char = esc_char
        self.base_datetime = base_datetime or datetime.utcnow()

        plugins = plugins if plugins is not None else self.default_plugins()
        self.plugins = plugins  # type: List[plugs.Plugin]
        self._main_expr = None  # type: peg.Expr
        self._field_exprs = {}  # type: Dict[str, peg.Expr]

    def set_field_expr(self, fieldname: str, expr: 'peg.Expr'):
        self._field_exprs[fieldname] = expr

    def default_plugins(self) -> 'List[plugs.Plugin]':
        # Remember to return a copy!
        return list(DEFAULT_PLUGINS)

    def plugin(self, name: str) -> 'plugs.Plugin':
        for p in self.plugins:
            if p.name == name:
                return p
        raise KeyError(name)

    def __getattr__(self, name: str):
        if name.endswith("_plugin"):
            return self.plugin(name[:-7])
        else:
            raise AttributeError(name)

    def _plugin_index(self, name: str):
        for i, p in enumerate(self.plugins):
            if p.name == name:
                return i
        raise NameError("No plugin named %r" % name)

    def add_plugin(self, plugin: 'plugs.Plugin'):
        self.plugins.append(plugin)

    def remove_plugin(self, name: str):
        self._main_expr = None  # Invalidate cached main expression
        self.plugins = [p for p in self.plugins if p.name != name]

    def remove_plugin_class(self, cls: type):
        self._main_expr = None  # Invalidate cached main expression
        self.plugins = [p for p in self.plugins if not isinstance(p, cls)]

    def has_plugin(self, name: str):
        return any(p.name == name for p in self.plugins)

    def has_plugin_class(self, cls: type):
        return any(isinstance(p, cls) for p in self.plugins)

    def replace_plugin(self, name: str, plugin: 'plugs.Plugin'):
        self._main_expr = None  # Invalidate cached main expression
        for i, p in enumerate(self.plugins):
            if p.name == name:
                self.plugins[i] = plugin
                return
        self.plugins.append(plugin)

    def make_group(self, contents: Sequence[query.Query]) -> query.Query:
        if isinstance(self.group, query.Query):
            return self.group.with_children(contents)
        else:
            return self.group(contents)

    def term_query(self, fieldname: str, text: str, termclass: type=None,
                   boost: float=1.0) -> query.Query:
        termclass = termclass or self.termclass
        return termclass(fieldname, text, boost=boost)

    def _priorized(self, methodname):
        # methodname is "taggers" or "filters". Returns a priorized list of
        # tagger objects or filter functions.
        items_and_priorities = []
        for plugin in self.plugins:
            # Call either .taggers() or .filters() on the plugin
            method = getattr(plugin, methodname)
            for item in method(self):
                assert len(item) == 2, (method, item)
                assert callable(item[0])
                assert isinstance(item[1], int)
                items_and_priorities.append(item)
        # Sort the list by priority (lower priority runs first)
        items_and_priorities.sort(key=lambda x: x[1])
        logger.debug("Items %s for %s", items_and_priorities, methodname)

        # Return the sorted list without the priorities
        return [item for item, _ in items_and_priorities]

    def syntaxes(self) -> 'List[peg.Expr]':
        return self._priorized("syntaxes")

    def filters(self) -> 'List[Callable[QueryParser, query.Query], query.Query]':
        return self._priorized("filters")

    def context(self, fieldname: str=None) -> 'peg.Context':
        # Put the main expr and current fieldname on the context so deeply
        # nested exprs can read and/or override them as they parse
        context = peg.Context(self.main_expr(), fieldname=None)

        # Copy any custom per-field exprs set up on this parser to the context
        context.field_exprs.update(self._field_exprs)

        # Give the plugins a chance to modify the context
        for plugin in self.plugins:
            plugin.modify_context(context)

        return context

    def main_expr(self) -> 'peg.Expr':
        if self._main_expr is None:
            syntaxes = self.syntaxes()
            logger.debug("Parser exprs %r", syntaxes)
            self._main_expr = peg.Or(syntaxes).named("MAIN")
        return self._main_expr

    def parse_single(self, s: str, at: int, ctx: 'peg.Context', name: str=None,
                     tokenize: bool=True) -> Tuple[int, query.Query]:
        if at == len(s):
            raise peg.Miss(s, at, "Called parse_element at EOS")

        # Get the current expression at the point this method was called; note
        # that this may not be the "main" expression, since higher-level exprs
        # may have passed down a modified version to where this was called
        expr = ctx.expr
        try:
            logger.debug("Parse single expr at %d (depth %d)", at, ctx.depth)
            at, value = expr.parse(s, at, ctx)
            logger.debug("Found %r, moved to %d", value, at)
            return at, value
        except peg.Miss:
            # Use StringUntil to take until the next parser match and turn
            # that into a term query
            logger.debug("No expr matched at %d", at)
            textexpr = peg.StringUntil(expr, self.esc_char, matches_end=True)
            try:
                newat, text = textexpr.parse(s, at, ctx)
            except peg.Miss:
                # This should never happen! Since textexpr has matches_end=True,
                # it should match when it gets to the end of the string
                raise Exception("Word parser missed")
            else:
                # Nothing matched until the end of the string; put it all in
                # one big term
                logger.debug("Took text %r", text)
                q = self.term_query(ctx.fieldname, text).set_extent(at, newat)
                q.analyzed = False
                logger.debug("Taken text converted to %r", q)

                # q = self.termclass(ctx.fieldname, string).set_extent(at, i)
                # q.analyzed = False
                # logger.debug("No match, took %r as %r, moved to %d",
                #              text, q, i)
                return newat, q

    def parse_to_list(self, text: str, fieldname: str=None, tokenize: bool=True,
                      ) -> 'List[query.Query]':
        logger.debug("Parsing string %r", text)
        ctx = self.context(fieldname=fieldname)

        i = 0
        buffer = []
        while i < len(text):
            newi, value = self.parse_single(text, i, ctx, "_",
                                            tokenize=tokenize)
            if newi <= i:
                raise Exception("Parser didn't move forward (%r)" % value)

            buffer.append(value)
            i = newi

        logger.debug("Parsed list %r", buffer)
        return buffer

    def parse(self, text: str, normalize: bool=True, fieldname: str=None,
              filters: bool=True, tokenize: bool=True) -> 'query.Query':
        qlist = self.parse_to_list(text, fieldname=fieldname, tokenize=tokenize)
        if not qlist:
            return query.ErrorQuery("Nothing parsed")

        q = self.make_group(qlist) if len(qlist) > 1 else qlist[0]
        logger.debug("Parsed query %r", q)

        if filters:
            q = self.apply_filters(q)

        if normalize:
            q = q.normalize()
            logger.debug("Normalized query %r", q)

        return q

    def apply_filters(self, q: query.Query):
        logger.debug("Applying filters")
        for fn in self.filters():
            logger.debug("Applying filter %r", fn)
            q = fn(self, q)
            logger.debug("Query now %r", q)

        logger.debug("Analyzing unanalyzed text")
        q = self.filter_unanalyzed_terms(q)
        logger.debug("Final query %r", q)

        return q

    def filter_unanalyzed_terms(self, q: query.Query) -> query.Query:
        """
        Takes the parsed query tree and recurses through it looking for Term
        queries  with `.analyzed=False`, indicating they were not taken for a
        specific field and have not been run through an analyzer. Since after
        parsing the queries now have a field assigned, we can retroactively
        analyze the text of these queries.
        """

        is_analyzed = getattr(q, "analyzed")
        logger.debug("Checking %r for analysis (analyzed=%s)", q, is_analyzed)

        # Note that Query.analyzed = True is the default; it's only synthesized
        # queries containing text from "between" parsed expressions that has
        # .analyzed=False
        if q.is_leaf() and not getattr(q, "analyzed"):
            text = q.query_text()
            logger.debug("Analyzing text %r in %r", text, q)

            assert isinstance(q.startchar, int)
            if isinstance(q, query.Term):
                fieldname = q.field() or self.fieldname
                q = self.text_to_query(fieldname, text, q.boost,
                                       startchar=q.startchar)
                logger.debug("Converted to %r", q)
            q.analyzed = True

        elif not q.is_leaf():
            logger.debug("Recursing analysis into %r", q)
            q = q.with_children([self.filter_unanalyzed_terms(q)
                                 for q in q.children()])
        return q

    def text_to_query(self, fieldname: str, text: str, boost: float=1.0,
                      startchar: int=0, tokenize: bool=True,
                      removestops: bool=True) -> query.Query:
        """
        Analyzes the given text and generates a Term query (or possibly another
        query type if the text analyzes to multiple tokens).

        :param fieldname: the name of the field to use to analyze the text.
        :param text: the text to analyze.
        :param boost: a boost to apply to the resulting query.
        :param startchar: treat tokens as if the analysis started at this
            character index in a larger text.
        :param tokenize: break the text into tokens. If this is False, the
            text is not tokenized but the analysis is applied to the entire
            string.
        :param removestops: the analyzer should remove stop-words if it's
            configured to do so.
        """

        logger.debug("Analyzing text %r with fieldname %r", text, fieldname)
        schema = self.schema
        if schema and fieldname in schema and schema[fieldname].self_parsing():
            # If the field wants to parse itself, let it
            q = schema[fieldname].parse_query(fieldname, text, boost=boost)
            logger.debug("Field self-parsed to %r", q)
            return q
        else:
            tokens = self.text_to_tokens(fieldname, text, startchar, tokenize,
                                         removestops)
            return self.tokens_to_query(fieldname, tokens, boost)

    def first_token(self, fieldname: str, text: str, tokenize: bool=True,
                    removestops: bool=True):
        tokens = self.text_to_tokens(fieldname, text, tokenize=tokenize,
                                     removestops=removestops)
        return tokens[0][0]

    def text_to_tokens(self, fieldname: str, text: str, startchar: int=0,
                       tokenize: bool=True, removestops: bool=True,
                       tag: str="_") -> 'List[Tuple[str, int, int]]':
        """
        Analyzes the given text and returns a list of
        `(text, startchar, endchar)` tuples.

        :param fieldname: the name of the field to use to analyze the text.
        :param text: the text to analyze
        :param startchar: treat tokens as if the analysis started at this
            character index in a larger text.
        :param tokenize: break the text into tokens. If this is False, the
            text is not tokenized but the analysis is applied to the entire
            string.
        :param removestops: the analyzer should remove stop-words if it's
            configured to do so.
        """

        schema = self.schema
        if not schema or fieldname not in schema:
            logger.debug("%s: No field %r, using fallback tokenizer",
                         tag, fieldname)
            tokens = self._fallback_tokens(text, startchar, tokenize)
        else:
            # Get the field
            field = schema[fieldname]

            logger.debug("%s: Converting %r to tokens using field %r",
                         tag, text, field)
            if isinstance(field, fields.TokenizedField):
                tokens = [
                    (token.text,
                     startchar + token.startchar,
                     startchar + token.endchar)
                    for token in field.tokenize(text, mode="query", chars=True,
                                                tokenize=tokenize,
                                                removestops=removestops)
                ]
            else:
                tokens = [(text, startchar, startchar + len(text))]

        logger.debug("%s: Found tokens %r", tag, tokens)
        return tokens

    def tokens_to_query(self, fieldname: str,
                        tokens: 'Sequence[Tuple[str, int, int]]',
                        boost: float=1.0) -> 'query.Query':
        if not tokens:
            return queries.IgnoreQuery()

        # Ask the field how to handle text that analyzes into multiple tokens
        schema = self.schema
        if schema and fieldname in schema:
            multitoken_style = self.schema[fieldname].multitoken_query
        else:
            multitoken_style = "default"

        if len(tokens) == 1 or multitoken_style == "first":
            # Throw away all but the first token
            term, sc, ec = tokens[0]
            q = self.term_query(fieldname, term, boost=boost).set_extent(sc, ec)

        elif multitoken_style == "phrase":
            # Turn the tokens into a phrase
            texts = [token[0] for token in tokens]
            sc = tokens[0][1]
            ec = tokens[-1][2]
            q = query.Phrase(fieldname, texts, boost=boost).set_extent(sc, ec)

        else:
            # The other multitoken styles all involve turning the tokens into
            # term queries and then wrapping them in a compound query
            term_qs = [
                self.term_query(fieldname, term, boost=boost).set_extent(sc, ec)
                for term, sc, ec in tokens
            ]

            if multitoken_style == "default":
                q = self.group(term_qs)
            elif multitoken_style == "and":
                q = query.And(term_qs)
            elif multitoken_style == "or":
                q = query.Or(term_qs)
            else:
                raise query.ValueError(
                    "Unknown multitoken_query value %r" % spec
                )
        logging.debug("Converted to query %r", q)

        return q

    def _fallback_tokens(self, text: str, startchar: int,
                         tokenize: bool) -> 'List[Tuple[str, int, int]]':
        # If we don't have a field to analyze text with, just use the simple
        # regular expression on this class to pull terms from the text

        if tokenize:
            tokens = [
                (match.group(0),
                 startchar + match.start(),
                 startchar + match.end())
                for match in self.word_expr.finditer(text)
            ]
        else:
            tokens = [(text, startchar, startchar + len(text))]
        return tokens






