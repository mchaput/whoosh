import copy
import logging
import re
import sys
from abc import abstractmethod
import typing
from typing import Any, Callable, Dict, Iterable, List, Sequence, Tuple, Union
# from typing.re import Pattern

from whoosh import query
from whoosh.parsing import parsing
from whoosh.util.text import rcompile


logger = logging.getLogger(__name__)


# Typing-only imports
if typing.TYPE_CHECKING:
    from whoosh import fields

# Type aliases

# Actions are called with the string, start, end, and parsed object
ActionFn = 'Callable[[str, int, int, Any], Any]'
# Conditions are like actions but they return a boolean
# ConditionType = Callable[[str, int, Sequence], bool]
# Operators can work with either an element or a string
OpArgType = 'Union[str, ParserElement]'
# `Do` and `If` functions
FuncType = 'Callable[[Context], Any]'


# Exceptions

class ParseException(Exception):
    def __init__(self, pstr: str, at: int=0, msg: str=None, elem=None):
        self.pstr = pstr
        self.at = at
        self.msg = msg
        self.elem = elem

    @property
    def line_number(self) -> int:
        return lineno(self.at, self.pstr)

    @property
    def column(self) -> int:
        return col(self.at, self.pstr)

    @property
    def line(self) -> str:
        return line(self.at, self.pstr)

    def marked_line(self, mark: str="|") -> str:
        linestr = self.line
        column = self.column
        return "".join((linestr[:column], mark, linestr[column:])).strip()

    def __repr__(self) -> str:
        return "<%s line:%d col:%d>" % (self.msg, self.line_number, self.column)


class Miss(ParseException):
    pass


class FatalError(ParseException):
    pass


class ParseError(ParseException):
    def __init__(self, pe: ParseException):
        super(ParseError, self).__init__(pe.pstr, pe.at, pe.msg, pe.elem)


class RecursiveGrammarError(Exception):
    def __init__(self, elems: 'List[Expr]'):
        self.elems = elems


# Utility classes

class Context:
    def __init__(self, expr: 'Expr', debug: bool=False, fieldname: str=None):
        self.expr = expr
        self.debug = debug
        self.env = {}
        self.lookup = {}
        self.cache = {}
        self.fieldname = fieldname
        self.field_exprs = {}  # type: Dict[str, Expr]
        self.depth = 0

    # def register(self, expr: 'Element', name: str=None):
    #     name = name or expr.name
    #     if not name:
    #         raise ValueError("No name given")
    #     if name in self.lookup:
    #         raise NameError("%r already in context" % (name,))
    #     self.lookup[name] = expr

    def push(self) -> 'Context':
        return PushedContext(self)

    def with_fieldname(self, fieldname: str) -> 'Context':
        c = copy.copy(self)
        c.fieldname = fieldname
        return c

    def __contains__(self, name: str) -> bool:
        return name in self.env

    def __getitem__(self, name: str) -> Any:
        return self.env[name]

    def __setitem__(self, name: str, value: Any):
        self.env[name] = value

    def get(self, name: str, default: Any=None) -> Any:
        return self.env.get(name, default)

    def update(self, d: Dict):
        self.env.update(d)

    def full_env(self) -> Dict:
        return self.env


class PushedContext(Context):
    def __init__(self, parent: Context):
        self.parent = parent
        self.env = {}
        self.fieldname = parent.fieldname
        self.depth = parent.depth + 1
        self._expr = None
        self._debug = None

    @property
    def expr(self) -> 'Expr':
        return self.parent.expr if self._expr is None else self._expr

    @expr.setter
    def expr(self, expr: 'Expr'):
        self._expr = expr

    @property
    def debug(self) -> bool:
        return self.parent.debug if self._debug is None else self._debug

    @debug.setter
    def debug(self, debug: bool):
        self._debug = debug

    @property
    def lookup(self):
        return self.parent.lookup

    @property
    def cache(self):
        return self.parent.cache

    @property
    def field_exprs(self):
        return self.parent.field_exprs

    # def register(self, expr: 'Element', name: str=None):
    #     return self.parent.register(expr, name)

    def __contains__(self, name: str) -> bool:
        return name in self.env or name in self.parent

    def __getitem__(self, name: str) -> Any:
        if name in self.env:
            return self.env[name]
        else:
            return self.parent[name]

    def get(self, name: str, default: Any=None) -> Any:
        if name in self.env:
            return self.env[name]
        else:
            return self.parent.get(name, default)

    def full_env(self) -> Dict:
        d = self.parent.full_env().copy()
        d.update(self.env)
        return d


# Helper functions

def col(at: int, s: str) -> int:
    return 1 if at < len(s) and s[at] == '\n' else at - s.rfind("\n", 0, at)


def lineno(at: int, s: str) -> int:
    return s.count("\n", 0, at) + 1


def line(at: int, s: str) -> str:
    last_nl = s.rfind("\n", 0, at)
    next_nl = s.find("\n", at)
    if next_nl >= 0:
        return s[last_nl + 1:next_nl]
    else:
        return s[last_nl + 1:]


def compile_expr(expr):
    return compile(expr, '<string>', 'eval', dont_inherit=True)


# Elements

class Expr:
    hidden = False

    def __init__(self, name: str=""):
        self.name = name
        self.may_be_empty = False
        self.debug = False
        self.error_template = ""
        self.is_plaintext = False

        self.debug_before_action = None
        self.debug_after_action = None
        self.debug_exception_action = None

    @property
    def error(self):
        return self.error_template

    # def register(self, context: Context):
    #     if self.name:
    #         context.lookup[self.name] = self

    def dump(self, stream=sys.stdout, level=0):
        print("  " * level, type(self).__name__, self.name, repr(self),
              file=stream)

    def named(self, name: str) -> 'Expr':
        obj = copy.copy(self)
        obj.name = name
        return obj

    def _debug(self, ctx: Context, *args):
        if self.debug or ctx.debug:
            head = "%s%s%s: " % (
                "  " * ctx.depth,
                type(self).__name__,
                "(%s)" % (self.name,) if self.name else ""
            )
            print(head, *args)

    @abstractmethod
    def _parse(self, s: str, at: int, ctx: Context) -> Tuple[int, Any]:
        raise NotImplementedError

    def parse(self, s: str, at: int, ctx: Context,
              ) -> Tuple[int, Any]:
        return self._parse(s, at, ctx)

    def try_parse(self, s: str, at: int, context: Context) -> int:
        return self.parse(s, at, context)[0]

    def matches(self, s: str, at: int=0, context: Context=None) -> bool:
        context = context or Context(self)
        try:
            self.try_parse(s, at, context)
        except (Miss, IndexError):
            return False
        else:
            return True

    def parse_string(self, s: str, context: Context=None,
                     parse_all: bool=False) -> Any:
        context = context or Context(self)
        at, value = self.parse(s, 0, context)
        if parse_all:
            StringEnd().parse(s, at, context)
        return value

    def scan_string(self, s: str, context: Context=None,
                    max_matches: int=None
                    ) -> Iterable[Tuple[Any, int, int]]:
        context = context or Context(None)
        parse = self.parse
        s_len = len(s)
        at = 0
        matches = 0
        while at <= s_len and (not max_matches or matches < max_matches):
            try:
                next_at, value = parse(s, at, context)
            except ParseException:
                at += 1
            else:
                if next_at > at:
                    yield value, at, next_at
                    at = next_at
                    matches += 1
                else:
                    at += 1

    # def transform_string(self, s: str) -> str:
    #     out = []
    #     last = 0
    #     try:
    #         for parsed, start, end in self.scan_string(s):
    #             out.append(s[last:s])
    #             if parsed:
    #                 if isinstance(parsed, ParsedValue):
    #                     out.append(parsed)
    #                 elif isinstance(parsed, ParsedList):
    #                     out.extend(parsed.values)
    #             last = end
    #         out.append(s[last:])
    #         out = [o for o in out if o]
    #         return "".join(_flatten(out))
    #
    #     except ParseException:
    #         raise

    def search_string(self, s: str, context: Context=None,
                      max_matches: int=None) -> List:
        return [value for value, start, end
                in self.scan_string(s, context, max_matches)]

    @staticmethod
    def _exprtize(other: OpArgType) -> 'Expr':
        if not isinstance(other, Expr):
            return Str(str(other))
        else:
            return other

    def find(self, name: str) -> 'Expr':
        if self.name == name:
            return self

    def replace(self, name: str, expr: 'Expr') -> 'Expr':
        if self.name == name:
            return expr
        else:
            return self

    def opt(self) -> 'Optional':
        return Optional(self)

    def star(self) -> 'ZeroOrMore':
        return ZeroOrMore(self)

    def plus(self) -> 'OneOrMore':
        return OneOrMore(self)

    def hide(self) -> 'Hidden':
        return Hidden(self)

    def set_ext(self, name: str="extent"):
        return AssignExtent(self, name)

    def set(self, name: str) -> 'Assign':
        return Assign(self, name)

    # def do(self, code: Callable) -> 'Seq':
    #     return self + Do(code)

    def __add__(self, other: OpArgType) -> 'Seq':
        if isinstance(self, Seq) and isinstance(other, Seq):
            return Seq(self.exprs + other.exprs)
        elif isinstance(self, Seq):
            return Seq(self.exprs + (self._exprtize(other),))
        elif isinstance(other, Seq):
            return Seq((self,) + other.exprs)
        else:
            return Seq((self, self._exprtize(other)))

    def __radd__(self, other: OpArgType) -> 'Seq':
        return Seq((self._exprtize(other), self))

    def __mul__(self, other: Union[int, Tuple[int, int]]) -> 'Expr':
        min_times = max_times = 0

        if isinstance(other, int):
            min_times = max_times = other
        elif isinstance(other, tuple):
            if len(other) != 2:
                raise ValueError("Repeat tuple must have two items")
            min_times, max_times = other

        if max_times is None:
            if min_times is None:
                min_times = 0

            if min_times == 0:
                return ZeroOrMore(self)
            elif min_times == 1:
                return OneOrMore(self)
            else:
                return self * min_times + ZeroOrMore(self)

        return Repeat(self, min_times, max_times)

    def __rmul__(self, other: Union[int, Tuple[int, int]]) -> 'Expr':
        return self.__mul__(other)

    def __or__(self, other: OpArgType) -> 'Expr':
        return Or([self, self._exprtize(other)])

    def __ror__(self, other: OpArgType) -> 'Expr':
        return Or([self._exprtize(other), self])

    def __invert__(self) -> 'Expr':
        return Not(self)

    def __call__(self, name: str=None) -> 'Expr':
        name = name or self.name
        return self.named(name)

    def check_recursion(self, elements: 'List[Expr]'):
        pass

    def validate(self, trace=None):
        self.check_recursion([])

    def __eq__(self, other: 'Expr') -> bool:
        return type(self) is type(other) and self.__dict__ == other.__dict__

    def __ne__(self, other):
        return not (self.__eq__(other))

    def __hash__(self):
        return hash(id(self))

    def __req__(self, other):
        return self.__eq__(other)

    def __rne__(self, other):
        return not (self.__eq__(other))


class Print(Expr):
    def __init__(self, msg: str):
        super(Print, self).__init__()
        self.msg = msg

    def _parse(self, s: str, at: int, ctx: Context) -> Tuple[int, Any]:
        print("-", self.msg, "(%d)" % at, ctx.full_env())
        return at, None


class Token(Expr):
    def __repr__(self):
        return "<%s>" % type(self).__name__

    @abstractmethod
    def _parse(self, s: str, at: int, ctx: Context) -> Tuple[int, Any]:
        raise NotImplementedError

    @property
    def error(self):
        return self.error_template % self.name


class Empty(Token):
    def __init__(self):
        super(Empty, self).__init__("Empty")
        self.may_be_empty = True

    def _parse(self, s: str, at: int, ctx: Context) -> Tuple[int, Any]:
        return at, None


class NoMatch(Token):
    """A token that will never match."""

    def __init__(self):
        super(NoMatch, self).__init__("NoMatch")
        self.may_be_empty = True

    @property
    def error(self):
        return "Unmatchable token"

    def _parse(self, s: str, at: int, ctx: Context) -> Tuple[int, Any]:
        raise Miss(s, at, self.error, self)


class Str(Token):
    """Token to exactly match a specified string."""

    def __init__(self, match: str, ignore_case: bool=False, name: str=None):
        if not match:
            raise ValueError("Can't match an empty string")
        assert isinstance(match, str)
        self.match = match

        super(Str, self).__init__(name or repr(self.match))
        self.ignore_case = ignore_case
        self.error_template = "Expected literal %s"
        self.may_be_empty = False

    def __repr__(self):
        return "<%s %r>" % (type(self).__name__, self.match)

    def _parse(self, s: str, at: int, ctx: Context) -> Tuple[int, Any]:
        match = self.match.lower() if self.ignore_case else self.match
        match_len = len(match)

        if self.ignore_case:
            target = s[at:at + match_len]
            self._debug(ctx, "Comparing", match, "to", target)
            if target.lower() == match:
                self._debug(ctx, "Found ->", at + match_len)
                return at + match_len, target

        else:
            self._debug(ctx, "Looking for", match, "at", at)
            if s.startswith(match, at):
                self._debug(ctx, "Found ->", at + match_len)
                return at + match_len, match

        raise Miss(s, at, self.error, self)


class Regex(Token):
    def __init__(self, pattern, flags: int=0, name: str=None,
                 ignore_case: bool=False, may_be_empty: bool=True):
        super(Regex, self).__init__(name or pattern)
        assert isinstance(pattern, str)
        self.pattern = pattern
        self.expr = rcompile(pattern, ignore_case=ignore_case)
        self.may_be_empty = may_be_empty
        self.name = name or repr(self.pattern)
        self.error_template = "Expected regex %s"
        self.may_be_empty = may_be_empty

    def __repr__(self):
        return "<%s %r>" % (type(self).__name__, self.pattern)

    def _parse(self, s: str, at: int, ctx: Context) -> Tuple[int, Any]:
        self._debug(ctx, "Trying", self.expr, "at", at)
        match = self.expr.match(s, at)
        if match:
            gd = match.groupdict()
            self._debug(ctx, "Found", gd, "->", match.end())
            ctx.update(gd)
            return match.end(), match.group(0)

        raise Miss(s, at, self.error, self)


class Patterns(Expr):
    """
    Takes a list of regular expression patterns. When one of the patterns
    matches, this element returns the (zero-based) index of the matching
    pattern. For example, if you supply five patterns and the third matches,
    the element will return `2`.
    """

    def __init__(self, patterns, may_be_empty=False):
        super(Patterns, self).__init__()
        self.exprs = tuple(re.compile(pattern, re.IGNORECASE)
                           for pattern in patterns)
        self.error_template = "Expected one of %r" % (patterns, )
        self.may_be_empty = may_be_empty

    def dump(self, stream=sys.stdout, level=0):
        super(Patterns, self).dump(stream, level)
        for e in self.exprs:
            e.dump(stream, level + 1)

    def _parse(self, s: str, at: int, ctx: Context):
        for i, e in enumerate(self.exprs):
            self._debug(ctx, "Trying", e, "at", at)
            match = e.match(s, at)
            if match:
                self._debug(ctx, "Found", i, "->", match.end())
                return match.end(), i

        raise Miss(s, at, self.error, self)


class QuotedString(Token):
    """Token for matching a string delimited by quote characters."""

    def __init__(self, start_char: str, end_char: str=None,
                 esc_char: str=None, name: str=None,
                 inner_extent: str=None):
        super(QuotedString, self).__init__(name or type(self).__name__)
        self.start_char = start_char
        self.end_char = end_char or start_char
        if (not self.start_char) or (not self.end_char):
            raise ValueError("Delimiters cannot be empty")
        self.esc_char = esc_char
        self.inner_extent = inner_extent

        self.error_template = "Expected qs %s"
        self.may_be_empty = False

    def __repr__(self):
        return "<%s %s%s %s>" % (type(self).__name__, self.start_char,
                                 self.end_char, self.esc_char)

    def _parse(self, s: str, at: int, ctx: Context) -> Tuple[int, Any]:
        start = at
        if at < len(s) and s.startswith(self.start_char, at):
            buf = []
            at += len(self.start_char)
            inner_start = at

            while at < len(s):
                next_end = s.find(self.end_char, at)
                next_esc = s.find(self.esc_char, at)
                if next_end < 0:
                    raise Miss(s, start, self.error, self)
                if 0 <= next_esc < next_end:
                    buf.append(s[at:next_esc])
                    at = next_esc + len(self.esc_char) + 1
                    buf.append(s[at - 1])
                else:
                    buf.append(s[at:next_end])
                    end = next_end + len(self.end_char)
                    if self.inner_extent:
                        ctx[self.inner_extent] = (inner_start, next_end)
                    return end, "".join(buf)

        raise Miss(s, start, self.error, self)


class Ws(Regex):
    def __init__(self, pattern: str=r"\s+"):
        super(Ws, self).__init__(pattern, name="ws")
        self.error_template = "Expected WS %s"

    def __repr__(self):
        return "<%s>" % self.name


ws = Ws()


class PositionToken(Token):
    def __init__(self):
        super(PositionToken, self).__init__(type(self).__name__)
        self.may_be_empty = True

    @property
    def error(self):
        return self.error_template

    @abstractmethod
    def _parse(self, s: str, at: int, ctx: Context) -> Tuple[int, Any]:
        raise NotImplementedError


class StringStart(PositionToken):
    """Matches the start of the string."""

    def __init__(self):
        super(StringStart, self).__init__()
        self.error_template = "Expected start of string"

    def _parse(self, s: str, at: int, ctx: Context) -> Tuple[int, Any]:
        if at == 0:
            return 0, None
        else:
            raise Miss(s, at, self.error, self)


stringstart = StringStart()


class StringEnd(PositionToken):
    """Matches the end of the string."""

    def __init__(self):
        super(StringEnd, self).__init__()
        self.error_template = "Expected end of string"

    def _parse(self, s: str, at: int, ctx: Context) -> Tuple[int, Any]:
        s_len = len(s)
        if at == s_len:
            return s_len, None
        else:
            raise Miss(s, at, self.error, self)


stringend = StringEnd()


class WordStart(PositionToken):
    """
    Matches a point where the next character is alphanumeric and the previous
    character was not alphanumeric.
    """

    def __init__(self):
        super(WordStart, self).__init__()
        self.error_template = "Not at word start"

    def _parse(self, s: str, at: int, ctx: Context) -> Tuple[int, Any]:
        if at < len(s) and at == 0 or not s[at - 1].isalnum():
            return at, None

        raise Miss(s, at, self.error, self)


wordstart = WordStart()


class WordEnd(PositionToken):
    """
    Matches a point where the next character is not alphanumeric and the
    previous character was alphanumeric.
    """

    def __init__(self):
        super(WordEnd, self).__init__()
        self.error_template = "Not at word end"

    def _parse(self, s: str, at: int, ctx: Context) -> Tuple[int, Any]:
        if at > 0 and at == len(s) or s[at].isspace():
            return at, None

        raise Miss(s, at, self.error, self)


wordend = WordEnd()


class TokenStart(PositionToken):
    """
    Uses the current field's analyzer to check if the current position is the
    start of an analyzed token.
    """

    def __init__(self, parser: 'parsing.QueryParser'):
        super(TokenStart, self).__init__()
        self.parser = parser
        self.name = "TokenStart"

    def _parse(self, s: str, at: int, ctx: Context) -> Tuple[int, Any]:
        from whoosh import fields

        if self.parser:
            fieldname = ctx.fieldname
            schema = self.parser.schema
            if schema and fieldname in schema:
                field = schema[fieldname]
                if isinstance(field, fields.TokenizedField):
                    if at < len(s) and field.analyzer.is_token_start(s, at):
                        return at, None

        # Fall back to the standard "alphanumeric" word start check
        return wordstart.parse(s, at, ctx)


class Compound(Expr):
    def __init__(self, exprs: Sequence[Expr]):
        super(Compound, self).__init__()
        self.exprs = tuple(exprs)

    def __repr__(self):
        typename = type(self).__name__
        if self.name:
            return "<%s:%s>" % (typename, self.name)
        else:
            return "<%s %r>" % (typename, self.exprs)

    def __getitem__(self, i: int) -> Expr:
        return self.exprs[i]

    def dump(self, stream=sys.stdout, level=0):
        super(Compound, self).dump(stream, level)
        for e in self.exprs:
            e.dump(stream, level + 1)

    @property
    def error(self):
        try:
            return self.error_template % (self.exprs,)
        except TypeError as e:
            raise TypeError("%s template=%s exprs=%r" % (e, self.error_template,
                                                         self.exprs))

    # def register(self, context: Context):
    #     super(Compound, self).register(context)
    #     for e in self.exprs:
    #         e.register(context)

    def find(self, name: str) -> Expr:
        if self.name == name:
            return self
        for e in self.exprs:
            x = e.find(name)
            if x:
                return x

    def replace(self, name: str, expr: Expr) -> Expr:
        if self.name == name:
            return expr
        else:
            obj = copy.copy(self)
            obj.exprs = tuple(e.replace(name, expr) for e in self.exprs)
            return obj

    def append(self, other: Expr) -> 'Compound':
        if type(self) is type(other):
            return self.__class__(self.exprs + other.exprs)
        else:
            return self.__class__(self.exprs + (self._exprtize(other),))

    @abstractmethod
    def _parse(self, s: str, at: int, context: Context
               ) -> Tuple[int, Any]:
        raise NotImplementedError

    def validate(self, trace: List=None):
        if trace is None:
            trace = [self]
        else:
            trace = trace + [self]

        for e in self.exprs:
            e.validate(trace)

        self.check_recursion([])


class Seq(Compound):
    """
    Matches each sub-element in the given order, returns the final match
    as the result for the entire sequence.
    """

    def __init__(self, exprs: Sequence[Expr]):
        super(Seq, self).__init__(exprs)
        self.error_template = "Didn't match all of %r"
        self.may_be_empty = all(e.may_be_empty for e in self.exprs)

    def _parse(self, s: str, at: int, ctx: Context) -> Tuple[int, Any]:
        exprs = self.exprs
        debug = self.debug
        value = None
        ctx = ctx.push()

        for i, e in enumerate(exprs):
            self._debug(ctx, "Trying #%d/%d" % (i + 1, len(exprs)), e, "at", at)
            at, new_value = e.parse(s, at, ctx)
            if debug:
                self._debug(ctx, "Found", new_value, "->", at)
            if not e.hidden:
                value = new_value

        self._debug(ctx, "Found:", value, "->", at)
        return at, value

    def __iadd__(self, other: OpArgType):
        if isinstance(other, Seq):
            return Seq(self.exprs + other.exprs)
        else:
            return Seq(self.exprs + (self._exprtize(other),))

    def __radd__(self, other):
        if isinstance(other, Seq):
            return Seq(other.exprs + self.exprs)
        else:
            return Seq((self._exprtize(other),) +self.exprs)

    def check_recursion(self, elems: List[Expr]):
        tmp = elems[:] + [self]
        for e in self.exprs:
            e.check_recursion(tmp)
            if not e.may_be_empty:
                break


class Collect(Compound):
    """
    Matches each sub-element in the given order, returns a list of all matches.
    """

    def __init__(self, exprs: Sequence[Expr]):
        super(Collect, self).__init__(exprs)
        self.error_template = "Didn't match all of %r"
        self.may_be_empty = all(e.may_be_empty for e in self.exprs)

    def _parse(self, s: str, at: int, ctx: Context) -> Tuple[int, Any]:
        result = []
        ctx = ctx.push()
        for e in self.exprs:
            at, value = e.parse(s, at, ctx)
            if not e.hidden:
                result.append(value)

        return at, result

    def check_recursion(self, elems: List[Expr]):
        tmp = elems[:] + [self]
        for e in self.exprs:
            e.check_recursion(tmp)
            if not e.may_be_empty:
                break


class Or(Compound):
    """Matches the subexpression that matches first."""

    def __init__(self, exprs: Sequence[Expr]):
        super(Or, self).__init__(exprs)
        self.error_template = "Didn't match any of %r"
        if self.exprs:
            self.may_be_empty = any(e.may_be_empty for e in self.exprs)
        else:
            self.may_be_empty = True

    def _parse(self, s: str, at: int, ctx: Context) -> Tuple[int, Any]:
        exprs = self.exprs
        max_exc = None
        max_exc_at = -1
        ctx = ctx.push()

        for i, e in enumerate(exprs):
            self._debug(ctx, "Trying #%d/%d" % (i + 1, len(exprs)), e, "at", at)
            try:
                at, value = e.parse(s, at, ctx)
            except Miss as err:
                if err.at > max_exc_at:
                    max_exc = err
                    max_exc_at = err.at
            else:
                self._debug(ctx, "Found #%d/%d" % (i + 1, len(exprs)),
                            value, "->", at)
                return at, value

        # Nothing matched
        if max_exc is not None:
            max_exc.msg = self.error
            raise max_exc
        else:
            raise Miss(s, at, self.error, self)

    def __ior__(self, other: Expr):
        if isinstance(other, Or):
            return Or(self.exprs + other.exprs)
        else:
            return Or(self.exprs + (self._exprtize(other),))

    def __ror__(self, other):
        if isinstance(other, Or):
            return Or(other.exprs + self.exprs)
        else:
            return Or((self._exprtize(other),) + self.exprs)

    def check_recursion(self, elems: List[Expr]):
        tmp = elems[:] + [self]
        for e in self.exprs:
            e.check_recursion(tmp)


class Bag(Or):
    """Matches any of the subexpressions in any order."""

    def __init__(self, exprs: Sequence[Expr], seperator: Expr=None):
        super(Bag, self).__init__(exprs)
        self.sep = seperator

    def _parse(self, s: str, at: int, ctx: Context) -> Tuple[int, Any]:
        sep = self.sep
        exprs = list(self.exprs)
        first = True
        output = []
        while at < len(s):
            newat = at
            if not first and sep:
                # After the first expr, optionally start looking for separators
                self._debug(ctx, "Checking for sep", sep, "at", newat)
                try:
                    newat, sepv = sep.parse(s, newat, ctx)
                    self._debug(ctx, "Found", repr(sepv), "->", newat)
                except ParseException:
                    break

            # Try each remaining expr in the bag
            for i in range(len(exprs)):
                e = exprs[i]
                try:
                    self._debug(ctx, "Trying", e, "at", newat)
                    at, value = e.parse(s, newat, ctx)
                except ParseException:
                    # Didn't match, try the next one
                    continue
                else:
                    # Matched, record the value and remove the expr from the bag
                    self._debug(ctx, "Found", value, "->", at)
                    output.append(value)
                    del exprs[i]
                    break
            else:
                # None of the exprs in the bag matched, break out of the while
                break

            if not exprs:
                # No exprs left in the bag, break out of the while
                break

            first = False

        if output:
            self._debug(ctx, "Bag output", output, "->", at)
            return at, output
        else:
            raise Miss(
                s, at, "None matched in bag of %r" % (self.exprs,)
            )


class Wrapper(Expr):
    def __init__(self, expr: Expr):
        super(Wrapper, self).__init__()
        expr = self._exprtize(expr)
        self.expr = expr
        self.error_template = "Expected %s(%r)"
        self.may_be_empty = expr.may_be_empty

    def __repr__(self):
        return "<%s %r>" % (type(self).__name__, self.expr)

    def dump(self, stream=sys.stdout, level=0):
        super(Wrapper, self).dump(stream, level)
        self.expr.dump(stream, level + 1)

    def unwrap(self) -> Expr:
        return self.expr

    def rebind(self, expr: Expr) -> 'Wrapper':
        obj = copy.copy(self)
        obj.expr = expr
        obj.may_be_empty = expr.may_be_empty
        return obj

    def _parse(self, s: str, at: int, ctx: Context) -> Tuple[int, Any]:
        return self.expr.parse(s, at, ctx)

    @property
    def error(self):
        return self.error_template % (self.name, self.expr)

    # def register(self, context: Context):
    #     super(Wrapper, self).register(context)
    #     self.expr.register(context)

    def find(self, name: str) -> Expr:
        if self.name == name:
            return self
        return self.expr.find(name)

    def replace(self, name: str, expr: Expr) -> Expr:
        if self.name == name:
            return expr
        else:
            return self.rebind(self.expr.replace(name, expr))

    def check_recursion(self, elems: List[Expr]):
        if self in elems:
            raise RecursiveGrammarError(elems + [self])
        self.expr.check_recursion(elems[:] + [self])

    def validate(self, trace: List=None):
        if trace is None:
            trace = [self]
        else:
            trace = trace + [self]

        if self.expr is not None:
            self.expr.validate(trace)

        self.check_recursion([])


class Hidden(Wrapper):
    hidden = True


class StringUntil(Wrapper):
    def __init__(self, expr: Expr, esc_char="\\", matches_end: bool=False,
                 add_context_expr: bool=False, may_be_empty: bool=True,
                 name: str=None):
        super(StringUntil, self).__init__(expr)
        self.esc_char = esc_char
        self.matches_end = matches_end
        self.add_context_expr = add_context_expr
        self.may_be_empty = may_be_empty
        self.name = name

    def __repr__(self):
        expr = self.expr
        return "<%s %x %r>" % (type(self).__name__, id(expr), expr)

    def _parse(self, s: str, at: int, ctx: Context) -> Tuple[int, Any]:
        ctx = ctx.push()
        expr = self.expr
        if self.add_context_expr:
            expr = expr | ctx.expr

        buffer = []
        esc_char = self.esc_char
        i = at
        while i < len(s):
            self._debug(ctx, "Looking for until at", i)
            char = s[i]
            if esc_char and char == esc_char:
                self._debug(ctx, "Found escaped character", char)
                buffer.append(s[i + len(esc_char)])
                i += len(esc_char) + 1
            elif expr.matches(s, i, ctx):
                self._debug(ctx, "Stop expression matched at", i)
                break
            else:
                buffer.append(char)
                i += 1

        if i == at:
            if self.may_be_empty:
                self._debug(ctx, "Found empty string")
                return i, ""
            else:
                raise Miss(s, at, "Empty string not allowed", self)

        if i == len(s) and not self.matches_end:
            raise Miss(s, at, "Fell off end looking for %r" % self.expr, self)

        result = "".join(buffer)
        self._debug(ctx, "Found:", result, "->", i)
        return i, result


class Follows(Wrapper):
    def __init__(self, expr: Expr, distance: int):
        super(Follows, self).__init__(expr)
        self.distance = distance
        self.error_template = "Expected to follow %s(%r)"
        self.may_be_empty = True

    def _parse(self, s: str, at: int, ctx: Context) -> Tuple[int, Any]:
        if at - self.distance >= 0:
            if self.expr.matches(s, at - self.distance, ctx):
                return at, None
        raise Miss(s, at, self.error, self)


class Not(Wrapper):
    def __init__(self, expr: Expr):
        super(Not, self).__init__(expr)
        self.error_template = "Expected not to match %s(%r)"
        self.may_be_empty = True

    def _parse(self, s: str, at: int, ctx: Context) -> Tuple[int, Any]:
        try:
            self.expr.try_parse(s, at, ctx)
        except Miss:
            return at, None
        else:
            raise Miss(s, at, self.error, self)


class Peek(Wrapper):
    def __init__(self, expr: Expr):
        super(Peek, self).__init__(expr)
        self.error_template = "Expected peek %s(%r)"
        self.may_be_empty = True

    def _parse(self, s: str, at: int, ctx: Context) -> Tuple[int, Any]:
        self.expr.try_parse(s, at, ctx)
        return at, None


class Repeat(Wrapper):
    def __init__(self, expr: Expr, min_times: int=1,
                 max_times: int=0):
        super(Repeat, self).__init__(expr)
        self.error_template = "Expected repeating %s(%r)"

        if min_times < 0:
            raise ValueError("Can't repeat less than 0 times")
        if max_times < 0 or max_times < min_times:
            raise ValueError("Max repeat must be gt 0 and gt min repeat")
        if min_times == max_times == 0:
            raise ValueError("Can't repeat min 0 and max 0 times")

        self.min_times = min_times
        self.max_times = max_times

    def __repr__(self):
        return "<%s %r %s,%s>" % (type(self).__name__, self.expr,
                                  self.min_times, self.max_times)

    def _parse(self, s: str, at: int, ctx: Context) -> Tuple[int, Any]:
        expr = self.expr
        max_times = self.max_times
        start = at
        results = []
        count = 0
        while True:
            try:
                at, value = expr.parse(s, at, ctx)
            except Miss:
                break
            results.append(value)
            count += 1
            if max_times and count == max_times:
                break

        if count < self.min_times:
            raise Miss(s, start, self.error, self)
        return at, results


class OneOrMore(Wrapper):
    def __init__(self, expr: Expr):
        super(OneOrMore, self).__init__(expr)
        if expr.may_be_empty:
            raise ValueError("Can't repeat an expression that may be empty")

    def __repr__(self):
        return "(%r)+" % self.expr

    def _parse(self, s: str, at: int, ctx: Context) -> Tuple[int, Any]:
        parse = self.expr.parse
        results = []

        at, value = parse(s, at, ctx)
        results.append(value)
        while True:
            try:
                at, value = parse(s, at, ctx)
            except Miss:
                break
            results.append(value)

        return at, results


class ZeroOrMore(Wrapper):
    def __init__(self, expr: Expr, allow_zero_match=False):
        super(ZeroOrMore, self).__init__(expr)
        self.may_be_empty = True
        if expr.may_be_empty and not allow_zero_match:
            raise ValueError("Can't repeat an expression that may be empty")

    def __repr__(self):
        return "(%r)*" % self.expr

    def _parse(self, s: str, at: int, ctx: Context) -> Tuple[int, Any]:
        _parse = self.expr.parse
        results = []

        while True:
            try:
                newat, value = _parse(s, at, ctx)
            except Miss:
                break
            results.append(value)

            if newat == at:
                break
            else:
                at = newat

        return at, results


class Optional(Wrapper):
    def __init__(self, expr: Expr):
        super(Optional, self).__init__(expr)
        self.may_be_empty = True

    def __repr__(self):
        return "(%r)?" % self.expr

    def _parse(self, s: str, at: int, ctx: Context) -> Tuple[int, Any]:
        _parse = self.expr.parse
        results = []
        try:
            at, value = _parse(s, at, ctx)
        except Miss:
            pass
        else:
            results.append(value)
        return at, results


class Until(Wrapper):
    def __init__(self, expr: Expr, include: bool=False):
        super(Until, self).__init__(expr)
        self.may_be_empty = True
        self.error_template = "Didn't match until %s(%r)"
        self.include = include

    def _parse(self, s: str, at: int, ctx: Context) -> Tuple[int, Any]:
        _parse = self.expr.parse
        start = at
        while at < len(s):
            try:
                new_at, endv = _parse(s, at, ctx)
            except Miss:
                at += 1
                continue

            value = s[start:at]
            if self.include:
                value = [value, endv]
                at = new_at

            return at, value

        raise Miss(s, at, self.error, self)


class Forward(Wrapper):
    def __init__(self, may_be_empty: bool=True):
        super(Forward, self).__init__(Empty())
        self.may_be_empty = may_be_empty
        self.error_template = "Expected %s(%r)"

    def __repr__(self):
        return "<%s %s>" % (type(self).__name__, self.expr.name)

    def assign(self, expr: Expr):
        self.expr = expr
        self.may_be_empty = expr.may_be_empty

    def validate(self, trace: List=None):
        trace = trace or []
        if self not in trace:
            trace = trace + [self]
            if self.expr is not None:
                self.expr.validate(trace)

        self.check_recursion([])


class TempWrapper(Wrapper):
    pass


class FieldExpr(TempWrapper):
    def __init__(self, first: Expr, rest: Expr):
        super(FieldExpr, self).__init__(Or([first, rest]))
        self.rest = rest

    def unwrap(self) -> Expr:
        return self.rest


class SelfParsingField(TempWrapper):
    def __init__(self, expr, fieldname: str, field: 'fields.Field'):
        super(SelfParsingField, self).__init__(expr)
        self.expr = expr
        self.fieldname = fieldname
        self.field = field
        self.may_be_empty = True

    def __repr__(self):
        return "<%s %s>" % (type(self).__name__, self.fieldname)

    def _parse(self, s: str, at: int, ctx: Context) -> Tuple[int, Any]:
        self._debug(ctx, "Trying main expression before self-parsing")
        # Try the main expression
        try:
            at, q = self.expr.parse(s, at, ctx)
        except Miss:
            pass
        else:
            return at, q

        # If that didn't match, try parsing with the field
        self._debug(ctx, "Fieldname=", self.fieldname, "field=", self.field,
                    "trying to parse at", at)
        endchar, q = self.field.parse_from(self.fieldname, s, at)
        if q is None:
            raise Miss(s, at, "Field did not generate a query", self)
        self._debug(ctx, "Found", repr(q), "->", endchar)

        q.set_extent(at, endchar)
        q.analyzed = True
        return endchar, q


# class Combine(Wrapper):
#     def _parse(self, s: str, at: int, context: Context,
#                ) -> Tuple[int, str]:
#         at, value = self.expr.parse(s, at, context)
#         print("value=", value)
#         if isinstance(value, (tuple, list)):
#             value = "".join(value)
#         return at, value


class Assign(Wrapper):
    def __init__(self, expr: Expr, name: str):
        super(Assign, self).__init__(expr)
        self.name = name

    def __repr__(self):
        return "<%s=%r>" % (self.name, self.expr)

    def _parse(self, s: str, at: int, ctx: Context) -> Tuple[int, Any]:
        at, value = self.expr._parse(s, at, ctx)
        ctx[self.name] = value
        return at, value


class AssignExtent(Wrapper):
    def __init__(self, expr: Expr, name: str="extent"):
        super(AssignExtent, self).__init__(expr)
        self.name = name

    def _parse(self, s: str, at: int, ctx: Context) -> Tuple[int, Any]:
        new_at, value = self.expr._parse(s, at, ctx)
        self._debug(ctx, "Saving extent", at, "->", new_at, "to", self.name)
        ctx[self.name] = (at, new_at)
        if isinstance(value, query.Query):
            self._debug(ctx, "Setting extents on", value)
            value.startchar = at
            value.endchar = new_at
        return new_at, value


class Apply(Wrapper):
    """
    Applies a function to the output of the wrapped expression.
    """

    def __init__(self, expr: Expr, fn: Callable[[Any], Any]):
        super(Apply, self).__init__(expr)
        self.fn = fn

    def _parse(self, s: str, at: int, ctx: Context) -> Tuple[int, Any]:
        at, value = self.expr._parse(s, at, ctx)
        return at, self.fn(value)


integer = Apply(Regex("[0-9]+"), lambda s: int(s))


class Do(Expr):
    def __init__(self, fn: FuncType, *extra_args):
        super(Do, self).__init__()
        self.fn = fn
        self.extra_args = extra_args
        self.may_be_empty = True

    def __repr__(self):
        return "<%s %r>" % (type(self).__name__, self.fn)

    def _parse(self, s: str, at: int, ctx: Context) -> Tuple[int, Any]:
        return at, self.fn(ctx, *self.extra_args)


class If(Expr):
    hidden = True

    def __init__(self, fn: FuncType):
        super(If, self).__init__()
        self.fn = fn
        self.error_template = "Condition not true %s(%r)"

    def _parse(self, s: str, at: int, ctx: Context) -> Tuple[int, Any]:
        if self.fn(ctx):
            return at, None
        else:
            raise Miss(s, at, self.error, self)


class Call(Expr):
    def __init__(self, name: str, may_be_empty: bool=True):
        super(Call, self).__init__()
        self.name = name
        self.may_be_empty = may_be_empty

    def _parse(self, s: str, at: int, ctx: Context) -> Tuple[int, Any]:
        expr = ctx.lookup[self.name]  # type: Expr
        return expr.parse(s, at, ctx)


class Get(Expr):
    def __init__(self, name: str):
        super(Get, self).__init__()
        self.name = name
        self.may_be_empty = True

    def __repr__(self):
        return "<%s %r>" % (type(self).__name__, self.name)

    def _parse(self, s: str, at: int, ctx: Context) -> Tuple[int, Any]:
        if self.name in ctx:
            return at, ctx[self.name]
        else:
            raise FatalError(s, at, "Unknown name %r" % self.name, self)


class Guard(Wrapper):
    """
    Prevents an expression from matching recursively -- if the expressions is
    called to parse inside itself, it will raise Miss. This prevents infinite
    loops where the expression tries to match at the same location recursively.

    This can happen when you try to use the "current" expression as a
    sub-element of an expression... since the expression itself is likely part
    of the "current" expression, it can easily cause infinite recursion.
    Wrapping the expression in a Guard can fix this.
    """

    def __init__(self, name: str, expr: Expr):
        super(Guard, self).__init__(expr)
        self.name = name

    def _parse(self, s: str, at: int, ctx: Context) -> Tuple[int, Any]:
        if self.name in ctx:
            raise Miss(s, at, "Hit %s guard" % self.name, self)
        else:
            ctx = ctx.push()
            ctx[self.name] = True
            return self.expr.parse(s, at, ctx)


class Parsed(Expr):
    """
    Takes a value from the stream (up to an optional "end" expression) using
    the QueryParser's parse_expr method to take a matching element or text.

    This allows an expr to say while matching "take the next thing" (whether
    that be a query or some text) without having to be exact. For example,
    inside brackets, the group plugin will keep "taking the next thing" until it
    sees its close bracket.
    """

    def __init__(self, parser: 'parsing.QueryParser', name: str,
                 end_expr: Expr=None, field_from: str=None,
                 tokenize: bool=True, take_plaintext: bool=True):
        super(Parsed, self).__init__()
        self.parser = parser
        self.name = name
        self.end_expr = end_expr
        self.field_from = field_from
        self.tokenize = tokenize
        self.take_plaintext = take_plaintext

    def __repr__(self):
        return "<%s %s>" % (type(self).__name__, self.name)

    def _parse(self, s: str, at: int, ctx: Context) -> Tuple[int, Any]:
        if at >= len(s):
            raise Miss(s, at, "Parsed at end of string", self)

        # Push a new context to use while parsing the next element
        ctx = ctx.push()

        # This object may be configured to switch to a different field named in
        # a context variable
        if self.field_from:
            fname = ctx[self.field_from]
            # Set the field name as current in the new context
            self._debug(ctx, "Pushing down fieldname", fname)
            ctx.fieldname = fname

        # Get the current "main" expression out of the context, and account for
        # field expressions, etc.
        expr = self.parser.expression(ctx)
        self._debug(ctx, "Parsing single expr", expr)
        return self.parser.parse_single(s, at, ctx, expr=expr,
                                        end_expr=self.end_expr,
                                        take_plaintext=self.take_plaintext)


class Fieldify(Expr):
    def __init__(self, parser: 'parsing.QueryParser', fn: Callable):
        self.parser = parser
        self.fn = fn
        self.may_be_empty = True

    def _parse(self, s: str, at: int, ctx: Context) -> Tuple[int, Any]:
        from whoosh.query import NullQuery, Or

        newctx = ctx.push()
        qs = []
        for fieldname in self.parser.fieldnames_for(ctx.fieldname):
            newctx.fieldname = fieldname
            qs.append(self.fn(newctx))

        if not qs:
            qs = NullQuery
        elif len(qs) == 1:
            qs = qs[0]
        else:
            qs = Or(qs)

        return at, qs

