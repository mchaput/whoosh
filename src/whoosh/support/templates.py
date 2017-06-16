import functools
import os.path
import re
from typing import List, Optional

from whoosh.compat import text_type


"""
A *very* simple template language for generating HTML pages for the index admin
interface.
"""


# Pattern for matching most representations of a Python string
_re_string = r'''
((?mx)         # verbose and dot-matches-newline mode
    [urbURB]*
    # Note: below is group 1 in the token pattern
    (?:  ''(?!')  # Empty single-quote string
        |""(?!")  # Empty double-quote string
        |'{6}     # Empty triple-single-quote string
        |"{6}     # Empty triple-double-quite string
        |'(?:[^\\']|\\.)+?'  # Single quote string
        |"(?:[^\\"]|\\.)+?"  # Double quote string
        |'{3}(?:[^\\]|\\.|\n)+?'{3}  # Triple-single-quote string
        |"{3}(?:[^\\]|\\.|\n)+?"{3}  # Triple-double-quote string
    )
)'''

# "Inline" version of the string pattern doesn't allow newlines
_re_inl = _re_string.replace(r'|\n', '')

# Token pattern matches stuff after line (%) or block (<%) start tokens
_re_tok = _re_string + r'''
    # Note: Group 1 is in the string matching pattern above

    # Group 2: Comments (until end of line, but not the newline itself)
    |(\#.*)

    # Group 3: open and Group 4: close grouping tokens
    |([\[\{\(])
    |([\]\}\)])

    # Group 5: Keywords that start a python block (only start of line)
    |^([\ \t]*(?:if|for|while|with|try|def|class)\b)
    # Group 6: Keywords that continue a python block (only start of line)
    |^([\ \t]*(?:elif|else|except|finally)\b)

    # Group 7: Our special 'end' keyword (but only if it stands alone)
    |((?:^|;)[\ \t]*end[\ \t]*(?=(?:%(block_close)s[\ \t]*)?\r?$|;|\#))

    # Group 8: End-of-code-block template token (only end of line)
    |(%(block_close)s[\ \t]*(?=\r?$))

    # Group 9: A single newline. The 10th group is 'everything else'
    |(\r?\n)
'''

# Split on the code start tokens in a template
_re_split = r'''(?m)^[ \t]*(\\?)((%(line_start)s)|(%(block_start)s))'''
# Match inline code (between {{ and }}, may contain python strings)
_re_inl = r'''%%(inline_start)s((?:%s|[^'"\n]+?)*?)%%(inline_end)s''' % _re_inl

# Template syntax strings
name2token = {
    "block_start": "<%",
    "block_close": "%>",
    "line_start": "%",
    "inline_start": "{{",
    "inline_end": "}}",
}

# Generate compiled expressions by combining patterns from above and the dict
# of syntax strings
re_split = re.compile(_re_split % name2token)
re_tok = re.compile(_re_tok % name2token)
re_inl = re.compile(_re_inl % name2token)


class TemplateParser(object):
    """
    Parses a template file and transforms it into a Python source code string.
    """

    def __init__(self, source: str):
        self.source = source
        self.text_buffer = []  # type: List[str]
        self.code_buffer = []  # type: List[str]
        self.indent = 0
        self.indent_mod = 0
        self.paren_depth = 0
        self.lineno = 1

    def _flush_text(self):
        text = "".join(self.text_buffer)
        del self.text_buffer[:]
        if not text:
            return

        parts = []
        pos = 0
        nl = '\\\n' + '  ' * self.indent

        for match in re_inl.finditer(text):
            prefix = text[pos: match.start()]
            pos = match.end()
            if prefix:
                parts.append(nl.join([repr(line) for line
                                      in prefix.splitlines(True)]))
            if prefix.endswith("\n"):
                parts[-1] += nl
            parts.append(self.process_inline(match.group(1).strip()))

        if pos < len(text):
            rest = text[pos:]
            lines = rest.splitlines(True)
            parts.append(nl.join([repr(line) for line in lines]))

        code = "_printlist((%s,))" % ", ".join(parts)
        self.lineno += code.count("\n") + 1
        self._write_code(code)

    def _write_code(self, line: str, comment: str= ''):
        self.code_buffer.append(
            '  ' * (self.indent + self.indent_mod) +  # Indent
            line.lstrip() + comment + "\n"  # Code text
        )

    def _read_code(self, source: str, multiline: bool):
        block_close = name2token["block_close"]

        code_line = ''
        comment = ''
        i = 0
        while True:
            match = re_tok.search(source, i)
            if not match:
                code_line += source[i:]
                return len(source)

            code_line += source[i: match.start()]
            i = match.end()
            _str, _com, _po, _pc, _blk1, _blk2, _end, _cend, _nl = \
                match.groups()

            if self.paren_depth > 0 and (_blk1 or _blk2):  # a if b else c
                code_line += _blk1 or _blk2
                continue

            if _str:  # Python string
                code_line += _str
            elif _com:  # Comment
                comment = _com
                if multiline and _com.strip().endswith(block_close):
                    multiline = False
            elif _po:  # Open paren
                self.paren_depth += 1
                code_line += _po
            elif _pc:  # Close paren
                if self.paren_depth > 0:
                    self.paren_depth -= 1
                else:
                    raise SyntaxError
                code_line += _pc
            elif _blk1:  # Start block keyword
                code_line = _blk1
                self.indent_mod = -1
                self.indent += 1
            elif _blk2:  # Continuing block keyword
                code_line = _blk2
                self.indent_mod = -1
            elif _end:  # End keyword
                self.indent -= 1
            elif _cend:  # End code block token %>
                if multiline:
                    multiline = False
                else:
                    code_line += _cend
            else:
                self._write_code(code_line.strip(), comment)
                self.lineno += 1
                code_line = ''
                comment = ''
                self.indent_mod = 0
                if not multiline:
                    break

        return i

    @staticmethod
    def process_inline(code):
        if code.startswith("!"):
            return "_str(%s)" % code[1:]
        else:
            return "_escape(%s)" % code

    def translate(self):
        source = self.source
        tb = self.text_buffer
        i = 0

        while True:
            match = re_split.search(source, i)
            if match:
                text = source[i: match.start()]
                tb.append(text)
                i = match.end()

                if match.group(1):  # escape
                    line, sep, _ = source[i:].partition("\n")
                    tb.append(source[match.start():match.start(1)] +\
                        match.group(2) + line + sep)
                    i += len(line) + len(sep)
                    continue

                self._flush_text()
                i += self._read_code(source[i:], multiline=bool(match.group(4)))
            else:
                break

        tb.append(source[i:])
        self._flush_text()
        return "".join(self.code_buffer)


class TemplateLoader(object):
    def load(self, name: str) -> str:
        raise NotImplementedError

    def get_template(self, name: str) -> 'Template':
        return Template(self.load(name))


class PackageResourceLoader(TemplateLoader):
    def __init__(self, package: object):
        self.package = package

    def load(self, name: str) -> str:
        import pkg_resources

        return pkg_resources.resource_string(self.package, name)


class FileTemplateLoader(TemplateLoader):
    def __init__(self, directory: str):
        self.directory = directory
        self._cache = {}

    def get_template(self, name: str) -> 'Template':
        try:
            cached, lastmod = self._cache[name]
        except KeyError:
            return self._cache_template(name)
        else:
            return self._check_cache(name, cached, lastmod)

    def _modtime(self, name: str) -> float:
        filepath = os.path.join(self.directory, name)
        modtime = os.path.getmtime(filepath)
        return filepath, modtime

    def _cache_template(self, name: str) -> 'Template':
        modtime = self._modtime(name)
        template = Template(self.load(name))
        self._cache[name] = template, modtime
        return template

    def _check_cache(self, name: str, template: 'Template',
                     lastmod: float) -> 'Template':
        modtime = self._modtime(name)
        if modtime <= lastmod:
            return template
        else:
            return self._cache_template(name)

    def load(self, name: str) -> str:
        import os.path

        filepath = os.path.join(self.directory, name)
        with open(filepath, "r") as f:
            return f.read()


class Template(object):
    def __init__(self, source: str, name: str="<template>",
                 loader: TemplateLoader=None):
        self.source = source
        self.name = name
        self.loader = loader

        self.defaults = {}
        self.pysource = TemplateParser(self.source).translate()
        print(self.pysource)
        self.code = compile(self.pysource, name, "exec")

    def _include(self, env, buffer, name, **kwargs):
        env = env.copy()
        env.update(kwargs)
        template = self.loader.get_template(name)
        return template.execute(buffer, kwargs)

    def _rebase(self, _env, _name=None, **kwargs):
        _env['_rebase'] = (_name, kwargs)

    @staticmethod
    def _str(value: str):
        if isinstance(value, bytes):
            return value.decode("utf8", "strict")
        elif value is None:
            return u""
        else:
            return text_type(value)

    @staticmethod
    def _escape(value: str):
        if value is None:
            return ""
        return str(value).replace('&', '&amp;') \
            .replace('<', '&lt;') \
            .replace('>', '&gt;') \
            .replace('"', '&quot;') \
            .replace("'", '&#039;')

    def execute(self, buffer, kwargs):
        env = self.defaults.copy()
        env.update(kwargs)
        env.update({
            "_printlist": buffer.extend,
            "include": functools.partial(self._include, env, buffer),
            "rebase": functools.partial(self._rebase, env),
            "_base": None,
            "_str": self._str,
            "_escape": self._escape,
            "get": env.get,
            "setdefault": env.setdefault,
            "defined": env.__contains__,
        })

        eval(self.code, env)

        if "_rebase" in env:
            name, args = env.pop("_rebase")
            args["base"] = "".join(buffer)
            del buffer[:]
            return self._include(env, name, **args)

        return env

    def render(self, **kwargs):
        buffer = []
        self.execute(buffer, kwargs)
        return "".join(buffer)


if __name__ == "__main__":
    source = """
<html>
    %# This is a comment
    %def test(name):
        <p>Hello {{name}}</p>
    %end
    {{ test("me") }}
</html>
    """.strip()

    t = Template(source)
    print(t.render(hello="hi", count=6))
