from nose.tools import assert_equal, assert_not_equal  #@UnresolvedImport

from whoosh.qparser import default2 as default
from whoosh.qparser import syntax2 as syntax
from whoosh.qparser import plugins2 as plugins


def test_whitespace():
    p = default.QueryParser("t", None, [plugins.WhitespacePlugin()])
    assert_equal(repr(p.tokenize("hello there amiga")), "<AndGroup <None:'hello'>, < >, <None:'there'>, < >, <None:'amiga'>>")

def test_singlequotes():
    p = default.QueryParser("t", None, [plugins.WhitespacePlugin(),
                                        plugins.SingleQuotePlugin()])
    assert_equal(repr(p.process("a 'b c' d")), "<AndGroup <None:'a'>, <None:'b c'>, <None:'d'>>")

def test_prefix():
    p = default.QueryParser("t", None, [plugins.WhitespacePlugin(),
                                        plugins.PrefixPlugin()])
    assert_equal(repr(p.process("a b* c")), "<AndGroup <None:'a'>, <None:'b'*>, <None:'c'>>")

def test_wild():
    p = default.QueryParser("t", None, [plugins.WhitespacePlugin(),
                                        plugins.WildcardPlugin()])
    assert_equal(repr(p.process("a b*c? d")), "<AndGroup <None:'a'>, <None:Wild 'b*c?'>, <None:'d'>>")

def test_range():
    p = default.QueryParser("t", None, [plugins.WhitespacePlugin(),
                                        plugins.RangePlugin()])
    ns = p.tokenize("a [b to c} d")
    assert_equal(repr(ns), "<AndGroup <None:'a'>, < >, <rangeopen '['>, <None:'b'>, < >, <None:'to'>, < >, <None:'c'>, <rangeclose '}'>, < >, <None:'d'>>")
    ns = p.filterize(ns)
    assert_equal(repr(ns), "<AndGroup <None:'a'>, <None:[('b') ('c')]>, <None:'d'>>")
    
    assert_equal(repr(p.process("a {b to]")), "<AndGroup <None:'a'>, <None:{('b') None}>>")
    
    assert_equal(repr(p.process("[to c] d")), "<AndGroup <None:[None ('c')]>, <None:'d'>>")
    
    assert_equal(repr(p.process("[to]")), "<AndGroup <None:'to'>>")
    
def test_sq_range():
    p = default.QueryParser("t", None, [plugins.WhitespacePlugin(),
                                        plugins.SingleQuotePlugin(),
                                        plugins.RangePlugin()])
    assert_equal(repr(p.process("['a b' to ']']")), "<AndGroup <None:[('a b') (']')]>>")

def test_phrase():
    p = default.QueryParser("t", None, [plugins.WhitespacePlugin(),
                                        plugins.PhrasePlugin()])
    assert_equal(repr(p.process('a "b c"')), "<AndGroup <None:'a'>, <None:PhraseNode 'b c'~1>>")

    assert_equal(repr(p.process('"b c" d')), "<AndGroup <None:PhraseNode 'b c'~1>, <None:'d'>>")

    assert_equal(repr(p.process('"b c"')), "<AndGroup <None:PhraseNode 'b c'~1>>")

def test_groups():
    p = default.QueryParser("t", None, [plugins.WhitespacePlugin(),
                                        plugins.GroupPlugin()])
    
    ns = p.process("a ((b c) d) e")
    assert_equal(repr(ns), "<AndGroup <None:'a'>, <AndGroup <AndGroup <None:'b'>, <None:'c'>>, <None:'d'>>, <None:'e'>>")

def test_fieldnames():
    p = default.QueryParser("t", None, [plugins.WhitespacePlugin(),
                                        plugins.FieldsPlugin(),
                                        plugins.GroupPlugin()])
    ns = p.process("a:b c d:(e f:(g h)) i j:")
    assert_equal(repr(ns), "<AndGroup <a:'b'>, <None:'c'>, <AndGroup <d:'e'>, <AndGroup <f:'g'>, <f:'h'>>>, <None:'i'>, <None:'j:'>>")
    
    assert_equal(repr(p.process("a:b:")), "<AndGroup <a:'b:'>>")

def test_operators():
    p = default.QueryParser("t", None, [plugins.WhitespacePlugin(),
                                        plugins.OperatorsPlugin()])
    ns = p.process("a OR b")
    assert_equal(repr(ns), "<AndGroup <OrGroup <None:'a'>, <None:'b'>>>")

def test_boost():
    p = default.QueryParser("t", None, [plugins.WhitespacePlugin(),
                                        plugins.GroupPlugin(),
                                        plugins.BoostPlugin()])
    ns = p.tokenize("a^3")
    assert_equal(repr(ns), "<AndGroup <None:'a'>, <^ 3.0>>")
    ns = p.filterize(ns)
    assert_equal(repr(ns), "<AndGroup <None:'a' ^3.0>>")
    
    assert_equal(repr(p.process("a (b c)^2.5")), "<AndGroup <None:'a'>, <AndGroup <None:'b'>, <None:'c'> ^2.5>>")
    assert_equal(repr(p.process("a (b c)^.5 d")), "<AndGroup <None:'a'>, <AndGroup <None:'b'>, <None:'c'> ^0.5>, <None:'d'>>")

    assert_equal(repr(p.process("^2 a")), "<AndGroup <None:'^2'>, <None:'a'>>")
    
    assert_equal(repr(p.process("a^2^3")), "<AndGroup <None:'a^2' ^3.0>>")




