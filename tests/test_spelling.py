from __future__ import with_statement
import gzip

from nose.tools import assert_equal, assert_not_equal, assert_raises

from whoosh import analysis, fields, highlight, spelling
from whoosh.compat import u, permutations
from whoosh.filedb.filestore import RamStorage
from whoosh.qparser import QueryParser
from whoosh.support import dawg
from whoosh.support.testing import TempStorage


def words_to_corrector(words):
    st = RamStorage()
    f = st.create_file("test")
    spelling.wordlist_to_graph_file(words, f)
    f = st.open_file("test")
    return spelling.GraphCorrector(dawg.GraphReader(f))


def test_graph_corrector():
    wordlist = sorted(["render", "animation", "animate", "shader",
                       "shading", "zebra", "koala", "lamppost",
                       "ready", "kismet", "reaction", "page",
                       "delete", "quick", "brown", "fox", "jumped",
                       "over", "lazy", "dog", "wicked", "erase",
                       "red", "team", "yellow", "under", "interest",
                       "open", "print", "acrid", "sear", "deaf",
                       "feed", "grow", "heal", "jolly", "kilt",
                       "low", "zone", "xylophone", "crown",
                       "vale", "brown", "neat", "meat", "reduction",
                       "blunder", "preaction"])

    sp = words_to_corrector(wordlist)
    sugs = sp.suggest("reoction", maxdist=2)
    assert_equal(sugs, ["reaction", "preaction", "reduction"])


def test_reader_corrector_nograph():
    schema = fields.Schema(text=fields.TEXT)
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(text=u("render zorro kaori postal"))
    w.add_document(text=u("reader zebra koala pastry"))
    w.add_document(text=u("leader libra ooala paster"))
    w.add_document(text=u("feeder lorry zoala baster"))
    w.commit()

    with ix.reader() as r:
        sp = spelling.ReaderCorrector(r, "text")
        assert_equal(sp.suggest(u("kaola"), maxdist=1), ['koala'])
        assert_equal(sp.suggest(u("kaola"), maxdist=2), ['koala', 'kaori',
                                                         'ooala', 'zoala'])


def test_reader_corrector():
    schema = fields.Schema(text=fields.TEXT(spelling=True))
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(text=u("render zorro kaori postal"))
    w.add_document(text=u("reader zebra koala pastry"))
    w.add_document(text=u("leader libra ooala paster"))
    w.add_document(text=u("feeder lorry zoala baster"))
    w.commit()

    with ix.reader() as r:
        assert r.has_word_graph("text")
        sp = spelling.ReaderCorrector(r, "text")
        assert_equal(sp.suggest(u("kaola"), maxdist=1), [u('koala')])
        assert_equal(sp.suggest(u("kaola"), maxdist=2), [u('koala'),
                                                         u('kaori'),
                                                         u('ooala'),
                                                         u('zoala')])


def test_add_spelling():
    schema = fields.Schema(text1=fields.TEXT, text2=fields.TEXT)
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(text1=u("render zorro kaori postal"), text2=u("alfa"))
    w.add_document(text1=u("reader zebra koala pastry"), text2=u("alpa"))
    w.add_document(text1=u("leader libra ooala paster"), text2=u("alpha"))
    w.add_document(text1=u("feeder lorry zoala baster"), text2=u("olfo"))
    w.commit()

    with ix.reader() as r:
        assert not r.has_word_graph("text1")
        assert not r.has_word_graph("text2")

    from whoosh.filedb.filewriting import add_spelling
    add_spelling(ix, ["text1", "text2"])

    with ix.reader() as r:
        assert r.has_word_graph("text1")
        assert r.has_word_graph("text2")

        sp = spelling.ReaderCorrector(r, "text1")
        assert_equal(sp.suggest(u("kaola"), maxdist=1), [u('koala')])
        assert_equal(sp.suggest(u("kaola"), maxdist=2), [u('koala'),
                                                         u('kaori'),
                                                         u('ooala'),
                                                         u('zoala')])

        sp = spelling.ReaderCorrector(r, "text2")
        assert_equal(sp.suggest(u("alfo"), maxdist=1), [u("alfa"), u("olfo")])


def test_multisegment():
    schema = fields.Schema(text=fields.TEXT(spelling=True))
    ix = RamStorage().create_index(schema)
    domain = u("special specious spectacular spongy spring specials").split()
    for word in domain:
        w = ix.writer()
        w.add_document(text=word)
        w.commit(merge=False)

    with ix.reader() as r:
        assert not r.is_atomic()
        assert r.has_word_graph("text")
        words = list(r.word_graph("text").flatten_strings())
        assert_equal(words, sorted(domain))

        corr = r.corrector("text")
        assert_equal(corr.suggest("specail", maxdist=2),
                     ["special", "specials"])

    ix.optimize()
    with ix.reader() as r:
        assert r.is_atomic()
        assert_equal(list(r.lexicon("text")), sorted(domain))
        assert r.has_word_graph("text")
        words = list(r.word_graph("text").flatten_strings())
        assert_equal(words, sorted(domain))

        corr = r.corrector("text")
        assert_equal(corr.suggest("specail", maxdist=2),
                     ["special", "specials"])


def test_multicorrector():
    schema = fields.Schema(text=fields.TEXT(spelling=True))
    ix = RamStorage().create_index(schema)
    domain = u("special specious spectacular spongy spring specials").split()
    for word in domain:
        w = ix.writer()
        w.add_document(text=word)
        w.commit(merge=False)

    c1 = ix.reader().corrector("text")

    wordlist = sorted(u("bear bare beer sprung").split())
    c2 = words_to_corrector(wordlist)

    mc = spelling.MultiCorrector([c1, c2])
    assert_equal(mc.suggest("specail"), ["special", "specials"])
    assert_equal(mc.suggest("beur"), ["bear", "beer"])
    assert_equal(mc.suggest("sprang"), ["sprung", "spring"])


def test_wordlist():
    domain = "special specious spectacular spongy spring specials".split()
    domain.sort()
    cor = words_to_corrector(domain)
    assert_equal(cor.suggest("specail", maxdist=1), ["special"])


def test_wordfile():
    import os.path

    files = os.listdir(".")
    testdir = "tests"
    fname = "english-words.10.gz"
    if testdir in files:
        path = os.path.join(testdir, fname)
    elif fname in files:
        path = fname
    else:
        return
    if not os.path.exists(path):
        return

    wordfile = gzip.open(path, "rb")
    cor = words_to_corrector(wordfile)
    wordfile.close()
    assert_equal(cor.suggest("specail"), ["special"])


def test_query_highlight():
    qp = QueryParser("a", None)
    hf = highlight.HtmlFormatter()

    def do(text, terms):
        q = qp.parse(text)
        tks = [tk for tk in q.all_tokens() if tk.text in terms]
        for tk in tks:
            if tk.startchar is None or tk.endchar is None:
                assert False, tk
        fragment = highlight.Fragment(text, tks)
        return hf.format_fragment(fragment)

    assert_equal(do("a b c d", ["b"]),
                 'a <strong class="match term0">b</strong> c d')
    assert_equal(do('a (x:b OR y:"c d") e', ("b", "c")),
                 'a (x:<strong class="match term0">b</strong> OR ' +
                 'y:"<strong class="match term1">c</strong> d") e')


def test_query_terms():
    qp = QueryParser("a", None)

    q = qp.parse("alfa b:(bravo OR c:charlie) delta")
    assert_equal(sorted(q.iter_all_terms()), [("a", "alfa"), ("a", "delta"),
                                              ("b", "bravo"),
                                              ("c", "charlie")])

    q = qp.parse("alfa brav*")
    assert_equal(sorted(q.iter_all_terms()), [("a", "alfa")])

    q = qp.parse('a b:("b c" d)^2 e')
    tokens = [(t.fieldname, t.text, t.boost) for t in q.all_tokens()]
    assert_equal(tokens, [('a', 'a', 1.0), ('b', 'b', 2.0), ('b', 'c', 2.0),
                          ('b', 'd', 2.0), ('a', 'e', 1.0)])


def test_correct_query():
    schema = fields.Schema(a=fields.TEXT(spelling=True), b=fields.TEXT)
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(a=u("alfa bravo charlie delta"))
    w.add_document(a=u("delta echo foxtrot golf"))
    w.add_document(a=u("golf hotel india juliet"))
    w.add_document(a=u("juliet kilo lima mike"))
    w.commit()

    s = ix.searcher()
    qp = QueryParser("a", ix.schema)
    qtext = u('alpha ("brovo november" OR b:dolta) detail')
    q = qp.parse(qtext, ix.schema)

    c = s.correct_query(q, qtext)
    assert_equal(c.query.__unicode__(),
                 '(a:alfa AND (a:"bravo november" OR b:dolta) AND a:detail)')
    assert_equal(c.string, 'alfa ("bravo november" OR b:dolta) detail')

    qtext = u('alpha b:("brovo november" a:delta) detail')
    q = qp.parse(qtext, ix.schema)
    c = s.correct_query(q, qtext)
    assert_equal(c.query.__unicode__(),
                 '(a:alfa AND b:"brovo november" AND a:delta AND a:detail)')
    assert_equal(c.string, 'alfa b:("brovo november" a:delta) detail')

    hf = highlight.HtmlFormatter(classname="c")
    assert_equal(c.format_string(hf),
                 '<strong class="c term0">alfa</strong> ' +
                 'b:("brovo november" a:delta) detail')


def test_bypass_stemming():
    ana = analysis.StemmingAnalyzer()
    schema = fields.Schema(text=fields.TEXT(analyzer=ana, spelling=True))
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(text=u("rendering shading modeling reactions"))
    w.commit()

    with ix.reader() as r:
        assert_equal(list(r.lexicon("text")),
                     ["model", "reaction", "render", "shade"])
        assert_equal(list(r.word_graph("text").flatten_strings()),
                     ["modeling", "reactions", "rendering", "shading"])


def test_spelling_field_order():
    ana = analysis.StemmingAnalyzer()
    schema = fields.Schema(a=fields.TEXT, b=fields.TEXT(analyzer=ana),
                           c=fields.TEXT, d=fields.TEXT(analyzer=ana),
                           e=fields.TEXT(analyzer=ana), f=fields.TEXT)
    ix = RamStorage().create_index(schema)

    domain = u("alfa bravo charlie delta").split()
    w = ix.writer()
    for ls in permutations(domain):
        value = " ".join(ls)
        w.add_document(a=value, b=value, c=value, d=value, e=value, f=value)
    w.commit()


def test_find_self():
    wordlist = sorted(u("book bake bike bone").split())
    st = RamStorage()
    f = st.create_file("test")
    spelling.wordlist_to_graph_file(wordlist, f)

    gr = dawg.GraphReader(st.open_file("test"))
    gc = spelling.GraphCorrector(gr)
    assert_not_equal(gc.suggest("book")[0], "book")
    assert_not_equal(gc.suggest("bake")[0], "bake")
    assert_not_equal(gc.suggest("bike")[0], "bike")
    assert_not_equal(gc.suggest("bone")[0], "bone")







