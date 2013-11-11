from __future__ import with_statement
import gzip

from whoosh import analysis, fields, highlight, spelling
from whoosh.automata import fst
from whoosh.compat import b, u, permutations
from whoosh.filedb.filestore import RamStorage
from whoosh.qparser import QueryParser
from whoosh.util.testing import TempIndex


def words_to_corrector(words):
    st = RamStorage()
    f = st.create_file("test")
    spelling.wordlist_to_graph_file(words, f)
    f = st.open_file("test")
    return spelling.GraphCorrector(fst.GraphReader(f))


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
    assert sugs == ["reaction", "preaction", "reduction"]


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
        assert sp.suggest(u("kaola"), maxdist=1) == ['koala']
        assert sp.suggest(u("kaola"), maxdist=2) == ['koala', 'kaori', 'ooala', 'zoala']


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
        assert sp.suggest(u("kaola"), maxdist=1) == [u('koala')]
        assert sp.suggest(u("kaola"), maxdist=2) == [u('koala'), u('kaori'), u('ooala'), u('zoala')]


def test_simple_spelling():
    schema = fields.Schema(text=fields.TEXT(spelling=True))

    domain = [u("alfa"), u("bravo"), u("charlie")]

    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        for word in domain:
            w.add_document(text=word)

    with ix.searcher() as s:
        r = ix.reader()
        assert r.has_word_graph("text")
        c = r._get_graph().cursor("text")
        assert list(r.word_graph("text").flatten_strings()) == domain


def test_unicode_spelling():
    schema = fields.Schema(text=fields.ID(spelling=True))

    domain = [u("\u0924\u092a\u093e\u0907\u0939\u0930\u0941"),
              u("\u65e5\u672c"),
              u("\uc774\uc124\ud76c"),
              ]

    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        for word in domain:
            w.add_document(text=word)

    with ix.reader() as r:
        assert r.has_word_graph("text")
        c = r._get_graph().cursor("text")
        assert list(c.flatten_strings()) == domain
        assert list(r.word_graph("text").flatten_strings()) == domain

        rc = spelling.ReaderCorrector(r, "text")
        assert rc.suggest(u("\u65e5\u672e\u672c")) == [u("\u65e5\u672c")]


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

    from whoosh.writing import add_spelling
    add_spelling(ix, ["text1", "text2"])

    with ix.reader() as r:
        assert r.has_word_graph("text1")
        assert r.has_word_graph("text2")

        sp = spelling.ReaderCorrector(r, "text1")
        assert sp.suggest(u("kaola"), maxdist=1) == [u('koala')]
        assert sp.suggest(u("kaola"), maxdist=2) == [u('koala'), u('kaori'), u('ooala'), u('zoala')]

        sp = spelling.ReaderCorrector(r, "text2")
        assert sp.suggest(u("alfo"), maxdist=1) == [u("alfa"), u("olfo")]


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
        assert words == sorted(domain)

        corr = r.corrector("text")
        assert corr.suggest("specail", maxdist=2) == ["special", "specials"]

    ix.optimize()
    with ix.reader() as r:
        assert r.is_atomic()
        fieldobj = schema["text"]
        assert [fieldobj.from_bytes(t) for t in r.lexicon("text")] == sorted(domain)
        assert r.has_word_graph("text")
        words = list(r.word_graph("text").flatten_strings())
        assert words == sorted(domain)

        corr = r.corrector("text")
        assert corr.suggest("specail", maxdist=2) == ["special", "specials"]


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
    assert mc.suggest("specail") == ["special", "specials"]
    assert mc.suggest("beur") == ["bear", "beer"]
    assert mc.suggest("sprang") == ["sprung", "spring"]


def test_wordlist():
    domain = "special specious spectacular spongy spring specials".split()
    domain.sort()
    cor = words_to_corrector(domain)
    assert cor.suggest("specail", maxdist=1) == ["special"]


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
    assert cor.suggest("specail") == ["special"]


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

    assert do("a b c d", ["b"]) == 'a <strong class="match term0">b</strong> c d'
    assert do('a (x:b OR y:"c d") e', ("b", "c")) == 'a (x:<strong class="match term0">b</strong> OR y:"<strong class="match term1">c</strong> d") e'


def test_query_terms():
    qp = QueryParser("a", None)

    q = qp.parse("alfa b:(bravo OR c:charlie) delta")
    assert sorted(q.iter_all_terms()) == [("a", "alfa"), ("a", "delta"),
                                          ("b", "bravo"), ("c", "charlie")]

    q = qp.parse("alfa brav*")
    assert sorted(q.iter_all_terms()) == [("a", "alfa")]

    q = qp.parse('a b:("b c" d)^2 e')
    tokens = [(t.fieldname, t.text, t.boost) for t in q.all_tokens()]
    assert tokens == [('a', 'a', 1.0), ('b', 'b', 2.0), ('b', 'c', 2.0),
                      ('b', 'd', 2.0), ('a', 'e', 1.0)]


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
    assert c.query.__unicode__() == '(a:alfa AND (a:"bravo november" OR b:dolta) AND a:detail)'
    assert c.string == 'alfa ("bravo november" OR b:dolta) detail'

    qtext = u('alpha b:("brovo november" a:delta) detail')
    q = qp.parse(qtext, ix.schema)
    c = s.correct_query(q, qtext)
    assert c.query.__unicode__() == '(a:alfa AND b:"brovo november" AND a:delta AND a:detail)'
    assert c.string == 'alfa b:("brovo november" a:delta) detail'

    hf = highlight.HtmlFormatter(classname="c")
    assert c.format_string(hf) == '<strong class="c term0">alfa</strong> b:("brovo november" a:delta) detail'


def test_bypass_stemming():
    ana = analysis.StemmingAnalyzer()
    schema = fields.Schema(text=fields.TEXT(analyzer=ana, spelling=True))

    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(text=u("rendering shading modeling reactions"))
    w.commit()

    with ix.reader() as r:
        fieldobj = schema["text"]
        assert [fieldobj.from_bytes(t) for t in r.lexicon("text")] == ["model", "reaction", "render", "shade"]
        assert list(r.word_graph("text").flatten_strings()) == ["modeling", "reactions", "rendering", "shading"]


def test_bypass_stemming2():
    ana = analysis.StemmingAnalyzer()
    schema = fields.Schema(content=fields.TEXT(analyzer=ana, spelling=True))

    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        w.add_document(content=u("IPFSTD1 IPFSTD_kdwq134 Kaminski-all Study00:00:00"))
        w.add_document(content=u("IPFSTD1 IPFSTD_kdwq134 Kaminski-all Study"))
        w.add_document(content=u("This is the first document we've added!"))


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

    gr = fst.GraphReader(st.open_file("test"))
    gc = spelling.GraphCorrector(gr)
    assert gc.suggest("book")[0] != "book"
    assert gc.suggest("bake")[0] != "bake"
    assert gc.suggest("bike")[0] != "bike"
    assert gc.suggest("bone")[0] != "bone"


def test_suggest_prefix():
    domain = ("Shoot To Kill",
              "Bloom, Split and Deviate",
              "Rankle the Seas and the Skies",
              "Lightning Flash Flame Shell",
              "Flower Wind Rage and Flower God Roar, Heavenly Wind Rage and "
              "Heavenly Demon Sneer",
              "All Waves, Rise now and Become my Shield, Lightning, Strike "
              "now and Become my Blade",
              "Cry, Raise Your Head, Rain Without end",
              "Sting All Enemies To Death",
              "Reduce All Creation to Ash",
              "Sit Upon the Frozen Heavens",
              "Call forth the Twilight")

    schema = fields.Schema(content=fields.TEXT(stored=True, spelling=True),
                           quick=fields.NGRAM(maxsize=10, stored=True))

    with TempIndex(schema, "sugprefix") as ix:
        with ix.writer() as w:
            for item in domain:
                content = u(item)
                w.add_document(content=content, quick=content)

        with ix.searcher() as s:
            sugs = s.suggest("content", u("ra"), maxdist=2, prefix=2)
            assert sugs == ['rage', 'rain']

            sugs = s.suggest("content", "ra", maxdist=2, prefix=1)
            assert sugs == ["rage", "rain", "roar"]


def test_prefix_address():
    fieldtype = fields.TEXT(spelling=True)
    schema = fields.Schema(f1=fieldtype, f2=fieldtype)
    with TempIndex(schema, "prefixaddr") as ix:
        with ix.writer() as w:
            w.add_document(f1=u("aabc aawx aaqr aade"),
                           f2=u("aa12 aa34 aa56 aa78"))

        with ix.searcher() as s:
            sugs = s.suggest("f1", u("aa"), maxdist=2, prefix=2)
            assert sorted(sugs) == ["aabc", "aade", "aaqr", "aawx"]

            sugs = s.suggest("f2", u("aa"), maxdist=2, prefix=2)
            assert sorted(sugs) == ["aa12", "aa34", "aa56", "aa78"]


def test_missing_suggestion():
    ana = analysis.StemmingAnalyzer()
    schema = fields.Schema(content=fields.TEXT(analyzer=ana, spelling=True),
                           organism=fields.ID)
    ix = RamStorage().create_index(schema)

    with ix.writer() as w:
        w.add_document(organism=u("hs"), content=u("cells"))
        w.add_document(organism=u("hs"), content=u("cell"))

    with ix.searcher() as s:
        r = s.reader()
        assert r.has_word_graph("content")
        gr = r.word_graph("content")
        assert list(gr.flatten()) == [b("cell"), b("cells")]

        c = s.corrector("content")
        # Note that corrector won't suggest the word you submit even though it's
        # in the index
        assert c.suggest("cell") == ["cells"]


def test_correct_correct():
    from whoosh import qparser

    schema = fields.Schema(a=fields.TEXT(spelling=True))
    ix = RamStorage().create_index(schema)
    ix_writer = ix.writer()

    ix_writer.add_document(a=u('dworska'))
    ix_writer.add_document(a=u('swojska'))

    ix_writer.commit()

    s = ix.searcher()
    qtext = u('dworska')

    qp = qparser.QueryParser('a', ix.schema)
    q = qp.parse(qtext, ix.schema)
    c = s.correct_query(q, qtext)

    assert c.string == "dworska"
    assert c.format_string(highlight.UppercaseFormatter()) == "dworska"


def test_very_long_words():
    import sys
    length = int(sys.getrecursionlimit() * 1.5)

    strings1 = [u(chr(i) * length) for i in range(65, 70)]
    strings2 = [u(chr(i) * length) for i in range(71, 75)]

    ana = analysis.StemmingAnalyzer()
    schema = fields.Schema(text=fields.TEXT(analyzer=ana, spelling=True))
    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        for string in strings1:
            w.add_document(text=string)

    with ix.writer() as w:
        for string in strings2:
            w.add_document(text=string)
        w.optimize = True


