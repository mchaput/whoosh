from __future__ import with_statement
import gzip

from whoosh import analysis, fields, highlight, query, spelling
from whoosh.compat import u
from whoosh.qparser import QueryParser
from whoosh.support.levenshtein import levenshtein
from whoosh.util.testing import TempIndex


_wordlist = sorted(u("render animation animate shader shading zebra koala"
                     "ready kismet reaction page delete quick fox jumped"
                     "over lazy dog wicked erase red team yellow under interest"
                     "open print acrid sear deaf feed grow heal jolly kilt"
                     "low zone xylophone crown vale brown neat meat reduction"
                     "blunder preaction lamppost").split())


def test_list_corrector():
    corr = spelling.ListCorrector(_wordlist)
    typo = "reoction"
    sugs = list(corr.suggest(typo, maxdist=2))
    target = []
    for lev_dist in range(1, 3):
        # sugs will return suggest first ordered by levenshtein distance
        # then second order by dictionary order
        target += [w for w in _wordlist
                   if levenshtein(typo, w) <= lev_dist and w not in target]
    assert sugs == target


def test_automaton():
    schema = fields.Schema(text=fields.TEXT)
    with TempIndex(schema, "automatonspell") as ix:
        with ix.writer() as w:
            w.add_document(text=u" ".join(_wordlist))

        with ix.reader() as r:
            bterms = list(r.lexicon("text"))
            words = [bterm.decode("utf8") for bterm in bterms]
            assert words == _wordlist

            typo = "reoction"
            sugs = list(r.terms_within("text", typo, maxdist=2))
            target = [w for w in _wordlist if levenshtein(typo, w) <= 2]
            assert sugs == target


def test_reader_corrector():
    schema = fields.Schema(text=fields.TEXT())
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(text=u"render zorro kaori postal")
            w.add_document(text=u"reader zebra koala pastry")
            w.add_document(text=u"leader libra oola paster")
            w.add_document(text=u"feeder lorry zoala baster")

        with ix.reader() as r:
            sp = spelling.ReaderCorrector(r, "text", schema["text"])
            assert sp.suggest(u"koala", maxdist=1) == [u'koala', u"zoala"]

            target = [u'kaori', u'koala', u'oola']
            sugs = sp.suggest(u"kaola", maxdist=2)
            assert sugs == target


def test_unicode_spelling():
    schema = fields.Schema(text=fields.ID())

    domain = [u"\u0924\u092a\u093e\u0907\u0939\u0930\u0941",
              u"\u65e5\u672c",
              u"\uc774\uc124\ud76c",
              ]

    with TempIndex(schema) as ix:
        with ix.writer() as w:
            for word in domain:
                w.add_document(text=word)

        with ix.reader() as r:
            rc = spelling.ReaderCorrector(r, "text", schema["text"])
            assert rc.suggest(u"\u65e5\u672e\u672c") == [u"\u65e5\u672c"]


def test_wordfile():
    import os.path

    path = os.path.join(os.path.dirname(__file__), "english-words.10.gz")
    wordfile = gzip.open(path, "rb")
    words = sorted(line.decode("latin1").strip().lower() for line in wordfile)

    cor = spelling.ListCorrector(words)
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
    schema = fields.Schema(a=fields.TEXT(), b=fields.TEXT)
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(a=u"alfa bravo charlie delta")
            w.add_document(a=u"delta echo foxtrot golf")
            w.add_document(a=u"golf hotel india juliet")
            w.add_document(a=u"juliet kilo lima mike")

        with ix.searcher() as s:
            qp = QueryParser("a", ix.schema)
            qtext = u'alpha ("brovo november" OR b:dolta) detail'
            q = qp.parse(qtext, ix.schema)

            c = s.correct_query(q, qtext)
            cq = c.query
            assert isinstance(cq, query.And)
            assert cq[0].text == "alfa"
            assert isinstance(cq[1], query.Or)
            assert isinstance(cq[1][0], query.Phrase)
            assert cq[1][0].words == ["bravo", "november"]

            qtext = u'alpha b:("brovo november" a:delta) detail'
            q = qp.parse(qtext, ix.schema)
            c = s.correct_query(q, qtext)
            assert c.query.__unicode__() == '(a:alfa AND b:"brovo november" AND a:delta AND a:detail)'
            assert c.string == 'alfa b:("brovo november" a:delta) detail'

            hf = highlight.HtmlFormatter(classname="c")
            assert c.format_string(hf) == '<strong class="c term0">alfa</strong> b:("brovo november" a:delta) detail'


def test_spelling_field():
    text = u"rendering shading modeling reactions"
    ana = analysis.StemmingAnalyzer()
    schema = fields.Schema(text=fields.TEXT(analyzer=ana, spelling=True))

    assert schema["text"].spelling
    assert schema["text"].separate_spelling()

    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(text=text)

        with ix.searcher() as s:
            r = s.reader()
            fieldobj = schema["text"]
            words = [fieldobj.from_bytes(t) for t in r.lexicon("text")]
            assert words == ["model", "reaction", "render", "shade"]

            words = [fieldobj.from_bytes(t) for t in r.lexicon("spell_text")]
            assert words == ["modeling", "reactions", "rendering", "shading"]

            # suggest() automatically looks in the spell_text field because
            # it calls fieldobj.spelling_fieldname() first
            assert s.suggest("text", "renderink") == ["rendering"]

        with ix.writer() as w:
            w.delete_document(0)


def test_correct_spell_field():
    ana = analysis.StemmingAnalyzer()
    schema = fields.Schema(text=fields.TEXT(analyzer=ana, spelling=True))
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(text=u"rendering shading modeling reactions")

        with ix.searcher() as s:
            text = s.schema["text"]
            spell_text = s.schema["spell_text"]

            r = s.reader()
            words = [text.from_bytes(t) for t in r.lexicon("text")]
            assert words == ["model", "reaction", "render", "shade"]

            words = [spell_text.from_bytes(t) for t in r.lexicon("spell_text")]
            assert words == ["modeling", "reactions", "rendering", "shading"]

            qp = QueryParser("text", s.schema)
            qtext = u"renderink"
            q = qp.parse(qtext, s.schema)

            r = s.search(q)
            assert len(r) == 0

            c = s.correct_query(q, qtext)
            assert c.string == "rendering"
            assert c.query == query.Term("text", "rendering")

            hf = highlight.HtmlFormatter(classname="c")
            assert c.format_string(hf) == '<strong class="c term0">rendering</strong>'


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

    schema = fields.Schema(content=fields.TEXT(stored=True, ),
                           quick=fields.NGRAM(maxsize=10, stored=True))
    with TempIndex(schema, "sugprefix") as ix:
        with ix.writer() as w:
            for item in domain:
                content = u(item)
                w.add_document(content=content, quick=content)

        with ix.searcher() as s:
            sugs = s.suggest("content", u"ra", maxdist=2, prefix=2)
            assert sugs == ['rage', 'rain']

            sugs = s.suggest("content", "ra", maxdist=2, prefix=1)
            assert sugs == ["rage", "rain", "roar"]


def test_prefix_address():
    fieldtype = fields.TEXT()
    schema = fields.Schema(f1=fieldtype, f2=fieldtype)
    with TempIndex(schema, "prefixaddr") as ix:
        with ix.writer() as w:
            w.add_document(f1=u"aabc aawx aaqr aade",
                           f2=u"aa12 aa34 aa56 aa78")

        with ix.searcher() as s:
            sugs = s.suggest("f1", u"aa", maxdist=2, prefix=2)
            assert sorted(sugs) == ["aabc", "aade", "aaqr", "aawx"]

            sugs = s.suggest("f2", u"aa", maxdist=2, prefix=2)
            assert sorted(sugs) == ["aa12", "aa34", "aa56", "aa78"]


def test_correct_correct():
    from whoosh import qparser

    schema = fields.Schema(a=fields.TEXT())
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            w.add_document(a=u'dworska')
            w.add_document(a=u'swojska')

        with ix.searcher() as s:
            s = ix.searcher()
            qtext = u'dworska'

            qp = qparser.QueryParser('a', ix.schema)
            q = qp.parse(qtext, ix.schema)
            c = s.correct_query(q, qtext)

            assert c.string == "dworska"
            string = c.format_string(highlight.UppercaseFormatter())
            assert string == "dworska"


def test_very_long_words():
    import sys
    length = int(sys.getrecursionlimit() * 1.5)

    strings1 = [u(chr(i) * length) for i in range(65, 70)]
    strings2 = [u(chr(i) * length) for i in range(71, 75)]

    ana = analysis.StemmingAnalyzer()
    schema = fields.Schema(text=fields.TEXT(analyzer=ana, ))
    with TempIndex(schema) as ix:
        with ix.writer() as w:
            for string in strings1:
                w.add_document(text=string)

        with ix.writer() as w:
            for string in strings2:
                w.add_document(text=string)
            w.optimize = True


# def test_add_spelling():
#     schema = fields.Schema(text1=fields.TEXT, text2=fields.TEXT)
#     ix = RamStorage().create_index(schema)
#     w = ix.writer()
#     w.add_document(text1=u"render zorro kaori postal", text2=u"alfa")
#     w.add_document(text1=u"reader zebra koala pastry", text2=u"alpa")
#     w.add_document(text1=u"leader libra ooala paster", text2=u"alpha")
#     w.add_document(text1=u"feeder lorry zoala baster", text2=u"olfo")
#     w.commit()
#
#     with ix.reader() as r:
#         assert not r.has_word_graph("text1")
#         assert not r.has_word_graph("text2")
#
#     from whoosh.writing import add_spelling
#     add_spelling(ix, ["text1", "text2"])
#
#     with ix.reader() as r:
#         assert r.has_word_graph("text1")
#         assert r.has_word_graph("text2")
#
#         sp = spelling.ReaderCorrector(r, "text1")
#         assert sp.suggest(u"kaola", maxdist=1) == [u'koala']
#         assert sp.suggest(u"kaola", maxdist=2) == [u'koala', u'kaori', u'ooala', u'zoala']
#
#         sp = spelling.ReaderCorrector(r, "text2")
#         assert sp.suggest(u"alfo", maxdist=1) == [u"alfa", u"olfo"]


# def test_multicorrector():
#     schema = fields.Schema(text=fields.TEXT())
#     ix = RamStorage().create_index(schema)
#     domain = u"special specious spectacular spongy spring specials".split()
#     for word in domain:
#         w = ix.writer()
#         w.add_document(text=word)
#         w.commit(merge=False)
#
#     c1 = ix.reader().corrector("text")
#
#     wordlist = sorted(u"bear bare beer sprung".split())
#     c2 = words_to_corrector(wordlist)
#
#     mc = spelling.MultiCorrector([c1, c2])
#     assert mc.suggest("specail") == ["special", "specials"]
#     assert mc.suggest("beur") == ["bear", "beer"]
#     assert mc.suggest("sprang") == ["sprung", "spring"]
