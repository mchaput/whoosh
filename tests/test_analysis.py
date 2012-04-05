from __future__ import with_statement

from nose.tools import assert_equal  #@UnresolvedImport

from whoosh import analysis, fields, qparser
from whoosh.compat import u, unichr
from whoosh.filedb.filestore import RamStorage
from whoosh.support.testing import skip_if_unavailable


def test_regextokenizer():
    value = u("AAAaaaBBBbbbCCCcccDDDddd")

    rex = analysis.RegexTokenizer("[A-Z]+")
    assert_equal([t.text for t in rex(value)], ["AAA", "BBB", "CCC", "DDD"])

    rex = analysis.RegexTokenizer("[A-Z]+", gaps=True)
    assert_equal([t.text for t in rex(value)], ["aaa", "bbb", "ccc", "ddd"])


def test_path_tokenizer():
    value = u("/alfa/bravo/charlie/delta/")
    pt = analysis.PathTokenizer()
    assert_equal([t.text for t in pt(value)], ["/alfa", "/alfa/bravo",
                                               "/alfa/bravo/charlie",
                                               "/alfa/bravo/charlie/delta"])


def test_composition1():
    ca = analysis.RegexTokenizer() | analysis.LowercaseFilter()
    assert_equal(ca.__class__.__name__, "CompositeAnalyzer")
    assert_equal(ca[0].__class__.__name__, "RegexTokenizer")
    assert_equal(ca[1].__class__.__name__, "LowercaseFilter")
    assert_equal([t.text for t in ca(u("ABC 123"))], ["abc", "123"])


def test_composition2():
    ca = analysis.RegexTokenizer() | analysis.LowercaseFilter()
    sa = ca | analysis.StopFilter()
    assert_equal(len(sa), 3)
    assert_equal(sa.__class__.__name__, "CompositeAnalyzer")
    assert_equal(sa[0].__class__.__name__, "RegexTokenizer")
    assert_equal(sa[1].__class__.__name__, "LowercaseFilter")
    assert_equal(sa[2].__class__.__name__, "StopFilter")
    assert_equal([t.text for t in sa(u("The ABC 123"))], ["abc", "123"])


def test_composition3():
    sa = analysis.RegexTokenizer() | analysis.StopFilter()
    assert_equal(sa.__class__.__name__, "CompositeAnalyzer")


def test_composing_functions():
    def filter(tokens):
        for t in tokens:
            t.text = t.text.upper()
            yield t

    analyzer = analysis.RegexTokenizer() | filter
    assert_equal([t.text for t in analyzer(u("abc def"))], ["ABC", "DEF"])


def test_shared_composition():
    shared = analysis.RegexTokenizer(r"\S+") | analysis.LowercaseFilter()

    ana1 = shared | analysis.NgramFilter(3)
    ana2 = shared | analysis.DoubleMetaphoneFilter()

    assert_equal([t.text for t in ana1(u("hello"))], ["hel", "ell", "llo"])
    assert_equal([t.text for t in ana2(u("hello"))], ["HL"])


def test_multifilter():
    f1 = analysis.LowercaseFilter()
    f2 = analysis.PassFilter()
    mf = analysis.MultiFilter(a=f1, b=f2)
    ana = analysis.RegexTokenizer(r"\S+") | mf
    text = u("ALFA BRAVO CHARLIE")
    assert_equal([t.text for t in ana(text, mode="a")],
                 ["alfa", "bravo", "charlie"])
    assert_equal([t.text for t in ana(text, mode="b")],
                 ["ALFA", "BRAVO", "CHARLIE"])


def test_tee_filter():
    target = u("Alfa Bravo Charlie")
    f1 = analysis.LowercaseFilter()
    f2 = analysis.ReverseTextFilter()
    ana = analysis.RegexTokenizer(r"\S+") | analysis.TeeFilter(f1, f2)
    result = " ".join([t.text for t in ana(target)])
    assert_equal(result, "alfa aflA bravo ovarB charlie eilrahC")

    class ucfilter(analysis.Filter):
        def __call__(self, tokens):
            for t in tokens:
                t.text = t.text.upper()
                yield t

    f2 = analysis.ReverseTextFilter() | ucfilter()
    ana = analysis.RegexTokenizer(r"\S+") | analysis.TeeFilter(f1, f2)
    result = " ".join([t.text for t in ana(target)])
    assert_equal(result, "alfa AFLA bravo OVARB charlie EILRAHC")

    f1 = analysis.PassFilter()
    f2 = analysis.BiWordFilter()
    ana = (analysis.RegexTokenizer(r"\S+")
           | analysis.TeeFilter(f1, f2)
           | analysis.LowercaseFilter())
    result = " ".join([t.text for t in ana(target)])
    assert_equal(result, "alfa alfa-bravo bravo bravo-charlie charlie")


def test_intraword():
    iwf = analysis.IntraWordFilter(mergewords=True, mergenums=True)
    ana = analysis.RegexTokenizer(r"\S+") | iwf

    def check(text, ls):
        assert_equal([(t.pos, t.text) for t in ana(text)], ls)

    check(u("PowerShot)"), [(0, "Power"), (1, "Shot"), (1, "PowerShot")])
    check(u("A's+B's&C's"), [(0, "A"), (1, "B"), (2, "C"), (2, "ABC")])
    check(u("Super-Duper-XL500-42-AutoCoder!"),
          [(0, "Super"), (1, "Duper"), (2, "XL"), (2, "SuperDuperXL"),
           (3, "500"), (4, "42"), (4, "50042"), (5, "Auto"), (6, "Coder"),
           (6, "AutoCoder")])


def test_intraword_chars():
    iwf = analysis.IntraWordFilter(mergewords=True, mergenums=True)
    ana = analysis.RegexTokenizer(r"\S+") | iwf | analysis.LowercaseFilter()

    target = u("WiKiWo-rd")
    tokens = [(t.text, t.startchar, t.endchar)
              for t in ana(target, chars=True)]
    assert_equal(tokens, [("wi", 0, 2), ("ki", 2, 4), ("wo", 4, 6),
                          ("rd", 7, 9), ("wikiword", 0, 9)])

    target = u("Zo WiKiWo-rd")
    tokens = [(t.text, t.startchar, t.endchar)
              for t in ana(target, chars=True)]
    assert_equal(tokens, [("zo", 0, 2), ("wi", 3, 5), ("ki", 5, 7),
                          ("wo", 7, 9), ("rd", 10, 12), ("wikiword", 3, 12)])


def test_intraword_possessive():
    iwf = analysis.IntraWordFilter(mergewords=True, mergenums=True)
    ana = analysis.RegexTokenizer(r"\S+") | iwf | analysis.LowercaseFilter()

    target = u("O'Malley's-Bar")
    tokens = [(t.text, t.startchar, t.endchar)
              for t in ana(target, chars=True)]
    assert_equal(tokens, [("o", 0, 1), ("malley", 2, 8), ("bar", 11, 14),
                          ("omalleybar", 0, 14)])


def test_word_segments():
    wordset = set(u("alfa bravo charlie delta").split())

    cwf = analysis.CompoundWordFilter(wordset, keep_compound=True)
    ana = analysis.RegexTokenizer(r"\S+") | cwf
    target = u("alfacharlie bravodelta delto bravo subalfa")
    tokens = [t.text for t in ana(target)]
    assert_equal(tokens, ["alfacharlie", "alfa", "charlie", "bravodelta",
                          "bravo", "delta", "delto", "bravo", "subalfa"])

    cwf = analysis.CompoundWordFilter(wordset, keep_compound=False)
    ana = analysis.RegexTokenizer(r"\S+") | cwf
    target = u("alfacharlie bravodelta delto bravo subalfa")
    tokens = [t.text for t in ana(target)]
    assert_equal(tokens, ["alfa", "charlie", "bravo", "delta", "delto",
                          "bravo", "subalfa"])


def test_biword():
    ana = analysis.RegexTokenizer(r"\w+") | analysis.BiWordFilter()
    result = [t.copy() for t
              in ana(u("the sign of four"), chars=True, positions=True)]
    assert_equal(["the-sign", "sign-of", "of-four"], [t.text for t in result])
    assert_equal([(0, 8), (4, 11), (9, 16)], [(t.startchar, t.endchar)
                                              for t in result])
    assert_equal([0, 1, 2], [t.pos for t in result])

    result = [t.copy() for t in ana(u("single"))]
    assert_equal(len(result), 1)
    assert_equal(result[0].text, "single")


def test_shingles():
    ana = analysis.RegexTokenizer(r"\w+") | analysis.ShingleFilter(3, " ")
    source = u("better a witty fool than a foolish wit")
    results = [t.copy() for t in ana(source, positions=True, chars=True)]
    assert_equal([t.text for t in results],
                 [u('better a witty'), u('a witty fool'), u('witty fool than'),
                  u('fool than a'), u('than a foolish'), u('a foolish wit')])
    assert_equal([t.pos for t in results], list(range(len(results))))
    for t in results:
        assert_equal(t.text, source[t.startchar:t.endchar])


def test_unicode_blocks():
    from whoosh.support.unicode import blocks, blockname, blocknum

    assert_equal(blockname(u('a')), 'Basic Latin')
    assert_equal(blockname(unichr(0x0b80)), 'Tamil')
    assert_equal(blockname(unichr(2048)), None)
    assert_equal(blocknum(u('a')), 0)
    assert_equal(blocknum(unichr(0x0b80)), 22)
    assert_equal(blocknum(unichr(2048)), None)
    assert_equal(blocknum(u('a')), blocks.Basic_Latin)  # @UndefinedVariable
    assert_equal(blocknum(unichr(0x0b80)), blocks.Tamil)  # @UndefinedVariable


def test_double_metaphone():
    mf = (analysis.RegexTokenizer()
          | analysis.LowercaseFilter()
          | analysis.DoubleMetaphoneFilter())
    results = [(t.text, t.boost) for t in mf(u("Spruce View"))]
    assert_equal(results, [('SPRS', 1.0), ('F', 1.0), ('FF', 0.5)])

    mf = (analysis.RegexTokenizer()
          | analysis.LowercaseFilter()
          | analysis.DoubleMetaphoneFilter(combine=True))
    results = [(t.text, t.boost) for t in mf(u("Spruce View"))]
    assert_equal(results, [('spruce', 1.0), ('SPRS', 1.0), ('view', 1.0),
                           ('F', 1.0), ('FF', 0.5)])

    namefield = fields.TEXT(analyzer=mf)
    texts = list(namefield.process_text(u("Spruce View"), mode="query"))
    assert_equal(texts, [u('spruce'), 'SPRS', u('view'), 'F', 'FF'])


def test_substitution():
    mf = analysis.RegexTokenizer(r"\S+") | analysis.SubstitutionFilter("-", "")
    assert_equal([t.text for t in mf(u("one-two th-re-ee four"))],
                 ["onetwo", "threee", "four"])

    mf = (analysis.RegexTokenizer(r"\S+")
          | analysis.SubstitutionFilter("([^=]*)=(.*)", r"\2=\1"))
    assert_equal([t.text for t in mf(u("a=b c=d ef"))], ["b=a", "d=c", "ef"])


def test_delimited_attribute():
    ana = analysis.RegexTokenizer(r"\S+") | analysis.DelimitedAttributeFilter()
    results = [(t.text, t.boost) for t in ana(u("image render^2 file^0.5"))]
    assert_equal(results, [("image", 1.0), ("render", 2.0), ("file", 0.5)])


def test_porter2():
    from whoosh.lang.porter2 import stem

    plurals = ['caresses', 'flies', 'dies', 'mules', 'denied',
               'died', 'agreed', 'owned', 'humbled', 'sized',
               'meeting', 'stating', 'siezing', 'itemization',
               'sensational', 'traditional', 'reference', 'colonizer',
               'plotted']
    singles = [stem(w) for w in plurals]

    assert_equal(singles, ['caress', 'fli', 'die', 'mule', 'deni', 'die',
                           'agre', 'own', 'humbl', 'size', 'meet', 'state',
                           'siez', 'item', 'sensat', 'tradit', 'refer',
                           'colon', 'plot'])
    assert_equal(stem("bill's"), "bill")
    assert_equal(stem("y's"), "y")


@skip_if_unavailable("Stemmer")
def test_pystemmer():
    ana = (analysis.RegexTokenizer()
           | analysis.LowercaseFilter()
           | analysis.PyStemmerFilter())
    schema = fields.Schema(text=fields.TEXT(analyzer=ana))
    st = RamStorage()

    ix = st.create_index(schema)
    with ix.writer() as w:
        w.add_document(text=u("rains falling strangely"))

    ix = st.open_index()
    with ix.writer() as w:
        w.add_document(text=u("pains stalling strongly"))

    ix = st.open_index()
    with ix.reader() as r:
        assert_equal(list(r.lexicon("text")), ["fall", "pain", "rain", "stall",
                                               "strang", "strong"])


def test_url():
    sample = u("Visit http://bitbucket.org/mchaput/whoosh or " +
               "urn:isbn:5930502 or http://www.apple.com/.")

    anas = [analysis.SimpleAnalyzer(analysis.url_pattern),
            analysis.StandardAnalyzer(analysis.url_pattern, stoplist=None)]
    for ana in anas:
        ts = [t.text for t in ana(sample)]
        assert_equal(ts, [u('visit'), u('http://bitbucket.org/mchaput/whoosh'),
                          u('or'), u('urn:isbn:5930502'), u('or'),
                          u('http://www.apple.com/')])


def test_name_field():
    ana = (analysis.RegexTokenizer(r"\S+")
           | analysis.LowercaseFilter()
           | analysis.DoubleMetaphoneFilter(combine=True))
    namefield = fields.TEXT(analyzer=ana, multitoken_query="or")
    schema = fields.Schema(id=fields.STORED, name=namefield)

    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(id=u("one"), name=u("Leif Ericson"))
    w.commit()

    s = ix.searcher()
    qp = qparser.QueryParser("name", schema)
    q = qp.parse(u("leaf eriksen"), normalize=False)
    r = s.search(q)
    assert_equal(len(r), 1)


def test_start_pos():
    from whoosh import formats
    ana = analysis.RegexTokenizer(r"\S+") | analysis.LowercaseFilter()
    kw = {"positions": True}
    tks = formats.tokens(u("alfa bravo charlie delta"), ana, kw)
    assert_equal([t.pos for t in tks], [0, 1, 2, 3])

    kw["start_pos"] = 3
    ts = [t.copy() for t in formats.tokens(u("A B C D").split(), ana, kw)]
    assert_equal(" ".join([t.text for t in ts]), "A B C D")
    assert_equal([t.pos for t in ts], [3, 4, 5, 6])


def test_frowny_face():
    # See https://bitbucket.org/mchaput/whoosh/issue/166/
    ana = analysis.RegexTokenizer(r"\S+") | analysis.IntraWordFilter()
    # text is all delimiters
    tokens = [t.text for t in ana(u(":-("))]
    assert_equal(tokens, [])

    # text has consecutive delimiters
    tokens = [t.text for t in ana(u("LOL:)"))]
    assert_equal(tokens, ["LOL"])


def test_ngrams():
    s = u("abcdefg h ij klm")
    tk = analysis.RegexTokenizer(r"\S+")

    def dotest(f):
        ana = tk | f
        tokens = ana(s, positions=True, chars=True)
        return "/".join(t.text for t in tokens)

    f = analysis.NgramFilter(3, 4)
    assert_equal(dotest(f), "abc/abcd/bcd/bcde/cde/cdef/def/defg/efg/klm")

    f = analysis.NgramFilter(3, 4, at="start")
    assert_equal(dotest(f), "abc/abcd/klm")

    f = analysis.NgramFilter(3, 4, at="end")
    assert_equal(dotest(f), "defg/efg/klm")

    ana = tk | analysis.NgramFilter(2, 5, at="end")
    tokens = [(t.text, t.startchar, t.endchar) for t in ana(s, chars=True)]
    assert_equal(tokens, [("cdefg", 2, 7), ("defg", 3, 7), ("efg", 4, 7),
                          ("fg", 5, 7), ("ij", 10, 12), ("klm", 13, 16),
                          ("lm", 14, 16)])



