from nose.tools import assert_equal

from whoosh import fields, qparser
from whoosh.analysis import *
from whoosh.filedb.filestore import RamStorage


def test_regextokenizer():
    value = u"AAAaaaBBBbbbCCCcccDDDddd"
    
    rex = RegexTokenizer("[A-Z]+")
    assert_equal([t.text for t in rex(value)], [u"AAA", u"BBB", u"CCC", u"DDD"])
    
    rex = RegexTokenizer("[A-Z]+", gaps=True)
    assert_equal([t.text for t in rex(value)], [u"aaa", u"bbb", u"ccc", u"ddd"])

def test_composition1():
    ca = RegexTokenizer() | LowercaseFilter()
    assert_equal(ca.__class__.__name__, "CompositeAnalyzer")
    assert_equal(ca[0].__class__.__name__, "RegexTokenizer")
    assert_equal(ca[1].__class__.__name__, "LowercaseFilter")
    assert_equal([t.text for t in ca(u"ABC 123")], ["abc", "123"])

def test_composition2():
    ca = RegexTokenizer() | LowercaseFilter()
    sa = ca | StopFilter()
    assert_equal(len(sa), 3)
    assert_equal(sa.__class__.__name__, "CompositeAnalyzer")
    assert_equal(sa[0].__class__.__name__, "RegexTokenizer")
    assert_equal(sa[1].__class__.__name__, "LowercaseFilter")
    assert_equal(sa[2].__class__.__name__, "StopFilter")
    assert_equal([t.text for t in sa(u"The ABC 123")], ["abc", "123"])

def test_composition3():
    sa = RegexTokenizer() | StopFilter()
    assert_equal(sa.__class__.__name__, "CompositeAnalyzer")

def test_composing_functions():
    def filter(tokens):
        for t in tokens:
            t.text = t.text.upper()
            yield t
            
    analyzer = RegexTokenizer() | filter
    assert_equal([t.text for t in analyzer(u"abc def")], ["ABC", "DEF"])

def test_shared_composition():
    shared = RegexTokenizer(r"\S+") | LowercaseFilter()
    
    ana1 = shared | NgramFilter(3)
    ana2 = shared | DoubleMetaphoneFilter()
    
    assert_equal([t.text for t in ana1(u"hello")], ["hel", "ell", "llo"])
    assert_equal([t.text for t in ana2(u"hello")], ["HL"])

def test_multifilter():
    f1 = LowercaseFilter()
    f2 = PassFilter()
    mf = MultiFilter(a=f1, b=f2)
    ana = RegexTokenizer(r"\S+") | mf
    text = u"ALFA BRAVO CHARLIE"
    assert_equal([t.text for t in ana(text, mode="a")], ["alfa", "bravo", "charlie"])
    assert_equal([t.text for t in ana(text, mode="b")], ["ALFA", "BRAVO", "CHARLIE"])

def test_intraword():
    iwf = IntraWordFilter(mergewords=True, mergenums=True)
    ana = RegexTokenizer(r"\S+") | iwf
    
    def check(text, ls):
        assert_equal([(t.pos, t.text) for t in ana(text)], ls)
        
    check(u"PowerShot", [(0, "Power"), (1, "Shot"), (1, "PowerShot")])
    check(u"A's+B's&C's", [(0, "A"), (1, "B"), (2, "C"), (2, "ABC")])
    check(u"Super-Duper-XL500-42-AutoCoder!",
          [(0, "Super"), (1, "Duper"), (2, "XL"), (2, "SuperDuperXL"),
           (3, "500"), (4, "42"), (4, "50042"), (5, "Auto"), (6, "Coder"),
           (6, "AutoCoder")])

def test_biword():
    ana = RegexTokenizer(r"\w+") | BiWordFilter()
    result = [t.copy() for t
              in ana(u"the sign of four", chars=True, positions=True)]
    assert_equal(["the-sign", "sign-of", "of-four"], [t.text for t in result])
    assert_equal([(0, 8), (4, 11), (9, 16)], [(t.startchar, t.endchar) for t in result])
    assert_equal([0, 1, 2], [t.pos for t in result])
    
    result = [t.copy() for t in ana(u"single")]
    assert_equal(len(result), 1)
    assert_equal(result[0].text, "single")

def test_shingles():
    ana = RegexTokenizer(r"\w+") | ShingleFilter(3, " ")
    source = u"better a witty fool than a foolish wit"
    results = [t.copy() for t in ana(source, positions=True, chars=True)]
    assert_equal([t.text for t in results],
                 [u'better a witty', u'a witty fool', u'witty fool than',
                  u'fool than a', u'than a foolish', u'a foolish wit'])
    assert_equal([t.pos for t in results], range(len(results)))
    for t in results:
        assert_equal(t.text, source[t.startchar:t.endchar])
    
def test_unicode_blocks():
    from whoosh.support.unicode import blocks, blockname, blocknum
    
    assert_equal(blockname(u'a'), 'Basic Latin')
    assert_equal(blockname(unichr(0x0b80)), 'Tamil')
    assert_equal(blockname(unichr(2048)), None)
    assert_equal(blocknum(u'a'), 0)
    assert_equal(blocknum(unichr(0x0b80)), 22)
    assert_equal(blocknum(unichr(2048)), None)
    assert_equal(blocknum(u'a'), blocks.Basic_Latin)
    assert_equal(blocknum(unichr(0x0b80)), blocks.Tamil)
    
def test_double_metaphone():
    mf = RegexTokenizer() | LowercaseFilter() | DoubleMetaphoneFilter()
    results = [(t.text, t.boost) for t in mf(u"Spruce View")]
    assert_equal(results, [('SPRS', 1.0), ('F', 1.0), ('FF', 0.5)])
    
    mf = RegexTokenizer() | LowercaseFilter() | DoubleMetaphoneFilter(combine=True)
    results = [(t.text, t.boost) for t in mf(u"Spruce View")]
    assert_equal(results, [('spruce', 1.0), ('SPRS', 1.0), ('view', 1.0),
                           ('F', 1.0), ('FF', 0.5)])

    namefield = fields.TEXT(analyzer=mf)
    texts = list(namefield.process_text(u"Spruce View", mode="query"))
    assert_equal(texts, [u'spruce', 'SPRS', u'view', 'F', 'FF'])

def test_substitution():
    mf = RegexTokenizer(r"\S+") | SubstitutionFilter("-", "")
    assert_equal([t.text for t in mf(u"one-two th-re-ee four")],
                 ["onetwo", "threee", "four"])
    
    mf = RegexTokenizer(r"\S+") | SubstitutionFilter("([^=]*)=(.*)", r"\2=\1")
    assert_equal([t.text for t in mf(u"a=b c=d ef")], ["b=a", "d=c", "ef"])

def test_delimited_attribute():
    ana = RegexTokenizer(r"\S+") | DelimitedAttributeFilter()
    results = [(t.text, t.boost) for t in ana(u"image render^2 file^0.5")]
    assert_equal(results, [("image", 1.0), ("render", 2.0), ("file", 0.5)])
    
def test_porter2():
    from whoosh.lang.porter2 import stem
    
    plurals = ['caresses', 'flies', 'dies', 'mules', 'denied',
               'died', 'agreed', 'owned', 'humbled', 'sized',
               'meeting', 'stating', 'siezing', 'itemization',
               'sensational', 'traditional', 'reference', 'colonizer',
               'plotted']
    singles = [stem(w) for w in plurals]
    
    assert_equal(singles, ['caress', 'fli', 'die', 'mule', 'deni', 'die', 'agre',
                           'own', 'humbl', 'size', 'meet', 'state', 'siez', 'item',
                           'sensat', 'tradit', 'refer', 'colon', 'plot'])
    assert_equal(stem("bill's"), "bill")
    assert_equal(stem("y's"), "y")

def test_url():
    sample = u"Visit http://bitbucket.org/mchaput/whoosh or urn:isbn:5930502 or http://www.apple.com/."
    
    for ana in (SimpleAnalyzer(url_pattern),
                StandardAnalyzer(url_pattern, stoplist=None)):
        ts = [t.text for t in ana(sample)]
        assert_equal(ts, [u'visit', u'http://bitbucket.org/mchaput/whoosh',
                          u'or', u'urn:isbn:5930502', u'or', u'http://www.apple.com/'])

def test_name_field():
    ana = (RegexTokenizer(r"\S+")
           | LowercaseFilter()
           | DoubleMetaphoneFilter(combine=True))
    namefield = fields.TEXT(analyzer=ana, multitoken_query="or")
    schema = fields.Schema(id=fields.STORED, name=namefield)
    
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(id=u"one", name=u"Leif Ericson")
    w.commit()
    
    s = ix.searcher()
    qp = qparser.QueryParser("name", schema)
    q = qp.parse(u"leaf eriksen", normalize=False)
    r = s.search(q)
    assert_equal(len(r), 1)





