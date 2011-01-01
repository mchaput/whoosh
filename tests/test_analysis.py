import unittest

from whoosh.analysis import *


class TestAnalysis(unittest.TestCase):
    def test_regextokenizer(self):
        value = u"AAAaaaBBBbbbCCCcccDDDddd"
        
        rex = RegexTokenizer("[A-Z]+")
        self.assertEqual([t.text for t in rex(value)],
                         [u"AAA", u"BBB", u"CCC", u"DDD"])
        
        rex = RegexTokenizer("[A-Z]+", gaps=True)
        self.assertEqual([t.text for t in rex(value)],
                         [u"aaa", u"bbb", u"ccc", u"ddd"])
    
    def test_composition1(self):
        ca = RegexTokenizer() | LowercaseFilter()
        self.assertEqual(ca.__class__.__name__, "CompositeAnalyzer")
        self.assertEqual(ca[0].__class__.__name__, "RegexTokenizer")
        self.assertEqual(ca[1].__class__.__name__, "LowercaseFilter")
        self.assertEqual([t.text for t in ca(u"ABC 123")], ["abc", "123"])
    
    def test_composition2(self):
        ca = RegexTokenizer() | LowercaseFilter()
        sa = ca | StopFilter()
        self.assertEqual(len(sa), 3)
        self.assertEqual(sa.__class__.__name__, "CompositeAnalyzer")
        self.assertEqual(sa[0].__class__.__name__, "RegexTokenizer")
        self.assertEqual(sa[1].__class__.__name__, "LowercaseFilter")
        self.assertEqual(sa[2].__class__.__name__, "StopFilter")
        self.assertEqual([t.text for t in sa(u"The ABC 123")], ["abc", "123"])
    
    def test_composition3(self):
        sa = RegexTokenizer() | StopFilter()
        self.assertEqual(sa.__class__.__name__, "CompositeAnalyzer")
    
    def test_composing_functions(self):
        def filter(tokens):
            for t in tokens:
                t.text = t.text.upper()
                yield t
                
        analyzer = RegexTokenizer() | filter
        self.assertEqual([t.text for t in analyzer(u"abc def")], ["ABC", "DEF"])
    
    def test_shared_composition(self):
        shared = RegexTokenizer(r"\S+") | LowercaseFilter()
        
        ana1 = shared | NgramFilter(3)
        ana2 = shared | DoubleMetaphoneFilter()
        
        self.assertEqual([t.text for t in ana1(u"hello")], ["hel", "ell", "llo"])
        self.assertEqual([t.text for t in ana2(u"hello")], ["HL"])
    
    def test_multifilter(self):
        f1 = LowercaseFilter()
        f2 = PassFilter()
        mf = MultiFilter(a=f1, b=f2)
        ana = RegexTokenizer(r"\S+") | mf
        text = u"ALFA BRAVO CHARLIE"
        self.assertEqual([t.text for t in ana(text, mode="a")], ["alfa", "bravo", "charlie"])
        self.assertEqual([t.text for t in ana(text, mode="b")], ["ALFA", "BRAVO", "CHARLIE"])
    
    def test_intraword(self):
        iwf = IntraWordFilter(mergewords=True, mergenums=True)
        ana = RegexTokenizer(r"\S+") | iwf
        
        def check(text, ls):
            self.assertEqual([(t.pos, t.text) for t in ana(text)], ls)
            
        check(u"PowerShot", [(0, "Power"), (1, "Shot"), (1, "PowerShot")])
        check(u"A's+B's&C's", [(0, "A"), (1, "B"), (2, "C"), (2, "ABC")])
        check(u"Super-Duper-XL500-42-AutoCoder!", [(0, "Super"), (1, "Duper"), (2, "XL"),
                                                   (2, "SuperDuperXL"), (3, "500"), (4, "42"),
                                                   (4, "50042"), (5, "Auto"), (6, "Coder"),
                                                   (6, "AutoCoder")])
    
    def test_biword(self):
        ana = RegexTokenizer(r"\w+") | BiWordFilter()
        result = [t.copy() for t in ana(u"the sign of four",
                                        chars=True, positions=True)]
        self.assertEqual(["the-sign", "sign-of", "of-four"],
                         [t.text for t in result])
        self.assertEqual([(0, 8), (4, 11), (9, 16)],
                         [(t.startchar, t.endchar) for t in result])
        self.assertEqual([0, 1, 2], [t.pos for t in result])
        
        result = [t.copy() for t in ana(u"single")]
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].text, "single")
    
    def test_shingles(self):
        ana = RegexTokenizer(r"\w+") | ShingleFilter(3, " ")
        source = u"better a witty fool than a foolish wit"
        results = [t.copy() for t in ana(source, positions=True, chars=True)]
        self.assertEqual([t.text for t in results],
                         [u'better a witty', u'a witty fool',
                          u'witty fool than', u'fool than a', u'than a foolish',
                          u'a foolish wit'])
        self.assertEqual([t.pos for t in results], range(len(results)))
        for t in results:
            self.assertEqual(t.text, source[t.startchar:t.endchar])
        
    def test_unicode_blocks(self):
        from whoosh.support.unicode import blocks, blockname, blocknum
        
        self.assertEqual(blockname(u'a'), 'Basic Latin')
        self.assertEqual(blockname(unichr(0x0b80)), 'Tamil')
        self.assertEqual(blockname(unichr(2048)), None)
        self.assertEqual(blocknum(u'a'), 0)
        self.assertEqual(blocknum(unichr(0x0b80)), 22)
        self.assertEqual(blocknum(unichr(2048)), None)
        self.assertEqual(blocknum(u'a'), blocks.Basic_Latin)
        self.assertEqual(blocknum(unichr(0x0b80)), blocks.Tamil)
        
    def test_double_metaphone(self):
        mf = RegexTokenizer() | DoubleMetaphoneFilter()
        results = [(t.text, t.boost) for t in mf(u"spruce view")]
        self.assertEqual(results, [('SPRS', 1.0), ('F', 1.0), ('FF', 0.5)])
    
    def test_substitution(self):
        mf = RegexTokenizer(r"\S+") | SubstitutionFilter("-", "")
        self.assertEqual([t.text for t in mf(u"one-two th-re-ee four")], ["onetwo", "threee", "four"])
        
        mf = RegexTokenizer(r"\S+") | SubstitutionFilter("([^=]*)=(.*)", r"\2=\1")
        self.assertEqual([t.text for t in mf(u"a=b c=d ef")], ["b=a", "d=c", "ef"])
    
    def test_delimited_attribute(self):
        ana = RegexTokenizer(r"\S+") | DelimitedAttributeFilter()
        results = [(t.text, t.boost) for t in ana(u"image render^2 file^0.5")]
        self.assertEqual(results, [("image", 1.0), ("render", 2.0), ("file", 0.5)])
        
    def test_porter2(self):
        from whoosh.lang.porter2 import stem
        
        plurals = ['caresses', 'flies', 'dies', 'mules', 'denied',
                   'died', 'agreed', 'owned', 'humbled', 'sized',
                   'meeting', 'stating', 'siezing', 'itemization',
                   'sensational', 'traditional', 'reference', 'colonizer',
                   'plotted']
        singles = [stem(w) for w in plurals]
        
        self.assertEqual(singles, ['caress', 'fli', 'die', 'mule', 'deni',
                                   'die', 'agre', 'own', 'humbl', 'size',
                                   'meet', 'state', 'siez', 'item', 'sensat',
                                   'tradit', 'refer', 'colon', 'plot'])
        
        self.assertEqual(stem("bill's"), "bill")
        self.assertEqual(stem("y's"), "y")
        

if __name__ == '__main__':
    unittest.main()
