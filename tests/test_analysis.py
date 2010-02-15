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
    
    def test_filter_composition(self):
        filtersonly = LowercaseFilter() | StopFilter()
        generator = filtersonly(u"Hello there")
        self.assertRaises(AssertionError, list, generator)
        
        analyzer = RegexTokenizer() | filtersonly
        self.assertEqual([t.text for t in analyzer(u"The ABC 123")], ["abc", "123"])
        
    def test_composing_functions(self):
        def filter(tokens):
            for t in tokens:
                t.text = t.text.upper()
                yield t
                
        analyzer = RegexTokenizer() | filter
        self.assertEqual([t.text for t in analyzer(u"abc def")], ["ABC", "DEF"])
    
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
        
        def do(text, ls):
            self.assertEqual([(t.pos, t.text) for t in ana(text)], ls)
            
        do(u"PowerShot", [(0, "Power"), (1, "Shot"), (1, "PowerShot")])
        do(u"A's+B's&C's", [(0, "A"), (1, "B"), (2, "C"), (2, "ABC")])
        do(u"Super-Duper-XL500-42-AutoCoder!", [(0, "Super"), (1, "Duper"), (2, "XL"),
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
        
        

if __name__ == '__main__':
    unittest.main()
