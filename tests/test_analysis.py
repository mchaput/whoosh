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
        
        
        

if __name__ == '__main__':
    unittest.main()
