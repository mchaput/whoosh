import unittest

from whoosh import analysis

class TestAnalysis(unittest.TestCase):
    def test_regextokenizer(self):
        value = u"AAAaaaBBBbbbCCCcccDDDddd"
        
        rex = analysis.RegexTokenizer("[A-Z]+")
        self.assertEqual([t.text for t in rex(value)],
                         [u"AAA", u"BBB", u"CCC", u"DDD"])
        
        rex = analysis.RegexTokenizer("[A-Z]+", gaps=True)
        self.assertEqual([t.text for t in rex(value)],
                         [u"aaa", u"bbb", u"ccc", u"ddd"])
    
        

if __name__ == '__main__':
    unittest.main()
