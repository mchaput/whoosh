import unittest

import whoosh.analysis as analysis
import whoosh.highlight as highlight


class TestHighlighting(unittest.TestCase):
    _doc = u"alfa bravo charlie delta echo foxtrot golf hotel india juliet kilo lima"
    
    def test_null_fragment(self):
        terms = frozenset(("bravo", "india"))
        sa = analysis.StandardAnalyzer()
        nf = highlight.NullFragmenter
        uc = highlight.UppercaseFormatter()
        htext = highlight.highlight(self._doc, terms, sa, nf, uc)
        self.assertEqual(htext, "alfa BRAVO charlie delta echo foxtrot golf hotel INDIA juliet kilo lima")

    def test_simple_fragment(self):
        terms = frozenset(("bravo", "india"))
        sa = analysis.StandardAnalyzer()
        sf = highlight.SimpleFragmenter(size=20)
        uc = highlight.UppercaseFormatter()
        htext = highlight.highlight(self._doc, terms, sa, sf, uc)
        self.assertEqual(htext, "alfa BRAVO charlie...hotel INDIA juliet kilo")
        
    def test_sentence_fragment(self):
        text = u"This is the first sentence. This one doesn't have the word. This sentence is the second. Third sentence here."
        terms = ("sentence", )
        sa = analysis.StandardAnalyzer(stoplist=None)
        sf = highlight.SentenceFragmenter()
        uc = highlight.UppercaseFormatter()
        htext = highlight.highlight(text, terms, sa, sf, uc)
        self.assertEqual(htext, "This is the first SENTENCE...This SENTENCE is the second...Third SENTENCE here")

    def test_context_fragment(self):
        terms = frozenset(("bravo", "india"))
        sa = analysis.StandardAnalyzer()
        cf = highlight.ContextFragmenter(terms, surround=6)
        uc = highlight.UppercaseFormatter()
        htext = highlight.highlight(self._doc, terms, sa, cf, uc)
        self.assertEqual(htext, "alfa BRAVO charlie...hotel INDIA juliet")
    
    def test_html_format(self):
        terms = frozenset(("bravo", "india"))
        sa = analysis.StandardAnalyzer()
        cf = highlight.ContextFragmenter(terms, surround=6)
        hf = highlight.HtmlFormatter()
        htext = highlight.highlight(self._doc, terms, sa, cf, hf)
        self.assertEqual(htext, 'alfa <strong class="match term0">bravo</strong> charlie...hotel <strong class="match term1">india</strong> juliet')





if __name__ == '__main__':
    unittest.main()
