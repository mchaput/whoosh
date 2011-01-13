from __future__ import with_statement
import unittest

from whoosh import analysis, highlight, fields, qparser
from whoosh.filedb.filestore import RamStorage


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

    def test_maxclasses(self):
        terms = frozenset(("alfa", "bravo", "charlie", "delta", "echo"))
        sa = analysis.StandardAnalyzer()
        cf = highlight.ContextFragmenter(terms, surround=6)
        hf = highlight.HtmlFormatter(tagname="b", termclass="t", maxclasses=2)
        htext = highlight.highlight(self._doc, terms, sa, cf, hf)
        self.assertEqual(htext, '<b class="match t0">alfa</b> <b class="match t1">bravo</b> <b class="match t0">charlie</b>...<b class="match t1">delta</b> <b class="match t0">echo</b> foxtrot')

    def test_workflow(self):
        st = RamStorage()
        schema = fields.Schema(id=fields.ID(stored=True),
                               title=fields.TEXT(stored=True))
        ix = st.create_index(schema)
        
        w = ix.writer()
        w.add_document(id=u"1", title=u"The man who wasn't there")
        w.add_document(id=u"2", title=u"The dog who barked at midnight")
        w.add_document(id=u"3", title=u"The invisible man")
        w.add_document(id=u"4", title=u"The girl with the dragon tattoo")
        w.add_document(id=u"5", title=u"The woman who disappeared")
        w.commit()
        
        with ix.searcher() as s:
            # Parse the user query
            parser = qparser.QueryParser("title", schema=ix.schema)
            q = parser.parse(u"man")
            
            # Extract the terms the user used in the field we're interested in
            terms = [text for fieldname, text in q.all_terms()
                     if fieldname == "title"]
            
            # Perform the search
            r = s.search(q)
            self.assertEqual(len(r), 2)
            
            # Use the same analyzer as the field uses. To be sure, you can
            # do schema[fieldname].format.analyzer. Be careful not to do this
            # on non-text field types such as DATETIME.
            analyzer = schema["title"].format.analyzer
            
            # Since we want to highlight the full title, not extract fragments,
            # we'll use NullFragmenter.
            nf = highlight.NullFragmenter
            
            # In this example we'll simply uppercase the matched terms
            fmt = highlight.UppercaseFormatter()
            
            outputs = []
            for d in r:
                text = d["title"]
                outputs.append(highlight.highlight(text, terms, analyzer, nf, fmt))
            
            self.assertEqual(outputs, ["The invisible MAN",
                                       "The MAN who wasn't there"])
        



if __name__ == '__main__':
    unittest.main()
