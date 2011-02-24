from __future__ import with_statement

from nose.tools import assert_equal

from whoosh import analysis, highlight, fields, qparser
from whoosh.filedb.filestore import RamStorage


_doc = u"alfa bravo charlie delta echo foxtrot golf hotel india juliet kilo lima"


def test_null_fragment():
    terms = frozenset(("bravo", "india"))
    sa = analysis.StandardAnalyzer()
    nf = highlight.WholeFragmenter()
    uc = highlight.UppercaseFormatter()
    htext = highlight.highlight(_doc, terms, sa, nf, uc)
    assert_equal(htext, "alfa BRAVO charlie delta echo foxtrot golf hotel INDIA juliet kilo lima")

def test_simple_fragment():
    terms = frozenset(("bravo", "india"))
    sa = analysis.StandardAnalyzer()
    sf = highlight.SimpleFragmenter(size=20)
    uc = highlight.UppercaseFormatter()
    htext = highlight.highlight(_doc, terms, sa, sf, uc)
    assert_equal(htext, "alfa BRAVO charlie...hotel INDIA juliet kilo")
    
def test_sentence_fragment():
    text = u"This is the first sentence. This one doesn't have the word. This sentence is the second. Third sentence here."
    terms = ("sentence", )
    sa = analysis.StandardAnalyzer(stoplist=None)
    sf = highlight.SentenceFragmenter()
    uc = highlight.UppercaseFormatter()
    htext = highlight.highlight(text, terms, sa, sf, uc)
    assert_equal(htext, "This is the first SENTENCE...This SENTENCE is the second...Third SENTENCE here")

def test_context_fragment():
    terms = frozenset(("bravo", "india"))
    sa = analysis.StandardAnalyzer()
    cf = highlight.ContextFragmenter(surround=6)
    uc = highlight.UppercaseFormatter()
    htext = highlight.highlight(_doc, terms, sa, cf, uc)
    assert_equal(htext, "alfa BRAVO charlie...hotel INDIA juliet")

def test_context_at_start():
    terms = frozenset(["alfa"])
    sa = analysis.StandardAnalyzer()
    cf = highlight.ContextFragmenter(surround=15)
    uc = highlight.UppercaseFormatter()
    htext = highlight.highlight(_doc, terms, sa, cf, uc)
    assert_equal(htext, "ALFA bravo charlie delta echo foxtrot")

def test_html_format():
    terms = frozenset(("bravo", "india"))
    sa = analysis.StandardAnalyzer()
    cf = highlight.ContextFragmenter(surround=6)
    hf = highlight.HtmlFormatter()
    htext = highlight.highlight(_doc, terms, sa, cf, hf)
    assert_equal(htext, 'alfa <strong class="match term0">bravo</strong> charlie...hotel <strong class="match term1">india</strong> juliet')

def test_maxclasses():
    terms = frozenset(("alfa", "bravo", "charlie", "delta", "echo"))
    sa = analysis.StandardAnalyzer()
    cf = highlight.ContextFragmenter(surround=6)
    hf = highlight.HtmlFormatter(tagname="b", termclass="t", maxclasses=2)
    htext = highlight.highlight(_doc, terms, sa, cf, hf)
    assert_equal(htext, '<b class="match t0">alfa</b> <b class="match t1">bravo</b> <b class="match t0">charlie</b>...<b class="match t1">delta</b> <b class="match t0">echo</b> foxtrot')

def test_workflow_easy():
    schema = fields.Schema(id=fields.ID(stored=True),
                           title=fields.TEXT(stored=True))
    ix = RamStorage().create_index(schema)
    
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
        r = s.search(q)
        assert_equal(len(r), 2)
        
        r.fragmenter = highlight.WholeFragmenter()
        r.formatter = highlight.UppercaseFormatter()
        outputs = [hit.highlights("title") for hit in r]
        assert_equal(outputs, ["The invisible MAN", "The MAN who wasn't there"])

def test_workflow_manual():
    schema = fields.Schema(id=fields.ID(stored=True),
                           title=fields.TEXT(stored=True))
    ix = RamStorage().create_index(schema)
    
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
        assert_equal(len(r), 2)
        
        # Use the same analyzer as the field uses. To be sure, you can
        # do schema[fieldname].format.analyzer. Be careful not to do this
        # on non-text field types such as DATETIME.
        analyzer = schema["title"].format.analyzer
        
        # Since we want to highlight the full title, not extract fragments,
        # we'll use WholeFragmenter.
        nf = highlight.WholeFragmenter()
        
        # In this example we'll simply uppercase the matched terms
        fmt = highlight.UppercaseFormatter()
        
        outputs = []
        for d in r:
            text = d["title"]
            outputs.append(highlight.highlight(text, terms, analyzer, nf, fmt))
        
        assert_equal(outputs, ["The invisible MAN", "The MAN who wasn't there"])
        



