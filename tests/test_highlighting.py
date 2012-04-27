from __future__ import with_statement

from nose.tools import assert_equal, assert_raises  #@UnresolvedImport

from whoosh import analysis, highlight, fields, qparser, query
from whoosh.compat import u
from whoosh.filedb.filestore import RamStorage


_doc = u("alfa bravo charlie delta echo foxtrot golf hotel india juliet " +
         "kilo lima")


def test_null_fragment():
    terms = frozenset(("bravo", "india"))
    sa = analysis.StandardAnalyzer()
    nf = highlight.WholeFragmenter()
    uc = highlight.UppercaseFormatter()
    htext = highlight.highlight(_doc, terms, sa, nf, uc)
    assert_equal(htext,
                 "alfa BRAVO charlie delta echo foxtrot golf hotel " +
                 "INDIA juliet kilo lima")


def test_sentence_fragment():
    text = u("This is the first sentence. This one doesn't have the word. " +
             "This sentence is the second. Third sentence here.")
    terms = ("sentence",)
    sa = analysis.StandardAnalyzer(stoplist=None)
    sf = highlight.SentenceFragmenter()
    uc = highlight.UppercaseFormatter()
    htext = highlight.highlight(text, terms, sa, sf, uc)
    assert_equal(htext,
                 "This is the first SENTENCE...This SENTENCE is the " +
                 "second...Third SENTENCE here")


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
    assert_equal(htext,
                 'alfa <strong class="match term0">bravo</strong> charlie...' +
                 'hotel <strong class="match term1">india</strong> juliet')


def test_html_escape():
    terms = frozenset(["bravo"])
    sa = analysis.StandardAnalyzer()
    wf = highlight.WholeFragmenter()
    hf = highlight.HtmlFormatter()
    htext = highlight.highlight(u('alfa <bravo "charlie"> delta'), terms, sa,
                                wf, hf)
    assert_equal(htext,
                 'alfa &lt;<strong class="match term0">bravo</strong>' +
                 ' "charlie"&gt; delta')


def test_maxclasses():
    terms = frozenset(("alfa", "bravo", "charlie", "delta", "echo"))
    sa = analysis.StandardAnalyzer()
    cf = highlight.ContextFragmenter(surround=6)
    hf = highlight.HtmlFormatter(tagname="b", termclass="t", maxclasses=2)
    htext = highlight.highlight(_doc, terms, sa, cf, hf)
    assert_equal(htext,
                 '<b class="match t0">alfa</b> <b class="match t1">' +
                 'bravo</b> <b class="match t0">charlie</b>...' +
                 '<b class="match t1">delta</b> ' +
                 '<b class="match t0">echo</b> foxtrot')


def test_workflow_easy():
    schema = fields.Schema(id=fields.ID(stored=True),
                           title=fields.TEXT(stored=True))
    ix = RamStorage().create_index(schema)

    w = ix.writer()
    w.add_document(id=u("1"), title=u("The man who wasn't there"))
    w.add_document(id=u("2"), title=u("The dog who barked at midnight"))
    w.add_document(id=u("3"), title=u("The invisible man"))
    w.add_document(id=u("4"), title=u("The girl with the dragon tattoo"))
    w.add_document(id=u("5"), title=u("The woman who disappeared"))
    w.commit()

    with ix.searcher() as s:
        # Parse the user query
        parser = qparser.QueryParser("title", schema=ix.schema)
        q = parser.parse(u("man"))
        r = s.search(q, terms=True)
        assert_equal(len(r), 2)

        r.fragmenter = highlight.WholeFragmenter()
        r.formatter = highlight.UppercaseFormatter()
        outputs = [hit.highlights("title") for hit in r]
        assert_equal(outputs, ["The invisible MAN",
                               "The MAN who wasn't there"])


def test_workflow_manual():
    schema = fields.Schema(id=fields.ID(stored=True),
                           title=fields.TEXT(stored=True))
    ix = RamStorage().create_index(schema)

    w = ix.writer()
    w.add_document(id=u("1"), title=u("The man who wasn't there"))
    w.add_document(id=u("2"), title=u("The dog who barked at midnight"))
    w.add_document(id=u("3"), title=u("The invisible man"))
    w.add_document(id=u("4"), title=u("The girl with the dragon tattoo"))
    w.add_document(id=u("5"), title=u("The woman who disappeared"))
    w.commit()

    with ix.searcher() as s:
        # Parse the user query
        parser = qparser.QueryParser("title", schema=ix.schema)
        q = parser.parse(u("man"))

        # Extract the terms the user used in the field we're interested in
        terms = [text for fieldname, text in q.all_terms()
                 if fieldname == "title"]

        # Perform the search
        r = s.search(q)
        assert_equal(len(r), 2)

        # Use the same analyzer as the field uses. To be sure, you can
        # do schema[fieldname].analyzer. Be careful not to do this
        # on non-text field types such as DATETIME.
        analyzer = schema["title"].analyzer

        # Since we want to highlight the full title, not extract fragments,
        # we'll use WholeFragmenter.
        nf = highlight.WholeFragmenter()

        # In this example we'll simply uppercase the matched terms
        fmt = highlight.UppercaseFormatter()

        outputs = []
        for d in r:
            text = d["title"]
            outputs.append(highlight.highlight(text, terms, analyzer, nf, fmt))

        assert_equal(outputs, ["The invisible MAN",
                               "The MAN who wasn't there"])


def test_unstored():
    schema = fields.Schema(text=fields.TEXT, tags=fields.KEYWORD)
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(text=u("alfa bravo charlie"), tags=u("delta echo"))
    w.commit()

    hit = ix.searcher().search(query.Term("text", "bravo"))[0]
    assert_raises(KeyError, hit.highlights, "tags")


def test_multifilter():
    iwf_for_index = analysis.IntraWordFilter(mergewords=True, mergenums=False)
    iwf_for_query = analysis.IntraWordFilter(mergewords=False, mergenums=False)
    mf = analysis.MultiFilter(index=iwf_for_index, query=iwf_for_query)
    ana = analysis.RegexTokenizer() | mf | analysis.LowercaseFilter()
    schema = fields.Schema(text=fields.TEXT(analyzer=ana, stored=True))
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(text=u("Our BabbleTron5000 is great"))
    w.commit()

    with ix.searcher() as s:
        assert ("text", "5000") in s.reader()
        hit = s.search(query.Term("text", "5000"))[0]
        assert_equal(hit.highlights("text"),
                     'Our BabbleTron<b class="match term0">5000</b> is great')


def test_pinpoint():
    domain = u("alfa bravo charlie delta echo foxtrot golf hotel india juliet "
               "kilo lima mike november oskar papa quebec romeo sierra tango")
    schema = fields.Schema(text=fields.TEXT(stored=True, chars=True))
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(text=domain)
    w.commit()

    assert ix.schema["text"].supports("characters")
    with ix.searcher() as s:
        r = s.search(query.Term("text", "juliet"), terms=True)
        hit = r[0]
        hi = highlight.Highlighter()
        hi.formatter = highlight.UppercaseFormatter()

        assert not hi.can_load_chars(r, "text")
        assert_equal(hi.highlight_hit(hit, "text"),
                     "golf hotel india JULIET kilo lima mike november")

        hi.fragmenter = highlight.PinpointFragmenter()
        assert hi.can_load_chars(r, "text")
        assert_equal(hi.highlight_hit(hit, "text"),
                     "ot golf hotel india JULIET kilo lima mike nove")

        hi.fragmenter.autotrim = True
        assert_equal(hi.highlight_hit(hit, "text"),
                     "golf hotel india JULIET kilo lima mike")


def test_highlight_wildcards():
    schema = fields.Schema(text=fields.TEXT(stored=True))
    ix = RamStorage().create_index(schema)
    with ix.writer() as w:
        w.add_document(text=u("alfa bravo charlie delta cookie echo"))

    with ix.searcher() as s:
        qp = qparser.QueryParser("text", ix.schema)
        q = qp.parse(u("c*"))
        r = s.search(q)
        assert_equal(r.scored_length(), 1)
        r.formatter = highlight.UppercaseFormatter()
        hit = r[0]
        assert_equal(hit.highlights("text"),
                     "alfa bravo CHARLIE delta COOKIE echo")











