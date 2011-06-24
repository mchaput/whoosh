from __future__ import with_statement
import gzip

from nose.tools import assert_equal, assert_not_equal  #@UnresolvedImport

import whoosh.support.dawg as dawg
from whoosh import fields, query, spelling
from whoosh.compat import u, text_type
from whoosh.filedb.filestore import RamStorage
from whoosh.support.testing import TempStorage


def test_graph_corrector():
    wordlist = sorted(["render", "animation", "animate", "shader",
                       "shading", "zebra", "koala", "lamppost",
                       "ready", "kismet", "reaction", "page",
                       "delete", "quick", "brown", "fox", "jumped",
                       "over", "lazy", "dog", "wicked", "erase",
                       "red", "team", "yellow", "under", "interest",
                       "open", "print", "acrid", "sear", "deaf",
                       "feed", "grow", "heal", "jolly", "kilt",
                       "low", "zone", "xylophone", "crown",
                       "vale", "brown", "neat", "meat", "reduction",
                       "blunder", "preaction"])
    
    sp = spelling.GraphCorrector.from_word_list(wordlist)
    sugs = sp.suggest("reoction", maxdist=2)
    assert_equal(sugs, ["reaction", "preaction", "reduction"])

def test_reader_corrector_nograph():
    schema = fields.Schema(text=fields.TEXT)
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(text=u("render zorro kaori postal"))
    w.add_document(text=u("reader zebra koala pastry"))
    w.add_document(text=u("leader libra ooala paster"))
    w.add_document(text=u("feeder lorry zoala baster"))
    w.commit()
    
    with ix.reader() as r:
        sp = spelling.ReaderCorrector(r, "text")
        assert_equal(sp.suggest(u("kaola"), maxdist=1), [u('koala')])
        assert_equal(sp.suggest(u("kaola"), maxdist=2), [u('koala'), u('kaori'), u('ooala'), u('zoala')])

def test_reader_corrector():
    schema = fields.Schema(text=fields.TEXT(spelling=True))
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(text=u("render zorro kaori postal"))
    w.add_document(text=u("reader zebra koala pastry"))
    w.add_document(text=u("leader libra ooala paster"))
    w.add_document(text=u("feeder lorry zoala baster"))
    w.commit()
    
    with ix.reader() as r:
        assert r.has_word_graph("text")
        sp = spelling.ReaderCorrector(r, "text")
        assert_equal(sp.suggest(u("kaola"), maxdist=1), [u('koala')])
        assert_equal(sp.suggest(u("kaola"), maxdist=2), [u('koala'), u('kaori'), u('ooala'), u('zoala')])

def test_add_spelling():
    schema = fields.Schema(text1=fields.TEXT, text2=fields.TEXT)
    ix = RamStorage().create_index(schema)
    w = ix.writer()
    w.add_document(text1=u("render zorro kaori postal"), text2=u("alfa"))
    w.add_document(text1=u("reader zebra koala pastry"), text2=u("alpa"))
    w.add_document(text1=u("leader libra ooala paster"), text2=u("alpha"))
    w.add_document(text1=u("feeder lorry zoala baster"), text2=u("olfo"))
    w.commit()
    
    with ix.reader() as r:
        assert not r.has_word_graph("text1")
        assert not r.has_word_graph("text2")
    
    from whoosh.filedb.filewriting import add_spelling
    add_spelling(ix, ["text1", "text2"])
    
    with ix.reader() as r:
        assert r.has_word_graph("text1")
        assert r.has_word_graph("text2")
        
        sp = spelling.ReaderCorrector(r, "text1")
        assert_equal(sp.suggest(u("kaola"), maxdist=1), [u('koala')])
        assert_equal(sp.suggest(u("kaola"), maxdist=2), [u('koala'), u('kaori'), u('ooala'), u('zoala')])

        sp = spelling.ReaderCorrector(r, "text2")
        assert_equal(sp.suggest(u("alfo"), maxdist=1), [u("alfa"), u("olfo")])

def test_dawg():
    from whoosh.support.dawg import DawgBuilder
    
    with TempStorage() as st:
        df = st.create_file("test.dawg")
        
        dw = DawgBuilder(reduce_root=False)
        dw.insert(["test"] + list("special"))
        dw.insert(["test"] + list("specials"))
        dw.write(df)
        
        assert_equal(list(dawg.flatten(dw.root.edge("test"))), ["special", "specials"])
    

def test_multisegment():
    schema = fields.Schema(text=fields.TEXT(spelling=True))
    ix = RamStorage().create_index(schema)
    domain = u("special specious spectacular spongy spring specials").split()
    for word in domain:
        w = ix.writer()
        w.add_document(text=word)
        w.commit(merge=False)
    
    with ix.reader() as r:
        assert not r.is_atomic()
        words = list(dawg.flatten(r.word_graph("text")))
        assert_equal(words, sorted(domain))

        corr = r.corrector("text")
        assert_equal(corr.suggest("specail", maxdist=2), ["special", "specials"])

    ix.optimize()
    with ix.reader() as r:
        assert r.is_atomic()
        
        assert_equal(list(r.lexicon("text")), sorted(domain))
        
        words = list(dawg.flatten(r.word_graph("text")))
        assert_equal(words, sorted(domain))

        corr = r.corrector("text")
        assert_equal(corr.suggest("specail", maxdist=2), ["special", "specials"])
        
def test_multicorrector():
    schema = fields.Schema(text=fields.TEXT(spelling=True))
    ix = RamStorage().create_index(schema)
    domain = u("special specious spectacular spongy spring specials").split()
    for word in domain:
        w = ix.writer()
        w.add_document(text=word)
        w.commit(merge=False)

    c1 = ix.reader().corrector("text")
    
    wordlist = sorted(u("bear bare beer sprung").split())
    c2 = spelling.GraphCorrector.from_word_list(wordlist)
    
    mc = spelling.MultiCorrector([c1, c2])
    assert_equal(mc.suggest("specail"), ["special", "specials"])
    assert_equal(mc.suggest("beur"), ["bear", "beer"])
    assert_equal(mc.suggest("sprang"), ["spring", "sprung"])

def test_wordlist():
    domain = sorted("special specious spectacular spongy spring specials".split())
    cor = spelling.GraphCorrector.from_word_list(domain)
    assert_equal(cor.suggest("specail", maxdist=1), ["special"])

def test_wordfile():
    import os.path
    
    files = os.listdir(".")
    testdir = "tests"
    fname = "english-words.10.gz"
    if testdir in files:
        path = os.path.join(testdir, fname)
    elif fname in files:
        path = fname
    else:
        return
    
    wordfile = gzip.open(path, "r")
    cor = spelling.GraphCorrector.from_word_list(word.decode("latin-1")
                                                 for word in wordfile)
    wordfile.close()
    
    #dawg.dump_dawg(cor.word_graph)
    assert_equal(cor.suggest("specail"), ["special"])
    
    st = RamStorage()
    gf = st.create_file("test.dawg")
    cor.to_file(gf)
    
    gf = st.open_file("test.dawg")
    cor = spelling.GraphCorrector.from_graph_file(gf)
    
    assert_equal(cor.suggest("specail", maxdist=1), ["special"])
    gf.close()

def test_query_terms():
    from whoosh.qparser import QueryParser
    
    qp = QueryParser("a", None)
    text = "alfa b:(bravo OR c:charlie) delta"
    q = qp.parse(text)
    assert_equal(sorted(q.iter_all_terms()), [("a", "alfa"), ("a", "delta"),
                                              ("b", "bravo"), ("c", "charlie")])
    assert_equal(query.term_lists(q), [("a", "alfa"),
                                       [("b", "bravo"), ("c", "charlie")],
                                       ("a", "delta")])
    
    text = "alfa brav*"
    q = qp.parse(text)
    assert_equal(sorted(q.iter_all_terms()), [("a", "alfa")])
    assert_equal(query.term_lists(q), [("a", "alfa")])
    
    text = 'alfa "bravo charlie" delta'
    q = qp.parse(text)
    assert_equal(query.term_lists(q), [("a", "alfa"),
                                       [("a", "bravo"), ("a", "charlie")],
                                       ("a", "delta")])
    
    text = 'alfa (b:"bravo charlie" c:del* echo) d:foxtrot'
    q = qp.parse(text)
    print [list(qq.terms()) for qq in q.leaves() if qq.has_terms()]
    assert False









