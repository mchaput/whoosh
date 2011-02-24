# encoding: utf8

from __future__ import with_statement
import random

from nose.tools import assert_equal

from whoosh.filedb.filestore import RamStorage
from whoosh.filedb.filetables import (HashReader, HashWriter,
                                      OrderedHashWriter, OrderedHashReader,
                                      StoredFieldWriter, StoredFieldReader,
                                      TermIndexWriter, TermIndexReader)
from whoosh.support.testing import TempStorage


def randstring(domain, minlen, maxlen):
    return "".join(random.sample(domain, random.randint(minlen, maxlen)))


def test_termkey():
    with TempStorage("termkey") as st:
        tw = TermIndexWriter(st.create_file("test.trm"))
        tw.add(("alfa", u"bravo"), (1.0, 2, 3))
        tw.add((u"alfa", u"Ã¦Ã¯Å�Ãº"), (4.0, 5, 6))
        tw.add((u"text", u"æ—¥æœ¬èªž"), (7.0, 8, 9))
        tw.close()
        
        tr = TermIndexReader(st.open_file("test.trm"))
        assert ("alfa", u"bravo") in tr
        assert (u"alfa", u"Ã¦Ã¯Å�Ãº") in tr
        assert (u"text", u"æ—¥æœ¬èªž") in tr
        tr.close()
    
def test_random_termkeys():
    def random_fieldname():
        return "".join(chr(random.randint(65, 90)) for _ in xrange(1, 20))
    
    def random_token():
        return "".join(unichr(random.randint(0, 0xd7ff)) for _ in xrange(1, 20))
    
    domain = sorted([(random_fieldname(), random_token()) for _ in xrange(1000)])
    
    st = RamStorage()
    tw = TermIndexWriter(st.create_file("test.trm"))
    for term in domain:
        tw.add(term, (1.0, 0, 1))
    tw.close()
    
    tr = TermIndexReader(st.open_file("test.trm"))
    for term in domain:
        assert term in tr
    
def test_hash():
    with TempStorage("hash") as st:
        hwf = st.create_file("test.hsh")
        hw = HashWriter(hwf)
        hw.add("foo", "bar")
        hw.add("glonk", "baz")
        hw.close()
        
        hrf = st.open_file("test.hsh")
        hr = HashReader(hrf)
        assert_equal(hr.get("foo"), "bar")
        assert_equal(hr.get("baz"), None)
        hr.close()

def test_hash_contents():
    samp = set((('alfa', 'bravo'), ('charlie', 'delta'), ('echo', 'foxtrot'),
               ('golf', 'hotel'), ('india', 'juliet'), ('kilo', 'lima'),
               ('mike', 'november'), ('oskar', 'papa'), ('quebec', 'romeo'),
               ('sierra', 'tango'), ('ultra', 'victor'), ('whiskey', 'xray')))
    
    with TempStorage("hashcontents") as st:
        hwf = st.create_file("test.hsh")
        hw = HashWriter(hwf)
        hw.add_all(samp)
        hw.close()
        
        hrf = st.open_file("test.hsh")
        hr = HashReader(hrf)
        assert_equal(set(hr.items()), samp)
        hr.close()
    
def test_random_hash():
    with TempStorage("randomhash") as st:
        domain = "abcdefghijklmnopqrstuvwxyz"
        domain += domain.upper()
        times = 1000
        minlen = 1
        maxlen = len(domain)
        
        samp = dict((randstring(domain, minlen, maxlen),
                     randstring(domain, minlen, maxlen)) for _ in xrange(times))
        
        hwf = st.create_file("test.hsh")
        hw = HashWriter(hwf)
        for k, v in samp.iteritems():
            hw.add(k, v)
        hw.close()
        
        keys = samp.keys()
        random.shuffle(keys)
        hrf = st.open_file("test.hsh")
        hr = HashReader(hrf)
        for k in keys:
            v = hr[k]
            assert_equal(v, samp[k])
        hr.close()

def test_ordered_hash():
    times = 10000
    with TempStorage("orderedhash") as st:
        hwf = st.create_file("test.hsh")
        hw = HashWriter(hwf)
        hw.add_all(("%08x" % x, str(x)) for x in xrange(times))
        hw.close()
        
        keys = range(times)
        random.shuffle(keys)
        hrf = st.open_file("test.hsh")
        hr = HashReader(hrf)
        for x in keys:
            assert_equal(hr["%08x" % x], str(x))
        hr.close()
    
def test_ordered_closest():
    keys = ['alfa', 'bravo', 'charlie', 'delta', 'echo', 'foxtrot', 'golf',
            'hotel', 'india', 'juliet', 'kilo', 'lima', 'mike', 'november']
    values = [''] * len(keys)
    
    with TempStorage("orderedclosest") as st:
        hwf = st.create_file("test.hsh")
        hw = OrderedHashWriter(hwf)
        hw.add_all(zip(keys, values))
        hw.close()
        
        hrf = st.open_file("test.hsh")
        hr = OrderedHashReader(hrf)
        ck = hr.closest_key
        assert_equal(ck(''), 'alfa')
        assert_equal(ck(' '), 'alfa')
        assert_equal(ck('alfa'), 'alfa')
        assert_equal(ck('bravot'), 'charlie')
        assert_equal(ck('charlie'), 'charlie')
        assert_equal(ck('kiloton'), 'lima')
        assert_equal(ck('oskar'), None)
        assert_equal(list(hr.keys()), keys)
        assert_equal(list(hr.values()), values)
        assert_equal(list(hr.keys_from('f')), keys[5:])
        hr.close()
    
def test_stored_fields():
    with TempStorage("storedfields") as st:
        sf = st.create_file("test.sf")
        sfw = StoredFieldWriter(sf, ["a", "b"])
        sfw.append({"a": "hello", "b": "there"})
        sfw.append({"a": "one", "b": "two"})
        sfw.append({"a": "alfa", "b": "bravo"})
        sfw.close()
        
        sf = st.open_file("test.sf")
        sfr = StoredFieldReader(sf)
        assert_equal(sfr[0], {"a": "hello", "b": "there"})
        assert_equal(sfr[2], {"a": "alfa", "b": "bravo"})
        assert_equal(sfr[1], {"a": "one", "b": "two"})
        sfr.close()
    
def test_termindex():
    terms = [("a", "alfa"), ("a", "bravo"), ("a", "charlie"), ("a", "delta"),
             ("b", "able"), ("b", "baker"), ("b", "dog"), ("b", "easy")]
    st = RamStorage()
    
    tw = TermIndexWriter(st.create_file("test.trm"))
    for i, t in enumerate(terms):
        tw.add(t, (1.0, i * 1000, 1))
    tw.close()
    
    tr = TermIndexReader(st.open_file("test.trm"))
    for i, (t1, t2) in enumerate(zip(tr.keys(), terms)):
        assert_equal(t1, t2)
        assert_equal(tr.get(t1), (1.0, i * 1000, 1))
        
    

