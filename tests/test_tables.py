import unittest
import random
from os import mkdir
from os.path import exists
from shutil import rmtree

from whoosh.filedb.filestore import FileStorage
from whoosh.filedb.filetables import (HashReader, HashWriter,
                                      OrderedHashWriter, OrderedHashReader,
                                      encode_termkey, decode_termkey)


class TestTables(unittest.TestCase):
    def make_storage(self, dirname):
        if not exists(dirname):
            mkdir(dirname)
        st = FileStorage(dirname)
        return st
    
    def destroy_dir(self, dirname):
        if exists(dirname):
            try:
                rmtree(dirname)
            except:
                pass
    
    def test_termkey(self):
        term = (2, "bravo")
        self.assertEqual(term, decode_termkey(encode_termkey(term)))
        
    def test_random_termkeys(self):
        for _ in xrange(100):
            term = (random.randint(0, 15000),
                    "".join(unichr(random.randint(0, 62000))
                            for _ in xrange(1, 20)))
            self.assertEqual(term, decode_termkey(encode_termkey(term)))
    
    def test_hash(self):
        st = self.make_storage("testindex")
        hwf = st.create_file("test.hsh")
        hw = HashWriter(hwf)
        hw.add("foo", "bar")
        hw.add("glonk", "baz")
        hw.close()
        
        hrf = st.open_file("test.hsh")
        hr = HashReader(hrf)
        self.assertEqual(hr.get("foo"), "bar")
        self.assertEqual(hr.get("baz"), None)
        hr.close()
        
        self.destroy_dir("testindex")
    
    def test_hash_contents(self):
        samp = set((('alfa', 'bravo'), ('charlie', 'delta'), ('echo', 'foxtrot'),
                   ('golf', 'hotel'), ('india', 'juliet'), ('kilo', 'lima'),
                   ('mike', 'november'), ('oskar', 'papa'), ('quebec', 'romeo'),
                   ('sierra', 'tango'), ('ultra', 'victor'), ('whiskey', 'xray')))
        
        st = self.make_storage("testindex")
        hwf = st.create_file("test.hsh")
        hw = HashWriter(hwf)
        hw.add_all(samp)
        hw.close()
        
        hrf = st.open_file("test.hsh")
        hr = HashReader(hrf)
        self.assertEqual(samp, set(hr.items()))
        hr.close()
        
        self.destroy_dir("testindex")
    
    def randstring(self, domain, minlen, maxlen):
        return "".join(random.sample(domain, random.randint(minlen, maxlen)))
    
    def test_random_hash(self):
        domain = "abcdefghijklmnopqrstuvwxyz"
        domain += domain.upper()
        times = 1000
        minlen = 1
        maxlen = len(domain)
        
        samp = dict((self.randstring(domain, minlen, maxlen),
                     self.randstring(domain, minlen, maxlen)) for _ in xrange(times))
        
        st = self.make_storage("testindex")
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
            self.assertEqual(v, samp[k])
        hr.close()
        
        self.destroy_dir("testindex")
    
    def test_ordered_hash(self):
        times = 10000
        st = self.make_storage("testindex")
        hwf = st.create_file("test.hsh")
        hw = HashWriter(hwf)
        hw.add_all(("%08x" % x, str(x)) for x in xrange(times))
        hw.close()
        
        keys = range(times)
        random.shuffle(keys)
        hrf = st.open_file("test.hsh")
        hr = HashReader(hrf)
        for x in keys:
            self.assertEqual(hr["%08x" % x], str(x))
        hr.close()
        
        self.destroy_dir("testindex")
        
    def test_ordered_closest(self):
        keys = ['alfa', 'bravo', 'charlie', 'delta', 'echo', 'foxtrot', 'golf',
                'hotel', 'india', 'juliet', 'kilo', 'lima', 'mike', 'november']
        values = [''] * len(keys)
        
        st = self.make_storage("testindex")
        hwf = st.create_file("test.hsh")
        hw = OrderedHashWriter(hwf)
        hw.add_all(zip(keys, values))
        hw.close()
        
        hrf = st.open_file("test.hsh")
        hr = OrderedHashReader(hrf)
        ck = hr.closest_key
        self.assertEqual(ck(''), 'alfa')
        self.assertEqual(ck(' '), 'alfa')
        self.assertEqual(ck('alfa'), 'alfa')
        self.assertEqual(ck('bravot'), 'charlie')
        self.assertEqual(ck('charlie'), 'charlie')
        self.assertEqual(ck('kiloton'), 'lima')
        self.assertEqual(ck('oskar'), None)
        self.assertEqual(list(hr.keys()), keys)
        self.assertEqual(list(hr.values()), values)
        self.assertEqual(list(hr.keys_from('f')), keys[5:])
        hr.close()
        
        self.destroy_dir("testindex")
        
    

if __name__ == '__main__':
    unittest.main()
