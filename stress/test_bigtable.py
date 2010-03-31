import unittest

import os.path
from random import randint, shuffle
from shutil import rmtree

from whoosh.filedb.filestore import FileStorage
from whoosh.filedb.filetables import HashWriter, HashReader, dump_hash

class Test(unittest.TestCase):
    def make_storage(self, dirname):
        if not os.path.exists(dirname):
            os.mkdir(dirname)
        return FileStorage(dirname)
    
    def destroy_storage(self, dirname):
        if os.path.exists(dirname):
            try:
                rmtree(dirname)
            except OSError:
                raise
    
    def test_bigtable(self):
        st = self.make_storage("testindex")
        
        def randstring(min, max):
            return "".join(chr(randint(1, 255))
                           for _ in xrange(randint(min, max)))
        
        count = 100000
        samp = dict((randstring(1,50), randstring(1,50))
                    for _ in xrange(count))
        
        fhw = HashWriter(st.create_file("big.hsh"))
        fhw.add_all(samp.iteritems())
        fhw.close()
        
        fhr = HashReader(st.open_file("big.hsh"))
        keys = samp.keys()
        shuffle(keys)
        for key in keys:
            self.assertEqual(samp[key], fhr[key])
        
        set1 = set(samp.iteritems())
        set2 = set(fhr.items())
        self.assertEqual(set1, set2)
        
        fhr.close()
        
        self.destroy_storage("testindex")


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
