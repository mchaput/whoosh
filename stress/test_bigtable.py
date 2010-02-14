import unittest

import os.path
from random import randint
from shutil import rmtree

from whoosh.filedb.filestore import FileStorage
from whoosh.filedb.filetables import FileHashWriter, FileHashReader

class Test(unittest.TestCase):
    def make_storage(self, dirname):
        if not os.path.exists(dirname):
            os.mkdir(dirname)
        return FileStorage(dirname)
    
    def destroy_storage(self, dirname):
        if os.path.exists(dirname):
            try:
                rmtree(dirname)
            except OSError, e:
                pass
    
    def test_bigtable(self):
        st = self.make_storage("testindex")
        
        def randstring(min, max):
            return "".join(chr(randint(1, 255))
                           for _ in xrange(randint(min, max)))
            
        samp = {}
        count = 100000
        for _ in xrange(count):
            samp[randstring(1,50)] = randstring(1,50)
        
        fhw = FileHashWriter(st.create_file("big.hsh"))
        fhw.add_all(samp.iteritems())
        fhw.close()
        
        fhr = FileHashReader(st.open_file("big.hsh"))
        for key, value in samp.iteritems():
            self.assertEqual(value, fhr[key])
        
        self.destroy_storage("testindex")


if __name__ == "__main__":
    #import sys;sys.argv = ['', 'Test.testName']
    unittest.main()
