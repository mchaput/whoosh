import unittest

import os.path, struct

from whoosh import formats
from whoosh.filedb.filestore import FileStorage
from whoosh.filedb.filepostings import FilePostingReader, FilePostingWriter
from whoosh.util import now


class Test(unittest.TestCase):
    def make_file(self, name):
        if not os.path.exists("testindex"):
            os.mkdir("testindex")
        return FileStorage("testindex").create_file(name+"_test.pst")
    
    def open_file(self, name):
        return FileStorage("testindex").open_file(name+"_test.pst")
    
    def delete_file(self, name):
        try:
            FileStorage("testindex").delete_file(name+"_test.pst")
        except OSError:
            raise
    
    def test_huge_postfile(self):
        pf = self.make_file("huge")
        
        gb5 = 5 * 1024 * 1024 * 1024
        pf.seek(gb5)
        pf.write("\x00\x00\x00\x00")
        self.assertEqual(pf.tell(), gb5 + 4)
        
        fpw = FilePostingWriter(pf)
        format = formats.Frequency(None)
        offset = fpw.start(format)
        for i in xrange(10):
            fpw.write(i, float(i), struct.pack("!I", i), 10)
        posttotal = fpw.finish()
        self.assertEqual(posttotal, 10)
        fpw.close()
        
        pf = self.open_file("huge")
        pfr = FilePostingReader(pf, offset, format)
        i = 0
        while pfr.is_active():
            self.assertEqual(pfr.id(), i)
            self.assertEqual(pfr.weight(), float(i))
            self.assertEqual(pfr.value(), struct.pack("!I", i))
            pfr.next()
            i += 1
        pf.close()
        
        #self.delete_file("huge")
        
        
        
        
    




if __name__ == "__main__":
    unittest.main()
