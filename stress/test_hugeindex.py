import unittest

import shutil, struct, tempfile

from whoosh import formats
from whoosh.filedb.filestore import FileStorage
from whoosh.filedb.filepostings import FilePostingReader, FilePostingWriter
from whoosh.util import now


class Test(unittest.TestCase):
    def test_huge_postfile(self):
        dir = tempfile.mkdtemp(prefix="hugeindex", suffix=".tmpix")
        st = FileStorage(dir)
        
        pf = st.create_file("test.pst")
        
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
        
        pf = st.open_file("test.pst")
        pfr = FilePostingReader(pf, offset, format)
        i = 0
        while pfr.is_active():
            self.assertEqual(pfr.id(), i)
            self.assertEqual(pfr.weight(), float(i))
            self.assertEqual(pfr.value(), struct.pack("!I", i))
            pfr.next()
            i += 1
        pf.close()
        
        shutil.rmtree()
        
        
    




if __name__ == "__main__":
    unittest.main()
