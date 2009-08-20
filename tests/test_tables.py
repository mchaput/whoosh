import unittest
from os import mkdir
from os.path import exists
from shutil import rmtree

from whoosh import analysis, fields, index, qparser
from whoosh.filedb.filestore import FileStorage
from whoosh.filedb.filetables import FileHashReader, FileHashWriter


class TestTables(unittest.TestCase):
    def make_dir(self, dirname):
        if not exists(dirname):
            mkdir(dirname)
    
    def destroy_dir(self, dirname):
        if exists(dirname):
            rmtree(dirname)
    
    def test_hash(self):
        self.make_dir("testindex")
        st = FileStorage("testindex")
        hwf = st.create_file("test.hsh")
        hw = FileHashWriter(hwf)
        hw.add("foo", "bar")
        hw.add("glonk", "baz")
        hw.close()
        
        hrf = st.open_file("test.hsh")
        hr = FileHashReader(hrf)
        self.assertEqual(hr.get("foo"), "bar")
        self.assertEqual(hr.get("baz"), None)
        hr.close()
        
        #self.destroy_dir("testindex")
    

if __name__ == '__main__':
    unittest.main()
