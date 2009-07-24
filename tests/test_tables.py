import unittest
from os import mkdir
from os.path import exists
from shutil import rmtree

from whoosh import analysis, fields, index, qparser
from whoosh.filedb.filestore import FileStorage
from whoosh.filedb import filetables


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
        hw = st.create_hash("test.hsh")
        hw.add("foo", "bar")
        hw.add("glonk", "baz")
        hw.close()
        
        hr = st.open_hash("test.hsh")
        self.assertEqual(hr.get("foo"), "bar")
        hr.close()
        
        self.destroy_dir("testindex")
    
    def test_posting_table(self):
        self.make_dir("testindex")
        st = FileStorage("testindex")
        ptw = st.create_posting_table("test.tiz", "test.pst",
                                     keycoder=filetables.encode_key,
                                     valuecoder=filetables.enpickle)
        f = fields.Frequency(None)
        writefn = f.write_postvalue
        readfn = f.read_postvalue
        
        for docnum, freq in [(1,2), (3,4), (5,6), (7,8)]:
            ptw.write_posting(docnum, freq, writefn)
        ptw.add((0, "foo"), "bar")
        
        for docnum, freq in [(10,20), (30,40), (50,60), (70,80)]:
            ptw.write_posting(docnum, freq, writefn)
        ptw.add((0, "glonk"), "baz")
        ptw.close()
        
        ptr = st.open_posting_table("test.tiz", "test.pst",
                                    keycoder=filetables.encode_key,
                                    keydecoder=filetables.decode_key,
                                    valuedecoder=filetables.depickle)
        self.assertEqual(ptr.get((0, "foo")), "bar")
        self.assertEqual(list(ptr.postings((0, "foo"), readfn)),
                         [(1,2), (3,4), (5,6), (7,8)])
        ptr.close()
        
        self.destroy_dir("testindex")


if __name__ == '__main__':
    unittest.main()
