import unittest

from whoosh import store, tables

class TestRecords(unittest.TestCase):
    def test_records(self):
        st = store.RamStorage()
        f = st.create_file("test")
        r = tables.RecordWriter(f, "!iiiii")
        r.add(1,2,-3,4,5)
        r.add(10,20,30,40,50)
        r.add(23,34,45,56,67)
        r.close()
        
        f = st.open_file("test")
        r = tables.RecordReader(f)
        self.assert_(r[0] == (1,2,-3,4,5))
        self.assert_(r[2] == (23,34,45,56,67))
        

if __name__ == '__main__':
    unittest.main()
