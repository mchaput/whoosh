import unittest

import os, os.path, threading, time

from whoosh.filedb.filestore import FileStorage


class TestMisc(unittest.TestCase):
    def make_dir(self, name):
        if not os.path.exists(name):
            os.mkdir(name)
    
    def destroy_dir(self, name):
        try:
            os.rmdir("test_index")
        except:
            pass
    
    def clean_file(self, path):
        if os.path.exists(path):
            os.remove(path)
    
    def test_filelock_simple(self):
        self.make_dir("test_index")
        st = FileStorage("test_index")
        lock1 = st.lock("testlock")
        lock2 = st.lock("testlock")
        
        self.assertTrue(lock1.acquire())
        self.assertFalse(lock2.acquire())
        lock1.release()
        self.assertTrue(lock2.acquire())
        self.assertFalse(lock1.acquire())
        lock2.release()
        
        self.clean_file("test_index/testlock")
        self.destroy_dir("test_index")
    
    def test_threaded_filelock(self):
        self.make_dir("test_index")
        st = FileStorage("test_index")
        lock1 = st.lock("testlock")
        result = []
        
        def fn():
            lock2 = st.lock("testlock")
            lock2.acquire(blocking=True)
            result.append(True)
            lock2.release()
            
        t = threading.Thread(target=fn)
        lock1.acquire()
        t.start()
        time.sleep(0.1)
        lock1.release()
        del lock1
        time.sleep(0.1)
        self.assertEqual(len(result), 1)
        
        self.clean_file("test_index/testlock")
        self.destroy_dir("test_index")


if __name__ == '__main__':
    unittest.main()
