import unittest

import os, os.path, threading, time
from shutil import rmtree

from whoosh.filedb.filestore import FileStorage
from whoosh.support.filelock import try_for
from whoosh.util import length_to_byte, byte_to_length


class TestMisc(unittest.TestCase):
    def make_storage(self, name):
        if not os.path.exists(name):
            os.mkdir(name)
        st = FileStorage(name)
        return st
    
    def destroy_dir(self, name):
        try:
            rmtree(name)
        except:
            raise
    
    def clean_file(self, path):
        if os.path.exists(path):
            try:
                os.remove(path)
            except:
                raise
    
    def test_filelock_simple(self):
        st = self.make_storage("testindex")
        lock1 = st.lock("testlock")
        lock2 = st.lock("testlock")
        self.assertFalse(lock1 is lock2)
        
        self.assertTrue(lock1.acquire())
        self.assertTrue(os.path.exists("testindex/testlock"))
        self.assertFalse(lock2.acquire())
        lock1.release()
        self.assertTrue(lock2.acquire())
        self.assertFalse(lock1.acquire())
        lock2.release()
        
        self.destroy_dir("testindex")
    
    def test_threaded_filelock(self):
        st = self.make_storage("testindex")
        lock1 = st.lock("testlock")
        result = []
        
        # The thread function tries to acquire the lock and then quits
        def fn():
            lock2 = st.lock("testlock")
            gotit = try_for(lock2.acquire, 1.0, 0.1)
            if gotit:
                result.append(True)
                lock2.release()
        t = threading.Thread(target=fn)
        
        # Acquire the lock in this thread
        lock1.acquire()
        # Start the other thread trying to acquire the lock
        t.start()
        # Wait for a bit
        time.sleep(0.15)
        # Release the lock
        lock1.release()
        # Wait for the other thread to finish
        t.join()
        # If the other thread got the lock, it should have appended True to the
        # "results" list.
        self.assertEqual(result, [True])
        
        self.destroy_dir("testindex")
        
    def test_length_byte(self):
        source = range(11)
        xform = [length_to_byte(n) for n in source]
        result = [byte_to_length(n) for n in xform]
        self.assertEqual(source, result)


if __name__ == '__main__':
    unittest.main()
