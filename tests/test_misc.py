from __future__ import with_statement
import os, threading, time

from whoosh.compat import u
from whoosh.util.filelock import try_for
from whoosh.util.numeric import length_to_byte, byte_to_length
from whoosh.util.testing import TempStorage


def test_now():
    from whoosh.util import now

    t1 = now()
    t2 = now()
    assert t1 <= t2


def test_storage_creation():
    import tempfile, uuid
    from whoosh import fields
    from whoosh.filedb.filestore import FileStorage

    schema = fields.Schema(text=fields.TEXT)
    uid = uuid.uuid4()
    dirpath = os.path.join(tempfile.gettempdir(), str(uid))
    assert not os.path.exists(dirpath)

    st = FileStorage(dirpath)
    st.create()
    assert os.path.exists(dirpath)

    ix = st.create_index(schema)
    with ix.writer() as w:
        w.add_document(text=u("alfa bravo"))
        w.add_document(text=u("bracho charlie"))

    st.destroy()
    assert not os.path.exists(dirpath)


def test_ramstorage():
    from whoosh.filedb.filestore import RamStorage

    st = RamStorage()
    lock = st.lock("test")
    lock.acquire()
    lock.release()


def test_filelock_simple():
    with TempStorage("simplefilelock") as st:
        lock1 = st.lock("testlock")
        lock2 = st.lock("testlock")
        assert lock1 is not lock2

        assert lock1.acquire()
        assert st.file_exists("testlock")
        assert not lock2.acquire()
        lock1.release()
        assert lock2.acquire()
        assert not lock1.acquire()
        lock2.release()


def test_threaded_filelock():
    with TempStorage("threadedfilelock") as st:
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
        assert result == [True]


def test_length_byte():
    source = list(range(11))
    xform = [length_to_byte(n) for n in source]
    result = [byte_to_length(n) for n in xform]
    assert source == result


def test_clockface_lru():
    from whoosh.util.cache import clockface_lru_cache

    @clockface_lru_cache(5)
    def test(n):
        return n * 2

    result = [test(n) for n in (1, 2, 3, 4, 5, 4, 3, 2, 10, 1)]
    assert result == [2, 4, 6, 8, 10, 8, 6, 4, 20, 2]
    assert test.cache_info() == (3, 7, 5, 5)
    test.cache_clear()
    assert test.cache_info() == (0, 0, 5, 0)


def test_double_barrel_lru():
    from whoosh.util.cache import lru_cache

    @lru_cache(5)
    def test(n):
        return n * 2

    result = [test(n) for n in (1, 2, 3, 4, 5, 4, 3, 2, 10, 1)]
    assert result == [2, 4, 6, 8, 10, 8, 6, 4, 20, 2]
    # # hits, misses, maxsize and currsize
    # assert test.cache_info() == (4, 6, 5, 5)
    test.cache_clear()
    # assert test.cache_info() == (0, 0, 5, 0)


def test_version_object():
    from whoosh.util.versions import SimpleVersion as sv

    assert sv.parse("1") == sv(1)
    assert sv.parse("1.2") == sv(1, 2)
    assert sv.parse("1.2b") == sv(1, 2, ex="b")
    assert sv.parse("1.2rc") == sv(1, 2, ex="rc")
    assert sv.parse("1.2b3") == sv(1, 2, ex="b", exnum=3)
    assert sv.parse("1.2.3") == sv(1, 2, 3)
    assert sv.parse("1.2.3a") == sv(1, 2, 3, "a")
    assert sv.parse("1.2.3rc") == sv(1, 2, 3, "rc")
    assert sv.parse("1.2.3a4") == sv(1, 2, 3, "a", 4)
    assert sv.parse("1.2.3rc2") == sv(1, 2, 3, "rc", 2)
    assert sv.parse("999.999.999c999") == sv(999, 999, 999, "c", 999)

    assert sv.parse("1.2") == sv.parse("1.2")
    assert sv("1.2") != sv("1.3")
    assert sv.parse("1.0") < sv.parse("1.1")
    assert sv.parse("1.0") < sv.parse("2.0")
    assert sv.parse("1.2.3a4") < sv.parse("1.2.3a5")
    assert sv.parse("1.2.3a5") > sv.parse("1.2.3a4")
    assert sv.parse("1.2.3c99") < sv.parse("1.2.4")
    assert sv.parse("1.2.3a4") != sv.parse("1.2.3a5")
    assert sv.parse("1.2.3a5") != sv.parse("1.2.3a4")
    assert sv.parse("1.2.3c99") != sv.parse("1.2.4")
    assert sv.parse("1.2.3a4") <= sv.parse("1.2.3a5")
    assert sv.parse("1.2.3a5") >= sv.parse("1.2.3a4")
    assert sv.parse("1.2.3c99") <= sv.parse("1.2.4")
    assert sv.parse("1.2") <= sv.parse("1.2")

    assert sv(1, 2, 3).to_int() == 17213488128
    assert sv.from_int(17213488128) == sv(1, 2, 3)
