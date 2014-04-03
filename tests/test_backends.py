import random
import tempfile

import pytest

from whoosh.kv import db as kvdb
from whoosh.compat import b, xrange
from whoosh.util import random_bytes, random_name
from whoosh.util.loading import find_object
from whoosh.util.testing import TempDir


tempfile.tempdir = "/Users/matt/dev/temp"


dblist = [
    "whoosh.kv.zodb.Zodb",
    "whoosh.kv.memory.MemoryDB",
    "whoosh.kv.blueline.Blueline",
    # "whoosh.kv.bsd.BSD",
    "whoosh.kv.pylmdb.LMDB",
    "whoosh.kv.plyveldb.Plyvel",
    "whoosh.kv.sqlite.Sqlite",
    # "whoosh.kv.tcdb.TC",
    # "whoosh.kv.dbm.DBM",
]


class DBEnv(object):
    def __init__(self, dbname):
        self.cls = find_object(dbname)
        self.obj = None

    def __enter__(self):
        self.obj = self.cls.temp()
        self.obj.create()
        return self.obj

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.obj.destroy()


@pytest.mark.parametrize("dbname", dblist)
def test_writable_method(dbname):
    with DBEnv(dbname) as db:
        with db.open(write=True) as w:
            assert w.writable()
            w[b"a"] = b"b"

        with db.open() as r:
            assert not r.writable()
            with pytest.raises(kvdb.ReadOnlyError):
                r[b"b"] = b"c"
            with pytest.raises(kvdb.ReadOnlyError):
                del r[b"a"]
            with pytest.raises(kvdb.ReadOnlyError):
                r.update({b"c": b"d"})


@pytest.mark.parametrize("dbname", dblist)
def test_write_read(dbname):
    with DBEnv(dbname) as db:
        items = [
            (b"alfa", b"bravo"),
            (b"charlie", b"delta"),
            (b"echo", b"foxtrot"),
            (b"golf", b"hotel"),
            (b"india", b"juliet"),
            (b"kilo", b"lima"),
            (b"mike", b"november"),
            (b"oskar", b"papa"),
            (b"quebec", b"romeo"),
        ]
        ritems = list(items)
        random.shuffle(ritems)

        with db.open(write=True) as w:
            assert isinstance(w, kvdb.DBWriter)
            for k, v in ritems:
                w[k] = v
            assert list(w.keys()) == [item[0] for item in items]
            assert list(w.items()) == items

        with db.open() as r:
            # assert not isinstance(r, kvdb.DBWriter)
            for k, v in items:
                assert r[k] == v
            assert list(r.keys()) == [item[0] for item in items]
            assert list(r.items()) == items


@pytest.mark.parametrize("dbname", dblist)
def test_len(dbname):
    with DBEnv(dbname) as db:
        with db.open(write=True) as w:
            w[b"alfa"] = b"bravo"
            w[b"charlie"] = b"delta"
            assert len(w) == 2

            w[b"echo"] = b"foxtrot"
            assert len(w) == 3

        with db.open() as r:
            assert len(r) == 3


@pytest.mark.parametrize("dbname", dblist)
def test_rewrite(dbname):
    with DBEnv(dbname) as db:
        with db.open(write=True) as w:
            w[b"alfa"] = b"bravo"
            w[b"charlie"] = b"delta"

        with db.open(write=True) as w:
            w[b"alfa"] = b"echo"
            w[b"foxtrot"] = b"golf"

            assert w[b"alfa"] == b"echo"
            assert w[b"foxtrot"] == b"golf"

        with db.open() as r:
            assert r[b"alfa"] == b"echo"
            assert r[b"charlie"] == b"delta"
            assert r[b"foxtrot"] == b"golf"


@pytest.mark.parametrize("dbname", dblist)
def test_roundtrip(dbname):
    with DBEnv(dbname) as db:
        data = [(random_bytes(10), random_bytes(128)) for _ in xrange(1000)]
        with db.open(write=True) as w:
            for k, v in data:
                w[k] = v

        with db.open() as r:
            for k, v in data:
                assert r[k] == v


@pytest.mark.parametrize("dbname", dblist)
def test_empty_value(dbname):
    with DBEnv(dbname) as db:
        with db.open(write=True) as w:
            w[b"key"] = b""

        with db.open() as r:
            assert r[b"key"] == b""


@pytest.mark.parametrize("dbname", dblist)
def test_db_find(dbname):
    with DBEnv(dbname) as db:
        keys = b"alfa bravo charlie delta echo foxtrot".split()
        with db.open(write=True) as w:
            for key in keys:
                w[key] = b"x"

            assert w.find(b"") == b"alfa"
            assert w.find(b"b") == b"bravo"
            assert w.find(b"delta") == b"delta"
            assert w.find(b"f") == b"foxtrot"
            assert w.find(b"g") is None

        with db.open() as r:
            assert r.find(b"") == b"alfa"
            assert r.find(b"b") == b"bravo"
            assert r.find(b"delta") == b"delta"
            assert r.find(b"g") is None

        with db.open(write=True) as w:
            w[b"foo"] = b"y"

            assert w.find(b"f") == b"foo"


@pytest.mark.parametrize("dbname", dblist)
def test_db_key_range(dbname):
    with DBEnv(dbname) as db:
        keys = b"a c e g".split()
        with db.open(write=True) as w:
            for key in keys:
                w[key] = b"x"

        with db.open() as r:
            assert list(r.key_range(b"a", b"g")) == b"a c e".split()
            assert list(r.key_range(b"b", b"f")) == b"c e".split()
            assert list(r.key_range(b"", b"\xff")) == b"a c e g".split()


@pytest.mark.parametrize("dbname", dblist)
def test_writer_range(dbname):
    with DBEnv(dbname) as db:
        def make_key_range(start, end):
            ks = [b("%04d" % i) for i in xrange(start, end)]
            random.shuffle(ks)
            return ks

        k1 = make_key_range(10, 20)
        k2 = make_key_range(30, 50)
        k3 = make_key_range(40, 60)

        leftkey = b"0015"
        rightkey = b"0045"

        with db.open(write=True) as w:
            for k in k1 + k3:
                w[k] = b"x"

        with db.open(write=True) as w:
            for k in k1 + k3:
                assert k in w

            allkeys = sorted(k1 + k3)
            lefti = allkeys.index(leftkey)
            righti = allkeys.index(rightkey)
            assert list(w.key_range(leftkey, rightkey)) == allkeys[lefti:righti]

            for k in k2:
                w[k] = b"y"

            allkeys = sorted(set(k1 + k2 + k3))
            lefti = allkeys.index(leftkey)
            righti = allkeys.index(rightkey)
            keys = list(w.key_range(leftkey, rightkey))
            print("keys=", keys)
            print("    =", allkeys[lefti:righti])
            assert keys == allkeys[lefti:righti]


@pytest.mark.parametrize("dbname", dblist)
def test_missing(dbname):
    with DBEnv(dbname) as db:
        with db.open(write=True) as w:
            w[b"alfa"] = b"bravo"

        with db.open() as r:
            assert r[b"alfa"] == b"bravo"
            assert r.get(b"foo") is None
            assert r.get(b"foo", b"x") == b"x"
            with pytest.raises(KeyError):
                _ = r[b"foo"]


@pytest.mark.parametrize("dbname", dblist)
def test_db_expand_prefix(dbname):
    with DBEnv(dbname) as db:
        keys = b"good alfa apple able apply ace ape aping are base".split()
        with db.open(write=True) as w:
            for key in keys:
                w[key] = b"x"

        with db.open() as r:
            assert list(r.expand_prefix(b"ap")) == b"ape aping apple apply".split()


@pytest.mark.parametrize("dbname", dblist)
def test_contains(dbname):
    with DBEnv(dbname) as db:
        with db.open(write=True) as w:
            w[b"alfa"] = b"bravo"
            w[b"charlie"] = b"delta"
            w[b"echo"] = b"foxtrot"

        with db.open() as r:
            assert b"alfa" in r
            assert b"charlie" in r
            assert b"echo" in r
            assert b"bravo" not in r
            assert b"foo" not in r


@pytest.mark.parametrize("dbname", dblist)
def test_delete(dbname):
    with DBEnv(dbname) as db:
        with db.open(write=True) as w:
            w[b"alfa"] = b"bravo"
            w[b"charlie"] = b"delta"
            w[b"echo"] = b"foxtrot"

        with db.open(write=True) as w:
            del w[b"alfa"]
            assert len(w) == 2
            del w[b"echo"]
            assert len(w) == 1

            # Implementations need to silently ignore trying to delete a
            # non-existant key (unfortunately)
            del w[b"alfa"]
            del w[b"foo"]

            assert b"alfa" not in w
            assert b"echo" not in w
            assert list(w.keys()) == [b"charlie"]

        with db.open() as r:
            assert b"alfa" not in r
            assert b"echo" not in r
            assert len(r) == 1
            assert list(r.keys()) == [b"charlie"]

        with db.open(write=True) as w:
            del w[b"alfa"]
            del w[b"foo"]

            del w[b"charlie"]
            assert len(w) == 0
            assert not w


@pytest.mark.parametrize("dbname", dblist)
def test_optimize(dbname):
    with DBEnv(dbname) as db:
        with db.open(write=True) as w:
            for i in xrange(100):
                w[b("%08d" % i)] = b(hex(i))
            assert len(w) == 100

        with db.open(write=True) as w:
            for i in xrange(10, 90):
                del w[b("%08d" % i)]
            assert len(w) == 20

            w.optimize()
            assert len(w) == 20


@pytest.mark.parametrize("dbname", dblist)
def test_fuzzing(dbname):
    with DBEnv(dbname) as db:
        data = {}
        commands = []
        with db.open(write=True) as w:
            for _ in xrange(1000):
                op = random.randint(0, 2)
                key = val = None
                if op == 0:  # Set
                    key = random_bytes(32)
                    val = random_bytes(32)
                elif op == 1 and data:  # Compare
                    key = random.choice(list(data))
                elif op == 2 and data:  # Delete
                    key = random.choice(list(data))
                commands.append((op, key, val))

                if op == 0:
                    data[key] = val
                    w[key] = val
                elif op == 1 and data:
                    assert w[key] == data[key]
                elif op == 2 and data:
                    del data[key]
                    del w[key]

        with db.open() as r:
            assert list(r.items()) == sorted(data.items())


@pytest.mark.parametrize("dbname", dblist)
def test_cursor(dbname):
    with DBEnv(dbname) as db:
        vs = []
        with db.open(write=True) as w:
            for i in xrange(0, 1000, 3):
                k = ("%08d" % i).encode("ascii")
                v = random_bytes(64)
                vs.append((k, v))
                w[k] = v
        vs.sort()

        with db.open() as r:
            assert list(r.keys())

            cur = r.cursor()

            assert cur.is_active()
            assert cur.key() == vs[0][0]
            assert cur.value() == vs[0][1]
            cur.next()
            assert cur.key() == vs[1][0]
            assert cur.value() == vs[1][1]

            pos = 54
            cur.find(vs[pos][0])
            assert cur.is_active()
            assert cur.key() == vs[pos][0]
            assert cur.value() == vs[pos][1]

            cur.find(b"00000298")
            assert cur.is_active()
            assert cur.key() == vs[100][0]
            assert cur.value() == vs[100][1]

        with db.open(write=True) as w:
            assert len(vs) == 334
            todel = (
                list(xrange(0, 10))
                + list(xrange(11, 300))
                + list(xrange(301, 334))
            )
            for i in todel:
                del w[vs[i][0]]

            cur = w.cursor()
            assert cur.is_active()
            assert cur.key() == vs[10][0]
            assert cur.value() == vs[10][1]

            cur.next()
            assert cur.is_active()
            assert cur.key() == vs[300][0]
            assert cur.value() == vs[300][1]

            cur.next()
            assert not cur.is_active()

            cur = w.cursor()
            cur.find(vs[200][0])
            assert cur.key() == vs[300][0]

            cur = w.cursor()
            cur.find(vs[310][0])
            assert not cur.is_active()


@pytest.mark.parametrize("dbname", dblist)
def test_backwards_find(dbname):
    with DBEnv(dbname) as db:
        with db.open(write=True) as w:
            w[b"alfa"] = b"bravo"
            w[b"charlie"] = b"delta"
            w[b"echo"] = b"foxtrot"
            w[b"golf"] = b"hotel"

        with db.open() as r:
            cur = r.cursor()
            cur.find(b"d")
            assert cur.key() == b"echo"
            cur.find(b"b")
            assert cur.key() == b"charlie"


@pytest.mark.parametrize("dbname", dblist)
def test_invalid_cursor(dbname):
    with DBEnv(dbname) as db:
        with db.open(write=True) as w:
            w[b"alfa"] = b"bravo"
            w[b"charlie"] = b"delta"
            w[b"echo"] = b"foxtrot"
            w[b"golf"] = b"hotel"

        with db.open() as r:
            cur = r.cursor()
            cur.find(b"india")
            assert not cur.is_active()
            assert cur.key() is None
            assert cur.value() is None

            cur = r.cursor()
            cur.find(b"go")
            cur.next()
            assert not cur.is_active()
            assert cur.key() is None
            assert cur.value() is None
            with pytest.raises(kvdb.OverrunError):
                cur.next()


@pytest.mark.parametrize("dbname", dblist)
def test_idempotent(dbname):
    with DBEnv(dbname) as db:
        db.create()
        db.create()
        db.destroy()
        db.destroy()


@pytest.mark.parametrize("dbname", dblist)
def test_deletion(dbname):
    keys = [random_bytes(16) for _ in xrange(100)]
    with DBEnv(dbname) as db:
        with db.open(write=True) as w:
            for k in keys:
                w[k] = k

        random.shuffle(keys)
        with db.open(write=True) as w:
            for k in keys:
                del w[k]

            # assert len(w) == 0

        with db.open() as r:
            assert len(r) == 0


@pytest.mark.parametrize("dbname", dblist)
def test_empty_value(dbname):
    empty = b""
    with DBEnv(dbname) as db:
        with db.open(write=True) as w:
            for i in xrange(512):
                w[b(str(i))] = empty

        with db.open() as r:
            for i in xrange(512):
                assert r[b(str(i))] == empty


@pytest.mark.parametrize("dbname", dblist)
def test_empty_out(dbname):
    with DBEnv(dbname) as db:
        with db.open(write=True) as w:
            for i in xrange(512):
                w[b(str(i))] = b"x"

        with db.open(write=True) as w:
            for i in xrange(512):
                del w[b(str(i))]

        with db.open() as r:
            assert len(r) == 0
            assert not r
            for i in xrange(512):
                assert b(str(i)) not in r


@pytest.mark.parametrize("dbname", dblist)
def test_delete_by_prefix(dbname):
    keys = b"01 02 03 04 05 11 12 13 14 15 20 21 22 23 24 25".split()

    with DBEnv(dbname) as db:
        with db.open(write=True) as w:
            for key in keys:
                w[key] = b"x"

        with db.open(write=True) as w:
            w.delete_by_prefix(b"1")
            assert b" ".join(w) == b"01 02 03 04 05 20 21 22 23 24 25"

        with db.open() as r:
            assert b" ".join(r) == b"01 02 03 04 05 20 21 22 23 24 25"
            assert b"12" not in r


@pytest.mark.parametrize("dbname", dblist)
def test_closed_attr(dbname):
    with DBEnv(dbname) as db:
        with db.open(write=True) as w:
            assert not w.closed
            w[b"foo"] = b"bar"
        assert w.closed

        with db.open() as r:
            assert r[b"foo"] == b"bar"
            assert not r.closed
        assert r.closed


@pytest.mark.parametrize("dbname", dblist)
def test_rollback(dbname):
    with DBEnv(dbname) as db:
        with db.open(write=True, create=True) as w:
            w[b"alfa"] = b"bravo"
            w[b"charlie"] = b"delta"
            w[b"echo"] = b"foxtrot"

        with db.open(write=True) as w:
            w[b"foo"] = b"bar"
            w[b"echo"] = b"echo"
            del w[b"charlie"]
            w.cancel()

        with db.open() as r:
            assert b"foo" not in r
            assert b"charlie" in r
            assert r[b"echo"] == b"foxtrot"




