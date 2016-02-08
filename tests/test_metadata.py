from whoosh import metadata


def test_meta():
    class Zoo(metadata.MetaData):
        magic_bytes = b"Zoo1"
        flags = "has_bear was_little"
        field_order = "foo bar baz"

        has_bear = False
        was_little = False

        foo = "i"  # type: int
        bar = "H"  # type: int
        baz = "f"  # type: float

    bs = Zoo(was_little=True, foo=100, bar=1000, baz=-1.5).encode()
    z = Zoo.decode(bs)

    assert z.was_little
    assert not z.has_bear
    assert z.foo == 100
    assert z.bar == 1000
    assert z.baz == -1.5


def test_chars():
    class Zoo(metadata.MetaData):
        magic_bytes = b"Zoo2"
        field_order = "code1 code2 code3"

        code1 = "c"
        code2 = "2s"
        code3 = "c"

    bs = Zoo(code1="a", code2=b"bb", code3="c").encode()
    z = Zoo.decode(bs)

    assert z.code1 == "a"
    assert z.code2 == b"bb"
    assert z.code3 == "c"


