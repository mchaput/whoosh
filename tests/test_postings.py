import random

import pytest

from whoosh import analysis
from whoosh import postings as p
from whoosh.postings import BasicIO as bio
from whoosh.compat import text_type, xrange
from whoosh.util.testing import TempStorage


def test_encode_docids():
    def _roundtrip(ids):
        bs = bio.encode_docids(ids)
        assert list(bio.decode_docids(bs, 0, len(bs))) == ids

        bs = b'12345678' + bs
        assert list(bio.decode_docids(bs, 8, len(bs))) == ids

    _roundtrip([1, 10, 57, 8402, 90210])
    _roundtrip(list(range(2, 100, 7)))

    nums = []
    base = 0
    for _ in xrange(1000):
        nums.append(base)
        base += random.randint(1, 10)
    _roundtrip(nums)

    with pytest.raises(ValueError):
        _roundtrip([])
    with pytest.raises(ValueError):
        _roundtrip([5, 10, 20, 7])
    with pytest.raises(ValueError):
        _roundtrip([-1, 0, 1, 2])


def test_encode_terms():
    def _roundtrip(terms):
        bs = bio.encode_terms(terms)
        assert list(bio.decode_terms(bs, 0, len(bs))) == terms

        bs = b'12345678' + bs
        assert list(bio.decode_terms(bs, 8, len(bs))) == terms

    _roundtrip(b"alfa bravo charlie delta echo foxtrot golf hotel".split())
    _roundtrip([b"A", b"C", b"D", b"e"])
    _roundtrip([b''])


def test_encode_lengths():
    def _roundtrip(lens):
        bs = bio.encode_lengths(lens)
        assert list(bio.decode_lengths(bs, 0, len(bs))) == lens

        bs = b'12345678' + bs
        assert list(bio.decode_lengths(bs, 8, len(bs))) == lens

    _roundtrip([1, 7, 2, 56, 1, 102, 90, 255])

    with pytest.raises(ValueError):
        _roundtrip([256])
    with pytest.raises(ValueError):
        _roundtrip([-1])


def test_encode_weights():
    def _roundtrip(ws):
        bs = bio.encode_weights(ws)
        assert list(bio.decode_weights(bs, 0, len(bs), len(ws))) == ws

        bs = b'12345678' + bs
        assert list(bio.decode_weights(bs, 8, len(bs), len(ws))) == ws

    _roundtrip([0.5, 1, 0.5, 2.5, 345.5])
    _roundtrip([1, 1, 1, 1, 1, 1, 1, 1])
    _roundtrip([2, 3, 4, 5, 6, 7, 8])

    with pytest.raises(ValueError):
        _roundtrip([])


def test_encode_positions():
    def _roundtrip(ps):
        bs = bio.encode_positions(ps)
        result = list(bio.decode_positions(bs, 0, len(bs)))
        assert result == ps

        bs = b'12345678' + bs
        result = list(bio.decode_positions(bs, 8, len(bs)))
        assert result == ps

    _roundtrip([1, 10, 20, 30])
    _roundtrip([5, 10, 15])
    _roundtrip([7, 14, 21, 2828])
    _roundtrip([100, 1000, 10000, 100000, 1000000])
    _roundtrip([0, 1])
    _roundtrip([65000])

    with pytest.raises(ValueError):
        _roundtrip([])


def test_encode_chars():
    def _roundtrip(cs):
        bs = bio.encode_chars(cs)
        result = list(bio.decode_chars(bs, 0, len(bs)))
        assert result == cs

        bs = b'12345678' + bs
        result = list(bio.decode_chars(bs, 8, len(bs)))
        assert result == cs

    _roundtrip([(0, 5), (7, 10), (12, 73), (75, 100)])
    _roundtrip([(100, 200), (300, 400)])
    _roundtrip([(6, 10)])

    with pytest.raises(ValueError):
        _roundtrip([])
    with pytest.raises(ValueError):
        _roundtrip([(10, 20), (15, 25)])
    with pytest.raises(ValueError):
        _roundtrip([(10, 11), (12, 9)])


def test_encode_payloads():
    def _roundtrip(ps):
        bs = bio.encode_payloads(ps)
        result = list(bio.decode_payloads(bs, 0, len(bs)))
        assert result == ps

        bs = b'12345678' + bs
        result = list(bio.decode_payloads(bs, 8, len(bs)))
        assert result == ps

    _roundtrip([b'alfa', b'bravo', b'charlie'])
    _roundtrip([b'V', b'N', b'Adj', b'Np'])


def test_roundtrip_docs():
    posts = [
        p.posting(1, b'', 5, 2.5, [1, 2, 3], [(5, 10), (15, 20)], [b"a"]),
        p.posting(7, b'', 4, 1.5, [4], [(7, 12), (13, 22)], [b"b", b"c"]),
        p.posting(20, b'', 3, 4.5, [7, 8, 9], [(2, 3), (8, 9)], [b"d", b"e"]),
        p.posting(50, b'', 2, 3.5, [10, 11, 12, 13], [(1, 4)], [b"f"]),
        p.posting(80, b'', 1, 3, [13, 14], [(5, 10)], [b"g", b"h", b"i"]),
    ]

    bf = p.Format(True, True, True, True, True)
    raw_posts = [bf.condition_post(x) for x in posts]

    bs = bf.doclist_to_bytes(raw_posts)

    br = bf.doclist_reader(bs)
    for i in xrange(len(posts)):
        assert br.id(i) == posts[i][p.DOCID]
        assert br.length(i) == posts[i][p.LENGTH]
        assert br.weight(i) == posts[i][p.WEIGHT]
        assert list(br.positions(i)) == posts[i][p.POSITIONS]
        assert list(br.chars(i)) == posts[i][p.CHARS]
        assert list(br.payloads(i)) == posts[i][p.PAYLOADS]


def test_payloads():
    posts = [
        p.posting(docid=1, length=3, payloads=[b'foo']),
        p.posting(docid=2, length=2, payloads=[b'bar', b'baz']),
        p.posting(docid=3, length=1, payloads=[b'a' * 1000, b'b' * 10000]),
    ]

    bf = p.Format(False, False, False, False, True)
    raw_posts = [bf.condition_post(p) for p in posts]
    bs = bf.doclist_to_bytes(raw_posts)

    br = bf.doclist_reader(bs)
    assert list(br.payloads(0)) == [b'foo']
    assert list(br.payloads(1)) == [b'bar', b'baz']
    assert list(br.payloads(2)) == [b'a' * 1000, b'b' * 10000]


def test_combos():
    fmts = []
    for has_lengths in (True, False):
        for has_weights in (True, False):
            for has_positions in (True, False):
                for has_chars in (True, False):
                    for has_payloads in (True, False):
                        fmt = p.Format(has_lengths, has_weights, has_positions,
                                       has_chars, has_payloads)
                        fmts.append(fmt)

    bs = bytearray()
    origs = []
    for fmt in fmts:
        ids = list(range(0, random.randint(10, 1000), random.randint(2, 10)))
        posts = []
        for i in ids:
            ln = random.randint(1, 10)
            w = random.randint(1, 1000) / 2

            ps = []
            base = 0
            for _ in xrange(ln):
                base += random.randint(0, 10)
                ps.append(base)

            cs = []
            base = 0
            for _ in xrange(ln):
                base += random.randint(10, 20)
                cs.append((base, base + 5))

            pys = [random.choice((b'a', b'b', b'c')) for _ in xrange(ln)]

            posts.append(p.posting(docid=i, length=5, weight=w, positions=ps,
                                   chars=cs, payloads=pys))

        origs.append((fmt, len(bs), ids, posts))
        raw_posts = [fmt.condition_post(x) for x in posts]
        bs += fmt.doclist_to_bytes(raw_posts)

    for fmt, offset, ids, posts in origs:
        br = fmt.doclist_reader(bs, offset)
        for n, i in enumerate(ids):
            assert br.id(n) == i
            if fmt.has_lengths:
                assert br.length(n) == posts[n][p.LENGTH]
            if fmt.has_weights:
                assert br.weight(n) == posts[n][p.WEIGHT]
            if fmt.has_positions:
                assert list(br.positions(n)) == posts[n][p.POSITIONS]
            if fmt.has_chars:
                assert list(br.chars(n)) == posts[n][p.CHARS]
            if fmt.has_payloads:
                assert list(br.payloads(n)) == posts[n][p.PAYLOADS]


def test_min_max():
    fmt = p.Format(has_lengths=True, has_weights=True)
    posts = [
        p.posting(docid=1, length=5, weight=6.5),
        p.posting(docid=3, length=2, weight=12.0),
        p.posting(docid=10, length=9, weight=1.5),
        p.posting(docid=13, length=7, weight=2.5),
        p.posting(docid=26, length=6, weight=3.0),
    ]
    bs = fmt.doclist_to_bytes(posts)

    br = fmt.doclist_reader(bs)
    assert br.min_id() == 1
    assert br.max_id() == 26
    assert br.min_length() == 2
    assert br.max_length() == 9
    assert br.max_weight() == 12.0


def test_roundtrip_vector():
    fmt = p.Format(True, True, True, True, True)
    posts = [
        p.posting(termbytes=b'abc', length=2, weight=2.0, positions=[1, 2],
                  chars=[(0, 1), (2, 3)], payloads=[b'N', b'V']),
        p.posting(termbytes=b'd', length=1, weight=2.5, positions=[2],
                  chars=[(7, 9)], payloads=[b'Q']),
        p.posting(termbytes=b'ef', length=2, weight=1.5, positions=[6, 7],
                  chars=[(0, 1), (1, 4)], payloads=[b'X', b'Y']),
        p.posting(termbytes=b'ghi', length=1, weight=1.0, positions=[3],
                  chars=[(4, 6)], payloads=[b'R']),
    ]
    bs = fmt.vector_to_bytes(posts)

    br = fmt.vector_reader(bs)
    for i, post in enumerate(posts):
        assert br.length(i) == post[p.LENGTH]
        assert br.weight(i) == post[p.WEIGHT]
        assert list(br.positions(i)) == post[p.POSITIONS]
        assert list(br.chars(i)) == post[p.CHARS]
        assert list(br.payloads(i)) == post[p.PAYLOADS]


#

def _check_index(content: text_type, fmt: p.Format,
                 ana: analysis.Analyzer=None):
    ana = ana or analysis.StandardAnalyzer()
    length, postiter = fmt.index(ana, lambda x: x.encode("utf8"), content)
    return list(postiter)


def test_existence_postings():
    content = u"alfa bravo charlie"
    form = p.Format()
    target = [
        p.posting(termbytes=b"alfa", length=3),
        p.posting(termbytes=b"bravo", length=3),
        p.posting(termbytes=b"charlie", length=3),
    ]
    assert _check_index(content, form) == target


def test_frequency_postings():
    content = u"alfa bravo charlie bravo alfa alfa"
    form = p.Format(has_lengths=True, has_weights=True)
    target = [
        p.posting(termbytes=b"alfa", weight=3, length=6),
        p.posting(termbytes=b"bravo", weight=2, length=6),
        p.posting(termbytes=b"charlie", weight=1, length=6)
    ]
    assert _check_index(content, form) == target


def test_position_postings():
    content = u"alfa bravo charlie bravo alfa alfa"
    form = p.Format(has_lengths=True, has_weights=True, has_positions=True)
    target = [
        p.posting(termbytes=b"alfa", weight=3, length=6, positions=[0, 4, 5]),
        p.posting(termbytes=b"bravo", weight=2, length=6, positions=[1, 3]),
        p.posting(termbytes=b"charlie", weight=1, length=6, positions=[2])
    ]
    assert _check_index(content, form) == target


def test_character_postings():
    content = u"alfa bravo charlie bravo alfa alfa"
    form = p.Format(has_lengths=True, has_weights=True, has_positions=True,
                    has_chars=True)
    target = [
        p.posting(termbytes=b"alfa", weight=3, length=6, positions=[0, 4, 5],
                  chars=[(0, 4), (25, 29), (30, 34)]),
        p.posting(termbytes=b"bravo", weight=2, length=6, positions=[1, 3],
                  chars=[(5, 10), (19, 24)]),
        p.posting(termbytes=b"charlie", weight=1, length=6, positions=[2],
                  chars=[(11, 18)])
    ]
    assert _check_index(content, form) == target


def test_payload_postings():
    def ana(value, **kwargs):
        for token in analysis.StandardAnalyzer()(value, **kwargs):
            following = value[token.endchar:token.endchar + 2]
            token.payload = following.encode("ascii")
            yield token

    content = u"alfa bravo charlie bravo alfa alfa"
    form = p.Format(has_lengths=True, has_weights=True, has_positions=True,
                    has_chars=True, has_payloads=True)
    target = [
        p.posting(termbytes=b"alfa", weight=3, length=6, positions=[0, 4, 5],
                  chars=[(0, 4), (25, 29), (30, 34)],
                  payloads=[b" b", b" a", b""]),
        p.posting(termbytes=b"bravo", weight=2, length=6, positions=[1, 3],
                  chars=[(5, 10), (19, 24)],
                  payloads=[b" c", b" a"]),
        p.posting(termbytes=b"charlie", weight=1, length=6, positions=[2],
                  chars=[(11, 18)], payloads=[b" b"])
    ]
    assert _check_index(content, form, ana) == target


def test_from_disk():
    fmt = p.Format(has_lengths=True, has_weights=True, has_positions=True,
                   has_chars=True, has_payloads=True)
    target = [
        p.posting(docid=1, weight=3, length=6, positions=[0, 4, 5],
                  chars=[(0, 4), (25, 29), (30, 34)],
                  payloads=[b" b", b" a", b""]),
        p.posting(docid=2, weight=2, length=6, positions=[1, 3],
                  chars=[(5, 10), (19, 24)],
                  payloads=[b" c", b" a"]),
        p.posting(docid=3, weight=1, length=6, positions=[2],
                  chars=[(11, 18)], payloads=[b" b"])
    ]

    with TempStorage() as st:
        raw_posts = [fmt.condition_post(x) for x in target]
        bs = fmt.doclist_to_bytes(raw_posts)
        with st.create_file("test") as f:
            f.write(bs)
            f.write(bs)

        with st.map_file("test") as mm:
            r = fmt.doclist_reader(mm)
            for i, post in enumerate(r.postings()):
                assert post[p.DOCID] == target[i][p.DOCID]
                assert post[p.LENGTH] == target[i][p.LENGTH]
                assert post[p.WEIGHT] == target[i][p.WEIGHT]
                assert list(post[p.POSITIONS]) == target[i][p.POSITIONS]
                assert list(post[p.CHARS]) == target[i][p.CHARS]
                assert list(post[p.PAYLOADS]) == target[i][p.PAYLOADS]


def test_minmax_length():
    # Make a format that DOESN'T store lengths
    fmt = p.Format(has_lengths=False, has_weights=True)

    # Make posts that DO have lengths
    posts = [
        p.posting(docid=1, weight=1.0, length=4),
        p.posting(docid=2, weight=3.0, length=6),
        p.posting(docid=3, weight=1.5, length=2),
    ]

    with TempStorage() as st:
        with st.create_file("test") as f:
            f.write(fmt.doclist_to_bytes(posts))

        with st.map_file("test") as m:
            r = fmt.doclist_reader(m)

            # The lengths were not stored
            assert not r.has_lengths
            with pytest.raises(p.UnsupportedFeature):
                r.length(0)

            # But the min and max were recorded
            assert r.min_length() == 2
            assert r.max_length() == 6


def test_formats_equal():
    empty = p.Format(False, False, False, False, False)

    def do(*flags):
        fmt1 = p.Format(*flags)
        fmt2 = p.Format(*flags)
        assert fmt1 == fmt2
        if flags != (False, False, False, False, False):
            assert fmt1 != empty

    for has_lengths in (0, 1):
        for has_weights in (0, 1):
            for has_poses in (0, 1):
                for has_chars in (0, 1):
                    for has_pays in (0, 1):
                        do(has_lengths, has_weights, has_poses, has_chars,
                           has_pays)

    fmt1 = p.Format(has_weights=True, has_positions=True,
                    io=p.BasicIO())
    fmt2 = p.Format(has_weights=True, has_positions=True,
                    io=p.BasicIO())
    assert fmt1 == fmt2

    class FakePostingsIO(p.PostingsIO):
        def __init__(self, label):
            super(FakePostingsIO, self).__init__()
            self.label = label

    fmt2.io = FakePostingsIO("foo")
    assert fmt1 != fmt2


#
# def test_vector_block():
#     data = [
#         (b"alfa", 1.5, [1, 2], [(1, 2), (3, 4)], [b"a", b"b"]),
#         (b"bravo", 2.5, [3, 4], [(5, 6), (7, 8)], [b"c", b"d"]),
#         (b"charlie", 3.5, [5], [(9, 10)], [b"e"]),
#         (b"delta", 2.0, [6], [(11, 12)], [b"f"]),
#         (b"echo", 1.0, [7, 8], [(13, 14), (15, 16)], [b"g", b"h"]),
#     ]
#     posts = [
#         Posting(id=tb, weight=w, positions=ps, chars=cs, payloads=ys)
#         for tb, w, ps, cs, ys in data
#     ]
#
#     form = postings.BasicFormat(True, True, True, True, True)
#     buff = form.buffer(vector=True).from_list(posts)
#     bs = buff.to_bytes()
#
#     r = form.reader(vector=True).from_bytes(bs)
#     assert b" ".join(r.all_ids()) == b"alfa bravo charlie delta echo"
#     assert r.id(0) == b"alfa"
#     assert r.weight(1) == 2.5
#     assert r.positions(2) == [5]
#     assert r.chars(3) == [(11, 12)]
#     assert r.payloads(4) == [b"g", b"h"]
#     assert list(r.all_values()) == posts
#
