from binascii import crc32
from zlib import adler32
from hashlib import md5

from whoosh.compat import xrange
from whoosh.util import now


def phash(d, key):
    # return (int(md5(key).hexdigest(), 16) >> d) & 0xffffffff
    if d == 0:
        d = 0x01000193
    for byte in key:
        d = ((d * 0x01000193) ^ byte) & 0xffffffff
    return d


def generate_phash(keylist):
    t = now()
    length = len(keylist)
    key_to_pos = dict((key, i) for i, key in enumerate(keylist))

    # Step 1: Place all of the keys into buckets
    buckets = [[] for _ in xrange(length)]
    g = [0] * length
    index = [None] * length

    for i, key in enumerate(keylist):
        buckets[phash(0, key) % length].append(key)

    # Step 2: Sort the buckets and process the ones with the most items first
    buckets.sort(key=len, reverse=True)
    bn = length
    for bn, bucket in enumerate(buckets):
        if len(bucket) <= 1:
            break
        d = 1
        item = 0
        slots = []
        while item < len(bucket):
            slot = phash(d, bucket[item]) % length
            if index[slot] is not None or slot in slots:
                d += 1
                item = 0
                slots = []
            else:
                slots.append(slot)
                item += 1

        g[phash(0, bucket[0]) % length] = d
        for i in xrange(len(bucket)):
            index[slots[i]] = key_to_pos[bucket[i]]

    # Only buckets with 1 item remain. Process them more quickly by directly
    # placing them into a free slot. Use a negative value of d to indicate
    # this.
    freelist = []
    for i in xrange(length):
        if index[i] is None:
            freelist.append(i)

    for bn in xrange(bn, length):
        bucket = buckets[bn]
        if len(bucket) == 0:
            break
        slot = freelist.pop()

        # We subtract one to ensure it's negative even if the zero-th slot was
        # used.
        g[phash(0, bucket[0]) % length] = -slot - 1
        index[slot] = key_to_pos[bucket[0]]

    return g, index


def lookup_phash(g, index, key):
    d = g[phash(0, key) % len(g)]
    if d < 0:
        return index[-d - 1]
    return index[phash(d, key) % len(index)]


