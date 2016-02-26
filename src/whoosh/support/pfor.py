from typing import List, Sequence


# Simple16 algorithm for storing arrays of positive integers (usually delta
# encoded lists of sorted integers)
#
# 1. http://www2008.org/papers/pdf/p387-zhangA.pdf
# 2. http://www2009.org/proceedings/pdf/p401.pdf

S16_NUMSIZE = 16
S16_BITSSIZE = 28
S16_NUM = [28, 21, 21, 21, 14, 9, 8, 7, 6, 6, 5, 5, 4, 3, 2, 1]
S16_BITS = [
    (1,) * 28,
    (2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1),
    (1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1),
    (1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2),
    (2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2, 2),
    (4, 3, 3, 3, 3, 3, 3, 3, 3),
    (3, 4, 4, 4, 4, 3, 3, 3),
    (4, 4, 4, 4, 4, 4, 4),
    (5, 5, 5, 5, 4, 4),
    (4, 4, 5, 5, 5, 5),
    (6, 6, 6, 5, 5),
    (5, 5, 6, 6, 6),
    (7, 7, 7, 7),
    (10, 9, 9),
    (14, 14),
    (28,),
]


def read_bits_for_s16(inp: Sequence[int], in_int_offset: int,
                      in_with_int_offset: int, bits: int) -> int:
    val = inp[in_int_offset] >> in_with_int_offset
    return val & (0xffffffff >> (32 - bits))


def s16_compress(out: List[int], out_offset: int, inp: Sequence[int],
                 in_offset: int, n: int, blocksize: int, ori_blocksize: int,
                 ori_input_block: Sequence[int]) -> int:
    # Compress an integer array using Simple16

    for num_idx in range(S16_NUMSIZE):
        out[out_offset] = num_idx << S16_BITSSIZE
        num = S16_NUM[num_idx] if (S16_NUM[num_idx] < n) else n

        j = bits = 0
        while j < num and inp[in_offset + j] < (1 << S16_BITS[num_idx][j]):
            out[out_offset] |= inp[in_offset + j] << bits
            bits += S16_BITS[num_idx][j]
            j += 1

        if j == num:
            return num

    raise Exception


def s16_decompress(out: List[int], out_offset: int, inp: Sequence[int],
                   in_offset: int, n: int) -> int:
    num_idx = inp[in_offset] >> S16_BITSSIZE
    num = S16_NUM[num_idx] if (S16_NUM[num_idx] < n) else n
    j = bits = 0
    while j < num:
        out[out_offset + j] = read_bits_for_s16(inp, in_offset, bits,
                                                S16_BITS[num_idx][j])
        bits += S16_BITS[num_idx][j]
        j += 1
    return num


# PFor2

# All possible values of b
POSSIBLE_B = (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 16, 20, 28)
# Max number of bits to store an uncompressed value
MAX_BITS = 32
# Header records the value of b and the number of exceptions in the block
HEADER_NUM = 1
# Header size in bits
HEADER_SIZE = MAX_BITS * HEADER_NUM

MASK = (
    0x00000000, 0x00000001, 0x00000003, 0x00000007, 0x0000000f, 0x0000001f,
    0x0000003f, 0x0000007f, 0x000000ff, 0x000001ff, 0x000003ff, 0x000007ff,
    0x00000fff, 0x00001fff, 0x00003fff, 0x00007fff, 0x0000ffff, 0x0001ffff,
    0x0003ffff, 0x0007ffff, 0x000fffff, 0x001fffff, 0x003fffff, 0x007fffff,
    0x00ffffff, 0x01ffffff, 0x03ffffff, 0x07ffffff, 0x0fffffff, 0x1fffffff,
    0x3fffffff,  0x7fffffff, 0xffffffff
)


def compress_one_block(block: Sequence[int], blocksize: int
                       ) -> Sequence[int]:
    # Compress one block of blockSize integers using PForDelta with the
    # optimal parameter b

    # find the best b that can lead to the smallest overall compressed size
    current_b = POSSIBLE_B[0]
    tmp_b = current_b
    opt_size = estimate_compressed_size(block, tmp_b, blocksize)
    for i in range(1, len(POSSIBLE_B)):
        tmp_b = POSSIBLE_B[i]
        cur_size = estimate_compressed_size(block, tmp_b, blocksize)
        if cur_size < opt_size:
            current_b = tmp_b
            opt_size = cur_size

    outblock = compress_one_block_core(block, current_b, blocksize)
    return outblock


def decompress_one_block(block: Sequence[int], blocksize: int) -> Sequence[int]:
    # Decompress one block using PForDelta

    exp_pos = [0] * (blocksize + 1)
    exp_high_bits = [0] * (blocksize + 1)
    outblock = [0] * (blocksize + 1)

    exp_num = block[0] & 0x3ff
    bits = (block[0] >> 10) & 0x1f

    # decompress the b-bit slots
    offset = HEADER_SIZE
    compressed_bits = 0
    if bits:
        compressed_bits = decompress_bbit_slots(outblock, block, blocksize,
                                                bits)
    offset += compressed_bits

    # decompress exceptions
    if exp_num:
        compressed_bits = decompress_block_by_s16(exp_pos, block, offset,
                                                  exp_num)
        offset += compressed_bits
        compressed_bits = decompress_block_by_s16(exp_high_bits, block, offset,
                                                  exp_num)
        offset += compressed_bits

        for i in range(exp_num):
            cur_exp_pos = exp_pos[i]
            cur_high_bits = exp_high_bits[i]
            outblock[cur_exp_pos] = (
                (outblock[cur_exp_pos] & MASK[bits]) |
                ((cur_high_bits & MASK[32 - bits]) << bits)
            )

    return outblock[:-1]


def estimate_compressed_size(block: Sequence[int], bits: int,
                             blocksize: int) -> int:
    # Estimate the compressed size in ints of a block

    max_no_exp = (1 << bits) - 1
    # Size of the header and the bits-bit slots
    output_offset = HEADER_SIZE + bits * blocksize
    exp_num = 0

    for i in range(blocksize):
        if block[i] > max_no_exp:
            exp_num += 1

    output_offset += exp_num << 5
    return output_offset


def compress_one_block_core(block: Sequence[int], bits: int,
                            blocksize: int) -> Sequence[int]:
    # The core implementation of compressing a block with blockSize integers
    # using PForDelta with the given parameter b

    exp_pos = [0] * blocksize
    exp_high_bits = [0] * blocksize

    max_comp_bit_size = (HEADER_SIZE +
                         blocksize * (MAX_BITS * 3)
                         + 32)
    tmp_block = [0] * (max_comp_bit_size >> 5)

    output_offset = HEADER_SIZE
    exp_upper_bound = 1 << bits
    exp_num = 0

    # compress the b-bit slots
    for i in range(blocksize):
        assert block[i] >= 0
        if block[i] < exp_upper_bound:
            writebits(tmp_block, block[i], output_offset, bits)
        else:
            # store the lower bits-bits of the exception
            writebits(tmp_block, block[i] & MASK[bits], output_offset, bits)
            # write the position of exception
            exp_pos[exp_num] = i
            # write the higher 32-bits bits of the exception
            exp_high_bits[exp_num] = (block[i] >> bits) & MASK[32 - bits]
            exp_num += 1

        output_offset += bits

    # the first int in the compressed block stores the value of b and the
    # number of exceptions
    tmp_block[0] = ((bits & MASK[10]) << 10) | (exp_num & 0x3ff)

    # compress exceptions
    if exp_num:
        compressed_bit_size = compress_block_by_s16(
            tmp_block, output_offset, exp_pos, exp_num, blocksize, block
        )
        output_offset += compressed_bit_size
        compressed_bit_size = compress_block_by_s16(
            tmp_block, output_offset, exp_high_bits, exp_num, blocksize,
            block
        )
        output_offset += compressed_bit_size

    # discard the redundant parts in the tmpCompressedBlock
    compressed_size_in_ints = (output_offset + 31) >> 5
    return tmp_block[:compressed_size_in_ints]


def decompress_bbit_slots(out_decomp_slots: List[int],
                          in_comp_block: Sequence[int], blocksize: int,
                          bits: int) -> int:
    offset = HEADER_SIZE
    for i in range(blocksize):
        out_decomp_slots[i] = readbits(in_comp_block, offset, bits)
        offset += bits
    compressed_bit_size = bits * blocksize
    return compressed_bit_size


def compress_block_by_s16(out_comp_block: List[int],
                          out_start_offset_in_bits: int,
                          block: Sequence[int], blocksize: int,
                          ori_blocksize: int,
                          ori_input_block: Sequence[int]) -> int:
    # Compress a block of blockSize integers using Simple16 algorithm
    out_offset = (out_start_offset_in_bits + 31) >> 5
    num = 0
    in_offset = 0
    num_left = blocksize

    while num_left:
        num = s16_compress(
            out_comp_block, out_offset, block, in_offset, num_left,
            blocksize, ori_blocksize, ori_input_block
        )
        out_offset += 1
        num_left -= num

    compressed_bit_size = (out_offset << 5) - out_start_offset_in_bits
    return compressed_bit_size


def decompress_block_by_s16(out_decomp_block: List[int],
                            in_comp_block: Sequence[int],
                            in_start_offset_in_bits: int,
                            blocksize: int) -> int:
    in_offset = (in_start_offset_in_bits + 31) >> 5
    num = 0
    out_offset = 0
    num_left = blocksize

    while num_left:
        num = s16_decompress(
            out_decomp_block, out_offset, in_comp_block, in_offset, num_left
        )
        out_offset += num
        in_offset += 1
        num_left -= num

    compressed_bit_size = (in_offset << 5) - in_start_offset_in_bits
    return compressed_bit_size


def writebits(out: List[int], val: int, out_offset: int, bits: int):
    # Write a certain number of bits of an integer into an integer array
    # starting from the given start offset

    if not bits:
        return
    index = out_offset >> 5
    skip = out_offset & 0x1f
    val &= 0xffffffff >> (32 - bits)
    out[index] |= (val << skip)
    if 32 - skip < bits:
        out[index + 1] |= (val >> (32 - skip))


def readbits(inp: Sequence[int], in_offset: int, bits: int) -> int:
    # Read a certain number of bits of an integer into an integer array
    # starting from the given start offset

    index = in_offset >> 5
    skip = in_offset & 0x1f
    val = inp[index] >> skip
    if 32 - skip < bits:
        val |= (inp[index + 1] << (32 - skip))
    return val & (0xffffffff >> (32 - bits))

