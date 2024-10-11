import sys
import struct

struct_fmt = '>II1s'

if len(sys.argv) != 2:
    print('Invalid argument: please provide a filename')
    exit(0)

struct_len = struct.calcsize(struct_fmt)
struct_unpack = struct.Struct(struct_fmt).unpack_from

max_lba = 0
max_size = 0
min_size = 1000000
num_writes = 0
num_reads = 0
num_blocks = 0

filename = sys.argv[1]
with open(filename, "rb") as f:
    while True:
        data = f.read(struct_len)
        if not data: break
        s = struct_unpack(data)
        lba, size, command = s
        if lba > max_lba:
            max_lba = lba
        if size > max_size:
            max_size = size
        if size < min_size:
            min_size = size
        if command == b'W':
            num_writes = num_writes + 1
            num_blocks = num_blocks + size
        else:
            num_reads = num_reads + 1


print("max_lba, max_size", max_lba, max_size)
print("min_size", min_size)

print(num_writes, num_reads)
print(num_blocks)