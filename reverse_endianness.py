import sys
import struct

struct_fmt = '>II1s'
struct_fmt1 = '<II1s'

if len(sys.argv) != 2:
    print('Invalid argument: please provide a filename')
    exit(0)

BLOCK_SIZE = 4096
some_bytes = b'\xAC' * BLOCK_SIZE

struct_len = struct.calcsize(struct_fmt)
struct_unpack = struct.Struct(struct_fmt).unpack_from

filename = sys.argv[1]
with open(filename, "rb") as f:
    with open(filename + '.end', "wb") as wf:
        while True:
            data = f.read(struct_len)
            if not data: break
            s = struct_unpack(data)
            lba, size, command = s
            # print(lba, size, str(command)[2])
            wf.write(struct.pack(struct_fmt1, lba, size, command))
