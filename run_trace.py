import sys
import struct

struct_fmt = '>II1s'
struct_fmt1 = '<II1s'

if len(sys.argv) != 3:
    print('Invalid argument: please provide a filename and a bdus version')
    exit(0)

bdus = int(sys.argv[2])

BLOCK_SIZE = 4096
some_bytes = b'\xAC' * BLOCK_SIZE

struct_len = struct.calcsize(struct_fmt)
struct_unpack = struct.Struct(struct_fmt).unpack_from

filename = sys.argv[1]
with open(filename, "rb") as f:
    with open(f"/dev/bdus-{bdus}", "wb+") as wf:
        while True:
            data = f.read(struct_len)
            if not data: break
            s = struct_unpack(data)
            lba, size, command = s

            wf.seek(lba * 512)
            if command == b'W':
                wf.write(some_bytes)
            elif command == b'R':
                wf.read(BLOCK_SIZE)
            else:
                print('invalid command')
                exit(0)
