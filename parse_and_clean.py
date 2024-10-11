import sys
import os

if len(sys.argv) != 2:
    print('Invalid argument: please provide a filename')
    exit(0)

import struct

struct_fmt = '>II1s'

for i in range(1, 21):
    filename = sys.argv[1] + str(i) + '.blkparse'
    with open(filename, 'r') as f:
        if i == 1:
            f.readline()
            f.readline()
            f.readline()
        with open('webmail/webmail-' + str(i) + '.bin', 'wb') as wf:
            for line in f:
                split = line.split(' ')
                if len(split) == 1:
                    continue
                lba = int(split[3])
                size = int(split[4])
                command = split[5].strip().encode()
                if lba >= 4294967296:
                    print(lba)
                wf.write(struct.pack(struct_fmt, lba, size, command))
    os.remove(filename)
