import sys
import os

if len(sys.argv) != 2:
    print('Invalid argument: please provide a filename')
    exit(0)

import struct

struct_fmt = '>II1s'
filename = sys.argv[1]
with open(filename[:-1] + '.bin', 'wb') as wf:
    for i in range(0, 4):
        with open(filename + str(i) + '.csv', 'r') as f:
                for line in f:
                    split = line.split(',')
                    if len(split) == 1:
                        continue
                    command = split[3].strip()[0].encode()
                    lba = int(int(split[4]) / 512)
                    size = int(int(split[5]) / 512)
                    
                    if lba >= 4294967296:
                        print(lba)
                    wf.write(struct.pack(struct_fmt, lba, size, command))
os.remove(filename + str(i) + '.csv')
