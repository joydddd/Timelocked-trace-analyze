import sys
import struct

if len(sys.argv) != 2:
    print('Invalid argument: please provide a filename')
    exit(0)

with open(sys.argv[1], 'wb') as wf:
    for i in range(1, 22):
        # if i == 21:
        #     continue
        filename = sys.argv[1] + str(i)
        with open(filename + '.bin', "rb") as f:
            wf.write(f.read())
