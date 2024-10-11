#include <fcntl.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

struct line {
    unsigned int lba;
    unsigned int size;
    char command;
};

// bdus = int(sys.argv[2])

// BLOCK_SIZE = 4096
// some_bytes = b'\xAC' * BLOCK_SIZE

// struct_len = struct.calcsize(struct_fmt)
// struct_unpack = struct.Struct(struct_fmt).unpack_from

// filename = sys.argv[1]
// with open(filename, "rb") as f:
//     while True:
//         data = f.read(struct_len)
//         if not data: break

// filename = sys.argv[1]
// with open(filename, "rb") as f:
//     with open(f"/dev/bdus-{bdus}", "wb+") as wf:
//         while True:
//             data = f.read(struct_len)
//             if not data: break
//             s = struct_unpack(data)
//             lba, size, command = s

//             wf.seek(lba * 512)
//             if command == b'W':
//                 wf.write(some_bytes)
//             elif command == b'R':
//                 wf.read(BLOCK_SIZE)
//             else:
//                 print('invalid command')
//                 exit(0)

int main(int argc, char* argv[])
{
    if (argc != 3) {
        printf("Invalid argument: please provide a filename and a bdus version\n");
        exit(0);
    }

    FILE *fptr;

    // Open a file in read mode
    fptr = fopen(argv[1], "r");
    int fd = open(argv[1], O_RDONLY);
    struct line l;
    char buf[9];
    int num_reads = 0;
    int num_writes = 0;
    while(read(fd, buf, sizeof(buf)) == 9) {
        memcpy(&l, buf, sizeof(buf));
        // printf("%u %u %c\n", l.lba, l.size, l.command);
        if (l.command == 'W') {
            num_writes += 1;
        }
        else if (l.command == 'R') {
            num_reads += 1;
        }
        else {
            printf("INVALID!!!\n");
        }
    }
    printf("%d %d\n", num_writes, num_reads);   

    fclose(fptr);
}
