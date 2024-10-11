// #ifdef _MSC_VER
// #include <intrin.h>
// #else
// #include <x86intrin.h>
// #endif
// #include <stdio.h>

// #include <stdio.h>
// #include <string.h>
// #include <sys/ioctl.h>

// int main() {

//     FILE *f = fopen("/dev/bdus-64", "w");
//     ioctl(int fd, unsigned long op, ...)
//     // //fseek(f, idx * sizeof(float), SEEK_SET);
//     // //float result;
//     // char buffer[512];
//     // memset(buffer, 5, sizeof(buffer));
//     // fwrite(f, &buffer, sizeof(buffer));
//     //fwrite(buffer,sizeof(buffer),1,f);
//     //fread(&result, sizeof(float), 1, f);
//     // long long time = __rdtsc() >> 32;
//     // unsigned int upper_32 = (unsigned int) (__rdtsc() >> 32);
//     // // int upper_32 = (int) time >> 1;
//     // printf("%d\n", upper_32);
//     // return 0;

//     // FILE* pFile;
//     // //std::string file_name = "/home/jonaher/fs-disk/" + std::to_string(TOTAL_NUM_BLOCKS + metadata_block_id);
//     // char a[10];
//     // pFile = fopen("/home/jonaher/abc_test.txt", "r+");
//     // if (pFile) {
//     //     int vals_read = fread(&a, sizeof(char), 10, pFile);
//     //     // fclose(pFile);
//     // } 
//     // printf("%s\n", a);

//     // // MetadataEntry &update_entry = update_block.arr[metadata_entry_id];
//     // // update_entry.logical_block_id = logical_block_id;
//     // // update_entry.free_bit = 0;
//     // // update_entry.time_written = (unsigned int) (__rdtsc() >> 32); // timestamp - maybe should come from gateway?
//     // for (int i = 0; i < 10; i++) {
//     //     a[i] = 'c';
//     // }

//     // rewind(pFile);

//     // int vals_written = fwrite(&a, sizeof(char), 10, pFile);
//     // fclose(pFile);
//     // if (vals_written != 1) {
//     //     throw std::runtime_error("2) Read failed, vals read doesn't match\n");
//     // }
// }

/***************************************************************************//**
*  \file       test_app.c
*
*  \details    Userspace application to test the Device driver
*
*  \author     EmbeTronicX
*
*  \Tested with Linux raspberrypi 5.10.27-v7l-embetronicx-custom+
*
*******************************************************************************/
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/ioctl.h>
 
#define WR_VALUE _IOW('a','a',int32_t*)
#define RD_VALUE _IOR('a','b',int32_t*)
 
int main()
{
        int fd;
        int32_t value, number;
        printf("*********************************\n");
        printf("*******WWW.EmbeTronicX.com*******\n");
 
        printf("\nOpening Driver\n");
        fd = open("/dev/bdus-66", O_RDWR);
        if(fd < 0) {
                printf("Cannot open device file...\n");
                return 0;
        }
 
        printf("Enter the Value to send\n");
        scanf("%d",&number);
        printf("Writing Value to Driver\n");
        ioctl(fd, WR_VALUE, (int32_t*) &number); 
 
        printf("Reading Value from Driver\n");
        ioctl(fd, RD_VALUE, (int32_t*) &value);
        printf("Value is %d\n", value);
 
        printf("Closing Driver\n");
        close(fd);
}