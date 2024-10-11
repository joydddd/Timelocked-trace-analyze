import sys
import struct
import math

struct_fmt = '>II1s'

if len(sys.argv) != 3:
    print('Invalid argument: please provide a filename + versioning factor')
    exit(0)

struct_len = struct.calcsize(struct_fmt)
struct_unpack = struct.Struct(struct_fmt).unpack_from

def block_number_to_GB(block_number):
    return block_number * 4.0 / 1e6 # 4KB per block


trace = list()

filename = sys.argv[1]
with open(filename, "rb") as f:
    while True:
        data = f.read(struct_len)
        if not data: break
        s = struct_unpack(data)
        trace.append(s)

versioning_factor = float(sys.argv[2])
versioning_histroy_len = int(math.floor(len(trace) * versioning_factor))
checkpoint = int(math.floor(len(trace) * 0.02))
    
max_blockid = 0
accessed_blocks = set()
written_blocks = set()
expire_blocks = {}
versioning_block_count = 0


trans_id = 0
disk_usage_trend = list()
for s in trace:
    lba, size, command = trace[trans_id] # size is in blocks. each block is 512 bytes. => 4KB blocks. 
    num_blocks = size // 8
    block_id = lba // 8
    
    if block_id > max_blockid:
        max_blockid = block_id
    
    if command == b'W':
        versioning_block_count += num_blocks
    
    for id in range(block_id, block_id + num_blocks):
        if command == b'W':
            if id in expire_blocks:
                expired_count = sum(1 for expire_id in expire_blocks[id] if expire_id <= trans_id)
                versioning_block_count -= expired_count
                # Remove entries with expire transaction later than current trans_id
                expire_blocks[id] = [expire_id for expire_id in expire_blocks[id] if expire_id > trans_id]
            written_blocks.add(id)
            if id not in expire_blocks:
                expire_blocks[id] = []
            expire_blocks[id].append(trans_id + versioning_histroy_len)  # Add the current transaction ID to the list
        accessed_blocks.add(id)

    if (trans_id % checkpoint) == 0:
        print(f"[ {filename} ] {float(trans_id) / len(trace) * 100.0:.2f} %... ",
            f"Accessed: {block_number_to_GB(len(accessed_blocks)):.2f} GB ",
            f"Written {block_number_to_GB(len(written_blocks)):.2f} GB ",
            f"+ {block_number_to_GB(versioning_block_count):.2f} GB ",
        )
        disk_usage_trend.append((len(accessed_blocks), len(written_blocks), versioning_block_count))
    trans_id += 1

with open(filename+sys.argv[2]+".trend", 'w') as file:
    for entry in disk_usage_trend:
        file.write(str(entry) + '\n')



print("MAX block id:" + str(max_blockid))
print("Estimated Disk Usage: " + f"{block_number_to_GB(len(accessed_blocks)):.2f}" + " GB / " + f"{block_number_to_GB(max_blockid):.2f}" + " GB")
print("Versioning Disk Overhead: " + f"{block_number_to_GB(versioning_block_count) - block_number_to_GB(len(written_blocks)):.2f}" + " GB")