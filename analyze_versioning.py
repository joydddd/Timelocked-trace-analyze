import datetime
import pickle
import pandas as pd
import matplotlib.pyplot as plt

def parse_trace(trace_filename, start_time, traces):
    last_checkpoint = start_time
    checkpoint_time = datetime.timedelta(hours=1) 
    with open(trace_filename, 'r') as file:
        for line in file:
            # Split the line into components
            parts = line.strip().split()
            if len(parts) != 9:
                print("Skipped"+line)
                continue  # Skip lines that don't have enough parts

            # Extract the fields
            time = datetime.timedelta(milliseconds=int(parts[0])//1e6) + start_time
            lba = int(parts[3])
            size_blocks = int(parts[4])
            operation = parts[5]
            
            traces.append((time, lba, size_blocks, operation))
            
            # Print Time
            if last_checkpoint + checkpoint_time <= time:
                print(time.strftime("%Y-%m-%d %H:%M:%S"))
                last_checkpoint = time



def parse_traces_to_bin(trace_filenames, bin_filename): 
    start_time = datetime.datetime(year=2000, month=1, day=1)
    traces = []
    for filename in trace_filenames:
        parse_trace(filename, start_time, traces)
        print("Finish filename " + filename)
    
    # Write the traces list to a binary file
    with open(bin_filename, 'wb') as bin_file:
        pickle.dump(traces, bin_file)
    print(f"Traces written to {bin_filename}")



def block_number_to_GB(block_number):
    return block_number * 4.0 / 1e6 # 4KB per block

def epoch(cached_blocks, versioning_blocks, expire_window, current_time):
    cached_count = len(cached_blocks)
    expired_count = 0
    for blockid, time in cached_blocks.items():
        if blockid not in versioning_blocks:
            versioning_blocks[blockid] = []
        versioning_blocks[blockid].append(time)
        expired_count += clear_expired_blocks(versioning_blocks, current_time - expire_window, blockid)
    cached_blocks.clear()
    return cached_count - expired_count


def clear_expired_blocks(versioning_blocks, expired_time, block_id):
    expired_count = 0
    if block_id in versioning_blocks:
        old_copy_count = len(versioning_blocks[block_id])
        versioning_blocks[block_id] = [t for t in versioning_blocks[block_id] if t >= expired_time]
        expired_count = old_copy_count - len(versioning_blocks[block_id])
    return expired_count

def execute_trace(trace, lock_win, expire_win, accessed_blocks, written_blocks, cached_blocks, versioning_blocks, last_epoch_time, versioning_block_count, w_count, f_disk_usage_trend):
    trend_checkpoint_time = datetime.timedelta(minutes=30)
    last_checkpoint = datetime.datetime.min
    
    
    for s in trace: 
        time, lba, size, command = s
        num_blocks = size // 8
        block_id = lba // 8
        for id in range(block_id, block_id + num_blocks):
            if command == "R":
                accessed_blocks.add(id)
            elif command == "W":
                written_blocks.add(id)
                w_count += 1
            else:
                print("Invalid command")
            
            if command == "W":
                cached_blocks[id] = time
        
        if time - last_epoch_time > lock_win:
            new_versioning_count = epoch(cached_blocks, versioning_blocks, expire_win, time)
            versioning_block_count += new_versioning_count
            last_epoch_time = time
            
        if time >= last_checkpoint + trend_checkpoint_time:
            print(time.strftime("%Y-%m-%d %H:%M:%S") + "... ",
                f"Accessed: {block_number_to_GB(len(accessed_blocks)):.2f} GB ",
                f"Written {block_number_to_GB(len(written_blocks)):.2f} GB ",
                f"vs Versioning {block_number_to_GB(versioning_block_count):.2f} GB ",
                f"+ cache {block_number_to_GB(len(cached_blocks)):.2f} GB "
                f"versioning meta {block_number_to_GB(w_count)/512.0:.2f} GB "
            )
            f_disk_usage_trend.write(str((time.strftime("%Y-%m-%d %H:%M:%S"), len(accessed_blocks), len(written_blocks), versioning_block_count+len(cached_blocks), w_count)) + "\n")
            last_checkpoint = time
    return w_count, versioning_block_count



def analyze_disk_usage(traces_filesname, lock_window, expire_window):
    start_time = datetime.datetime(year=2000, month=1, day=1)
    accessed_blocks = set()
    written_blocks = set()
    versioning_blocks = {}
    cached_blocks = {}
    
    
    last_epoch_time = start_time
    versioning_block_count = 0
    w_count = 0
    
    import re
    trend_filename = re.sub('\.time.bin$', '', traces_filesname[0])
    trend_filename = trend_filename + "-" + str(int(lock_window.total_seconds()) // (60 * 60)) + "H-" + str(int(expire_window.total_seconds()) // (60 * 60)) + "H.trend"
    
    with open(trend_filename, 'w') as f_disk_usage_trend:
        for filename in traces_filesname:
            with open(filename, 'rb') as file:
                traces = pickle.load(file)
                w_count, versioning_block_count = execute_trace(traces, lock_window, expire_window, accessed_blocks, written_blocks, cached_blocks, versioning_blocks, last_epoch_time, versioning_block_count, w_count, f_disk_usage_trend)
    print("Written to " + trend_filename)


blue0 = '#045275'
blue1 = '#089099'
green = '#7ccba2'
yellow = '#fcde9c'
orange = '#f0746e'
red = '#dc3977'
purple = '#7c1d6f'
grey = '#e3e3e3'

def parse_line(line):
    start_time = datetime.datetime(year=2000, month=1, day=1)
    # Remove parentheses and split by comma
    line = line.strip().strip('()')
    parts = line.split(', ')
    # Convert parts to appropriate types
    time = datetime.datetime.strptime(parts[0].strip("'"), '%Y-%m-%d %H:%M:%S')
    read_only_blocks = int(parts[1])
    written_blocks = int(parts[2])
    versioning_blocks = int(parts[3])
    w_blocks = int(parts[4])
    
    versioning_overhead = versioning_blocks - written_blocks
    return time, read_only_blocks, written_blocks, versioning_overhead, w_blocks

def plot_blocks_over_time(filename, logic_disk_usage_GB, plot_FS_size=True):
    # Step 1: Read and parse the data from the file
    with open(filename, 'r') as file:
        lines = file.readlines()
    
    data = [parse_line(line) for line in lines]
    
    # Step 2: Convert the data into a pandas DataFrame
    df = pd.DataFrame(data, columns=['time', 'read_only_blocks', 'written_blocks', 'versioning_overhead', 'w_blocks'])
    
    # Step 3: Convert the data to GB if necessary
    # Assuming the data is in bytes, convert to GB
    df['readonly_GB'] = block_number_to_GB(df['read_only_blocks'])
    df['written_GB'] = block_number_to_GB(df['written_blocks'])
    df['not_touched_GB'] = [logic_disk_usage_GB - block_number_to_GB(r + w) for r, w in zip(df['read_only_blocks'], df['written_blocks'])]
    df['versioning_GB'] = block_number_to_GB(df['versioning_overhead'])
    df['tl_metadata_GB'] = [logic_disk_usage_GB / 512.0] * len(df['read_only_blocks'])
    df['versioning_meta_GB'] = block_number_to_GB(df['w_blocks']) / 512.0
    
    
    print("written GB: ", df['written_GB'].iloc[-1])
    print("read GB: ", df['readonly_GB'].iloc[-1])
    print("old versions GB: ", df.max()['versioning_GB'])
    print("TL Metadata GB: ", logic_disk_usage_GB / 512.0)
    print("Versioning Metadata GB: ", df.max()['versioning_meta_GB'])
    print("filesystem size GB: ", logic_disk_usage_GB)
    
    # Step 4: Plot the data using a stacked area plot
    plt.figure(figsize=(10, 6))
    if plot_FS_size:
        plt.stackplot(df['time'], 
                    df['readonly_GB'], 
                    df['written_GB'], 
                    df['not_touched_GB'],
                    df['tl_metadata_GB'],
                    df['versioning_meta_GB'],
                    df['versioning_GB'], 
                    labels=['Read Only', 'Dirty', 'Untouch Filesystem', 'TL MetaData',  'Versioning MetaData', 'Old Versions'],
                    colors=[blue0, green, grey, purple, red, yellow])
    else:
        plt.stackplot(df['time'], 
                    df['readonly_GB'], 
                    df['written_GB'], 
                    df['tl_metadata_GB'],
                    df['versioning_meta_GB'],
                    df['versioning_GB'], 
                    labels=['Read Only', 'Dirty', 'TL MetaData', 'Versioning MetaData', 'Old Versions'],
                    colors=[blue0, green, purple, red, yellow])
    
    # Step 5: Add labels and legend
    plt.xlabel('Time')
    plt.ylabel('Blocks in GB')
    plt.title('Disk Usage Over Time: ' + filename)
    plt.legend(loc='upper left')
    
    # Step 6: Show the plot
    plt.show()

def main():
    import sys
    mail_bins = sys.argv[1:]
    print("Running bins", mail_bins)
    analyze_disk_usage(mail_bins, datetime.timedelta(hours=0), datetime.timedelta(days=2))

if __name__ == "__main__":
    main()
