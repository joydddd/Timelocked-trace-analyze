import datetime
import pickle
import pandas as pd
import matplotlib.pyplot as plt
import os

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
    traces = []
    is_csv = trace_filenames[0].endswith('.csv')
    
    if is_csv:
        start_date = datetime.datetime(1601,1,1)
    else:
        start_time = datetime.datetime(year=2000, month=1, day=1)
    
    for filename in trace_filenames:
        if is_csv:
            parse_csv_trace(filename, start_date, traces)
        else:
            parse_trace(filename, start_time, traces)
        print("Finish filename " + filename)
    
    # Write the traces list to a binary file
    with open(bin_filename, 'wb') as bin_file:
        pickle.dump(traces, bin_file)
    print(f"Traces written to {bin_filename}")


def parse_csv_trace(trace_filename, start_date, traces):
    last_checkpoint = start_date
    checkpoint_time = datetime.timedelta(hours=1) 
    with open(trace_filename, 'r') as file:
        for line in file:
            # Split the line into components
            parts = line.strip().split(',')
            if len(parts) != 7:
                print("Skipped"+line)
                continue  # Skip lines that don't have enough parts
            
            windows_filetime_us = int(parts[0]) / 10
            time = start_date + datetime.timedelta(microseconds=windows_filetime_us)
            operation = "W" if parts[3] == "Write" else "R"
            lba = int(parts[4]) // 512
            size_blocks = int(parts[5]) // 512
            
            assert(int(parts[5])  % 512 == 0)

            traces.append((time, lba, size_blocks, operation))
            
            # Print Time
            if last_checkpoint + checkpoint_time <= time:
                print(time.strftime("%Y-%m-%d %H:%M:%S"))
                last_checkpoint = time

def block_number_to_GB(block_number, block_size=4096): # default block size 4KB
    return block_number * block_size / 1e9 

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

def execute_trace(trace, lock_win, expire_win, accessed_blocks, written_blocks, cached_blocks, versioning_blocks, last_epoch_time, versioning_block_count, w_count, f_disk_usage_trend, block_size):
    trend_checkpoint_time = datetime.timedelta(minutes=30)
    last_checkpoint = datetime.datetime.min
    max_lba = 0
    
    for s in trace: 
        time, lba, size, command = s
        num_blocks = size // (block_size // 512)
        block_id = lba // (block_size // 512)
        for id in range(block_id, block_id + num_blocks):
            if id > max_lba:
                max_lba = id
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
                f"Max {block_number_to_GB(max_lba):.2f} GB"
            )
            f_disk_usage_trend.write(str((time.strftime("%Y-%m-%d %H:%M:%S"), len(accessed_blocks), len(written_blocks), versioning_block_count+len(cached_blocks), w_count)) + "\n")
            last_checkpoint = time
    return w_count, versioning_block_count



def analyze_disk_usage(traces_filesname, lock_window, expire_window, block_size=4096):
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
                w_count, versioning_block_count = execute_trace(traces, lock_window, expire_window, accessed_blocks, written_blocks, cached_blocks, versioning_blocks, last_epoch_time, versioning_block_count, w_count, f_disk_usage_trend, block_size)
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

def plot_blocks_over_time(filename, logic_disk_usage_GB, plot_FS_size="True", inf=False):
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
    df['versioning_meta_GB'] = block_number_to_GB(df['w_blocks']) / 512.0
    df['version_and_FS_GB'] = [x + y + logic_disk_usage_GB for x, y in zip(df['versioning_GB'], df['versioning_meta_GB'])]
    
    max_version_FS = df.max() ['version_and_FS_GB']
    max_y = max_version_FS * (1 + 1/512.0)
    df['tl_metadata_GB'] = ([0] if inf else [max_version_FS / 512.0]) * len(df['read_only_blocks'])
    df['tl_metadata_padding'] = [max_y - x for x in df['version_and_FS_GB']]
    df['disk_usage_GB'] = [max_y] * len(df['read_only_blocks'])

    
    # Step 4: Plot the data using a stacked area plot
    x_min = pd.DatetimeIndex(df['time']).min()
    x_max = pd.DatetimeIndex(df['time']).max()
    base_date = x_min
    df['time_since_base'] = (df['time'] - base_date).dt.days
    
    parts = os.path.splitext(os.path.basename(filename))[0].split('-')
    title = parts[0] + " epoch=" + parts[1] + " expire_win=" + parts[2] 
    
    plt.figure(figsize=(6, 4), dpi=300)
    if plot_FS_size == "True":
        plt.stackplot(df['time'], 
                    df['readonly_GB'], 
                    df['written_GB'], 
                    df['not_touched_GB'],
                    df['versioning_meta_GB'],
                    df['versioning_GB'], 
                    df['tl_metadata_padding'],
                    df['tl_metadata_GB'],
                    labels=['Read Only', 'Dirty', 'Untouch Data', 'Versioning MetaData', 'Old Versions', '', 'TL MetaData'],
                    colors=[blue0, green, grey, orange, yellow, 'white', purple])
        plt.ylabel('Disk Usage in GB', fontsize=14)
        plt.title(title, fontsize=14)
    elif plot_FS_size == "False":
        plt.stackplot(df['time'],  
                    df['tl_metadata_GB'],
                    df['versioning_meta_GB'],
                    df['versioning_GB'], 
                    labels=['TL MetaData', 'Versioning MetaData', 'Old Versions'],
                    colors=[purple, orange, yellow])
        plt.ylabel('Overhead in GB', fontsize=14)
        plt.title(title, fontsize=14)
    elif plot_FS_size == "Log":
        plt.stackplot(df['time'], 
                    df['tl_metadata_GB'],
                    df['readonly_GB'], 
                    df['written_GB'], 
                    df['versioning_meta_GB'],
                    df['versioning_GB'], 
                    labels=['TL MetaData', 'Read Only', 'Dirty', 'Versioning MetaData', 'Old Versions'],
                    colors=[purple, blue0, green, orange, yellow])
        plt.ylabel('Disk Usage in GB', fontsize=14)
        
       
        plt.title(title, fontsize=14)
        ax = plt.gca()
        ax.set_yscale('log')
    else:
        print("Invalid plot_FS_size")
        return
    
    # Step 5: Add labels and legend
    plt.xlabel('Days', fontsize=14)
    plt.yticks(fontsize=12)
    # Set x-ticks to one per 5 days and label them with time_since_base
    x_ticks = pd.date_range(start=x_min, end=x_max, freq='3D')
    x_labels = [(tick - base_date).days for tick in x_ticks]
    plt.xticks(x_ticks, x_labels, fontsize=12)
    
    ax = plt.gca()
    ax.spines['top'].set_visible(False)

    

    
    print("logic_disk_usage_GB: ", logic_disk_usage_GB)
    print("max_version_FS: ", max_version_FS)
    
    
    # Step 7: Add horizontal lines and text annotations for sections
    max_y = max_version_FS * (1 + 1/512.0)
    
    if plot_FS_size == "Log":
        plt.hlines(max_y, x_min, x_max, color='b', linestyles='dashed', label='Disk Size')
        plt.ylim(logic_disk_usage_GB / 512.0 / 2, max_y*1.001)
    
    if plot_FS_size == "True":
        # Step 7: Set y-axis limit
        plt.ylim(0, max_y*1.001)
        x_margin = 0.05 * (x_max - x_min)
        plt.xlim(x_min-x_margin, x_max)
        plt.plot([x_min-x_margin, x_min], [logic_disk_usage_GB, logic_disk_usage_GB], color='b')
        plt.plot([x_min-x_margin, x_min], [max_version_FS, max_version_FS], color='b')
        plt.plot([x_min-x_margin, x_min], [max_version_FS*(1 + 1/512.0), max_version_FS*(1 + 1/512.0)], color='b')
        
        # Add annotation with arrow
        plt.annotate('Filesystem', xy=(x_min-x_margin/2, logic_disk_usage_GB), xytext=(x_min-x_margin/2, logic_disk_usage_GB / 2),
                    arrowprops=dict(arrowstyle='->', edgecolor='blue', lw=1.5),
                    fontsize=12, ha='center', va='center', rotation=90, color='b')
        plt.annotate('', xy=(x_min-x_margin/2, 0), xytext=(x_min-x_margin/2, logic_disk_usage_GB / 2 - 0.15*max_version_FS),
                    arrowprops=dict(arrowstyle='->', edgecolor='blue', lw=1.5), color='b')
        if not inf:
            plt.annotate('Timelocked', xy=(x_min-x_margin/2, max_y), xytext=(x_min-x_margin, max_y+ 0.1*max_version_FS),
                        arrowprops=dict(edgecolor='blue', width=0.5, headwidth=8),
                        fontsize=12, ha='right', va='center', color='b')
        if max_version_FS - logic_disk_usage_GB > 0.35 * max_version_FS:
            plt.annotate('', xy=(x_min-x_margin/2, logic_disk_usage_GB), xytext=(x_min-x_margin/2, logic_disk_usage_GB + (max_version_FS - logic_disk_usage_GB) / 2 - 0.12*max_version_FS),
                    arrowprops=dict(arrowstyle='->', edgecolor='blue', lw=1.5), color='b')
            plt.annotate('Versioning', xy=(x_min-x_margin/2, max_version_FS), xytext=(x_min-x_margin/2, logic_disk_usage_GB + (max_version_FS - logic_disk_usage_GB)/2),
                    arrowprops=dict(arrowstyle='->', edgecolor='blue', lw=1.5),
                    fontsize=12, ha='center', va='center', rotation=90, color='b')
        else: 
            plt.annotate('Versioning', xy=(x_min-x_margin/2, logic_disk_usage_GB + (max_version_FS - logic_disk_usage_GB)/2), xytext=(x_min-x_margin * 2, logic_disk_usage_GB + (max_version_FS - logic_disk_usage_GB)/2),
                    arrowprops=dict(edgecolor='blue', width=0.5, headwidth=8),
                    fontsize=12, ha='right', va='center', color='b')
            if max_version_FS - logic_disk_usage_GB > 0.1 * max_version_FS:
                plt.annotate('', xy=(x_min-x_margin/2, logic_disk_usage_GB), xytext=(x_min-x_margin/2, max_version_FS),
                            arrowprops=dict(arrowstyle='<->', edgecolor='blue', lw=1.5))
            else: 
                plt.annotate('', xy=(x_min-x_margin/2, logic_disk_usage_GB), xytext=(x_min-x_margin/2, max_version_FS),
                            arrowprops=dict(arrowstyle='-', edgecolor='blue', lw=1.5))
    else:
        plt.xlim(x_min, x_max)
    
    
    # legend = plt.legend(loc='lower right', fontsize=14)
    # fig_legend = plt.figure(figsize=(3, 2))
    # ax = fig_legend.add_subplot(111)
    # ax.legend(handles=legend.legendHandles, labels=[text.get_text() for text in legend.get_texts()], loc='center', fontsize=14)
    # ax.axis('off')
    
    # # Save the legend as a separate image
    # fig_legend.savefig('legend-' + plot_FS_size + '.png')
    

    # Step 6: Show the plot
    plt.savefig('graphs/'+parts[0] + "-" + parts[1] + "-" + parts[2] + "-" + plot_FS_size + ".png")
    print("written GB: ", df['written_GB'].iloc[-1])
    print("read GB: ", df['readonly_GB'].iloc[-1])
    print("old versions GB: ", df.max()['versioning_GB'])
    print("TL Metadata GB: ", logic_disk_usage_GB / 512.0)
    print("Versioning Metadata GB: ", df.max()['versioning_meta_GB'])
    print("filesystem size GB: ", logic_disk_usage_GB)
    plt.show()
    
    return (df['readonly_GB'].iloc[-1], df['written_GB'].iloc[-1], logic_disk_usage_GB, df.max()['versioning_meta_GB'], df.max()['versioning_GB'], logic_disk_usage_GB / 512.0)
    

def main():
    import sys
    mail_bins = sys.argv[1:]
    print("Running bins", mail_bins)
    analyze_disk_usage(mail_bins, datetime.timedelta(hours=0), datetime.timedelta(days=2))

if __name__ == "__main__":
    main()