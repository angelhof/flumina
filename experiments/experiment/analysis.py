import results
import os

def get_max_throughput_flumina(archive_dir, prefix, suffix):

    subdirectories = [os.path.join(archive_dir, o) for o in os.listdir(archive_dir) 
                      if os.path.isdir(os.path.join(archive_dir,o))
                      and o.startswith(prefix)
                      and o.endswith(suffix)]

    throughputs = [results.get_erlang_throughput(subdir)
                   for subdir in subdirectories]
    try:
        max_throughput = max(throughputs)
    except:
        max_throughput = 0
    return max_throughput


def get_max_throughputs_flumina(archive_dir, prefix, suffix_format, scale):
    print("Max throughputs for experiment:", prefix)
    for nodes in scale:
        suffix = suffix_format.format(nodes)
        max_throughput = get_max_throughput_flumina(archive_dir, prefix, suffix)
        print("|-- Parallelism:", nodes, " Maximum throughput:", max_throughput)

def get_max_throughputs_flumina_stream_table_join(archive_dir, prefix_format, scale):
    print("Max throughputs for experiment:", prefix_format.split("{")[0])
    for nodes in scale:
        suffix = ""
        prefix = prefix_format.format(nodes)
        max_throughput = get_max_throughput_flumina(archive_dir, prefix, suffix)
        print("|-- Parallelism:", nodes, " Maximum throughput:", max_throughput)

ARCHIVE_DIR = "archive"
prefix = "ab_exp_1"
suffix_format = "10000_100_{}_optimizer_greedy"
scale = range(0,21,2)
get_max_throughputs_flumina(ARCHIVE_DIR, prefix, suffix_format, scale)

prefix = "ab_exp_full_1"
suffix_format = "10000_100_{}_optimizer_greedy"
scale = range(0,21,2)
get_max_throughputs_flumina(ARCHIVE_DIR, prefix, suffix_format, scale)

prefix_format = "stream_table_join_2_{}_"
scale = [1] + list(range(2,21,2))
get_max_throughputs_flumina_stream_table_join(ARCHIVE_DIR, prefix_format, scale)
