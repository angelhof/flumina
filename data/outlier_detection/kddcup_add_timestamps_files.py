import sys

src_prefix = sys.argv[1]
dst_prefix = sys.argv[2]
number = int(sys.argv[3])

def add_timestamps_to_file(src_filename, dst_filename):
    src_file = open(src_filename, 'r')
    dst_file = open(dst_filename, 'w')

    timestamp_period = 1000 # 1 second

    timestamp = 0
    for src_line in src_file:
        dst_line = "{},{}".format(timestamp, src_line)
        timestamp += timestamp_period
        dst_file.write(dst_line)

    src_file.close()
    dst_file.close()


for i in range(number):
    src_file = src_prefix + "_" + str(i)
    dst_file = dst_prefix + "_" + str(i)
    add_timestamps_to_file(src_file, dst_file)
