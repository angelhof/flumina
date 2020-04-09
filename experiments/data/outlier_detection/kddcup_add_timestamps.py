import sys

src_filename = sys.argv[1]
dst_filename = sys.argv[2]

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
