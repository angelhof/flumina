import sys

src_filename = sys.argv[1]
dst_filename = sys.argv[2]
number = int(sys.argv[3])

src_file = open(src_filename, 'r')
dst_files = [open("{}_{}".format(dst_filename,i), 'w') for i in range(number)]

counter = 0
for src_line in src_file:
    dst_file = dst_files[counter % number]
    dst_file.write(src_line)
    counter += 1

src_file.close()
[dst_file.close() for dst_file in dst_files]
