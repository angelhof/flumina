import sys

house_filename = sys.argv[1]


# Open the big file
file = open(house_filename)

# For each line of the file, append it to the correct
# house file. If the house file for this id doesn't exist,
# just create the new house file

work_file = open(house_filename + "_work", "w")
load_file = open(house_filename + "_load", "w")
for line in file:
    words = line.rstrip().split(',')
    # print words
    work_or_load = words[-4]
        
    if work_or_load == '0':
        work_file.write(line)
    else:
        load_file.write(line)
