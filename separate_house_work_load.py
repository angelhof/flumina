import sys

def separate_house_file(house_filename):
    
    # Open the house file
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

    file.close()
    work_file.close()
    load_file.close()

house_ids = range(0,41)

for house_id in house_ids:
    filename = "sample_debs_house_%d" % (house_id)
    separate_house_file(filename)
