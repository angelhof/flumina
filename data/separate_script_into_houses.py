# Open the big file
file = open("sorted.csv")

# For each line of the file, append it to the correct
# house file. If the house file for this id doesn't exist,
# just create the new house file
house_filenames = {}
for line in file:
    words = line.rstrip().split(',')
    # print words
    house_id = words[-1]

    # If there is no open file for this house yet create it
    if not house_id in house_filenames:
        house_filename = 'debs_house_%s' % (house_id)
        house_filenames[house_id] = open(house_filename, 'w')
        
    house_file = house_filenames[house_id]
    house_file.write(line)

    
