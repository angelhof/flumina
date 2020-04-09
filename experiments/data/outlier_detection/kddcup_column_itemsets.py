import sys

src_filename = sys.argv[1]
dst_filename = sys.argv[2]

## A list of indexes of the categorical features
categorical = [2,  # Protocol Type
               3,  # Service
               4,  # Flag
               7,  # Land
               12, # Logged In
               21, # Is Host Login
               22  # Is Guest Login
               ]

src_file = open(src_filename, 'r')

itemsets = [{} for c in categorical]
for line in src_file:
    words = line.split(",")

    ## TODO: Enumerate index
    for i, c in enumerate(categorical):
        item = words[c]
        if item in itemsets[i]:
            itemsets[i][item] += 1
        else:
            itemsets[i][item] = 1

src_file.close()

# print(itemsets)

dst_file = open(dst_filename, 'w')
for itemset in itemsets:
    print(itemset)
    keys_csv = ",".join(itemset.keys())
    dst_file.write(keys_csv + "\n")
dst_file.close()
