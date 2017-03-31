import sys

curr_id = None
curr_cnt = 1
id = None

# The input comes from standard input (line by line)
for line in sys.stdin:

    line = line.strip()
    # parse the line and split it by '\t'
    ln = line.split('\t')
    # grab the key
    id = ln[0]

    if curr_id == id:
        curr_cnt += 1
    else:
        if curr_id: # output the count, single key completed
            print '%s\t%d' % (curr_id, curr_cnt)

        curr_id = id
        curr_cnt = 1

# output the last key!
if curr_id == id:
    print '%s\t%d' % (curr_id, curr_cnt)
