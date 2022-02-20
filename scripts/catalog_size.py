import os
import sys

total = 0
with open(sys.argv[1], 'r') as f:
    for line in f:
        total += os.path.getsize(line.strip())
print(f"{total:,}, {total / 1024 ** 3:,.2f} GiB")
