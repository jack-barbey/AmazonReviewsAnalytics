# This file compresses a json file into a json.gz file
# Useful if you want to take subset (e.g. first 100 lines) from json file
# and then compress it to use in other files

# Usage: python3 <compress_json.py> <json file> <new json.gz file>

import sys
import io
import json
import gzip

def compress(json_file, outfile):
    f = gzip.open(outfile, "w")
    with open(json_file) as data:
        for line in data.readlines():
            data_str = line
            data_bytes = data_str.encode("utf-8")
            f.write(data_bytes)


if __name__ == "__main__":
    num_args = len(sys.argv)
    if num_args != 3:
        print("usage: python3", sys.argv[0], "<json file> <new json.gz file>")
        sys.exit(0)
    compress(sys.argv[1], sys.argv[2])