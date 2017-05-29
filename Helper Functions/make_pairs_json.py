# This file converts review data into a json.gz file of all pairs

# Usage: python3 <.json.gz file with review data> 
# <duplicate json.gz file with review data> <new json filename> 

import json
import gzip
import csv
import sys
import io
from time import time

def parse(path):
    '''
    Parse json.gz file. Commented-out code taken from creator of our dataset:
    http://jmcauley.ucsd.edu/data/amazon/links.html

    Second version is slightly faster (10% difference). Found here:
    https://gist.github.com/theJollySin/6eeda4a44db830a35365503178f88788
    '''
    # with gzip.open(path, 'r') as g:
    #     for l in g:
    #         yield eval(l)

    gz = gzip.open(path, "r")
    f = io.BufferedReader(gz)
    for line in f.readlines():
        yield eval(line)
    gz.close()


def make_json_gz(data_gz_file1, data_gz_file2, json_gz_outfile):
    last_columns = ["reviewText", "overall", "summary",
                    "unixReviewTime", "reviewTime"]
    with gzip.open(json_gz_outfile, "w") as outfile:
        ### Time is for testing purposes, can delete once working
        i = 1
        old_time = time()

        for r1 in parse(data_gz_file1):
            new_time = time()
            print(i, new_time - old_time)
            old_time = new_time
            i += 1

            for r2 in parse(data_gz_file2):
                vals = {}
                # Deleted if statements because ID's are always included
                # if "reviewerID" in r1 and "reviewerID" in r2:
                vals["reviewerID"] = [r1["reviewerID"], r2["reviewerID"]]
                # if "asin" in r1 and "asin" in r2:
                vals["productID"] = [r1["asin"], r2["asin"]]
                if vals["productID"][0] > vals["productID"][1]:
                    continue  # avoid double-counting pairs
                if vals["productID"][0] == vals["productID"][1] and \
                   vals["reviewerID"][0] >= vals["reviewerID"][1]:
                   continue  # same product, different reviewers counted once
                if "helpful" in r1 and "helpful" in r2:
                    vals["helpfulVotes"] = [r1["helpful"][0], r2["helpful"][0]]
                    vals["totalVotes"] = [r1["helpful"][1], r2["helpful"][1]]
                for col in last_columns:
                    if col in r1 and col in r2:
                        vals[col] = [r1[col], r2[col]]

                vals_str = json.dumps(vals) + "\n"
                vals_bytes = vals_str.encode("utf-8")
                outfile.write(vals_bytes)


if __name__ == "__main__":
    num_args = len(sys.argv)

    if num_args != 4:
        print("usage: python3 " + sys.argv[0] +
            " <.json.gz file with review data>" +
            " <duplicate .json.gz file with review data>" +
            " <new .json.gz filename>")
        sys.exit(0)

    make_json_gz(sys.argv[1], sys.argv[2], sys.argv[3])

