# This file converts review data into a csv file

# Usage: python3 <.json.gz file with review data>" <new csv filename> 
#             <columns>
#         column options: reviewerID, productID, reviewerName, helpfulVotes,
#         totalVotes, reviewText, overall, summary, unixReviewTime, reviewTime

import json
import gzip
import csv
import sys

def parse(path):
    '''
    Parse json.gz file. Code taken from creator of our dataset:
    http://jmcauley.ucsd.edu/data/amazon/links.html
    '''
    g = gzip.open(path, 'r')
    for l in g:
        yield eval(l)

def make_csv(data_gz_file, csv_filename, columns):
    with open(csv_filename, "w") as csvfile:
        wr = csv.writer(csvfile, quoting=csv.QUOTE_ALL)
        for review in parse(data_gz_file):
            vals = []
            for col in columns:
                if col == "productID" and "asin" in review:
                    vals.append(review["asin"])
                elif col == "helpfulVotes" and "helpful" in review:
                    vals.append(review["helpful"][0])
                elif col == "totalVotes" and "helpful" in review:
                        vals.append(review["helpful"][1])
                elif col in review:
                    vals.append(review[col])
                else:
                    vals.append("")
            wr.writerow(vals)


if __name__ == "__main__":
    num_args = len(sys.argv)

    if num_args < 4 or num_args > 13:
        print("usage: python3 " + sys.argv[0] +
            " <.json.gz file with review data>" +
            " <new csv filename> + <columns>")
        print("column options: reviewerID, productID, reviewerName, " +
            "helpfulVotes, totalVotes, reviewText, overall, summary, " +
            "unixReviewTime, reviewTime")
        sys.exit(0)

    make_csv(sys.argv[1], sys.argv[2], sys.argv[3:])

