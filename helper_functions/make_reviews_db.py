# This file converts review data into a SQLite database.

# This code was heavily informed by StackOverflow, especially this answer:
# http://stackoverflow.com/questions/8811783/convert-json-to-sqlite-in-python-how-to-map-json-keys-to-database-columns-prop

# Usage: python3 make_reviews_db.py <new sqlite table name> 
#            <.json.gz file with reviews>

import sqlite3
import json
import gzip
import sys

def parse(path):
    '''
    Parse json.gz file. Code taken from creator of our dataset:
    http://jmcauley.ucsd.edu/data/amazon/links.html
    '''
    g = gzip.open(path, 'r')
    for l in g:
        yield eval(l)

def create_table(new_table_name, data_gz_file):
    '''
    Create sqlite table based on review data in file
    '''
    db = sqlite3.connect("./reviews_db.sqlite")
    c = db.cursor()

    c.execute("DROP TABLE IF EXISTS " + new_table_name)
    c.execute("CREATE TABLE " + new_table_name + ''' (
        reviewerID TEXT NOT NULL,
        productID TEXT NOT NULL,
        reviewerName TEXT,
        helpfulVotes INT NOT NULL,
        totalVotes INT NOT NULL,
        reviewText TEXT,
        overall INT NOT NULL,
        summary TEXT,
        unixReviewTime INT NOT NULL,
        reviewTime TEXT,
        PRIMARY KEY (reviewerID, productID) 
        )''')

    insert_query = "INSERT INTO "+new_table_name+" VALUES (?,?,?,?,?,?,?,?,?,?)"

    json_columns = ["reviewerID", "asin", "reviewerName", "helpful",
        "reviewText", "overall", "summary", "unixReviewTime", "reviewTime"]

    for review in parse(data_gz_file):
        vals = []
        for col_name in json_columns:
            if col_name in review:
                if col_name == "helpful":
                    vals.append(review["helpful"][0])
                    vals.append(review["helpful"][1])
                else:
                    vals.append(review[col_name])
            else:
                vals.append(None) # some reviewerName values are missing
        c.execute(insert_query, tuple(vals))

    c.close()
    db.commit()


if __name__ == "__main__":
    num_args = len(sys.argv)

    if num_args != 3:
        print("usage: python3 " + sys.argv[0] + " <new sqlite table name> " +
              "<.json.gz file with reviews>\n")
        sys.exit(0)

    create_table(sys.argv[1], sys.argv[2])
