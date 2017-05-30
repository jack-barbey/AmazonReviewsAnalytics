from mrjob.job import MRJob
import json
import sqlite3
import re

class Mega_MRJob(MRJob):
    '''
    Class to test the concepts of dynamically accessing pairs
    Usage: python3 final_job_simple.py <file1.json.gz> 
      --database <metadata_db.sqlite> > <output.txt>
   
    '''

    def configure_options(self):
        super(Mega_MRJob, self).configure_options()
        self.add_file_option('--database')

    def mapper_init(self):
      # make sqlite3 database available to mapper
        self.sqlite_conn = sqlite3.connect(self.options.database)
        self.c = self.sqlite_conn.cursor()

    def mapper(self, _, f1_str):

       # extracts review info
        f1_line = json.loads(f1_str) # convert string to dictionary
        reviewerID1, productID1, uniqueID1 = get_IDs(f1_line)
        if uniqueID1:
            overall1, totalVotes1, helpfulVotes1, reviewText1 \
                = get_attrs(f1_line)

            # review's product info from metadata
            price1 = single_value_query(self.c, "price",
                "products_books", productID1)
            title1 = single_value_query(self.c, "title",
                "products_books", productID1)
            if title1:
                title1_length = len(re.findall("\w+", title1))
                pct_upper1_long = min(len(re.findall("[A-Z]", title1)) /
                    len(re.findall("[a-zA-Z]", title1)), 1)
                pct_upper1 = int(pct_upper1_long * 100) / 100.0
            rank1 = single_value_query(self.c, "salesRank",
                "products_books", productID1)
            reviewLen1 = len(reviewText1)

            if title1:
                if (rank1 and (rank1 <= 250)):
                    yield [0, pct_upper1, rank1], 1
                    yield [1, title1_length, rank1], 1
                yield [2, pct_upper1, overall1], 1               
                yield [3, title1_length, overall1], 1

            yield [4, totalVotes1, reviewLen1], 1
            yield [5, helpfulVotes1, reviewLen1], 1

            yield[6, price1, totalVotes1], 1
            yield[7, price1, overall1], 1



    def combiner(self, obs, counts):
        yield obs, sum(counts)


    def reducer(self, obs, counts):
        yield obs, sum(counts)


# Use to get price, title, brand, etc. of a product
def single_value_query(c, column, table, productID):
    result_cursor = c.execute("SELECT " + column + " FROM " + table +
        " WHERE productID = (?);", (productID,))
    return result_cursor.fetchone()[0]

# Subtract two numbers. If one/both is None, return None.
def diff(val1, val2):
    if val1 == None or val2 == None:
        return None
    return val2 - val1

# SQLite query to get reviewerID, productID, and a concatenation
def get_IDs(f_line):
    if "reviewerID" in f_line:
        reviewerID = f_line["reviewerID"]
    else:
        reviewerID = None
    if "asin" in f_line:
        productID = f_line["asin"]
    else:
        productID = None
    if reviewerID != None and productID != None:
        uniqueID = f_line["reviewerID"] + f_line["asin"]
    else:
        uniqueID = None
    return reviewerID, productID, uniqueID

# Get necessary attributes of review: overall (e.g. 5 stars), timestamp
def get_attrs(f_line):
    if "overall" in f_line:
        overall = f_line["overall"]
    else:
        overall = None
    if "helpful" in f_line:
        helpfulVotes = f_line["helpful"][0]
        totalVotes = f_line["helpful"][1]
    else:
        helpfulVotes = None
        totalVotes = None
    if "reviewText" in f_line:
        reviewText = f_line["reviewText"]
    else:
        reviewText = None
    return overall, totalVotes, helpfulVotes, reviewText


if __name__ == '__main__':
    Mega_MRJob.run()
