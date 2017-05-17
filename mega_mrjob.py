from mrjob.job import MRJob
from mrjob.step import MRStep
import json
import gzip
import sqlite3
from time import time
import random

class Mega_MRJob(MRJob):
    ''' 
    Class to test the concepts of dynamically accessing pairs
    Usage: python3 first_job.py <file1.json.gz> --file <file2.json.gz>
        --database <metadata_db.sqlite>
    file2.json.gz will be accessible by all runners

    Note: change gzip.open() line to match file2.json.gz filename

    To output to file, add: > output.txt
    '''

    def configure_options(self):
        super(Mega_MRJob, self).configure_options()
        self.add_file_option('--database')

    def mapper_init(self):
        # make sqlite3 database available to mapper
        self.sqlite_conn = sqlite3.connect(self.options.database)
        self.c = self.sqlite_conn.cursor()

    def mapper(self, _, f1_str):
        # 1st review info from reviews file
        f1_line = eval(f1_str) # convert string to dictionary
        reviewerID1, productID1, ID1 = get_ID(f1_line)
        helpfulVotes1, totalVotes1, reviewText1, overall1, summary1, \
            unixReviewTime1, reviewTime1 = get_attrs(f1_line)

        # 1st review's product info from metadata
        price1 = single_value_query(self.c, "price",
                "products_instruments", productID1)
        alsoViewed1 = related_product_query(self.c, "alsoViewedID",
                "also_viewed_instruments", productID1)
        alsoBought1 = related_product_query(self.c, "alsoBoughtID",
                "also_bought_instruments", productID1)
        boughtTogether1 = related_product_query(self.c, "boughtTogetherID",
                "bought_together_instruments", productID1)

        # Re-open each time in order to start at top of file
        self.f2 = gzip.open("reviews_musical_instruments_5.json.gz", "r")
        for f2_str in self.f2:
            f2_line = eval(f2_str)
            reviewerID2, productID2, ID2 = get_ID(f2_line)
            # only compare pairs once, don't compare review to itself
            if ID1 > ID2:
                helpfulVotes2, totalVotes2, reviewText2, overall2, summary2, \
                    unixReviewTime2, reviewTime2 = get_attrs(f2_line)
                helpfulVotesDiff = diff(helpfulVotes1, helpfulVotes2)
                totalVotesDiff = diff(totalVotes1, totalVotes2)
                overallDiff = diff(overall1, overall2)
                timeGap = diff(unixReviewTime1, unixReviewTime2)
                cossimReview = get_cossim(reviewText1, reviewText2)
                cossimSummary = get_cossim(summary1, summary2)

                # Yield results - can be any pair of variables desired
                if None not in [cossimReview, overallDiff]:
                    yield ["cosine similarity vs. overall diff", 
                            cossimReview, abs(overallDiff)], 1
                if None not in [helpfulVotesDiff, totalVotesDiff]:
                    yield ["helpful votes diff vs. overall votes diff",
                            helpfulVotesDiff, totalVotesDiff], 1

        self.f2.close() # close, then re-open later at top of file

    def combiner(self, obs, counts):
        yield obs, sum(counts)

    def reducer(self, obs, counts):
        yield obs, sum(counts)

# Use to get price, title, brand, etc. of a product
def single_value_query(c, column, table, productID):
    result_cursor = c.execute("SELECT " + column + " FROM " + table + 
        " WHERE productID = (?);", (productID,))
    rv = result_cursor.fetchone()[0]
    return rv

# Use to get list of other products that were also viewed/bought with product
def related_product_query(c, column, table, productID):
    rv = []
    for tup in c.execute("SELECT " + column + " FROM " + table +
        " WHERE productID = (?);", (productID,)):
        if tup[0] != None:
            rv.append(tup[0])
    return rv

# Subtract two numbers. If one/both is None, return None.
def diff(val1, val2):
    if val1 == None or val2 == None:
        return None
    return val2 - val1

# Placeholder for cosine similarity function
def get_cossim(str1, str2):
    if str1 == None or str2 == None:
        return None
    return random.randint(0, 10)

def get_ID(f_line):
    reviewerID = f_line["reviewerID"]
    productID = f_line["asin"]
    ID = f_line["reviewerID"] + f_line["asin"]
    return reviewerID, productID, ID

def get_attrs(f_line):
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
    if "overall" in f_line:
        overall = f_line["overall"]
    else:
        overall = None
    if "summary" in f_line:
        summary = f_line["summary"]
    else:
        summary = None
    if "unixReviewTime" in f_line:
        unixReviewTime = f_line["unixReviewTime"]
    else:
        unixReviewTime = None
    if "reviewTime" in f_line:
        reviewTime = f_line["reviewTime"]
    else:
        reviewTime = None
    return helpfulVotes, totalVotes, reviewText, overall, summary, unixReviewTime, reviewTime


if __name__ == '__main__':
    begin = time()
    Mega_MRJob.run()                
    end = time()
    print("time elapsed:", end - begin, "seconds")
