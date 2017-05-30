from mrjob.job import MRJob
import json
import gzip
import sqlite3
import re

class Mega_MRJob(MRJob):
  '''
  Class to test the concepts of dynamically accessing pairs
  Usage: python3 fast_pairs_mrjob.py <file1.json.gz> --file <file2.json.gz>
    --database <metadata_db.sqlite> > <output.txt>
  file2.json.gz will be accessible by all runners

  Note: change gzip.open() line to match file2.json.gz filename
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
    f1_line = json.loads(f1_str) # convert string to dictionary
    reviewerID1, productID1 = get_IDs(f1_line)
    if productID1:
      overall1, unixReviewTime1 = get_attrs(f1_line)
      if overall1 and unixReviewTime1:
        # Re-open each time in order to start at top of file
        self.f2 = gzip.open("full_instruments2.json.gz", "r")
        for f2_bytes in self.f2:
          f2_line = json.loads(f2_bytes.decode())
          reviewerID2, productID2 = get_IDs(f2_line)

          # Only compare different reviews for same product
          if productID1 == productID2 and reviewerID1 != reviewerID2:
            overall2, unixReviewTime2 = get_attrs(f2_line)
            if overall2 and unixReviewTime2:
              overall_diff = diff(overall1, overall2)
              time_gap = diff(unixReviewTime1, unixReviewTime2)
              day_gap = int(time_gap / (60 * 60 * 24)) # days between reviews

              if day_gap > 0 and day_gap <= 365:
                yield [0, day_gap, overall_diff], 1
              elif day_gap < 0 and day_gap >- -365:
                yield [0, -day_gap, -overall_diff], 1

      self.f2.close() # close, then re-open later at top of file


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
    return reviewerID, productID

# Get necessary attributes of review: overall (e.g. 5 stars), timestamp
def get_attrs(f_line):
  if "overall" in f_line:
    overall = f_line["overall"]
  else:
    overall = None
  if "unixReviewTime" in f_line:
    unixReviewTime = f_line["unixReviewTime"]
  else:
    unixReviewTime = None
  return overall, unixReviewTime


if __name__ == '__main__':
  Mega_MRJob.run()
