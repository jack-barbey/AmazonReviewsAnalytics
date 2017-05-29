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
    reviewerID1, productID1, uniqueID1 = get_IDs(f1_line)
    if uniqueID1:
      overall1, unixReviewTime1 = get_attrs(f1_line)

      # 1st review's product info from metadata
      price1 = single_value_query(self.c, "price",
          "products_instruments", productID1)
      title1 = single_value_query(self.c, "title",
          "products_instruments", productID1)
      if title1:
        title1_length = len(re.findall("\w+", title1))
        pct_upper1 = min(len(re.findall("[A-Z]", title1)) /
            len(re.findall("[a-zA-Z]", title1)), 1)
      category1 = single_value_query(self.c, "salesCategory",
          "products_instruments", productID1)
      rank1 = single_value_query(self.c, "salesRank",
          "products_instruments", productID1)

      # Re-open each time in order to start at top of file
      self.f2 = gzip.open("full_instruments2.json.gz", "r")
      for f2_bytes in self.f2:
        f2_line = json.loads(f2_bytes.decode())
        reviewerID2, productID2, uniqueID2 = get_IDs(f2_line)
        # Don't compare reviews for same product
        if productID1 != productID2:
          overall2, unixReviewTime2 = get_attrs(f2_line)

          overall_diff = diff(overall1, overall2)
          time_gap = diff(unixReviewTime1, unixReviewTime2)
          day_gap = int(time_gap/86400) # number of days between reviews

          if price1:
            price2 = single_value_query(self.c, "price",
                "products_instruments", productID2)

          if title1:
            title2 = single_value_query(self.c, "title",
                "products_instruments", productID2)
            if title2:
              title2_length = len(re.findall("\w+", title2))
              pct_upper2 = min(len(re.findall("[A-Z]", title2)) / \
                  len(re.findall("[a-zA-Z]", title2)), 1)
              title_len_diff = diff(title1_length, title2_length)
              pct_upper_diff = int(100 * diff(pct_upper1, pct_upper2))

          if category1 and rank1:
            category2 = single_value_query(self.c, "salesCategory",
                "products_instruments", productID2)
            if category1 == category2:
              rank2 = single_value_query(self.c, "salesRank",
                  "products_instruments", productID2)

          # Ranks are only comparable within a category
          if category1 and category1 == category2:
            if title_len_diff >= 0:
              # Middle element is 1 if longer review is better, else 0
              yield [0, int(rank2 > rank1), title_len_diff], 1
            else:
              yield [0, int(rank1 > rank2), -title_len_diff], 1

            if day_gap >= 0 and day_gap <= 365:
              yield [1, int(rank2 > rank1), day_gap], 1
            elif day_gap < 0 and day_gap >= -365:
              yield [1, int(rank1 > rank2), -day_gap], 1

            if pct_upper_diff >= 0:
              yield [2, int(rank2 > rank1), pct_upper_diff], 1
            else:
              yield [2, int(rank1 > rank2), -pct_upper_diff], 1

          if price1 and price2 and overall_diff:
            if price1 > price2:
              # Interpretation: one product is X% the price, got Y more stars
              yield [3, int(100 * price2 / price1), overall_diff], 1
            else:
              yield [3, int(100 * price1 / price2), -overall_diff], 1

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
  if "unixReviewTime" in f_line:
    unixReviewTime = f_line["unixReviewTime"]
  else:
    unixReviewTime = None
  return overall, unixReviewTime


if __name__ == '__main__':
  Mega_MRJob.run()
