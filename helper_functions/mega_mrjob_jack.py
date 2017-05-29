from mrjob.job import MRJob
import json
import gzip
import random
import math
import numpy as np
import pickle
import re
from stop_words import get_stop_words
from time import time
import sqlite3

class Mega_MRJob(MRJob):
  '''
  Class to test the concepts of dynamically accessing pairs
  Usage: python3 first_job.py <file1.json.gz> --file <file2.json.gz>
    --database <metadata_db.sqlite> --file all_words_dict.pkl
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
    stop_words, num_words, all_words_dict = load_obj("all_words_dict.pkl")
    self.stop_words = stop_words
    self.num_words = num_words
    self.all_words_dict = all_words_dict


  def mapper(self, _, f1_str):
    begin_mapper = time()
    # 1st review info from reviews file
    f1_line = eval(f1_str) # convert string to dictionary
    reviewerID1, productID1, ID1 = get_ID(f1_line)
    if ID1:
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


      # Creates the two vectors which will be filled by the review text
      r1_vec = [0] * self.num_words
      r2_vec = [0] * self.num_words

      # Re-open each time in order to start at top of file
      self.f2 = gzip.open("instruments_very_small2.json.gz", "r")
      for f2_str in self.f2:
        f2_line = eval(f2_str)
        reviewerID2, productID2, ID2 = get_ID(f2_line)
        # only compare pairs once, don't compare review to itself
        if ID2:
          if ID1 > ID2:
            helpfulVotes2, totalVotes2, reviewText2, overall2, summary2, \
              unixReviewTime2, reviewTime2 = get_attrs(f2_line)
            helpfulVotesDiff = diff(helpfulVotes1, helpfulVotes2)
            totalVotesDiff = diff(totalVotes1, totalVotes2)
            overallDiff = diff(overall1, overall2)
            timeGap = diff(unixReviewTime1, unixReviewTime2)
            cossimReview = cos_dist(reviewText1, reviewText2, r1_vec, r2_vec,
                self.stop_words, self.all_words_dict, self.num_words)
            #cossimSummary = get_cossim(summary1, summary2)

            # # Yield results - can be any pair of variables desired
            if None not in [cossimReview, overallDiff]:
              yield [1, cossimReview, abs(overallDiff)], 1
            if None not in [helpfulVotesDiff, totalVotesDiff]:
              yield [2, helpfulVotesDiff, totalVotesDiff], 1

      self.f2.close() # close, then re-open later at top of file
      end_mapper = time()
      # print("time elapsed (one loop):", end_mapper - begin_mapper, "seconds")


  def combiner(self, obs, counts):
    yield obs, sum(counts)

  def reducer(self, obs, counts):
    yield obs, sum(counts)


# Loads the dictionary mapping all words to an index in the vector
def load_obj(name):
    stop_words = set(get_stop_words('en'))
    replace_chars = {".", "?", "!", "\\", "(", ")", ",", "/", "*", "&", "#",
    ";", ":", "-", "_", "=", "@", "[", "]", "+", "$", "~", "'", '"', "`", '\\\"'}
    replace_chars = set(re.escape(k) for k in replace_chars)
    global pattern
    pattern = re.compile("|".join(replace_chars))

    with open(name, 'rb') as f:
        obj = pickle.load(f)
        num_words = obj[0]
        all_words_dict = obj[1]

    return stop_words, num_words, all_words_dict


def cos_dist(r1, r2, r1_vec, r2_vec, stop_words, all_words_dict, num_words):
    '''Computes the ln of the cosine distance given two strings of reviews'''
    #r1 = "thiiiis tonelab snow tonight "
    #r2 = "tonight thiiiis tonelab snow"
    t1 = time()
    for rev, vec in zip([r1,r2],[r1_vec, r2_vec]):
        chars_removed = pattern.sub(" ", rev)
        words_list = chars_removed.lower().split()

        for word in words_list:
            if word in stop_words: continue
            index_in_vector = all_words_dict[word]
            vec[index_in_vector] += 1
    t2 = time()
    prod = np.dot(r1_vec, r2_vec)
    t3 = time()
    print("{} s {} s".format((t2-t1),(t3-t2)))
    len1 = math.sqrt(np.dot(r1_vec, r1_vec))
    len2 = math.sqrt(np.dot(r2_vec, r2_vec))

    r1_vec = [0] * num_words
    r2_vec = [0] * num_words


    return prod/(len1*len2)

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

def get_ID(f_line):
  if "reviewerID" in f_line:
    reviewerID = f_line["reviewerID"]
  else:
    reviewerID = None
  if "asin" in f_line:
    productID = f_line["asin"]
  else:
    productID = None
  if reviewerID != None and productID != None:
    ID = f_line["reviewerID"] + f_line["asin"]
  else:
    ID = None
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
  return helpfulVotes, totalVotes, reviewText, overall, summary, \
    unixReviewTime, reviewTime


if __name__ == '__main__':
  begin = time()
  Mega_MRJob.run()
  end = time()
  print("time elapsed:", end - begin, "seconds")
