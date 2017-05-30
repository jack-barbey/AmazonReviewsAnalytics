from mrjob.job import MRJob
import json
import gzip
import math
import pickle
import re


import sqlite3


class Mega_MRJob(MRJob):
  '''
  Class to test the concepts of dynamically accessing pairs
  Usage: python3 first_job.py <file1.json.gz> --file <file2.json.gz>
    --database <metadata_db.sqlite> --file vocab.pkl
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
    self.num_words, self.all_words_dict = load_obj("vocab.pkl")




  def mapper(self, _, f1_str):

    # 1st review info from reviews file
    f1_line = json.loads(f1_str) # convert string to dictionary
    reviewerID1, productID1, ID1 = get_ID(f1_line)
    if ID1:
      helpfulVotes1, totalVotes1, reviewText1, overall1, summary1, \
        unixReviewTime1, reviewTime1 = get_attrs(f1_line)

      # 1st review's product info from metadata
      price1 = single_value_query(self.c, "price",
          "products_cells", productID1)
      '''
      alsoViewed1 = related_product_query(self.c, "alsoViewedID",
          "also_viewed_instruments", productID1)
      alsoBought1 = related_product_query(self.c, "alsoBoughtID",
          "also_bought_instruments", productID1)
      boughtTogether1 = related_product_query(self.c, "boughtTogetherID",
          "bought_together_instruments", productID1)
       '''


      # Creates two lists of tuples that will be overwriten each time
      r1_vec = []
      r2_vec = []
      r1_dict = {}
      r2_dict = {}

      # Re-open each time in order to start at top of file
      self.f2 = gzip.open("cells_2000_2.json.gz", "r")

      for f2_bytes in self.f2:
        f2_line = json.loads(f2_bytes.decode())
        reviewerID2, productID2, ID2 = get_ID(f2_line)
        # only compare pairs once, don't compare review to itself
        if ID2:
          if ID1 > ID2:
            helpfulVotes2, totalVotes2, reviewText2, overall2, summary2, \
              unixReviewTime2, reviewTime2 = get_attrs(f2_line)
            helpfulVotesDiff = diff(helpfulVotes1, helpfulVotes2)
            totalVotesDiff = diff(totalVotes1, totalVotes2)
            overallDiff = diff(overall1, overall2)
            timeGap = abs(diff(unixReviewTime1, unixReviewTime2))

            cossimReview = round(cos_dist(reviewText1, reviewText2, r1_vec, r2_vec, r1_dict, r2_dict,
                 self.all_words_dict, self.num_words), 2)




            # # Yield results - can be any pair of variables desired
            # Yields difference in rating
            if None not in [cossimReview, overallDiff]:
                yield [1, cossimReview, abs(overallDiff)], 1
            # Difference in helpful votes
            if None not in [cossimReview, helpfulVotesDiff]:
                yield [2, cossimReview, abs(helpfulVotesDiff)], 1
            # Difference in total votes
            if None not in [cossimReview, totalVotesDiff]:
                yield [3, cossimReview, abs(totalVotesDiff)], 1
            # Difference in time
            if None not in [cossimReview, timeGap]:
                days = int(timeGap/86400)
                if days <= 364:
                    yield [4, cossimReview, days], 1
            if price1:
                price2 = single_value_query(self.c, "price", "products_cells", productID2)
                if price2:
                    if price1 > price2:
                        yield [5, int(100*price1/price2), overallDiff], 1
                        yield [6, cossimReview, int(100*price2/price1)], 1
                    else:
                        yield [5, int(100*price2/price1), -overallDiff], 1
                        yield [6, cossimReview, int(100*price1/price2)], 1



            # interpretation: [3, 120, 2] means the product that was
            # 20% more expensive got 2 more points overall in the review


      self.f2.close() # close, then re-open later at top of file
      r1_vec = []
      r1_dict = {}



  def combiner(self, obs, counts):
    yield obs, sum(counts)

  def reducer(self, obs, counts):
    yield obs, sum(counts)


# Loads the dictionary mapping all words to an index in the vector
def load_obj(name):
    replace_chars = {".", "?", "!", "\\", "(", ")", ",", "/", "*", "&", "#",
    ";", ":", "-", "_", "=", "@", "[", "]", "+", "$", "~", "'", '"', "`", '\\\"'}
    replace_chars = set(re.escape(k) for k in replace_chars)
    global pattern
    pattern = re.compile("|".join(replace_chars))

    with open(name, 'rb') as f:
        obj = pickle.load(f)
        num_words = obj[0]
        all_words_dict = obj[1]

    return num_words, all_words_dict



def cos_dist(r1, r2, r1_vec, r2_vec, r1_dict, r2_dict, all_words_dict, num_words):
    '''Computes the ln of the cosine distance given two strings of reviews'''

    # Only resets review 1's vector and dictionary when mrjob moves onto the next review
    # If r1_vec is not populated, then this is the first pass for the review 1
    if r1_vec == []:
        for rev, vec, dic in zip([r1,r2],[r1_vec, r2_vec], [r1_dict, r2_dict]):
            chars_removed = pattern.sub(" ", rev)
            words_list = chars_removed.lower().split()

            for word in words_list:
                if word in all_words_dict:
                    index_of_word = all_words_dict[word]
                    vec.append((index_of_word, 1))
                    if index_of_word not in dic:
                        dic[index_of_word] = 0
                    dic[index_of_word] += 1

    else:
        chars_removed = pattern.sub(" ", r2)
        words_list = chars_removed.lower().split()
        for word in words_list:
            if word in all_words_dict:
                index_of_word = all_words_dict[word]
                r2_vec.append((index_of_word, 1))
                if index_of_word not in r2_dict:
                    r2_dict[index_of_word] = 0
                r2_dict[index_of_word] += 1


    prod = calc_dot_product(r1_vec, r2_dict)
    len1 = math.sqrt(calc_dot_product(r1_vec, r1_dict))
    len2 = math.sqrt(calc_dot_product(r2_vec, r2_dict))


    r2_vec = []
    r2_dict = {}

    cossim = prod/(len1*len2 + 0.0001)

    return cossim

def calc_dot_product(vector, dic):
    '''Given a vector and a dictionary for either two different reviews or the same review,
    this functions returns the dot product between the two reviews
    -Vector of the form: [(index1, count), (index2, count)]
    -Dic of the form {index1: count, index2: count}

    '''
    dot_product = 0

    for index, count in vector:
        if index in dic:
            dot_product += dic[index]*count

    return dot_product


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

  Mega_MRJob.run()
