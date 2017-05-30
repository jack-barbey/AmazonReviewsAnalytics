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

    # 1st review info from reviews file
    f1_line = json.loads(f1_str) # convert string to dictionary
    reviewerID1, productID1, ID1 = get_ID(f1_line)
    if ID1:
      helpfulVotes1, totalVotes1, reviewText1, overall1, summary1, \
        unixReviewTime1, reviewTime1 = get_attrs(f1_line)

      # 1st review's product info from metadata
      price1 = single_value_query(self.c, "price",
          "products_automotive", productID1)
      alsoViewed1 = related_product_query(self.c, "alsoViewedID",
          "also_viewed_automotive", productID1)
      alsoBought1 = related_product_query(self.c, "alsoBoughtID",
          "also_bought_automotive", productID1)
      boughtTogether1 = related_product_query(self.c, "boughtTogetherID",
          "bought_together_automotive", productID1)


      # Creates two lists of tuples that will be overwriten each time
      r1_vec = []
      r2_vec = []
      r1_dict = {}
      r2_dict = {}

      # Re-open each time in order to start at top of file
      self.f2 = gzip.open("automotive2.json.gz", "r")
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
            timeGap = diff(unixReviewTime1, unixReviewTime2)

            cossimReview = round(cos_dist(reviewText1, reviewText2, r1_vec, r2_vec, r1_dict, r2_dict,
                self.stop_words, self.all_words_dict, self.num_words), 2)




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
                yield [4, cossimReview, abs(timeGap)], 1
            if price1:
                price2 = single_value_query(self.c, "price", "products_automotive", productID2)
                if price2:
                    if price1 > price2:
                        yield [5, int(100*price1/price2), overallDiff], 1
                        yield [6, cossimReview, int(100*price1/price2)], 1
                    else:
                        yield [5, int(100*price2/price1), -overallDiff], 1
                        yield [6, cossimReview, int(100*price2/price1)], 1



            # interpretation: [3, 120, 2] means the product that was
            # 20% more expensive got 2 more points overall in the review


      self.f2.close() # close, then re-open later at top of file



  def combiner(self, obs, counts):
    yield obs, sum(counts)

  def reducer(self, obs, counts):
    yield obs, sum(counts)


# Loads the dictionary mapping all words to an index in the vector
def load_obj(name):
    stop_words = {'a', 'about', 'above', 'after', 'again', 'against', 'all', 'am', 'an', 'and', 'any', 'are', "aren't", 'as', 'at', 'be', 'because', 'been', 'before', 'being', 'below', 'between', 'both', 'but', 'by', "can't", 'cannot', 'could', "couldn't", 'did', "didn't", 'do', 'does', "doesn't", 'doing', "don't", 'down', 'during', 'each', 'few', 'for', 'from', 'further', 'had', "hadn't", 'has', "hasn't", 'have', "haven't", 'having', 'he', "he'd", "he'll", "he's", 'her', 'here', "here's", 'hers', 'herself', 'him', 'himself', 'his', 'how', "how's", 'i', "i'd", "i'll", "i'm", "i've", 'if', 'in', 'into', 'is', "isn't", 'it', "it's", 'its', 'itself', "let's", 'me', 'more', 'most', "mustn't", 'my', 'myself', 'no', 'nor', 'not', 'of', 'off', 'on', 'once', 'only', 'or', 'other', 'ought', 'our', 'ours', 'ourselves', 'out', 'over', 'own', 'same', "shan't", 'she', "she'd", "she'll", "she's", 'should', "shouldn't", 'so', 'some', 'such', 'than', 'that', "that's", 'the', 'their', 'theirs', 'them', 'themselves', 'then', 'there', "there's", 'these', 'they', "they'd", "they'll", "they're", "they've", 'this', 'those', 'through', 'to', 'too', 'under', 'until', 'up', 'very', 'was', "wasn't", 'we', "we'd", "we'll", "we're", "we've", 'were', "weren't", 'what', "what's", 'when', "when's", 'where', "where's", 'which', 'while', 'who', "who's", 'whom', 'why', "why's", 'with', "won't", 'would', "wouldn't", 'you', "you'd", "you'll", "you're", "you've", 'your', 'yours', 'yourself', 'yourselves'}
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


def cos_dist(r1, r2, r1_vec, r2_vec, r1_dict, r2_dict, stop_words, all_words_dict, num_words):
    '''Computes the ln of the cosine distance given two strings of reviews'''

    for rev, vec, dic in zip([r1,r2],[r1_vec, r2_vec], [r1_dict, r2_dict]):
        chars_removed = pattern.sub(" ", rev)
        words_list = chars_removed.lower().split()

        for word in words_list:
            if word not in stop_words:
                index_of_word = all_words_dict[word]
                vec.append((index_of_word, 1))
                if index_of_word not in dic:
                    dic[index_of_word] = 0
                dic[index_of_word] += 1

    prod = calc_dot_product(r1_vec, r2_dict)
    len1 = math.sqrt(calc_dot_product(r1_vec, r1_dict))
    len2 = math.sqrt(calc_dot_product(r2_vec, r2_dict))

    r1_vec = []
    r2_vec = []
    r1_dict = {}
    r2_dict = {}


    return prod/(len1*len2+0.000001)

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
