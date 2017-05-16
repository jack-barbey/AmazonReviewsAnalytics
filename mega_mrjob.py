from mrjob.job import MRJob
import json
import gzip
import random

class Mega_MRJob(MRJob):
    ''' 
    Class to test the concepts of dynamically accessing pairs
    Usage: python3 first_job.py <file1.json.gz> --file <file2.json.gz>
    file2.json.gz will be accessible by all runners

    To output to file, add: > output.txt
    '''

    def mapper(self, _, f1_str):
        # Get info for 1st review in pair only once
        f1_line = eval(f1_str)
        ID1 = f1_line["reviewerID"] + f1_line["asin"]
        if "helpful" in f1_line:
            helpfulVotes1 = f1_line["helpful"][0]
            totalVotes1 = f1_line["helpful"][1]
        else:
            helpfulVotes1 = None
            helpfulVotes2 = None
        if "reviewText" in f1_line:
            reviewText1 = f1_line["reviewText"]
        else:
            reviewText1 = None
        if "overall" in f1_line:
            overall1 = f1_line["overall"]
        else:
            overall1 = None
        if "summary" in f1_line:
            summary1 = f1_line["summary"]
        else:
            summary1 = None
        if "unixReviewTime" in f1_line:
            unixReviewTime1 = f1_line["unixReviewTime"]
        else:
            unixReviewTime1 = None
        if "reviewTime" in f1_line:
            reviewTime1 = f1_line["reviewTime"]
        else:
            reviewTime1 = None

        # Loop over complete file of reviews
        self.f2 = gzip.open("medium_instruments2.json.gz", "r") # re-open each time
        for f2_str in self.f2:
            f2_line = eval(f2_str) # convert string to dictionary
            ID2 = f2_line["reviewerID"] + f2_line["asin"] # unique review ID
            # only compare pairs once, don't compare review to itself
            if ID1 > ID2:
                # Get comparisons
                if helpfulVotes1 != None and "helpful" in f2_line:
                    helpfulVotesDiff = f2_line["helpful"][0] - helpfulVotes1
                    totalVotesDiff = f2_line["helpful"][1] - totalVotes1
                else:
                    helpfulVotesDiff = None
                    totalVotesDiff = None
                if reviewText1 != None and "reviewText" in f2_line:
                    reviewText2 = f2_line["reviewText"]
                    cossimReview = random.randint(0, 10) # placeholder
                else:
                    cossimReview = None
                if overall1 != None and "overall" in f2_line:
                    overallDiff = f2_line["overall"] - overall1
                else:
                    overall2 = None
                if summary1 != None and "summary" in f2_line:
                    summary2 = f2_line["summary"]
                    cossimSummary = random.randint(0, 5) # placeholder
                else:
                    summary2 = None
                    cossimSummary = None
                if unixReviewTime1 != None and "unixReviewTime" in f2_line:
                    timeGap = f2_line["unixReviewTime"] - unixReviewTime1
                else:
                    timeGap = None

                # Yield results - can be any pair of variables desired
                if cossimReview != None and overallDiff != None:
                    yield ["cosine similarity vs. overall diff", 
                            cossimReview, abs(overallDiff)], 1
                if helpfulVotesDiff != None and totalVotesDiff != None:
                    yield ["helpful votes diff vs. overall votes diff",
                            helpfulVotesDiff, totalVotesDiff], 1

        self.f2.close() # close, then open again later at top of file


    def combiner(self, obs, counts):
        yield obs, sum(counts)


    def reducer(self, obs, counts):
        yield obs, sum(counts)


if __name__ == '__main__':
    Mega_MRJob.run()                
